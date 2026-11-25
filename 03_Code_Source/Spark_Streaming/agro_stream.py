# AgroVision - Spark Structured Streaming.
# Source  : dossier surveille <base>/incoming (JSON Lines), trigger toutes les 5 s
# Requete 'rejected' : lignes invalides + motif -> <base>/rejected (JSON)
# Requete 'events'   : evenements valides, watermark 10 min, dropDuplicates(event_id, event_time)
#                      -> <base>/output/events (Parquet)
# Requete 'metrics'  : fenetres de 5 min x region (8 indicateurs), foreachBatch -> output/metrics_by_batch
# Reprise : un checkpoint par requete dans <base>/checkpoint/
# Usage   : spark-submit --driver-memory 1g agro_stream.py --base <dossier> --input <dossier>/incoming
import argparse, os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

HOME = os.path.expanduser("~/tp6_elk")
ap = argparse.ArgumentParser()
ap.add_argument("--input", default=f"{HOME}/stream/incoming")
ap.add_argument("--base", default=f"{HOME}/stream")
ap.add_argument("--vertices", default=f"{HOME}/data/graph/vertices.csv")
ap.add_argument("--trigger", default="5 seconds")
ap.add_argument("--max-files", type=int, default=10, help="fichiers lus par micro-lot")
ap.add_argument("--run-seconds", type=int, default=0, help="0 = jusqu'a Ctrl-C")
a = ap.parse_args()

spark = (SparkSession.builder.master("local[2]").appName("agro_stream")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

FIELDS = ["event_id", "event_type", "field_id", "sensor_id", "event_time", "soil_moisture_pct", "ndvi",
          "temperature_c", "rainfall_mm", "water_m3", "duration_min", "alert_type", "severity"]
schema = StructType([StructField(n, StringType()) for n in FIELDS])

parcels = (spark.read.csv(a.vertices, header=True).filter("vertex_type = 'PARCELLE'")
           .select(F.col("vertex_id").alias("field_id"), "region", "crop_type"))

raw = (spark.readStream.format("text").option("maxFilesPerTrigger", a.max_files).load(a.input))
p = raw.select(F.col("value").alias("raw_line"), F.from_json("value", schema).alias("e")).select("raw_line", "e.*")

d = (p.withColumn("event_time_ts", F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss"))
     .withColumn("soil", F.col("soil_moisture_pct").cast("double"))
     .withColumn("ndvi_d", F.col("ndvi").cast("double"))
     .withColumn("temp", F.col("temperature_c").cast("double"))
     .withColumn("rain", F.col("rainfall_mm").cast("double"))
     .withColumn("water", F.col("water_m3").cast("double"))
     .withColumn("dur", F.col("duration_min").cast("double"))
     .withColumn("sev", F.col("severity").cast("double"))
     .join(F.broadcast(parcels), "field_id", "left"))

sensor_ok = (F.col("soil").between(0, 100) & F.col("ndvi_d").between(0, 1)
             & F.col("temp").between(-10, 55) & F.col("rain").between(0, 300))
irrig_ok = (F.col("water") > 0) & (F.col("water") <= 5000) & (F.col("dur") > 0)
alert_ok = F.col("alert_type").isin("DROUGHT_RISK", "LOW_NDVI", "SENSOR_FAULT", "HEAVY_RAIN") & F.col("sev").between(1, 3)
type_ok = F.coalesce(F.when(F.col("event_type") == "SENSOR_READING", sensor_ok)
                      .when(F.col("event_type") == "IRRIGATION_EVENT", irrig_ok)
                      .when(F.col("event_type") == "ALERT", alert_ok), F.lit(False))

reason = (F.when(~F.coalesce(F.col("event_id").rlike(r"^EVT-\d{8}$"), F.lit(False)), "BAD_EVENT_ID")
          .when(~F.coalesce(F.col("event_type").isin("SENSOR_READING", "IRRIGATION_EVENT", "ALERT"), F.lit(False)), "BAD_EVENT_TYPE")
          .when(F.col("event_time_ts").isNull(), "BAD_EVENT_TIME")
          .when(F.col("region").isNull(), "UNKNOWN_FIELD")
          .when(~type_ok, "OUT_OF_RANGE"))
d = d.withColumn("reject_reason", reason)

# 1) evenements rejetes (avec la raison)
rejected = (d.filter("reject_reason IS NOT NULL")
            .select("raw_line", "reject_reason", F.current_timestamp().alias("rejected_at")))
q_rej = (rejected.writeStream.format("json").outputMode("append")
         .option("path", f"{a.base}/rejected").option("checkpointLocation", f"{a.base}/checkpoint/rejected")
         .trigger(processingTime=a.trigger).queryName("rejected").start())

# 2) evenements valides : watermark + deduplication
valid = (d.filter("reject_reason IS NULL")
         .select("event_id", "event_type", "field_id", "sensor_id", "region", "crop_type",
                 F.col("event_time_ts").alias("event_time"), F.col("soil").alias("soil_moisture_pct"),
                 F.col("ndvi_d").alias("ndvi"), F.col("temp").alias("temperature_c"),
                 F.col("rain").alias("rainfall_mm"), F.col("water").alias("water_m3"),
                 F.col("dur").alias("duration_min"), "alert_type", F.col("sev").alias("severity"))
         .withWatermark("event_time", "10 minutes")
         .dropDuplicates(["event_id", "event_time"]))

q_evt = (valid.writeStream.format("parquet").outputMode("append")
         .option("path", f"{a.base}/output/events")
         .option("checkpointLocation", f"{a.base}/checkpoint/events")
         .trigger(processingTime=a.trigger).queryName("events").start())

# 3) indicateurs par fenetre de 5 min et par region
S, I, A = "SENSOR_READING", "IRRIGATION_EVENT", "ALERT"
agg = (valid.groupBy(F.window("event_time", "5 minutes"), "region").agg(
    F.count("*").alias("n_events"),
    F.sum(F.when(F.col("event_type") == S, 1).otherwise(0)).alias("n_sensor"),
    F.sum(F.when(F.col("event_type") == I, 1).otherwise(0)).alias("n_irrigation"),
    F.sum(F.when(F.col("event_type") == A, 1).otherwise(0)).alias("n_alert"),
    F.round(F.avg("soil_moisture_pct"), 2).alias("avg_soil_moisture"),
    F.round(F.avg("ndvi"), 3).alias("avg_ndvi"),
    F.round(F.sum("rainfall_mm"), 1).alias("rain_mm"),
    F.round(F.sum("water_m3"), 1).alias("water_m3")))


def write_metrics(df, bid):
    (df.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
               "region", "n_events", "n_sensor", "n_irrigation", "n_alert", "avg_soil_moisture",
               "avg_ndvi", "rain_mm", "water_m3")
       .withColumn("batch_id", F.lit(bid)).write.mode("append").parquet(f"{a.base}/output/metrics_by_batch"))


q_met = (agg.writeStream.outputMode("update").foreachBatch(write_metrics)
         .option("checkpointLocation", f"{a.base}/checkpoint/metrics")
         .trigger(processingTime=a.trigger).queryName("metrics").start())

if a.run_seconds > 0:
    spark.streams.awaitAnyTermination(a.run_seconds)
    for q in spark.streams.active:
        q.stop()
else:
    spark.streams.awaitAnyTermination()
