# AgroVision - exporte les sorties d'une execution du flux (events, rejets, metriques)
# vers un dossier de resultats, pour comparaison ulterieure. Usage : spark-submit export_run.py <dossier>
import os, sys
from pyspark.sql import SparkSession
S = os.path.expanduser("~/tp6_elk/stream")
out = sys.argv[1]
spark = SparkSession.builder.master("local[2]").appName("export").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ev = spark.read.parquet(f"{S}/output/events")
rej = spark.read.json(f"{S}/rejected").select("raw_line", "reject_reason")
met = spark.read.parquet(f"{S}/output/metrics_by_batch")
ev.coalesce(1).write.mode("overwrite").parquet(f"{out}/export/events")
rej.coalesce(1).write.mode("overwrite").parquet(f"{out}/export/rejected")
met.coalesce(1).write.mode("overwrite").parquet(f"{out}/export/metrics_all")
print("EXPORT_OK", ev.count(), rej.count(), met.count())
