# AgroVision - meme verification que verify_stream.py, sur le dossier de demonstration
# ~/tp6_elk/stream_demo (manifeste manifest_demo.json). Usage : spark-submit verify_stream_demo.py
import json, os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

H = os.path.expanduser("~/tp6_elk")
spark = SparkSession.builder.master("local[2]").appName("verify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
m = json.load(open(f"{H}/stream_demo/manifest_demo.json"))
lines = spark.read.text(f"{H}/stream_demo/incoming").count()
rej = spark.read.json(f"{H}/stream_demo/rejected")
ev = spark.read.parquet(f"{H}/stream_demo/output/events")
met = spark.read.parquet(f"{H}/stream_demo/output/metrics_by_batch")
w = Window.partitionBy("window_start", "region").orderBy(F.col("batch_id").desc())
last = met.withColumn("rk", F.row_number().over(w)).filter("rk = 1")
n_rej, n_ev = rej.count(), ev.count()
n_dist, n_met = ev.select("event_id").distinct().count(), last.agg(F.sum("n_events")).first()[0]
checks = [("Lignes lues = lignes produites", lines, m["total_lines"]),
          ("Rejetes = invalides injectes", n_rej, m["invalid_total"]),
          ("Valides ecrits = valides distincts produits", n_ev, m["valid_distinct"]),
          ("Aucun doublon en sortie", n_dist, n_ev),
          ("Somme des fenetres = evenements valides", n_met, n_ev)]
for label, got, exp in checks:
    print(("PASS" if got == exp else "FAIL"), f"{label:<48} obtenu={got} attendu={exp}")
print("\nRejets par raison :"); rej.groupBy("reject_reason").count().show()
print("Evenements par type :"); ev.groupBy("event_type").count().show()
print("Fenetres (dernier etat) :"); last.orderBy("window_start", "region").show(12, False)
