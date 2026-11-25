# AgroVision - verification du flux (5 controles PASS/FAIL) sur ~/tp6_elk/stream :
# lignes lues = produites, rejets = invalides injectes, valides = distincts, 0 doublon,
# somme des fenetres = evenements valides. Usage : spark-submit verify_stream.py
import json, os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

H = os.path.expanduser("~/tp6_elk")
spark = SparkSession.builder.master("local[2]").appName("verify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
m = json.load(open(f"{H}/results/stream_manifest.json"))
lines = spark.read.text(f"{H}/stream/incoming").count()
rej = spark.read.json(f"{H}/stream/rejected")
ev = spark.read.parquet(f"{H}/stream/output/events")
met = spark.read.parquet(f"{H}/stream/output/metrics_by_batch")
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
