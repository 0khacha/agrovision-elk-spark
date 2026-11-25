# AgroVision - compare deux executions du flux (sans arret / avec reprise) :
# evenements valides, event_id manquants, rejets, fenetres et lots ecrits deux fois.
# Usage : spark-submit compare_runs.py <dossier_run1> <dossier_run2>
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

R1, R2 = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.master("local[2]").appName("compare").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def load(b):
    ev = spark.read.parquet(f"{b}/export/events")
    rej = spark.read.parquet(f"{b}/export/rejected")
    met = spark.read.parquet(f"{b}/export/metrics_all")
    w = Window.partitionBy("window_start", "region").orderBy(F.col("batch_id").desc())
    last = met.withColumn("rk", F.row_number().over(w)).filter("rk = 1").drop("rk", "batch_id")
    return ev, rej, last, met


ev1, rej1, win1, _ = load(R1)
ev2, rej2, win2, met2 = load(R2)
ids1, ids2 = ev1.select("event_id"), ev2.select("event_id")
checks = [
    ("Evenements valides : run1 = run2", ev1.count(), ev2.count()),
    ("event_id presents en run1 et absents en run2", ids1.exceptAll(ids2).count(), 0),
    ("event_id presents en run2 et absents en run1", ids2.exceptAll(ids1).count(), 0),
    ("Rejets : run1 = run2", rej1.count(), rej2.count()),
    ("Rejets differents (ligne + raison)", rej1.exceptAll(rej2).count() + rej2.exceptAll(rej1).count(), 0),
    ("Fenetres (dernier etat) differentes", win1.exceptAll(win2).count() + win2.exceptAll(win1).count(), 0),
]
for label, got, exp in checks:
    print("PASS" if got == exp else "FAIL", f"{label:<50} obtenu={got} attendu={exp}")
print("\nLots de metriques du run2 :")
print("  lots distincts :", met2.select("batch_id").distinct().count(), "| dernier batch_id :", met2.agg(F.max("batch_id")).first()[0])
dup = met2.groupBy("window_start", "region", "batch_id").count().filter("count > 1").count()
print("  (fenetre, region, batch_id) ecrits plusieurs fois (lot rejoue) :", dup)
