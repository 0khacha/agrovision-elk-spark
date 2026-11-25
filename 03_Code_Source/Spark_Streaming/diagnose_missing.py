# AgroVision - outil de diagnostic : recherche les evenements attendus absents de la sortie du flux.
import os
from pyspark.sql import SparkSession, functions as F
S = os.path.expanduser("~/tp6_elk/stream")
spark = SparkSession.builder.master("local[2]").appName("diag").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1) lecture du sink via le journal _spark_metadata vs lecture directe des fichiers
print("events via dossier (journal)  :", spark.read.parquet(f"{S}/output/events").count())
print("events via glob (fichiers)    :", spark.read.parquet(f"{S}/output/events/*.parquet").count())
print("rejected via dossier          :", spark.read.json(f"{S}/rejected").count())
print("rejected via glob             :", spark.read.json(f"{S}/rejected/*.json").count())

# 2) lignes d'entree absentes des sorties, par fichier source
inc = (spark.read.text(f"{S}/incoming/*.jsonl")
       .withColumn("file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
       .withColumn("eid", F.get_json_object("value", "$.event_id")))
ev = spark.read.parquet(f"{S}/output/events/*.parquet").select("event_id").distinct()
rej = spark.read.json(f"{S}/rejected/*.json").select(F.col("raw_line").alias("value")).distinct()
miss = (inc.dropDuplicates(["value"]).join(rej, "value", "left_anti")
        .join(ev, inc.eid == ev.event_id, "left_anti"))
print("lignes d'entree sans sortie (valides ou rejetees) :", miss.count())
miss.groupBy("file").count().orderBy("file").show(20, False)
