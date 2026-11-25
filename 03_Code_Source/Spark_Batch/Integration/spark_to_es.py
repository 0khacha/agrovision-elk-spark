"""Publie les resultats Spark (batch, streaming, GraphX) dans Elasticsearch
pour les visualiser dans Kibana (integration ELK <-> Spark).
Usage : spark-submit --driver-memory 1g spark_to_es.py [--base ~/tp6_elk] [--es http://node-1:9200]"""
import argparse, json, urllib.request, urllib.error
from pyspark.sql import SparkSession, Window, functions as F

p = argparse.ArgumentParser()
p.add_argument("--es", default="http://node-1:9200")
p.add_argument("--base", default="/home/vm4/tp6_elk")
a = p.parse_args()
spark = SparkSession.builder.appName("spark_to_es").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def enc(o):  # dates -> ISO 8601, reconnues comme date par Elasticsearch
    return o.isoformat() if hasattr(o, "isoformat") else str(o)

def http(method, url, body=None, ctype="application/json"):
    req = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": ctype})
    try:
        return json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        return {"http_error": e.code}

def publish(index, df, key_cols):
    rows = [r.asDict(recursive=True) for r in df.collect()]
    http("DELETE", f"{a.es}/{index}")          # republication idempotente
    errors = 0
    for i in range(0, len(rows), 2000):
        lines = []
        for j, r in enumerate(rows[i:i + 2000], start=i):
            _id = "_".join(enc(r[c]) for c in key_cols) if key_cols else str(j)
            lines.append(json.dumps({"index": {"_index": index, "_id": _id}}))
            lines.append(json.dumps(r, default=enc))
        res = http("POST", f"{a.es}/_bulk?refresh=true",
                   ("\n".join(lines) + "\n").encode(), "application/x-ndjson")
        errors += sum(1 for it in res.get("items", []) if it["index"].get("error"))
    cnt = http("GET", f"{a.es}/{index}/_count").get("count")
    print(f"PUBLIE {index:30s} lignes={len(rows):6d} indexes={cnt} erreurs={errors}")

def csv(path):
    return spark.read.csv(f"{a.base}/{path}", header=True, inferSchema=True)

def keys(df, wanted):
    return [c for c in wanted if c in df.columns]

def step(name, fn):
    try:
        fn()
    except Exception as e:
        print(f"[IGNORE] {name} : {str(e)[:200]}")

# --- Spark Batch ---
def b1():
    df = csv("results/batch/analyse1_region_culture")
    publish("agro-spark-batch-b1", df, keys(df, ["region", "crop_type"]))
step("batch B1", b1)

def b3():
    df = csv("results/batch/analyse3_etat_capteur")
    publish("agro-spark-batch-b3-capteurs", df, keys(df, ["sensor_status"]))
step("batch B3", b3)

# --- Spark Structured Streaming ---
def events():
    df = spark.read.parquet(f"{a.base}/stream/output/events")
    publish("agro-stream-events", df, keys(df, ["event_id", "event_time"]))
step("stream events", events)

def metrics():
    df = spark.read.parquet(f"{a.base}/stream/output/metrics_by_batch")
    if "window" in df.columns and "window_start" not in df.columns:
        df = df.withColumn("window_start", F.col("window.start")).withColumn("window_end", F.col("window.end")).drop("window")
    if "batch_id" in df.columns:   # dernier etat de chaque (fenetre, region)
        w = Window.partitionBy("window_start", "region").orderBy(F.col("batch_id").desc())
        df = df.withColumn("_rk", F.row_number().over(w)).filter("_rk = 1").drop("_rk")
    publish("agro-stream-metrics", df, keys(df, ["window_start", "region"]))
step("stream metrics", metrics)

# --- GraphX ---
def g_sources():
    df = csv("results/graphx/analyse2_toutes_sources")
    publish("agro-graphx-sources", df, keys(df, ["source", "src", "vertex_id", "id"])[:1])
step("graphx sources", g_sources)

def g_degres():
    df = csv("results/graphx/analyse1_parcelles_degre")
    publish("agro-graphx-parcelles", df, keys(df, ["sommet", "field_id", "vertex_id", "id"])[:1])
step("graphx parcelles", g_degres)

spark.stop()
