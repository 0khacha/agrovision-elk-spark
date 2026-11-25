# AgroVision - Spark Batch : nettoyage de l'historique et 3 analyses.
# Entree  : ~/tp6_elk/data/history/agro_hist.csv (lu en texte, sans inference de schema)
# Etapes  : typage explicite, 10 regles de rejet, dedoublonnage par row_number() sur observation_id
# Sorties : ~/tp6_elk/results/batch/ (CSV)
#   B1 analyse1_region_culture : NDVI, humidite, irrigation/ha, taux d'alerte par region x culture
#   B2 analyse2_mensuel_*      : agregats mensuels nationaux et regionaux, delta_ndvi (lag)
#   B3 analyse3_*              : rejets par motif, alertes par etat de capteur, comparaison verite terrain
# Usage   : spark-submit --driver-memory 1g batch_analyses.py
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

H = os.path.expanduser("~/tp6_elk")
OUT = f"{H}/results/batch"
spark = (SparkSession.builder.master("local[2]").appName("agro_batch")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

REG = ["Souss-Massa", "Marrakech-Safi", "Fes-Meknes", "Gharb", "Oriental", "Doukkala"]
CROPS = ["ble", "orge", "tomate", "agrumes", "olivier", "mais"]
NUM = ["ndvi", "soil_moisture", "temperature", "rainfall_mm", "irrigation_liters", "area_ha"]

raw = spark.read.csv(f"{H}/data/history/agro_hist.csv", header=True, inferSchema=False)
raw = raw.withColumn("rid", F.monotonically_increasing_id())
d = raw.withColumn("obs_date", F.to_date("observation_date", "yyyy-MM-dd"))
for c in NUM:
    d = d.withColumn(c + "_n", F.col(c).cast("double"))

def nz(cond):  # une condition inconnue (NULL) compte comme fausse
    return F.coalesce(cond, F.lit(False))

bad_num = F.lit(False)
for c in NUM:
    bad_num = bad_num | F.col(c + "_n").isNull() | F.isnan(F.col(c + "_n"))
in_range = nz(F.col("ndvi_n").between(0, 1) & F.col("soil_moisture_n").between(0, 100)
              & F.col("temperature_n").between(-10, 55) & F.col("rainfall_mm_n").between(0, 300)
              & (F.col("irrigation_liters_n") >= 0))
rules = {
    "BAD_ID": ~nz(F.col("observation_id").rlike(r"^OBS-\d{6,}$")),
    "BAD_FIELD": ~nz(F.col("field_id").rlike(r"^FLD-\d{3,}$")),
    "BAD_REGION": ~nz(F.col("region").isin(REG)),
    "BAD_CROP": ~nz(F.col("crop_type").isin(CROPS)),
    "BAD_DATE": F.col("obs_date").isNull(),
    "BAD_NUMERIC": bad_num,
    "OUT_OF_RANGE": (~bad_num) & (~in_range),
    "BAD_AREA": (~bad_num) & ~nz(F.col("area_ha_n").between(0.0001, 1000)),
    "BAD_SENSOR": ~nz(F.col("sensor_id").rlike(r"^SNS-\d{4}$")) | ~nz(F.col("sensor_status").isin("OK", "MAINTENANCE", "LOW_BATTERY")),
    "BAD_FLAG": ~nz(F.col("alert_flag").isin("true", "false")),
}
for k, v in rules.items():
    d = d.withColumn("f_" + k, v)
first = None
for k in rules:
    first = F.when(F.col("f_" + k), k) if first is None else first.when(F.col("f_" + k), k)
d = d.withColumn("reject_reason", first)

# doublons : meme observation_id deja vue (on garde la premiere occurrence)
key = F.coalesce(F.col("observation_id"), F.col("rid").cast("string"))
w = Window.partitionBy(key).orderBy("rid")
d = d.withColumn("dup_rank", F.row_number().over(w)).cache()

total = d.count()
distinct_ids = d.select("observation_id").distinct().count()
dups = d.filter("dup_rank > 1").count()
nodup = d.filter("dup_rank = 1")
invalid = nodup.filter("reject_reason IS NOT NULL").count()
clean = (nodup.filter("reject_reason IS NULL")
         .withColumn("irr_per_ha", F.col("irrigation_liters_n") / F.col("area_ha_n"))
         .withColumn("alert", (F.col("alert_flag") == "true").cast("int")).cache())
n_clean = clean.count()

def save(df, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")

# ---------- Analyse 3 : qualite ----------
print("\n===== ANALYSE 3 : QUALITE DES DONNEES =====")
print(f"Lignes lues                 : {total}")
print(f"observation_id distincts    : {distinct_ids}")
print(f"Lignes en doublon (rang>1)  : {dups}")
print(f"Invalides (hors doublons)   : {invalid}")
print(f"Valides et dedoublonnees    : {n_clean}")
print(f"Controle : {total} = {dups} + {invalid} + {n_clean} -> {'PASS' if total == dups + invalid + n_clean else 'FAIL'}")
print("\nRejets par raison principale (premiere regle violee) :")
q_reason = nodup.filter("reject_reason IS NOT NULL").groupBy("reject_reason").count().orderBy(F.desc("count"))
q_reason.show(20, False)
print("Lignes violant chaque regle (une ligne peut en violer plusieurs) :")
q_rules = spark.createDataFrame(
    [(k, nodup.filter(F.col("f_" + k)).count()) for k in rules], ["regle", "lignes"]).orderBy(F.desc("lignes"))
q_rules.show(20, False)
print("Etat du capteur (donnees valides) :")
q_sensor = (clean.groupBy("sensor_status").agg(F.count("*").alias("n"), F.round(F.avg("alert"), 4).alias("taux_alerte"),
            F.round(F.avg("ndvi_n"), 3).alias("ndvi_moyen")).orderBy(F.desc("n")))
q_sensor.show(5, False)
save(q_reason, "analyse3_rejets_par_raison"); save(q_rules, "analyse3_rejets_par_regle"); save(q_sensor, "analyse3_etat_capteur")

# ---------- Analyse 1 : agregation metier ----------
print("\n===== ANALYSE 1 : REGION x CULTURE =====")
a1 = (clean.groupBy("region", "crop_type").agg(
        F.count("*").alias("n_obs"), F.countDistinct("field_id").alias("n_parcelles"),
        F.round(F.avg("ndvi_n"), 3).alias("ndvi_moyen"), F.round(F.avg("soil_moisture_n"), 2).alias("humidite_moy"),
        F.round(F.avg("temperature_n"), 2).alias("temp_moy"), F.round(F.sum("rainfall_mm_n"), 1).alias("pluie_totale_mm"),
        F.round(F.avg("irr_per_ha"), 2).alias("irrigation_l_par_ha"), F.round(F.avg("alert"), 4).alias("taux_alerte"))
      .orderBy("region", "crop_type"))
a1.show(40, False)
print("Top 5 combinaisons par taux d'alerte :")
a1.orderBy(F.desc("taux_alerte")).select("region", "crop_type", "n_obs", "taux_alerte", "ndvi_moyen").show(5, False)
save(a1, "analyse1_region_culture")

# ---------- Analyse 2 : evolution mensuelle ----------
print("\n===== ANALYSE 2 : EVOLUTION MENSUELLE PAR REGION =====")
m = (clean.withColumn("mois", F.trunc("obs_date", "month")).groupBy("region", "mois").agg(
        F.count("*").alias("n_obs"), F.round(F.avg("ndvi_n"), 3).alias("ndvi_moyen"),
        F.round(F.avg("soil_moisture_n"), 2).alias("humidite_moy"), F.round(F.sum("rainfall_mm_n"), 1).alias("pluie_mm")))
wm = Window.partitionBy("region").orderBy("mois")
a2 = (m.withColumn("ndvi_mois_prec", F.lag("ndvi_moyen").over(wm))
        .withColumn("delta_ndvi", F.round(F.col("ndvi_moyen") - F.col("ndvi_mois_prec"), 3))
        .orderBy("region", "mois"))
a2.show(30, False)
print("Mois national (toutes regions) :")
a2n = (clean.withColumn("mois", F.trunc("obs_date", "month")).groupBy("mois").agg(
         F.count("*").alias("n_obs"), F.round(F.avg("ndvi_n"), 3).alias("ndvi_moyen"),
         F.round(F.avg("soil_moisture_n"), 2).alias("humidite_moy"), F.round(F.sum("rainfall_mm_n"), 1).alias("pluie_mm")).orderBy("mois"))
a2n.show(24, False)
print("5 plus fortes baisses mensuelles de NDVI :")
a2.filter("delta_ndvi IS NOT NULL").orderBy("delta_ndvi").select("region", "mois", "ndvi_mois_prec", "ndvi_moyen", "delta_ndvi").show(5, False)
save(a2, "analyse2_mensuel_region"); save(a2n, "analyse2_mensuel_national")

# ---------- Validation contre la verite terrain ----------
print("\n===== VALIDATION CONTRE LA VERITE TERRAIN =====")
truth = (spark.read.csv(f"{H}/data/history/anomalies_truth.csv", header=True)
         .dropDuplicates(["observation_id"]))
dtruth = spark.read.csv(f"{H}/data/history/duplicates_truth.csv", header=True).distinct()
jt = nodup.join(truth, "observation_id", "left")
n_truth = truth.count()
found = jt.filter("anomaly_category IS NOT NULL AND reject_reason IS NOT NULL").count()
missed = jt.filter("anomaly_category IS NOT NULL AND reject_reason IS NULL")
fpos = jt.filter("anomaly_category IS NULL AND reject_reason IS NOT NULL").count()
print(f"Anomalies injectees (verite)       : {n_truth}")
print(f"Detectees par les regles Spark     : {found}")
print(f"Manquees                           : {missed.count()}")
print(f"Fausses alertes (rejet hors verite): {fpos}")
print("\nCategorie reelle x raison Spark :")
jt.filter("anomaly_category IS NOT NULL").groupBy("anomaly_category", "reject_reason").count() \
  .orderBy("anomaly_category", F.desc("count")).show(40, False)
print("Anomalies manquees (echantillon) :")
missed.select("observation_id", "anomaly_category", "region", "crop_type", "observation_date", "ndvi",
              "soil_moisture", "temperature", "rainfall_mm", "irrigation_liters", "area_ha").show(15, False)
dd = d.filter("dup_rank > 1").select("observation_id").distinct()
print("Doublons : detectes absents de la verite =", dd.exceptAll(dtruth).count(),
      "| verite absents des detectes =", dtruth.exceptAll(dd).count())
