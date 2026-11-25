#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MISSION 7 : coherence croisee batch / streaming / graphe (TP6 ELK + Spark)."""
import argparse, os, sys, csv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

ap = argparse.ArgumentParser()
ap.add_argument("--history", required=True)
ap.add_argument("--vertices", required=True)
ap.add_argument("--edges", required=True)
ap.add_argument("--stream-events", default=None, help="evenements VALIDES ecrits par le streaming")
ap.add_argument("--out", required=True)
ap.add_argument("--id-regex", default=r"^FLD-[0-9]+$", help="convention d'identifiant parcelle")
ap.add_argument("--alert-values", default="1,true,oui,yes,alert,alerte,critique",
                help="valeurs (minuscules) de la colonne d'alerte comptees comme alerte")
for a in ["col-parcel", "col-region", "col-alert", "v-id", "v-type", "v-region",
          "e-src", "e-dst", "e-rel", "s-parcel", "s-region"]:
    ap.add_argument("--" + a, default=None)
args = ap.parse_args()

spark = SparkSession.builder.appName("TP6_Mission7_Coherence").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
os.makedirs(args.out, exist_ok=True)
LINES, FAILS = [], []

def log(s=""):
    print(s); LINES.append(s)

def check(name, ok, detail=""):
    log(("PASS  " if ok else "FAIL  ") + name + (" -> " + str(detail) if detail != "" else ""))
    if not ok: FAILS.append(name)

def read(path):
    p = os.path.expanduser(path)
    if p.endswith(".parquet"): return spark.read.parquet(p)
    if p.endswith((".json", ".jsonl")): return spark.read.json(p)
    return spark.read.option("header", True).option("inferSchema", True).csv(p)

def pick(df, cands, override, label):
    if override:
        if override not in df.columns:
            sys.exit(f"[ERREUR] colonne {override} absente : {df.columns}")
        return override
    low = {c.lower(): c for c in df.columns}
    for c in cands:
        if c in low: return low[c]
    sys.exit(f"[ERREUR] colonne '{label}' introuvable parmi {df.columns} -> utiliser l'option dediee")

def ranks(v):
    o = sorted(range(len(v)), key=lambda i: v[i]); r = [0.0] * len(v); i = 0
    while i < len(o):
        j = i
        while j + 1 < len(o) and v[o[j + 1]] == v[o[i]]: j += 1
        for k in range(i, j + 1): r[o[k]] = (i + j) / 2 + 1
        i = j + 1
    return r

def spearman(x, y):
    if len(x) < 3: return None
    rx, ry = ranks(x), ranks(y); n = len(x)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = (sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry)) ** 0.5
    return None if den == 0 else num / den

log("===== MISSION 7 : COHERENCE CROISEE BATCH / STREAMING / GRAPHE =====")

# ---------- Lecture ----------
H = spark.read.option("header", True).csv(os.path.expanduser(args.history))  # tout en texte, comme le batch
V, E = read(args.vertices), read(args.edges)
hp = pick(H, ["parcelle_id", "field_id", "id_parcelle", "parcel_id", "parcelle", "field"], args.col_parcel, "parcelle (historique)")
hr = pick(H, ["region"], args.col_region, "region (historique)")
vid = pick(V, ["id", "vertex_id", "node_id", "sommet", "id_texte"], args.v_id, "id sommet")
vty = pick(V, ["type", "vertex_type", "type_sommet", "node_type"], args.v_type, "type sommet")
vre = pick(V, ["region"], args.v_region, "region sommet")
es = pick(E, ["src", "source", "src_id", "from", "origine"], args.e_src, "arete src")
ed = pick(E, ["dst", "target", "dst_id", "to", "destination"], args.e_dst, "arete dst")
er = pick(E, ["relation", "rel", "type", "type_relation", "label"], args.e_rel, "arete relation")
log(f"Historique : {H.count()} lignes | colonnes utilisees : parcelle={hp}, region={hr}")
log(f"Sommets    : {V.count()} | id={vid}, type={vty}, region={vre}")
log(f"Aretes     : {E.count()} | src={es}, dst={ed}, relation={er}")

alert_col = args.col_alert
if not alert_col:
    for c in H.columns:
        if "alert" in c.lower(): alert_col = c; break
log(f"Colonne d'alerte batch : {alert_col}")

# ---------- Historique VALIDE : memes regles que batch_analyses.py ----------
REG = ["Doukkala", "Fes-Meknes", "Gharb", "Marrakech-Safi", "Oriental", "Souss-Massa"]
CROPS = [r[0] for r in V.filter(F.col(vty) == "PARCELLE").select("crop_type").distinct().collect() if r[0]]
NUM = ["ndvi", "soil_moisture", "temperature", "rainfall_mm", "irrigation_liters", "area_ha"]
EXPECTED_VALID = 46752
D = (H.withColumn("rid", F.monotonically_increasing_id())
      .withColumn("obs_date", F.to_date("observation_date", "yyyy-MM-dd")))
for c in NUM:
    D = D.withColumn(c + "_n", F.col(c).cast("double"))
def nz(cond):
    return F.coalesce(cond, F.lit(False))
bad_num = F.lit(False)
for c in NUM:
    bad_num = bad_num | F.col(c + "_n").isNull() | F.isnan(F.col(c + "_n"))
in_range = nz(F.col("ndvi_n").between(0, 1) & F.col("soil_moisture_n").between(0, 100)
              & F.col("temperature_n").between(-10, 55) & F.col("rainfall_mm_n").between(0, 300)
              & (F.col("irrigation_liters_n") >= 0))
rules = [
    ~nz(F.col("observation_id").rlike(r"^OBS-\d{6,}$")),
    ~nz(F.col("field_id").rlike(r"^FLD-\d{3,}$")),
    ~nz(F.col("region").isin(REG)),
    ~nz(F.col("crop_type").isin(CROPS)),
    F.col("obs_date").isNull(),
    bad_num,
    (~bad_num) & (~in_range),
    (~bad_num) & ~nz(F.col("area_ha_n").between(0.0001, 1000)),
    ~nz(F.col("sensor_id").rlike(r"^SNS-\d{4}$")) | ~nz(F.col("sensor_status").isin("OK", "MAINTENANCE", "LOW_BATTERY")),
    ~nz(F.col("alert_flag").isin("true", "false")),
]
anyrule = rules[0]
for r_ in rules[1:]:
    anyrule = anyrule | r_
key = F.coalesce(F.col("observation_id"), F.col("rid").cast("string"))
D = D.withColumn("dup_rank", F.row_number().over(Window.partitionBy(key).orderBy("rid")))
n_all = D.count()
Hv = (D.filter((F.col("dup_rank") == 1) & (~anyrule))
        .select(F.col(hp).cast("string").alias("pid"), F.col(hr).cast("string").alias("reg"),
                F.lower(F.col("alert_flag").cast("string")).alias("alert_raw"))
        .cache())
n_ok = Hv.count()
log(f"Historique : {n_all} lignes brutes -> {n_ok} lignes valides sans doublon (memes regles que le batch)")
check("Lignes valides retrouvees == valeur du batch", n_ok == EXPECTED_VALID, f"{n_ok} vs {EXPECTED_VALID}")

GP = V.filter(F.col(vty) == "PARCELLE").select(F.col(vid).cast("string").alias("pid"), F.col(vre).alias("reg_graph"))
GS = V.filter(F.col(vty) == "SOURCE_EAU").select(F.col(vid).cast("string").alias("sid"), F.col(vre).alias("sreg"))

# ---------- Controles batch <-> graphe ----------
log("\n--- Controles batch <-> graphe ---")
hist_p = Hv.select("pid").distinct()
n_hp, n_gp = hist_p.count(), GP.count()
check("Parcelles distinctes historique == parcelles du graphe", n_hp == n_gp, f"{n_hp} vs {n_gp}")
orph_h = hist_p.join(GP, "pid", "left_anti").count()
check("Toute parcelle de l'historique existe dans le graphe", orph_h == 0, f"{orph_h} orphelines")
check("Toute parcelle du graphe apparait dans l'historique",
      GP.join(hist_p, "pid", "left_anti").count() == 0)
multi = Hv.groupBy("pid").agg(F.countDistinct("reg").alias("k")).filter("k > 1").count()
check("Une seule region par parcelle dans l'historique", multi == 0, f"{multi} parcelles ambigues")
hreg = Hv.groupBy("pid").agg(F.first("reg").alias("reg_hist"))
mism = hreg.join(GP, "pid").filter(F.col("reg_hist") != F.col("reg_graph")).count()
check("Region identique historique / graphe pour chaque parcelle", mism == 0, f"{mism} divergences")
nr_h, nr_g = Hv.select("reg").distinct().count(), GP.select("reg_graph").distinct().count()
check("Nombre de regions identique", nr_h == nr_g, f"{nr_h} vs {nr_g}")
bad_rel = E.filter(F.col(er) == "IRRIGUEE_PAR").join(GP.select(F.col("pid").alias(es)), es, "left_anti").count()
check("Aretes IRRIGUEE_PAR : origine = parcelle", bad_rel == 0, bad_rel)

# ---------- Croisement metier par region ----------
log("\n--- Croisement metier par region (batch x graphe) ---")
irr = (E.filter(F.col(er) == "IRRIGUEE_PAR")
         .select(F.col(es).cast("string").alias("pid"), F.col(ed).cast("string").alias("sid")))
g_reg = (irr.join(GS, "sid").groupBy("sreg")
            .agg(F.count("*").alias("liens_irrigation"), F.countDistinct("sid").alias("sources"))
            .withColumnRenamed("sreg", "region"))
g_par = GP.groupBy(F.col("reg_graph").alias("region")).agg(F.count("*").alias("parcelles"))
if alert_col:
    vals = [v.strip() for v in args.alert_values.split(",")]
    b = (Hv.groupBy(F.col("reg").alias("region"))
           .agg(F.count("*").alias("lignes"),
                F.sum(F.col("alert_raw").isin(vals).cast("int")).alias("alertes")))
    b = b.withColumn("taux_alerte_pct", F.round(100 * F.col("alertes") / F.col("lignes"), 2))
else:
    b = Hv.groupBy(F.col("reg").alias("region")).agg(F.count("*").alias("lignes"))
cross = (g_par.join(g_reg, "region", "left").join(b, "region", "left")
         .withColumn("parcelles_par_source", F.round(F.col("liens_irrigation") / F.col("sources"), 2))
         .orderBy("region"))
cross.show(truncate=False)
rows = [r.asDict() for r in cross.collect()]
with open(os.path.join(args.out, "mission7_croisement_regions.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
for r in rows: log("  " + " | ".join(f"{k}={r[k]}" for k in r))
if alert_col:
    rho = spearman([r["taux_alerte_pct"] for r in rows], [r["parcelles_par_source"] for r in rows])
    log(f"Spearman(taux d'alerte batch, parcelles par source) sur {len(rows)} regions = {rho}")
    log("Lecture : avec si peu de regions, ce coefficient est indicatif, jamais une preuve statistique.")

# ---------- Streaming (optionnel) ----------
if args.stream_events:
    log("\n--- Volumes du flux : produits / traites / rejetes ---")
    SB = os.path.expanduser(args.stream_events).rstrip("/")
    base = os.path.dirname(os.path.dirname(SB))
    S = spark.read.parquet(SB)
    log("Schema evenements valides : " + ", ".join(S.columns))
    n_valid = S.count()
    n_in = spark.read.text(base + "/incoming/*.jsonl").count()
    try:
        R = spark.read.json(base + "/rejected")
        n_rej = R.count()
        if "reject_reason" in R.columns:
            for r_ in R.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
                log(f"    rejet {r_[0]} : {r_[1]}")
    except Exception as ex:
        n_rej = 0
        log(f"[WARN] lecture des rejets impossible : {ex}")
    n_ecart = n_in - n_valid - n_rej
    log(f"Produits (lignes incoming) : {n_in} | valides traites : {n_valid} | rejetes : {n_rej} | ecartes (doublons + tardifs) : {n_ecart}")
    check("Flux >= 10000 evenements produits", n_in >= 10000, n_in)
    check("Bilan des volumes coherent (produits >= valides + rejetes)", n_ecart >= 0, n_ecart)

    log("\n--- Controles streaming <-> graphe ---")
    sp = pick(S, ["parcelle_id", "field_id", "id_parcelle", "parcel_id", "parcelle", "field"], args.s_parcel, "parcelle (flux)")
    badid = S.filter(~F.col(sp).cast("string").rlike(args.id_regex)).count()
    check("Identifiants de parcelle du flux conformes a la convention", badid == 0, f"{badid} non conformes")
    orph = S.select(F.col(sp).cast("string").alias("pid")).distinct().join(GP, "pid", "left_anti").count()
    check("Toutes les parcelles du flux existent dans le graphe", orph == 0, f"{orph} orphelines")
    sreg = args.s_region or next((c for c in S.columns if c.lower() == "region"), None)
    if sreg:
        mm = (S.select(F.col(sp).cast("string").alias("pid"), F.col(sreg).alias("rs")).distinct()
                .join(GP, "pid").filter(F.col("rs") != F.col("reg_graph")).count())
        check("Region du flux == region du graphe", mm == 0, f"{mm} divergences")
    dk = [c for c in ("event_id", "event_time") if c in S.columns]
    if dk:
        d = n_valid - S.dropDuplicates(dk).count()
        check("Aucun doublon (event_id, event_time) dans la sortie valide", d == 0, f"{d} doublons")
    evt = next((c for c in S.columns if c.lower() in ("event_type", "type_evenement", "type")), None)
    if evt:
        log(f"Types d'evenements ({evt}) :")
        for r_ in S.groupBy(evt).count().orderBy(F.desc("count")).collect():
            log(f"    {r_[0]} : {r_[1]}")
        check("Au moins 3 types d'evenements", S.select(evt).distinct().count() >= 3)
    sal = "event_type" if "event_type" in S.columns else None
    if sal:
        vals = [v.strip() for v in args.alert_values.split(",")]
        St = (S.select(F.col(sp).cast("string").alias("pid"), F.lower(F.col(sal).cast("string")).alias("a"))
                .join(GP, "pid").groupBy(F.col("reg_graph").alias("region"))
                .agg(F.count("*").alias("evts_flux"), F.sum(F.col("a").isin(vals).cast("int")).alias("alertes_flux"))
                .withColumn("taux_flux_pct", F.round(100 * F.col("alertes_flux") / F.col("evts_flux"), 2))
                .orderBy("region"))
        srows = {r_["region"]: r_.asDict() for r_ in St.collect()}
        log("\n--- Taux d'alerte par region : batch vs flux ---")
        out = []
        for r_ in rows:
            sr = srows.get(r_["region"], {})
            log(f"  {r_['region']:15s} batch={r_.get('taux_alerte_pct')}%  flux={sr.get('taux_flux_pct')}%  (evts flux={sr.get('evts_flux')}, alertes={sr.get('alertes_flux')})")
            out.append({"region": r_["region"], "taux_batch_pct": r_.get("taux_alerte_pct"), **{k: sr.get(k) for k in ("evts_flux", "alertes_flux", "taux_flux_pct")}})
        with open(os.path.join(args.out, "mission7_batch_vs_flux.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
        log("Lecture : le flux est un echantillon synthetique distinct de l'historique ; on attend des taux du meme ordre de grandeur, pas identiques.")
    else:
        log("[INFO] aucune colonne d'alerte dans les evenements valides : comparaison des taux ignoree.")
else:
    log("\n[INFO] --stream-events non fourni : controles streaming ignores.")

log("\n===== BILAN =====")
log(f"Controles en echec : {len(FAILS)}" + (" -> " + ", ".join(FAILS) if FAILS else " (aucun)"))
with open(os.path.join(args.out, "mission7_report.txt"), "w", encoding="utf-8") as f:
    f.write("\n".join(LINES) + "\n")
log(f"Rapport ecrit dans {os.path.join(args.out, 'mission7_report.txt')}")
spark.stop()
