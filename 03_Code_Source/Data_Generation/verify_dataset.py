#!/usr/bin/env python3
"""AgroVision - verification du jeu de donnees de charge (TP3), en lecture seule.

Controle : identifiants, types, plages, distribution, anomalies et doublons
(compares a la verite terrain), puis compare le jeu au jeu du TP4.
Ecrit un rapport Markdown dans ~/tp3_elk/docs/dataset_verification.md
"""
import argparse, collections, csv, glob, os, re, sys
from datetime import date

COLUMNS = ["observation_id", "field_id", "region", "crop_type", "observation_date", "ndvi",
           "soil_moisture", "temperature", "rainfall_mm", "irrigation_liters", "sensor_id",
           "sensor_status", "area_ha", "alert_flag", "notes"]
REG = {"Souss-Massa", "Marrakech-Safi", "Fes-Meknes", "Gharb", "Oriental", "Doukkala"}
CROP = {"ble", "orge", "tomate", "agrumes", "olivier", "mais"}
STAT = {"OK", "LOW_BATTERY", "MAINTENANCE"}
REQ = ("observation_id", "field_id", "region", "crop_type", "observation_date", "ndvi",
       "soil_moisture", "temperature", "sensor_id")
RNG = {"ndvi": (0, 1), "soil_moisture": (0, 100), "temperature": (-10, 55), "rainfall_mm": (0, 300),
       "irrigation_liters": (0, 100000), "area_ha": (0.0001, 1000)}
NUMS = ("ndvi", "soil_moisture", "temperature", "rainfall_mm", "irrigation_liters", "area_ha")
CATS = ["MISSING_FIELD", "INVALID_DATE", "INVALID_NUMERIC", "UNKNOWN_CATEGORY"]
G, R, Y, B, X = "\033[32m", "\033[31m", "\033[33m", "\033[1m", "\033[0m"


def problems(r):
    """Memes regles que agro_validate.rb (hors doublons)."""
    out = []
    for f in REQ:
        if r[f].strip() == "":
            out.append("MISSING_FIELD")
    d = r["observation_date"].strip()
    if d:
        ok = re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) is not None
        if ok:
            try:
                date.fromisoformat(d)
            except ValueError:
                ok = False
        if not ok:
            out.append("INVALID_DATE")
    for f, (lo, hi) in RNG.items():
        v = r[f].strip()
        if v == "":
            continue
        try:
            x = float(v)
        except ValueError:
            out.append("INVALID_NUMERIC")
            continue
        if x < lo or x > hi:
            out.append("INVALID_NUMERIC")
    for f, allowed, fn in (("region", REG, str), ("crop_type", CROP, str.lower), ("sensor_status", STAT, str.upper)):
        v = fn(r[f].strip())
        if v and v not in allowed:
            out.append("UNKNOWN_CATEGORY")
    return out


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def files_for(d, prefix):
    parts = sorted(glob.glob(os.path.join(d, prefix + "_part_*.csv")))
    return parts if parts else [os.path.join(d, prefix + ".csv")]


def scan(paths, truth_cat, ref=None, keep=False):
    S = dict(lines=0, header_ok=True, cols=None, cnt=collections.Counter(), idnum=[], id_bad=0,
             det={c: set() for c in CATS}, dup_pos=[], valid=0, type_err=0,
             crop=collections.Counter(), region=collections.Counter(), status=collections.Counter(),
             month=collections.Counter(), alert=0, fields=set(), dmin="9999", dmax="0000",
             num={f: [0, 0.0, 1e18, -1e18] for f in NUMS}, cmp_total=0, cmp_same=0, cmp_ex=[], rows={})
    for path in paths:
        with open(path, newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            if S["cols"] is None:
                S["cols"] = rd.fieldnames
            if rd.fieldnames != COLUMNS:
                S["header_ok"] = False
            for r in rd:
                S["lines"] += 1
                oid = r["observation_id"]
                S["cnt"][oid] += 1
                m = re.fullmatch(r"OBS-(\d{6,})", oid)
                if m:
                    if S["cnt"][oid] == 1:
                        S["idnum"].append(int(m.group(1)))
                else:
                    S["id_bad"] += 1
                pr = problems(r)
                if S["cnt"][oid] > 1:
                    S["dup_pos"].append(S["lines"])
                    continue                                  # 2e occurrence : doublon
                if pr:
                    S["det"][pr[0]].add(oid)
                    continue
                # ---- enregistrement valide ----
                S["valid"] += 1
                bad = (r["alert_flag"] not in ("true", "false") or not re.fullmatch(r"\d+", r["irrigation_liters"])
                       or not ("2026-01-01" <= r["observation_date"] <= "2026-09-30"))
                S["type_err"] += bad
                S["crop"][r["crop_type"]] += 1
                S["region"][r["region"]] += 1
                S["status"][r["sensor_status"]] += 1
                S["month"][r["observation_date"][:7]] += 1
                S["alert"] += r["alert_flag"] == "true"
                S["fields"].add(r["field_id"])
                S["dmin"] = min(S["dmin"], r["observation_date"])
                S["dmax"] = max(S["dmax"], r["observation_date"])
                for fld in NUMS:
                    v = float(r[fld])
                    a = S["num"][fld]
                    a[0] += 1; a[1] += v; a[2] = min(a[2], v); a[3] = max(a[3], v)
                tup = tuple(r[c] for c in COLUMNS)
                if keep:
                    S["rows"][oid] = tup
                if ref is not None and oid in ref:
                    S["cmp_total"] += 1
                    if ref[oid] == tup:
                        S["cmp_same"] += 1
                    elif len(S["cmp_ex"]) < 3:
                        S["cmp_ex"].append(oid)
    return S


def pct(a, b):
    return f"{100 * a / b:.1f} %" if b else "n/a"


def show(title, headers, rows, md):
    print(f"\n{B}{title}{X}")
    w = [max(len(str(x)) for x in col) for col in zip(headers, *rows)]
    print("  " + "  ".join(h.ljust(n) for h, n in zip(headers, w)))
    print("  " + "  ".join("-" * n for n in w))
    for r in rows:
        print("  " + "  ".join(str(c).ljust(n) for c, n in zip(r, w)))
    md.append(f"\n### {title}\n")
    md.append("| " + " | ".join(headers) + " |")
    md.append("|" + "---|" * len(headers))
    for r in rows:
        md.append("| " + " | ".join(str(c) for c in r) + " |")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.expanduser("~/tp3_elk/data"))
    ap.add_argument("--prefix", default="agro_load")
    ap.add_argument("--tp4-dir", default=os.path.expanduser("~/tp4/data"))
    ap.add_argument("--tp4-file", default="agro_observations.csv")
    ap.add_argument("--min-records", type=int, default=25000)
    ap.add_argument("--report", default=os.path.expanduser("~/tp3_elk/docs/dataset_verification.md"))
    a = ap.parse_args()

    truth = {r["observation_id"]: r["anomaly_category"] for r in read_csv(os.path.join(a.dir, "anomalies_truth.csv"))}
    dups_t = {r["observation_id"] for r in read_csv(os.path.join(a.dir, "duplicates_truth.csv"))}

    tp4_path = os.path.join(a.tp4_dir, a.tp4_file)
    have4 = os.path.exists(tp4_path) and os.path.exists(os.path.join(a.tp4_dir, "anomalies_truth.csv"))
    T4 = None
    if have4:
        truth4 = {r["observation_id"]: r["anomaly_category"] for r in read_csv(os.path.join(a.tp4_dir, "anomalies_truth.csv"))}
        T4 = scan([tp4_path], truth4, keep=True)
    S = scan(files_for(a.dir, a.prefix), truth, ref=(T4["rows"] if T4 else None))

    md = ["# Verification du jeu de donnees de charge", "",
          f"Fichiers : {', '.join(os.path.basename(p) for p in files_for(a.dir, a.prefix))}"]
    distinct = len(S["cnt"])
    dup_ids = {i for i, c in S["cnt"].items() if c > 1}
    ids = sorted(S["idnum"])
    contiguous = bool(ids) and ids[0] == 1 and ids[-1] == len(ids) and len(set(ids)) == len(ids)
    det_all = sum(len(v) for v in S["det"].values())
    checks = [
        ("15 colonnes dans l'ordre du mapping TP4", "oui", "oui" if S["header_ok"] else "non", S["header_ok"]),
        (f"Enregistrements distincts >= {a.min_records}", f">= {a.min_records}", distinct, distinct >= a.min_records),
        ("Lignes = distincts + doublons sources", distinct + len(dups_t), S["lines"], S["lines"] == distinct + len(dups_t)),
        ("Identifiants au format OBS-NNNNNN", 0, S["id_bad"], S["id_bad"] == 0),
        ("Identifiants continus de 1 a N (stables)", "oui", "oui" if contiguous else "non", contiguous),
        ("Doublons detectes = doublons injectes", len(dups_t), len(dup_ids), dup_ids == dups_t),
    ]
    for c in CATS:
        inj = {i for i, k in truth.items() if k == c}
        checks.append((f"{c} : detectees = injectees", len(inj), len(S["det"][c]), S["det"][c] == inj))
    checks += [
        ("Enregistrements valides = distincts - anomalies", distinct - len(truth), S["valid"], S["valid"] == distinct - len(truth)),
        ("Valides avec erreur de type/format", 0, S["type_err"], S["type_err"] == 0),
        ("Taux d'anomalies >= 5 %", ">= 5 %", pct(len(truth), distinct), len(truth) >= 0.05 * distinct),
    ]
    if T4:
        checks.append(("Enregistrements de base identiques au TP4", "100 %", pct(S["cmp_same"], S["cmp_total"]),
                       S["cmp_total"] > 0 and S["cmp_same"] == S["cmp_total"]))

    print(f"\n{B}=== 1. CONTROLES ==={X}")
    md += ["", "## 1. Controles", "", "| Controle | Attendu | Obtenu | Resultat |", "|---|---|---|---|"]
    ok_all = True
    for lab, exp, got, ok in checks:
        ok_all &= bool(ok)
        print(f"  {(G + 'PASS' if ok else R + 'FAIL') + X}  {lab:<52} attendu {str(exp):>10}   obtenu {str(got):>10}")
        md.append(f"| {lab} | {exp} | {got} | {'PASS' if ok else 'FAIL'} |")

    # ---- volumes
    vol = [("Lignes de donnees", S["lines"]), ("Enregistrements distincts", distinct),
           ("Anomalies injectees", f"{len(truth)} ({pct(len(truth), distinct)})"),
           ("Doublons sources", f"{len(dups_t)} ({pct(len(dups_t), distinct)})"),
           ("Enregistrements valides attendus a l'indexation", S["valid"]),
           ("Parcelles (field_id) distinctes", len(S["fields"])), ("Periode", f"{S['dmin']} a {S['dmax']}")]
    show("2. Volumes", ["Mesure", "Valeur"], vol, md)

    # ---- anomalies
    rows = [(c, len({i for i, k in truth.items() if k == c}), len(S["det"][c])) for c in CATS]
    rows.append(("DUPLICATE", len(dups_t), len(dup_ids)))
    show("3. Anomalies et doublons : injectes / detectes", ["Categorie", "Injectes", "Detectes"], rows, md)

    # ---- distribution
    n = S["valid"]
    show("4. Distribution (enregistrements valides)", ["Culture", "Nombre", "Part"],
         [(k, v, pct(v, n)) for k, v in sorted(S["crop"].items(), key=lambda x: -x[1])], md)
    show("   Regions", ["Region", "Nombre", "Part"],
         [(k, v, pct(v, n)) for k, v in sorted(S["region"].items(), key=lambda x: -x[1])], md)
    show("   Etat du capteur", ["Etat", "Nombre", "Part"],
         [(k, v, pct(v, n)) for k, v in sorted(S["status"].items(), key=lambda x: -x[1])], md)
    show("   Mesures numeriques", ["Champ", "Min", "Moyenne", "Max"],
         [(f, round(v[2], 2), round(v[1] / v[0], 2), round(v[3], 2)) for f, v in S["num"].items()], md)
    show("   Observations par mois", ["Mois", "Nombre"], sorted(S["month"].items()), md)
    print(f"\n  Alertes de stress (alert_flag = true) : {S['alert']} ({pct(S['alert'], n)})")
    md.append(f"\nAlertes de stress : {S['alert']} ({pct(S['alert'], n)})")

    # ---- comparaison TP4
    if T4:
        n4 = T4["valid"]
        pos4 = sum(T4["dup_pos"]) / len(T4["dup_pos"]) / T4["lines"] if T4["dup_pos"] else 0
        pos = sum(S["dup_pos"]) / len(S["dup_pos"]) / S["lines"] if S["dup_pos"] else 0
        cmpr = [("Lignes de donnees", T4["lines"], S["lines"]),
                ("Enregistrements distincts", len(T4["cnt"]), distinct),
                ("Anomalies (4 categories)", f"{len(truth4)} ({pct(len(truth4), len(T4['cnt']))})", f"{len(truth)} ({pct(len(truth), distinct)})"),
                ("Doublons sources", sum(1 for c in T4["cnt"].values() if c > 1), len(dup_ids)),
                ("Position des doublons (0 = debut, 1 = fin)", f"{pos4:.2f} (en fin de fichier)", f"{pos:.2f} (repartis)"),
                ("Parcelles distinctes", len(T4["fields"]), len(S["fields"])),
                ("Mesures par parcelle (moyenne)", round(n4 / len(T4["fields"])), round(n / len(S["fields"]))),
                ("Periode", f"{T4['dmin']} a {T4['dmax']}", f"{S['dmin']} a {S['dmax']}"),
                ("Champs / mapping", "15 / identique", "15 / identique"),
                ("Fichiers", "1", len(files_for(a.dir, a.prefix))),
                ("Enregistrements de base communs compares", "-", f"{S['cmp_total']}, identiques : {S['cmp_same']}")]
        show("5. Differences avec le jeu du TP4", ["Caracteristique", "TP4", "Test de charge"], cmpr, md)
        show("   Repartition des cultures : TP4 / charge", ["Culture", "TP4", "Charge"],
             [(k, pct(T4["crop"][k], n4), pct(S["crop"][k], n)) for k in sorted(CROP)], md)
        show("   Repartition des regions : TP4 / charge", ["Region", "TP4", "Charge"],
             [(k, pct(T4["region"][k], n4), pct(S["region"][k], n)) for k in sorted(REG)], md)
    else:
        print(f"\n{Y}Jeu du TP4 introuvable ({tp4_path}) : comparaison ignoree.{X}")

    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    print(f"\nRapport ecrit : {a.report}")
    print(f"{B}RESULTAT : {(G + 'TOUS LES CONTROLES REUSSIS') if ok_all else (R + 'ECHEC : voir les lignes FAIL')}{X}\n")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
