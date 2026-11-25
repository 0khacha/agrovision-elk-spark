#!/usr/bin/env python3
"""AgroVision - generateur du jeu de donnees de charge (TP3)."""

import argparse
import csv
import glob
import hashlib
import json
import math
import os
import random
import time
from datetime import date, timedelta


COLUMNS = [
    "observation_id",
    "field_id",
    "region",
    "crop_type",
    "observation_date",
    "ndvi",
    "soil_moisture",
    "temperature",
    "rainfall_mm",
    "irrigation_liters",
    "sensor_id",
    "sensor_status",
    "area_ha",
    "alert_flag",
    "notes",
]

REGIONS = [
    "Souss-Massa",
    "Marrakech-Safi",
    "Fes-Meknes",
    "Gharb",
    "Oriental",
    "Doukkala",
]

CROPS = {
    "ble": (0.62, 28, 20),
    "orge": (0.58, 25, 21),
    "tomate": (0.70, 38, 27),
    "agrumes": (0.75, 35, 26),
    "olivier": (0.55, 22, 25),
    "mais": (0.72, 33, 28),
}

STATUS = (
    ["OK"] * 90
    + ["LOW_BATTERY"] * 7
    + ["MAINTENANCE"] * 3
)

NOTES = [
    "routine reading",
    "sensor calibrated",
    "irrigation scheduled",
    "leaf stress observed",
    "normal growth",
    "dry period",
    "after rainfall",
    "weeding done",
]

CATEGORIES = [
    "MISSING_FIELD",
    "INVALID_DATE",
    "INVALID_NUMERIC",
    "UNKNOWN_CATEGORY",
]


def clip(x, lo, hi):
    return max(lo, min(hi, x))


def make_fields(rng, n_fields):
    names = list(CROPS)
    fields = []

    for n in range(1, n_fields + 1):
        region = rng.choice(REGIONS)
        area = round(rng.uniform(2, 80), 1)

        fields.append({
            "field_id": f"FLD-{n:03d}",
            "region": region,
            "crop": names[(n - 1) % len(names)],
            "area": area,
            "sensor": f"SNS-{n:04d}",
        })

    return fields


def make_row(rng, fields, idw, i):
    f = rng.choice(fields)

    d = date(2026, 1, 1) + timedelta(
        days=rng.randint(0, 272)
    )

    ndvi_m, sm_m, t_m = CROPS[f["crop"]]

    doy = d.timetuple().tm_yday

    ndvi = round(
        clip(
            rng.gauss(ndvi_m, 0.10),
            0.05,
            0.95
        ),
        2
    )

    sm = round(
        clip(
            rng.gauss(sm_m, 7),
            5,
            60
        ),
        1
    )

    temp = round(
        clip(
            t_m
            + 7 * math.sin(
                2 * math.pi * (doy - 100) / 365
            )
            + rng.gauss(0, 2.5),
            -5,
            48,
        ),
        1,
    )

    rain = (
        0.0
        if rng.random() < 0.7
        else round(
            rng.expovariate(1 / 8),
            1
        )
    )

    irrig = (
        int(max(0, 35 - sm) * rng.uniform(80, 160))
        if rain < 2
        else 0
    )

    return {
        "observation_id": f"OBS-{i:0{idw}d}",
        "field_id": f["field_id"],
        "region": f["region"],
        "crop_type": f["crop"],
        "observation_date": d.isoformat(),
        "ndvi": ndvi,
        "soil_moisture": sm,
        "temperature": temp,
        "rainfall_mm": rain,
        "irrigation_liters": irrig,
        "sensor_id": f["sensor"],
        "sensor_status": rng.choice(STATUS),
        "area_ha": f["area"],
        "alert_flag": str(
            ndvi < 0.40 or sm < 15
        ).lower(),
        "notes": rng.choice(NOTES),
    }


# ============================================================
# ANOMALIES
# ============================================================

def anomaly_missing(r, g):
    field = g.choice([
        "field_id",
        "crop_type",
        "ndvi"
    ])

    r[field] = ""


def anomaly_date(r, g):
    r["observation_date"] = g.choice([
        "2026-13-45",
        "2026-00-10",
        "45/13/2026",
        "not_a_date",
        "2026-99-99",
    ])


def anomaly_numeric(r, g):
    field = g.choice([
        "ndvi",
        "soil_moisture",
        "temperature",
        "rainfall_mm",
    ])

    bad = {
        "ndvi": [
            1.8,
            2.5,
            -0.4,
            9.99
        ],
        "soil_moisture": [
            135.0,
            -20.0,
            250.0
        ],
        "temperature": [
            85.0,
            -60.0,
            999.0
        ],
        "rainfall_mm": [
            -15.0,
            950.0
        ],
    }

    r[field] = g.choice(bad[field])


def anomaly_category(r, g):
    if g.random() < 0.5:
        r["crop_type"] = g.choice([
            "xyz",
            "unknown",
            "kiwi",
            "???",
        ])
    else:
        r["region"] = g.choice([
            "Mars",
            "Atlantis",
            "N/A",
        ])


INJECT = {
    "MISSING_FIELD": anomaly_missing,
    "INVALID_DATE": anomaly_date,
    "INVALID_NUMERIC": anomaly_numeric,
    "UNKNOWN_CATEGORY": anomaly_category,
}


# ============================================================
# SHA256
# ============================================================

def sha256(path):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        for chunk in iter(
            lambda: f.read(1 << 20),
            b""
        ):
            h.update(chunk)

    return h.hexdigest()


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Generateur AgroVision pour le test de charge"
    )

    parser.add_argument(
        "--records",
        type=int,
        default=50000,
        help="enregistrements DISTINCTS (defaut 50000)",
    )

    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.065,
        help="part d'anomalies (defaut 0.065 = 6.5 %)",
    )

    parser.add_argument(
        "--dup-rate",
        type=float,
        default=0.02,
        help="part de doublons sources (defaut 0.02)",
    )

    parser.add_argument(
        "--fields",
        type=int,
        default=60,
        help="nombre de parcelles (defaut 60)",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="graine (defaut 42)",
    )

    parser.add_argument(
        "--parts",
        type=int,
        default=1,
        help="nombre de fichiers de sortie (defaut 1)",
    )

    parser.add_argument(
        "--out-dir",
        default=os.path.expanduser(
            "~/tp3_elk/data"
        ),
    )

    parser.add_argument(
        "--prefix",
        default="agro_load",
    )

    args = parser.parse_args()

    N = args.records

    if N < 25000:
        print(
            f"ATTENTION : {N} < 25 000, "
            "le TP exige au moins 25 000 enregistrements."
        )

    if args.anomaly_rate < 0.05:
        print(
            "ATTENTION : le TP4 exigeait "
            "au moins 5 % d'anomalies controlees."
        )

    start_time = time.time()

    os.makedirs(
        args.out_dir,
        exist_ok=True
    )

    # Supprimer les anciens fichiers
    old_files = (
        glob.glob(
            os.path.join(
                args.out_dir,
                args.prefix + ".csv"
            )
        )
        +
        glob.glob(
            os.path.join(
                args.out_dir,
                args.prefix + "_part_*.csv"
            )
        )
    )

    for old in old_files:
        os.remove(old)

    # Generateurs aleatoires
    rng = random.Random(args.seed)
    rng_a = random.Random(args.seed + 1000)
    rng_d = random.Random(args.seed + 2000)

    fields = make_fields(
        rng,
        args.fields
    )

    idw = max(
        6,
        len(str(N))
    )

    # ========================================================
    # Selection des anomalies
    # ========================================================

    per_cat = round(
        N * args.anomaly_rate / 4
    )

    picked = rng_a.sample(
        range(N),
        4 * per_cat
    )

    anomaly_of = {}

    for k, category in enumerate(CATEGORIES):

        for i in picked[
            k * per_cat:
            (k + 1) * per_cat
        ]:
            anomaly_of[i] = category

    # ========================================================
    # Selection des doublons
    # ========================================================

    n_dup = round(
        N * args.dup_rate
    )

    cand = rng_d.sample(
        range(N),
        n_dup + len(anomaly_of)
    )

    sources = [
        i
        for i in cand
        if i not in anomaly_of
    ][:n_dup]

    src_set = set(sources)

    insert_after = {}

    for original in sources:

        position = rng_d.randint(
            min(original + 1, N - 1),
            N - 1
        )

        insert_after.setdefault(
            position,
            []
        ).append(original)

    total = N + n_dup

    # ========================================================
    # Fichiers de sortie
    # ========================================================

    if args.parts == 1:

        names = [
            args.prefix + ".csv"
        ]

    else:

        names = [
            f"{args.prefix}_part_{k + 1:03d}.csv"
            for k in range(args.parts)
        ]

    paths = [
        os.path.join(
            args.out_dir,
            name
        )
        for name in names
    ]

    handles = [
        open(
            path,
            "w",
            newline="",
            encoding="utf-8"
        )
        for path in paths
    ]

    writers = [
        csv.writer(
            handle,
            lineterminator="\n"
        )
        for handle in handles
    ]

    for writer in writers:
        writer.writerow(COLUMNS)

    state = {
        "line": 0
    }

    def emit(values):

        index = min(
            args.parts - 1,
            state["line"] * args.parts // total
        )

        writers[index].writerow(values)

        state["line"] += 1

    # ========================================================
    # Generation
    # ========================================================

    stash = {}
    truth = []

    for i in range(N):

        row = make_row(
            rng,
            fields,
            idw,
            i + 1
        )

        # Injection d'une anomalie
        if i in anomaly_of:

            category = anomaly_of[i]

            INJECT[category](
                row,
                rng_a
            )

            truth.append(
                (
                    row["observation_id"],
                    category
                )
            )

        values = [
            row[column]
            for column in COLUMNS
        ]

        emit(values)

        # Sauvegarder les lignes qui seront dupliquees
        if i in src_set:
            stash[i] = values

        # Inserer les doublons
        for original in insert_after.pop(
            i,
            ()
        ):

            emit(
                stash.pop(original)
            )

    # Fermer les fichiers
    for handle in handles:
        handle.close()

    # ========================================================
    # Verite terrain des anomalies
    # ========================================================

    truth_path = os.path.join(
        args.out_dir,
        "anomalies_truth.csv"
    )

    with open(
        truth_path,
        "w",
        newline=""
    ) as f:

        writer = csv.writer(
            f,
            lineterminator="\n"
        )

        writer.writerow([
            "observation_id",
            "anomaly_category"
        ])

        writer.writerows(truth)

    # ========================================================
    # Verite terrain des doublons
    # ========================================================

    duplicates_path = os.path.join(
        args.out_dir,
        "duplicates_truth.csv"
    )

    with open(
        duplicates_path,
        "w",
        newline=""
    ) as f:

        writer = csv.writer(
            f,
            lineterminator="\n"
        )

        writer.writerow([
            "observation_id"
        ])

        writer.writerows([
            [
                f"OBS-{o + 1:0{idw}d}"
            ]
            for o in sorted(sources)
        ])

    # ========================================================
    # Rapport
    # ========================================================

    elapsed = time.time() - start_time

    report = {
        "seed": args.seed,
        "distinct_records": N,
        "duplicate_lines": n_dup,
        "data_lines": total,
        "anomalies": len(truth),
        "anomalies_per_category": per_cat,
        "anomaly_rate_pct": round(
            100 * len(truth) / N,
            2
        ),
        "fields": args.fields,
        "parts": args.parts,
        "id_width": idw,
        "generation_seconds": round(
            elapsed,
            2
        ),
        "files": {
            name: {
                "bytes": os.path.getsize(path),
                "sha256": sha256(path),
            }
            for name, path
            in zip(names, paths)
        },
    }

    report_path = os.path.join(
        args.out_dir,
        "generation_report.json"
    )

    with open(
        report_path,
        "w"
    ) as f:

        json.dump(
            report,
            f,
            indent=2
        )

    # ========================================================
    # Affichage final
    # ========================================================

    print(
        f"Lignes de donnees ecrites : {total}"
    )

    print(
        f"Enregistrements distincts : {N}"
    )

    print(
        f"Anomalies injectees       : "
        f"{len(truth)} "
        f"({report['anomaly_rate_pct']} %), "
        f"{per_cat} par categorie"
    )

    print(
        f"Doublons sources injectes : {n_dup}"
    )

    print(
        f"Fichiers                  : "
        f"{', '.join(names)}"
    )

    print(
        f"Duree de generation       : "
        f"{elapsed:.1f} s"
    )

    print(
        f"Rapport                   : "
        f"{report_path}"
    )


if __name__ == "__main__":
    main()
