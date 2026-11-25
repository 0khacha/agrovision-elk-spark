#!/usr/bin/env python3
"""AgroVision TP6 - generateur de flux progressif (JSON Lines)."""
import argparse, csv, json, os, random, time, datetime as dt, collections

HOME = os.path.expanduser("~/tp6_elk")
FMT = "%Y-%m-%dT%H:%M:%S"
TYPES = ["SENSOR_READING", "IRRIGATION_EVENT", "ALERT"]
ALERTS = ["DROUGHT_RISK", "LOW_NDVI", "SENSOR_FAULT", "HEAVY_RAIN"]


def load_parcels(path):
    with open(path, newline="", encoding="utf-8") as f:
        return [(r["vertex_id"], r["sensor_id"]) for r in csv.DictReader(f) if r["vertex_type"] == "PARCELLE"]


def make_event(rng, seq, t, parcels):
    fid, sid = rng.choice(parcels)
    typ = rng.choices(TYPES, [70, 20, 10])[0]
    e = {"event_id": f"EVT-{seq:08d}", "event_type": typ, "field_id": fid,
         "sensor_id": sid, "event_time": t.strftime(FMT)}
    if typ == "SENSOR_READING":
        e["soil_moisture_pct"] = round(min(60, max(5, rng.gauss(30, 10))), 1)
        e["ndvi"] = round(rng.uniform(0.2, 0.85), 2)
        e["temperature_c"] = round(min(45, max(-5, rng.gauss(24, 6))), 1)
        e["rainfall_mm"] = round(rng.expovariate(1 / 5), 1) if rng.random() < 0.1 else 0.0
    elif typ == "IRRIGATION_EVENT":
        e["water_m3"] = round(rng.uniform(20, 800), 1)
        e["duration_min"] = rng.randint(10, 240)
    else:
        e["alert_type"] = rng.choice(ALERTS)
        e["severity"] = rng.choices([1, 2, 3], [50, 35, 15])[0]
    return e


def corrupt(rng, e):
    kind = rng.choice(["no_id", "unknown_field", "bad_time", "bad_type", "range", "broken_json"])
    e = dict(e)
    if kind == "no_id":
        e["event_id"] = None
    elif kind == "unknown_field":
        e["field_id"] = "FLD-999"
    elif kind == "bad_time":
        e["event_time"] = "2026-13-45 xx:yy"
    elif kind == "bad_type":
        e["event_type"] = "METEO_UNKNOWN"
    elif kind == "range":
        if e["event_type"] == "SENSOR_READING":
            e["soil_moisture_pct"] = 150.0
        elif e["event_type"] == "IRRIGATION_EVENT":
            e["water_m3"] = -5.0
        else:
            e["severity"] = 9
    s = json.dumps(e)
    if kind == "broken_json":
        s = s[:rng.randint(20, 60)]
    return s, kind


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vertices", default=f"{HOME}/data/graph/vertices.csv")
    ap.add_argument("--out-dir", default=f"{HOME}/stream/incoming")
    ap.add_argument("--tmp-dir", default=f"{HOME}/stream/tmp")
    ap.add_argument("--manifest", default=f"{HOME}/results/stream_manifest.json")
    ap.add_argument("--total", type=int, default=12000, help="evenements distincts a produire")
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--interval", type=float, default=2.0, help="secondes entre deux fichiers")
    ap.add_argument("--step-sec", type=int, default=60, help="avance de l'horloge simulee par lot")
    ap.add_argument("--invalid-rate", type=float, default=0.04)
    ap.add_argument("--dup-rate", type=float, default=0.03)
    ap.add_argument("--late-rate", type=float, default=0.08)
    ap.add_argument("--max-late-min", type=float, default=8.0)
    ap.add_argument("--start-time", default="2026-10-27T08:00:00")
    ap.add_argument("--start-seq", type=int, default=1)
    ap.add_argument("--file-prefix", default="batch")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    parcels = load_parcels(a.vertices)
    for d in (a.out_dir, a.tmp_dir, os.path.dirname(a.manifest)):
        os.makedirs(d, exist_ok=True)
    clock = dt.datetime.strptime(a.start_time, FMT)
    seq, produced, nfile = a.start_seq, 0, 0
    recent, by_type, inv = [], collections.Counter(), collections.Counter()
    st = dict(lines=0, valid=0, dups=0, late=0)
    tmin = tmax = None

    while produced < a.total:
        n = min(a.batch_size, a.total - produced)
        lines = []
        for _ in range(n):
            t = clock + dt.timedelta(seconds=rng.uniform(0, a.step_sec))
            if rng.random() < a.late_rate:
                t -= dt.timedelta(minutes=rng.uniform(1, a.max_late_min))
                st["late"] += 1
            e = make_event(rng, seq, t, parcels)
            seq += 1
            if rng.random() < a.invalid_rate:
                s, kind = corrupt(rng, e)
                inv[kind] += 1
                lines.append(s)
            else:
                s = json.dumps(e)
                lines.append(s)
                recent.append(s)
                st["valid"] += 1
                by_type[e["event_type"]] += 1
                tmin = t if tmin is None or t < tmin else tmin
                tmax = t if tmax is None or t > tmax else tmax
        recent = recent[-1000:]
        for _ in range(int(n * a.dup_rate)):
            if recent:
                lines.append(rng.choice(recent))
                st["dups"] += 1
        rng.shuffle(lines)
        nfile += 1
        name = f"{a.file_prefix}_{nfile:05d}.jsonl"
        tmp = os.path.join(a.tmp_dir, name)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, os.path.join(a.out_dir, name))
        st["lines"] += len(lines)
        produced += n
        clock += dt.timedelta(seconds=a.step_sec)
        print(f"[{name}] lignes={len(lines)} total_evenements={produced}/{a.total}", flush=True)
        time.sleep(a.interval)

    man = {"params": vars(a), "new_events": produced, "total_lines": st["lines"],
           "valid_distinct": st["valid"], "invalid_total": sum(inv.values()),
           "invalid_by_kind": dict(inv), "duplicate_lines": st["dups"],
           "late_events": st["late"], "valid_by_type": dict(by_type), "files": nfile,
           "valid_event_time_min": tmin.strftime(FMT) if tmin else None,
           "valid_event_time_max": tmax.strftime(FMT) if tmax else None}
    with open(a.manifest, "w") as f:
        json.dump(man, f, indent=2)
    print(json.dumps(man, indent=2))


if __name__ == "__main__":
    main()
