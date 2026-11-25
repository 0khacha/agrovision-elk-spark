#!/usr/bin/env python3
"""AgroVision TP6 - donnees du graphe metier (sommets et aretes).

Sommets : PARCELLE (relues dans l'historique), SOURCE_EAU, EXPLOITATION.
Aretes  : VOISINE_DE (k plus proches voisines d'une meme region),
          IRRIGUEE_PAR (parcelle -> source d'eau), EXPLOITEE_PAR (parcelle -> exploitation).
Positions (x_km, y_km) et exploitations : synthetiques. Reproductible (graine fixe).
"""
import argparse, collections, csv, hashlib, json, math, os, random, re, sys

REG = ["Souss-Massa", "Marrakech-Safi", "Fes-Meknes", "Gharb", "Oriental", "Doukkala"]
CROPS = {"ble", "orge", "tomate", "agrumes", "olivier", "mais"}
VCOLS = ["vertex_id", "vertex_type", "region", "subtype", "crop_type", "area_ha", "sensor_id",
         "capacity_m3_day", "x_km", "y_km"]
ECOLS = ["edge_id", "src_id", "dst_id", "relation", "distance_km", "share_pct", "since_year"]


def load_parcels(path):
    cnt = collections.defaultdict(collections.Counter)
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fid = r["field_id"].strip()
            crop = r["crop_type"].strip().lower()
            if not re.fullmatch(r"FLD-\d{3,}", fid) or r["region"] not in REG or crop not in CROPS:
                continue
            try:
                area = float(r["area_ha"])
            except ValueError:
                continue
            if not 0 < area <= 1000:
                continue
            cnt[fid][(r["region"], crop, r["area_ha"].strip(), r["sensor_id"].strip())] += 1
    return {fid: c.most_common(1)[0][0] for fid, c in cnt.items()}


def dist(p, q):
    return math.hypot(p[0] - q[0], p[1] - q[1])


def build(parcels, a):
    rng = random.Random(a.seed)
    ids = sorted(parcels)
    by_reg = {r: [f for f in ids if parcels[f][0] == r] for r in REG}
    pos = {f: (round(rng.uniform(0, 40), 2), round(rng.uniform(0, 40), 2)) for f in ids}
    V = []
    for f in ids:
        reg, crop, area, sensor = parcels[f]
        V.append(dict(vertex_id=f, vertex_type="PARCELLE", region=reg, subtype="", crop_type=crop,
                      area_ha=area, sensor_id=sensor, capacity_m3_day="", x_km=pos[f][0], y_km=pos[f][1]))
    src, n = {}, 0
    for reg in REG:
        for _ in range(max(3, round(len(by_reg[reg]) / a.parcels_per_source))):
            n += 1
            kind = rng.choices(["FORAGE", "CANAL", "BARRAGE"], [50, 35, 15])[0]
            lo, hi = {"FORAGE": (500, 2000), "CANAL": (3000, 12000), "BARRAGE": (10000, 40000)}[kind]
            sid = f"WTR-{n:03d}"
            xy = (round(rng.uniform(0, 40), 2), round(rng.uniform(0, 40), 2))
            src[sid] = (reg, xy)
            V.append(dict(vertex_id=sid, vertex_type="SOURCE_EAU", region=reg, subtype=kind, crop_type="",
                          area_ha="", sensor_id="", capacity_m3_day=rng.randint(lo, hi), x_km=xy[0], y_km=xy[1]))
    owner, n = {}, 0
    for reg in REG:
        lst = by_reg[reg][:]
        rng.shuffle(lst)
        i = 0
        while i < len(lst):
            chunk = lst[i:i + rng.randint(2, 9)]
            i += len(chunk)
            n += 1
            eid = f"EXP-{n:03d}"
            kind = rng.choices(["FAMILIALE", "COOPERATIVE", "AGRO_INDUSTRIELLE"], [60, 25, 15])[0]
            V.append(dict(vertex_id=eid, vertex_type="EXPLOITATION", region=reg, subtype=kind, crop_type="",
                          area_ha="", sensor_id="", capacity_m3_day="", x_km="", y_km=""))
            for f in chunk:
                owner[f] = eid
    E, pairs = [], {}
    for reg in REG:
        for f in by_reg[reg]:
            near = sorted((dist(pos[f], pos[g]), g) for g in by_reg[reg] if g != f)[:a.k]
            for d, g in near:
                pairs[(min(f, g), max(f, g))] = round(d, 2)
    for (f, g), d in sorted(pairs.items()):
        E.append(dict(src_id=f, dst_id=g, relation="VOISINE_DE", distance_km=d, share_pct="", since_year=""))
    for f in ids:
        reg = parcels[f][0]
        cand = sorted((dist(pos[f], src[s][1]), s) for s in src if src[s][0] == reg)[:3]
        i1 = rng.choices([0, 1, 2], [60, 30, 10])[0]
        chosen = [cand[i1]]
        if rng.random() < a.secondary_prob:
            chosen.append(rng.choice([c for j, c in enumerate(cand) if j != i1]))
        p = 100 if len(chosen) == 1 else rng.randint(60, 80)
        shares = [p] if len(chosen) == 1 else [p, 100 - p]
        for (d, s), sh in zip(chosen, shares):
            E.append(dict(src_id=f, dst_id=s, relation="IRRIGUEE_PAR", distance_km=round(d, 2),
                          share_pct=sh, since_year=""))
    for f in ids:
        E.append(dict(src_id=f, dst_id=owner[f], relation="EXPLOITEE_PAR", distance_km="", share_pct="",
                      since_year=rng.randint(1995, 2024)))
    for i, e in enumerate(E, 1):
        e["edge_id"] = f"REL-{i:06d}"
    return V, E


def components(nodes, edges):
    parent = {n: n for n in nodes}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for s, d in edges:
        rs, rd = find(s), find(d)
        if rs != rd:
            parent[rs] = rd
    return sorted(collections.Counter(find(n) for n in nodes).values(), reverse=True)


def check(V, E, a):
    vid = [v["vertex_id"] for v in V]
    vset = set(vid)
    typ = {v["vertex_id"]: v["vertex_type"] for v in V}
    reg = {v["vertex_id"]: v["region"] for v in V}
    rels = {"VOISINE_DE": ("PARCELLE", "PARCELLE"), "IRRIGUEE_PAR": ("PARCELLE", "SOURCE_EAU"),
            "EXPLOITEE_PAR": ("PARCELLE", "EXPLOITATION")}
    irr, farm = collections.defaultdict(list), collections.Counter()
    for e in E:
        if e["relation"] == "IRRIGUEE_PAR":
            irr[e["src_id"]].append(e["share_pct"])
        if e["relation"] == "EXPLOITEE_PAR":
            farm[e["src_id"]] += 1
    P = [v for v in vid if typ[v] == "PARCELLE"]
    missing = sum(1 for e in E if e["src_id"] not in vset or e["dst_id"] not in vset)
    ok_e = [e for e in E if e["src_id"] in vset and e["dst_id"] in vset]
    dup = sum(1 for c in collections.Counter((e["src_id"], e["dst_id"], e["relation"]) for e in E).values() if c > 1)
    badtype = sum(1 for e in ok_e if e["relation"] not in rels or (typ[e["src_id"]], typ[e["dst_id"]]) != rels[e["relation"]])
    badreg = sum(1 for e in ok_e if e["relation"] != "VOISINE_DE" and reg[e["src_id"]] != reg[e["dst_id"]])
    return [
        (f"Sommets >= {a.min_vertices}", len(V), len(V) >= a.min_vertices),
        (f"Aretes >= {a.min_edges}", len(E), len(E) >= a.min_edges),
        ("Identifiants de sommets uniques (doublons)", len(vid) - len(vset), len(vid) == len(vset)),
        ("Aretes avec une extremite inconnue", missing, missing == 0),
        ("Boucles (source = destination)", sum(1 for e in E if e["src_id"] == e["dst_id"]),
         all(e["src_id"] != e["dst_id"] for e in E)),
        ("Aretes en double", dup, dup == 0),
        ("Types d'extremites non conformes a la relation", badtype, badtype == 0),
        (f"Parcelles = {a.expected_parcels} (celles de l'historique)", len(P), len(P) == a.expected_parcels),
        ("Parcelles sans source d'eau", sum(1 for p in P if not irr[p]), all(irr[p] for p in P)),
        ("Parcelles dont les parts d'irrigation != 100 %", sum(1 for p in P if sum(irr[p]) != 100),
         all(sum(irr[p]) == 100 for p in P)),
        ("Parcelles sans exactement 1 exploitation", sum(1 for p in P if farm[p] != 1), all(farm[p] == 1 for p in P)),
        ("Source ou exploitation hors de la region de la parcelle", badreg, badreg == 0),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", default=os.path.expanduser("~/tp6_elk/data/history/agro_hist.csv"))
    ap.add_argument("--out-dir", default=os.path.expanduser("~/tp6_elk/data/graph"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--k", type=int, default=4, help="voisines par parcelle")
    ap.add_argument("--parcels-per-source", type=int, default=12)
    ap.add_argument("--secondary-prob", type=float, default=0.35)
    ap.add_argument("--expected-parcels", type=int, default=600)
    ap.add_argument("--min-vertices", type=int, default=500)
    ap.add_argument("--min-edges", type=int, default=2000)
    a = ap.parse_args()

    parcels = load_parcels(a.history)
    V, E = build(parcels, a)
    os.makedirs(a.out_dir, exist_ok=True)
    vp, ep = os.path.join(a.out_dir, "vertices.csv"), os.path.join(a.out_dir, "edges.csv")
    for path, cols, rows in ((vp, VCOLS, V), (ep, ECOLS, E)):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)

    print("=== CONTROLES DU GRAPHE ===")
    ok_all = True
    for label, got, ok in check(V, E, a):
        ok_all &= ok
        print(f"  {'PASS' if ok else 'FAIL'}  {label:<58} obtenu {got}")
    vt = collections.Counter(v["vertex_type"] for v in V)
    et = collections.Counter(e["relation"] for e in E)
    print("\nSommets par type :", dict(vt), "| total", len(V))
    print("Aretes par relation :", dict(et), "| total", len(E))
    P = [v["vertex_id"] for v in V if v["vertex_type"] == "PARCELLE"]
    S = [v["vertex_id"] for v in V if v["vertex_type"] == "SOURCE_EAU"]
    comp_all = components([v["vertex_id"] for v in V], [(e["src_id"], e["dst_id"]) for e in E])
    comp_irr = components(P + S, [(e["src_id"], e["dst_id"]) for e in E if e["relation"] == "IRRIGUEE_PAR"])
    load = collections.Counter(e["dst_id"] for e in E if e["relation"] == "IRRIGUEE_PAR")
    print("\n--- Apercu Python (a recouper plus tard avec GraphX, ne remplace pas l'analyse) ---")
    print("Composantes, graphe complet :", len(comp_all), "| 5 plus grandes :", comp_all[:5])
    print("Composantes, irrigation seule (parcelles + sources) :", len(comp_irr), "| 5 plus grandes :", comp_irr[:5])
    print("Sources les plus sollicitees (parcelles reliees) :", load.most_common(3))
    report = {"seed": a.seed, "vertices": len(V), "edges": len(E), "vertices_by_type": dict(vt),
              "edges_by_relation": dict(et), "components_full": len(comp_all),
              "components_irrigation": len(comp_irr),
              "sha256": {os.path.basename(p): hashlib.sha256(open(p, "rb").read()).hexdigest() for p in (vp, ep)}}
    with open(os.path.join(a.out_dir, "graph_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nFichiers : {vp}\n           {ep}")
    print("RESULTAT :", "TOUS LES CONTROLES REUSSIS" if ok_all else "ECHEC : voir les lignes FAIL")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
