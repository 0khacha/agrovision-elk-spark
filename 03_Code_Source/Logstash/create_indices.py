#!/usr/bin/env python3
"""Cree agro-load-v1 (valides) et agro-load-rejected-v1 (rejets). --reset supprime et recree."""
import argparse, copy, json, os, urllib.request, urllib.error
ES = "http://node-1:9200"

def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ES + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")

ap = argparse.ArgumentParser()
ap.add_argument("--valid", default="agro-load-v1")
ap.add_argument("--rejected", default="agro-load-rejected-v1")
ap.add_argument("--replicas", type=int, default=1)
ap.add_argument("--refresh", default="1s")
ap.add_argument("--shards", type=int, default=3)
ap.add_argument("--reset", action="store_true")
ap.add_argument("--tp4-mapping", default=os.path.expanduser("~/tp4/mapping/agro_observations_mapping.json"))
a = ap.parse_args()

src = json.load(open(a.tp4_mapping))
mp = copy.deepcopy(src.get("mappings", src))
mp["dynamic"] = "strict"
mp["properties"]["run_id"] = {"type": "keyword"}
mp["properties"]["source_file"] = {"type": "keyword"}
valid = {"settings": {"number_of_shards": a.shards, "number_of_replicas": a.replicas, "refresh_interval": a.refresh},
         "mappings": mp}
rej = {"settings": {"number_of_shards": 1, "number_of_replicas": 1},
       "mappings": {"dynamic": False, "properties": {
           "@timestamp": {"type": "date"}, "observation_id": {"type": "keyword"},
           "anomaly_category": {"type": "keyword"}, "anomaly_detail": {"type": "text"},
           "error_class": {"type": "keyword"}, "raw_line": {"type": "text"},
           "run_id": {"type": "keyword"}, "source_file": {"type": "keyword"}}}}
for name, body in ((a.valid, valid), (a.rejected, rej)):
    if a.reset:
        print("DELETE", name, http("DELETE", "/" + name)[0])
    st, r = http("PUT", "/" + name, body)
    print("PUT", name, st, "" if st == 200 else json.dumps(r)[:300])
