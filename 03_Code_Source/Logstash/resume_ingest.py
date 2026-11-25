#!/usr/bin/env python3
"""Relance Logstash apres un kill -9 sur la meme file persistante et le meme sincedb."""
import argparse, json, os, signal, subprocess, sys, time, urllib.request
BASE = os.path.expanduser("~/tp3_elk"); ES = "http://node-1:9200"
def get(url, body=None, method="GET"):
    d = json.dumps(body).encode() if body else None
    r = urllib.request.Request(url, data=d, method=method, headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(r, timeout=30))
def count(i):
    get(f"{ES}/{i}/_refresh", method="POST"); return get(f"{ES}/{i}/_count")["count"]
ap = argparse.ArgumentParser(); ap.add_argument("--run-id", required=True); a = ap.parse_args()
st = f"{BASE}/state"
env = dict(os.environ, RUN_ID=a.run_id, SINCEDB=f"{st}/sincedb_{a.run_id}",
           COMPLETED_LOG=f"{st}/completed_{a.run_id}.log", LS_JAVA_OPTS="-Xms512m -Xmx512m")
cmd = ["/usr/share/logstash/bin/logstash", "-f", f"{BASE}/logstash/agro_pipeline_v2.conf",
       "--path.settings", f"{st}/settings_{a.run_id}"]
print("Avant reprise : valides", count("agro-load-v1"), "| rejets", count("agro-load-rejected-v1"))
log = open(f"{BASE}/results/{a.run_id}_resume_logstash.log", "w")
t0 = time.time(); p = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
last = None; t_last = time.time()
while True:
    time.sleep(1)
    if p.poll() is not None: sys.exit("Logstash arrete : voir le log de reprise")
    try: s = get("http://localhost:9600/_node/stats/pipelines/main")["pipelines"]["main"]
    except Exception: continue
    if not s.get("events"): continue
    cur = (s["events"]["out"], s.get("queue", {}).get("events_count"))
    if cur != last: last, t_last = cur, time.time()
    if time.time() - t_last > 15 and cur[1] == 0: break
    if time.time() - t0 > 900: p.kill(); sys.exit("Timeout")
print("Apres reprise : out (cette session)", cur[0], "| file restante", cur[1], f"| {time.time()-t0:.0f} s")
p.send_signal(signal.SIGTERM); p.wait(timeout=90)
print("Apres reprise : valides", count("agro-load-v1"), "| rejets", count("agro-load-rejected-v1"))
