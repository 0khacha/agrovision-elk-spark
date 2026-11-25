#!/usr/bin/env python3
"""Lance Logstash sur le jeu de charge, detecte la fin, mesure, ecrit results/runs.csv."""
import argparse, csv, datetime, glob, json, os, signal, subprocess, sys, time, urllib.request

BASE = os.path.expanduser("~/tp3_elk")
ES, API = "http://node-1:9200", "http://localhost:9600"
LS_BIN = "/usr/share/logstash/bin/logstash"

def http(method, url, body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)

def es_count(index, run_id=None):
    try:
        http("POST", f"{ES}/{index}/_refresh")
        if run_id:
            return http("POST", f"{ES}/{index}/_count", {"query": {"term": {"run_id": run_id}}})["count"]
        return http("GET", f"{ES}/{index}/_count")["count"]
    except Exception:
        return None

def reject_cats(index, run_id):
    try:
        r = http("POST", f"{ES}/{index}/_search", {"size": 0, "query": {"term": {"run_id": run_id}},
                 "aggs": {"c": {"terms": {"field": "anomaly_category", "size": 20}}}})
        return {b["key"]: b["doc_count"] for b in r["aggregations"]["c"]["buckets"]}
    except Exception:
        return {}

def dir_bytes(p):
    total = 0
    for root, _, files in os.walk(p):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total

def infra(st):
    resp, ok, nonretry = {}, 0, 0
    for o in st.get("plugins", {}).get("outputs", []):
        if o.get("name") != "elasticsearch":
            continue
        for code, n in o.get("bulk_requests", {}).get("responses", {}).items():
            resp[code] = resp.get(code, 0) + n
        d = o.get("documents", {})
        ok += d.get("successes", 0)
        nonretry += d.get("non_retryable_failures", 0)
    return resp, ok, nonretry

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=datetime.datetime.now().strftime("R%Y%m%d_%H%M%S"))
    ap.add_argument("--label", default="")
    ap.add_argument("--data-glob", default=f"{BASE}/data/agro_load*.csv")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--batch", type=int, default=125)
    ap.add_argument("--queue", default="persisted", choices=["memory", "persisted"])
    ap.add_argument("--heap", default="512m")
    ap.add_argument("--conf", default=f"{BASE}/logstash/agro_pipeline_v2.conf")
    ap.add_argument("--valid-index", default="agro-load-v1")
    ap.add_argument("--reject-index", default="agro-load-rejected-v1")
    ap.add_argument("--sincedb", default=None, help="fichier sincedb a reutiliser (reprise)")
    ap.add_argument("--kill-after", type=int, default=0, help="kill -9 quand out >= N (test d'interruption)")
    ap.add_argument("--timeout", type=int, default=1800)
    a = ap.parse_args()

    if subprocess.run(["pgrep", "-f", "logstash/logstash-core"], capture_output=True).stdout.strip():
        sys.exit("Un Logstash tourne deja : arretez-le avant (pkill -f logstash).")
    files = sorted(glob.glob(a.data_glob))
    if not files:
        sys.exit("Aucun fichier pour " + a.data_glob)
    rep = json.load(open(f"{BASE}/data/generation_report.json"))
    expected = rep["data_lines"]
    state, results = f"{BASE}/state", f"{BASE}/results"
    os.makedirs(results, exist_ok=True)
    ls_data = f"{state}/ls_data"
    sincedb = a.sincedb or f"{state}/sincedb_{a.run_id}"
    if not a.sincedb and os.path.exists(sincedb):
        os.remove(sincedb)
    completed = f"{state}/completed_{a.run_id}.log"
    if os.path.exists(completed):
        os.remove(completed)

    env = dict(os.environ, RUN_ID=a.run_id, DATA_GLOB=a.data_glob, SINCEDB=sincedb, COMPLETED_LOG=completed,
               VALID_INDEX=a.valid_index, REJECT_INDEX=a.reject_index, LS_JAVA_OPTS=f"-Xms{a.heap} -Xmx{a.heap}")
    sdir = f"{state}/settings_{a.run_id}"
    os.makedirs(sdir, exist_ok=True)
    for fn in ("jvm.options", "log4j2.properties"):
        if os.path.exists("/etc/logstash/" + fn):
            subprocess.run(["cp", "/etc/logstash/" + fn, sdir])
    with open(sdir + "/logstash.yml", "w") as yf:
        yf.write("pipeline.workers: %d\n" % a.workers)
        yf.write("pipeline.batch.size: %d\n" % a.batch)
        yf.write("queue.type: %s\n" % a.queue)
        yf.write("queue.max_bytes: 1gb\n")
        yf.write("dead_letter_queue.enable: true\n")
        yf.write("api.http.port: 9600\n")
        yf.write("path.data: %s\n" % ls_data)
        yf.write("path.logs: %s/logs\n" % state)
    cmd = [LS_BIN, "-f", a.conf, "--path.settings", sdir]
    with open(f"{results}/{a.run_id}_conditions.txt", "w") as f:
        subprocess.run("date; echo CPU=$(nproc); free -m; echo kibana=$(systemctl is-active kibana); "
                       "curl -s 'http://node-1:9200/_cat/nodes?v&h=name,heap.percent,ram.percent,cpu'",
                       shell=True, stdout=f, stderr=subprocess.STDOUT)
        f.write("CMD: " + " ".join(cmd) + "\n")

    print(f"== {a.run_id} [{a.label}] workers={a.workers} batch={a.batch} queue={a.queue} heap={a.heap} "
          f"files={len(files)} lignes attendues={expected}")
    logf = open(f"{results}/{a.run_id}_logstash.log", "w")
    t_launch = time.time()
    proc = subprocess.Popen(cmd, env=env, stdout=logf, stderr=subprocess.STDOUT)
    t_first = t_changed = None
    last_out, st, ev = -1, {}, {}
    while True:
        time.sleep(0.5)
        now = time.time()
        if proc.poll() is not None:
            sys.exit(f"Logstash s'est arrete (code {proc.returncode}) : voir {results}/{a.run_id}_logstash.log")
        if now - t_launch > a.timeout:
            proc.kill()
            sys.exit("Timeout")
        try:
            st = http("GET", f"{API}/_node/stats/pipelines/main", timeout=5)["pipelines"]["main"]
        except Exception:
            continue
        ev = st.get("events")
        if not ev:
            continue
        if t_first is None and ev["in"] > 0:
            t_first = now
        if ev["out"] != last_out:
            last_out, t_changed = ev["out"], now
        if a.kill_after and ev["out"] >= a.kill_after:
            proc.kill(); proc.wait()
            print(f"INTERRUPTION kill -9 : out={ev['out']}, file d'attente restante={st.get('queue',{}).get('events_count')}")
            print("Valides dans ES :", es_count(a.valid_index), "| rejets :", es_count(a.reject_index))
            print("Sincedb conserve :", sincedb)
            return
        done_files = sum(1 for _ in open(completed)) if os.path.exists(completed) else 0
        if (done_files >= len(files) and st.get("queue", {}).get("events_count", 0) == 0
                and ev["out"] > 0 and now - t_changed >= 2.0):
            break
    duration = t_changed - t_first
    startup = t_first - t_launch
    json.dump(st, open(f"{results}/{a.run_id}_stats.json", "w"), indent=2)
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=90)
    except Exception:
        proc.kill()

    resp, ok, nonretry = infra(st)
    n_valid, n_rej = es_count(a.valid_index, a.run_id), es_count(a.reject_index, a.run_id)
    cats = reject_cats(a.reject_index, a.run_id)
    dlq = dir_bytes(f"{ls_data}/dead_letter_queue")
    tp = ev["out"] / duration if duration > 0 else 0
    print(f"Evenements sortis (out) : {ev['out']} (attendu {expected})  | in={ev['in']} filtered={ev['filtered']}")
    print(f"Duree : {duration:.1f} s (demarrage JVM exclu : {startup:.1f} s)  Debit : {tp:.0f} evt/s")
    print(f"ES valides (run) : {n_valid} [total index {es_count(a.valid_index)}] | rejets (run) : {n_rej} {cats}")
    print(f"Reponses HTTP du bulk : {resp} | echecs non reessayables : {nonretry} | DLQ : {dlq} octets")
    new = not os.path.exists(f"{results}/runs.csv")
    with open(f"{results}/runs.csv", "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["run_id", "label", "lines_expected", "workers", "batch", "queue", "heap", "duration_s",
                        "throughput_evt_s", "events_in", "events_out", "es_valid_run", "es_rejected_run",
                        "duplicates_rejected", "dlq_bytes", "http_responses", "non_retryable", "startup_s", "date"])
        w.writerow([a.run_id, a.label, expected, a.workers, a.batch, a.queue, a.heap, round(duration, 2),
                    round(tp, 1), ev["in"], ev["out"], n_valid, n_rej, cats.get("DUPLICATE", 0), dlq,
                    json.dumps(resp), nonretry, round(startup, 1), datetime.datetime.now().isoformat(timespec="seconds")])

if __name__ == "__main__":
    main()
