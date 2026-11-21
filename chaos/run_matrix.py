"""Orchestrator (full study). usage (ICE_VER selects Iceberg version; run via scripts/run.sh):
   run_matrix.py control17202 | control17304 | matrix [--limit N] [--only op[,op]]
State under work/runs/v<VER>/ ; results appended to results/v<VER>/*.jsonl"""
import sys, os, json, time, random, signal, csv, shutil, subprocess, collections
sys.path.insert(0, "/Users/sajishkrishnan/b2-iceberg-chaos/chaos")
from lib import *
import checker as ck
from pyspark.sql import SparkSession

SEED_BATCHES = int(os.environ.get("SEED_BATCHES", "120")); PIN_BATCH = 9
PYBIN = f"{B2}/work/venv/bin/python3.14"
s = SparkSession.builder.appName("orchestrator").getOrCreate()
s.sparkContext.setLogLevel("ERROR"); s.conf.set("spark.sql.session.timeZone", "UTC")
s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
os.makedirs(RES, exist_ok=True); os.makedirs(RUNS, exist_ok=True)
LOADED = iceberg_version(s)
assert LOADED == (VER if VER != "main" else "2d89aa4"), f"requested Iceberg {VER} but JVM loaded {LOADED}"   # main = commit 2d89aa4 (built locally)
NARROW = {"post-write-pre-commit", "during-commit", "during-deletes", "deleting"}
DWELL = collections.defaultdict(lambda: 0.6)          # (op, phase) -> seconds, updated from uninterrupted arm-a timelines

# ------------------------------------------------------------------ infra control (own processes only)
def pg_pid():
    return int(open(f"{B2}/work/pgdata/postmaster.pid").readline())
def pg_kill_and_restart(outage=3.0):
    pid = pg_pid(); subprocess.run(["pkill", "-9", "-P", str(pid)]); os.kill(pid, signal.SIGKILL); time.sleep(outage)
    for f in ("postmaster.pid",):
        try: os.remove(f"{B2}/work/pgdata/{f}")
        except FileNotFoundError: pass
    subprocess.run(f'LC_ALL=en_US.UTF-8 /opt/homebrew/opt/postgresql@16/bin/pg_ctl -D {B2}/work/pgdata -o "-p 5433 -k /tmp -c listen_addresses=127.0.0.1" -l {B2}/work/pg.log -w start',
                   shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def minio_pid():
    out = subprocess.run(["pgrep", "-f", r"minio server .*--address 127\.0\.0\.1:9100 "], capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None
def minio_kill_and_restart(outage=3.0):
    pid = minio_pid(); os.kill(pid, signal.SIGKILL); time.sleep(outage)
    env = dict(os.environ, MINIO_ROOT_USER="chaos", MINIO_ROOT_PASSWORD="chaos-pilot-pw")
    subprocess.Popen([f"{B2}/bin/minio", "server", f"{B2}/work/minio-data", "--address", "127.0.0.1:9100", "--console-address", "127.0.0.1:9101"],
                     stdout=open(f"{B2}/work/minio.log", "a"), stderr=subprocess.STDOUT, start_new_session=True, env=env)
    for _ in range(60):
        r = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "http://127.0.0.1:9100/minio/health/live"], capture_output=True, text=True)
        if r.stdout == "200": return
        time.sleep(0.5)

# ------------------------------------------------------------------ seeding
def seed(tbl, rundir, op, tag_every=None):
    s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
    s.sql(f"CREATE TABLE ice.db.{tbl} (batch_id int,rid long,payload string) USING iceberg")
    chain = (tag_every is not None)
    ing = Ingest(s, tbl, f"{rundir}/ledger.jsonl", f"{rundir}/tags.jsonl", 0,
                 tag_every=(tag_every or 4), tag_from=(0 if chain else SEED_BATCHES))
    for _ in range(SEED_BATCHES): ing.one()
    led = ck.ledger_sets(ing.ledger)[0]
    pin = led[PIN_BATCH]["snapshot_id"]
    if op in ("expire", "chain"):   # make replaced files so expire has data files to delete
        s.sql(f"CALL ice.system.rewrite_data_files(table=>'db.{tbl}', options=>map('min-input-files','2'))").collect()
    return ing, pin

DW_N = collections.defaultdict(int)
def dwell_from(timeline, op):
    for (t1, p1), (t2, p2) in zip(timeline, timeline[1:]):
        if p1 in ("jvm-startup", "finished"): continue
        k = (op, p1); n = DW_N[k]; DWELL[k] = (t2 - t1) if n == 0 else (DWELL[k] * n + (t2 - t1)) / (n + 1); DW_N[k] += 1

def run_py_reader(tbl, rundir):
    out = f"{rundir}/py.json"
    t0 = now()
    try:
        subprocess.run([PYBIN, f"{B2}/chaos/pyreader.py", tbl, f"{rundir}/ledger.jsonl", f"{rundir}/tags.jsonl", out], timeout=300,
                       stdout=subprocess.DEVNULL, stderr=open(f"{rundir}/py.err", "w"))
        py = json.load(open(out))
    except Exception as e:
        py = {"reader_error": f"{type(e).__name__}: {str(e)[:200]}"}
    py["secs"] = round(now() - t0, 1); return py

# ------------------------------------------------------------------ one cell
def run_cell(spec, idx):
    op, arm, target = spec["op"], spec["arm"], spec.get("target")
    rnd = random.Random(spec["seed"])
    name = f"{op}_{arm}_{(target or spec.get('kind') or 'na')}_{idx:03d}".replace("-", "_")
    tbl = "t_" + name; rundir = f"{RUNS}/{name}"
    shutil.rmtree(rundir, ignore_errors=True); os.makedirs(rundir)
    t_start = now()
    while free_bytes() < 6e9: time.sleep(20)               # disk guard: never start a cell below 6 GB free
    rec = {"cell": name, "ice_version": LOADED, "op": op, "arm": arm, "target_phase": target, "kind": spec.get("kind"), "seed": spec["seed"], "free_gb_at_start": round(free_bytes() / 1e9, 1)}
    ing, pin = seed(tbl, rundir, "expire" if op in ("expire",) else ("chain" if op == "chain" else "rewrite"),
                    tag_every=10 if op == "chain" else None)
    F0 = table_files(s, tbl); D0 = all_data_files(s, tbl)
    rec["files_at_seed"] = len(F0); rec["orphans_pre_maint"] = ck.orphan_data_files(s, tbl)
    mop = {"chain": "expire", "race": "rewrite_small_groups"}.get(op, op)   # maintenance op under test
    pre_orph = set(); cutoff_ms = None; ob = None; res = {}; logtext = ""; res2 = {}
    if op == "orphan":               # create real orphans: kill a rewrite mid-write, ingest stopped
        p0 = spawn_maint("rewrite_small_groups", tbl, f"{rundir}/setup.json", f"{rundir}/setup.log"); o0 = Observer("rewrite", f"{rundir}/setup.log")
        t0 = now()
        while p0.poll() is None and o0.poll() != "during-writes" and now() - t0 < 120: time.sleep(0.005)
        time.sleep(rnd.uniform(0.6, 1.6)); killpg(p0); p0.wait()
        time.sleep(1.2); cutoff_ms = int(now() * 1000); time.sleep(0.3)
        pre_orph = table_objects(tbl)
        rec["orphans_created"] = len(pre_orph - referenced_objects(s, tbl))
        F0 = table_files(s, tbl)
    rd = PinnedReader(s, tbl, pin, (PIN_BATCH + 1) * BATCH_ROWS, f"{rundir}/reader.jsonl")
    ing.start(); rd.start(); time.sleep(1.5)
    mlog, mjson = f"{rundir}/maint.log", f"{rundir}/maint.json"
    if arm == "c":
        time.sleep(7.0)
    elif op == "race":
        res, logtext, res2 = run_race(spec, rnd, tbl, rundir, rec)
    elif op == "infra":
        res, logtext = run_infra(spec, rnd, tbl, rundir, rec)
    else:
        p = spawn_maint(mop, tbl, mjson, mlog, cutoff_ms); ob = Observer("expire" if mop == "expire" else mop, mlog)
        killed = False; t0 = now(); seen = []
        obop = "expire" if mop == "expire" else ("orphan" if mop == "orphan" else "rewrite")
        ob = Observer(obop, mlog)
        while p.poll() is None and now() - t0 < 240:
            ph = ob.poll()
            if not seen or seen[-1][1] != ph: seen.append((round(now() - t0, 3), ph))
            if arm == "b" and ph == target:
                delay = 0.0 if target in NARROW else rnd.uniform(0, 0.9 * DWELL[(obop, target)])
                time.sleep(delay); ph2 = ob.poll()
                killpg(p, signal.SIGKILL); killed = True
                rec.update(detected_phase=ph, observed_phase_at_signal=ph2, kill_delay_s=round(delay, 4), dwell_est_s=round(DWELL[(obop, target)], 3),
                           kill_t=round(now() - t0, 3), reached_target=(ph2 == target)); break
            time.sleep(0.002)
        if p.poll() is None and not killed: killpg(p, signal.SIGKILL); rec["hung_killed"] = True
        p.wait(); time.sleep(0.2); ob.poll()
        logtext = ob.text
        rec.update(killed=killed, phase_timeline=seen, maint_exit=p.returncode)
        if arm == "a": dwell_from(seen, obop)
        if arm == "b" and not killed: rec["kill_miss"] = True
        if os.path.exists(mjson): res = json.load(open(mjson))
        time.sleep(3.0)
    ing.stop_ev.set(); rd.stop_ev.set(); ing.join(); rd.join()
    # ----- checker (unattended; checker.py is frozen + hashed per run, see STEPLOG)
    rec["A"] = ck.check_A(s, tbl, ing.ledger)
    rec["B"] = ck.check_B(s, tbl, ing.ledger, ing.tags)
    rec["B_pinned"] = ck.check_B_pinned(s, tbl, f"{rundir}/reader.jsonl", pin, excuse=rec.get("infra_window"))
    if arm == "c": rec["C"] = {"C_applicable": False, "C_violation": False}
    elif op == "race":
        rec["C"] = ck.check_C_rewrite(s, tbl, ing.ledger, res, logtext, F0, race=True)
        rec["C_expire_raw"] = res2
    elif op == "infra": rec["C"] = {"C_applicable": False, "C_violation": False, "C_note": "infra-kill cell: success claims not scored"}
    elif op == "orphan": rec["C"] = ck.check_C_orphan(tbl, res, pre_orph)
    elif mop in ("rewrite", "rewrite_small_groups"): rec["C"] = ck.check_C_rewrite(s, tbl, ing.ledger, res, logtext, F0)
    else: rec["C"] = ck.check_C_expire(s, tbl, res, D0)
    rec["orphan_data_objects_observed"] = ck.orphan_data_files(s, tbl)
    rec["py"] = run_py_reader(tbl, rundir)
    rec["reader_compare"] = ck.compare_readers(rec["A"], rec["B"], rec["py"])
    rec["secs"] = round(now() - t_start, 1)
    viol = rec["A"]["A_violation"] or rec["B"]["B_violation"] or rec["B_pinned"]["B_pinned_violation"] or rec["C"]["C_violation"]
    rec["any_violation"] = bool(viol)
    rec["reader_disagree"] = bool(rec["reader_compare"].get("disagree"))
    json.dump(rec, open(f"{rundir}/result.json", "w"), indent=1, default=str)
    with open(f"{RES}/interleavings.jsonl", "a") as f: f.write(json.dumps(rec, default=str) + "\n")
    if not viol and not rec["reader_disagree"]:
        try: s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
        except Exception as e: rec["drop_error"] = str(e)[:100]
    print(name, "A" if rec["A"]["A_violation"] else "-", "B" if rec["B"]["B_violation"] else "-",
          "P" if rec["B_pinned"]["B_pinned_violation"] else "-", "C" if rec["C"]["C_violation"] else "-",
          "D" if rec["reader_disagree"] else "-", rec.get("observed_phase_at_signal"), "MISS" if rec.get("kill_miss") else "", f"{rec['secs']}s", flush=True)
    return rec

def run_race(spec, rnd, tbl, rundir, rec):
    """Natural #17202 shape inside the matrix: rewrite (multi-group) running while a second JVM runs expire(retain_last=1)."""
    pr = spawn_maint("rewrite_small_groups", tbl, f"{rundir}/maint.json", f"{rundir}/maint.log"); obr = Observer("rewrite", f"{rundir}/maint.log")
    t0 = now()
    while pr.poll() is None and obr.poll() in ("jvm-startup", "before-writes") and now() - t0 < 60: time.sleep(0.005)
    lag = spec.get("expire_lag", rnd.uniform(0.0, 0.8)); time.sleep(lag)
    pe = spawn_maint("expire", tbl, f"{rundir}/maint_expire.json", f"{rundir}/maint_expire.log"); obe = Observer("expire", f"{rundir}/maint_expire.log")
    rec.update(expire_lag_s=round(lag, 3), expire_launched_at=round(now() - t0, 3))
    seen_r, seen_e = [], []; killed = False
    kill_after = rnd.uniform(0.5, 3.0) if spec["arm"] == "b" else None; tl = now()
    while (pr.poll() is None or pe.poll() is None) and now() - t0 < 300:
        a, b = obr.poll(), obe.poll()
        if not seen_r or seen_r[-1][1] != a: seen_r.append((round(now() - t0, 3), a))
        if not seen_e or seen_e[-1][1] != b: seen_e.append((round(now() - t0, 3), b))
        if kill_after and not killed and now() - tl > kill_after and pr.poll() is None:
            rec.update(race_killed="rewrite", race_kill_phase_rewrite=a, race_kill_phase_expire=b); killpg(pr, signal.SIGKILL); killed = True
        time.sleep(0.005)
    for q in (pr, pe):
        if q.poll() is None: killpg(q, signal.SIGKILL)
    pr.wait(); pe.wait(); obr.poll(); obe.poll()
    rec.update(phase_timeline_rewrite=seen_r, phase_timeline_expire=seen_e, killed=killed, rewrite_exit=pr.returncode, expire_exit=pe.returncode)
    # did the expire commit land while the rewrite was still in progress? (only then is it a true race)
    rec["race_overlap"] = any(p in ("during-writes", "post-write-pre-commit", "between-groups", "during-commit") for t, p in seen_r if t >= rec["expire_launched_at"])
    res = json.load(open(f"{rundir}/maint.json")) if os.path.exists(f"{rundir}/maint.json") else {}
    res2 = json.load(open(f"{rundir}/maint_expire.json")) if os.path.exists(f"{rundir}/maint_expire.json") else {}
    time.sleep(3.0)
    return res, obr.text, res2

def run_infra(spec, rnd, tbl, rundir, rec):
    """Kill the catalog (own Postgres) or object store (own MinIO) with SIGKILL, restart, let everything settle."""
    kind, tgt = spec["kind"], spec.get("target")
    mjson, mlog = f"{rundir}/maint.json", f"{rundir}/maint.log"
    p = None; ob = None; logtext = ""
    if tgt != "ingest-steady":
        op = "expire" if tgt == "expire-pre-commit" else "rewrite_small_groups"
        p = spawn_maint(op, tbl, mjson, mlog); ob = Observer("expire" if op == "expire" else "rewrite", mlog)
        want = {"rewrite-commit-window": "post-write-pre-commit", "rewrite-writing": "during-writes", "expire-pre-commit": "pre-commit"}[tgt]
        t0 = now()
        while p.poll() is None and ob.poll() != want and now() - t0 < 120: time.sleep(0.002)
        rec["infra_phase_at_kill"] = ob.poll()
    else: time.sleep(rnd.uniform(0.5, 2.5))
    t_kill = now()
    (pg_kill_and_restart if kind == "pg" else minio_kill_and_restart)(outage=rnd.uniform(2.0, 5.0))
    rec.update(infra_killed=kind, infra_target=tgt, infra_outage_s=round(now() - t_kill, 1), infra_window=[t_kill, now() + 8.0])
    if p is not None:
        try: p.wait(timeout=90)
        except subprocess.TimeoutExpired: killpg(p, signal.SIGKILL); rec["maint_hung_killed"] = True
        ob.poll(); logtext = ob.text; rec["maint_exit"] = p.returncode
    time.sleep(4.0)
    res = json.load(open(mjson)) if os.path.exists(mjson) else {}
    return res, logtext

# ------------------------------------------------------------------ controls
def control17202():
    name = "control_17202"; tbl = "t_" + name; rundir = f"{RUNS}/{name}"
    shutil.rmtree(rundir, ignore_errors=True); os.makedirs(rundir)
    ing, pin = seed(tbl, rundir, "rewrite")
    start_snap = s.sql(f"select snapshot_id from ice.db.{tbl}.snapshots order by committed_at desc limit 1").collect()[0][0]
    F0 = table_files(s, tbl)
    rd = PinnedReader(s, tbl, pin, (PIN_BATCH + 1) * BATCH_ROWS, f"{rundir}/reader.jsonl")
    ing.start(); rd.start(); time.sleep(1.0)
    mlog, mjson = f"{rundir}/maint.log", f"{rundir}/maint.json"
    p = spawn_maint("rewrite_small_groups", tbl, mjson, mlog); ob = Observer("rewrite", mlog)
    rec = {"cell": name, "ice_version": LOADED, "starting_snapshot_expected": start_snap}
    t0 = now()
    while p.poll() is None and ob.poll() != "during-writes" and now() - t0 < 120: time.sleep(0.002)
    if p.poll() is not None: rec["control_error"] = "rewrite finished before hold point"
    else:
        killpg(p, signal.SIGSTOP); rec["held_at"] = round(now() - t0, 3)
        n0 = len(ck.ledger_sets(ing.ledger)[0]); time.sleep(2.5)
        rec["acks_while_held"] = len(ck.ledger_sets(ing.ledger)[0]) - n0
        cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - 1))
        er = s.sql(f"CALL ice.system.expire_snapshots(table=>'db.{tbl}', older_than=>TIMESTAMP '{cutoff}', retain_last=>1)").collect()
        rec["concurrent_expire_result"] = [r.asDict() for r in er]
        rec["start_snapshot_still_present"] = s.sql(f"select count(*) from ice.db.{tbl}.snapshots where snapshot_id={start_snap}").collect()[0][0] == 1
        killpg(p, signal.SIGCONT)
        p.wait(timeout=180); time.sleep(0.3); ob.poll()
    time.sleep(2.0)
    ing.stop_ev.set(); rd.stop_ev.set(); ing.join(); rd.join()
    res = json.load(open(mjson)) if os.path.exists(mjson) else {}
    rec["maint_result"] = res
    rec["A"] = ck.check_A(s, tbl, ing.ledger); rec["B"] = ck.check_B(s, tbl, ing.ledger, ing.tags)
    rec["B_pinned"] = ck.check_B_pinned(s, tbl, f"{rundir}/reader.jsonl", pin)
    rec["C"] = ck.check_C_rewrite(s, tbl, ing.ledger, res, ob.text, F0)
    rec["py"] = run_py_reader(tbl, rundir); rec["reader_compare"] = ck.compare_readers(rec["A"], rec["B"], rec["py"])
    rec["detected"] = bool(rec["C"]["C_violation"])
    json.dump(rec, open(f"{rundir}/result.json", "w"), indent=1, default=str)
    with open(f"{RES}/control.jsonl", "a") as f: f.write(json.dumps(rec, default=str) + "\n")
    print("CONTROL17202", json.dumps({k: rec[k] for k in rec if k in ("held_at", "acks_while_held", "start_snapshot_still_present", "maint_result", "detected")}, default=str)[:500])
    print("C:", json.dumps(rec["C"], default=str)); print("A:", rec["A"]["A_violation"], "B:", rec["B"]["B_violation"], "Bpin:", rec["B_pinned"]["B_pinned_violation"])
    return rec["detected"]

def control17304(path):
    """#17304: remove + re-add the same data file in one commit (RewriteFiles), append, then expire. path = 'core' (the issue's
    ExpireSnapshots API, which may use IncrementalFileCleanup) or 'procedure' (Spark expire_snapshots)."""
    name = f"control_17304_{path}"; tbl = "t_" + name; rundir = f"{RUNS}/{name}"
    shutil.rmtree(rundir, ignore_errors=True); os.makedirs(rundir)
    s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
    s.sql(f"CREATE TABLE ice.db.{tbl} (batch_id int,rid long,payload string) USING iceberg")
    ing = Ingest(s, tbl, f"{rundir}/ledger.jsonl", f"{rundir}/tags.jsonl", 0, tag_from=10**9)
    for _ in range(12): ing.one()
    jv = s._jvm
    jt = jv.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(s._jsparkSession, f"ice.db.{tbl}")
    it = jt.newScan().planFiles().iterator(); F = it.next().file()
    rec = {"cell": name, "ice_version": LOADED, "path": path, "re_added_file": str(F.path())}
    hs = jv.java.util.HashSet(); hs.add(F); hs2 = jv.java.util.HashSet(); hs2.add(F)
    try:
        jt.newRewrite().rewriteFiles(hs, hs2).commit(); rec["readd_commit"] = "ok"
    except Exception as e:
        rec["readd_commit"] = f"rejected: {type(e).__name__}: {str(e)[:300]}"
    time.sleep(1.2); ing.one(); time.sleep(1.2)                     # S2, then age the snapshots
    jt.refresh()
    try:
        if path == "core":
            jt.expireSnapshots().expireOlderThan(int(now() * 1000) - 500).commit()
        else:
            cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - 0.5))
            s.sql(f"CALL ice.system.expire_snapshots(table=>'db.{tbl}', older_than=>TIMESTAMP '{cutoff}')").collect()
        rec["expire"] = "ok"
    except Exception as e: rec["expire"] = f"error: {type(e).__name__}: {str(e)[:300]}"
    rec["file_still_exists"] = path_exists(str(F.path()))
    rec["A"] = ck.check_A(s, tbl, ing.ledger); rec["B"] = ck.check_B(s, tbl, ing.ledger, ing.tags)
    rec["py"] = run_py_reader(tbl, rundir); rec["reader_compare"] = ck.compare_readers(rec["A"], rec["B"], rec["py"])
    rec["detected"] = bool(rec["A"]["A_violation"] or rec["B"]["B_violation"])
    json.dump(rec, open(f"{rundir}/result.json", "w"), indent=1, default=str)
    with open(f"{RES}/control.jsonl", "a") as f: f.write(json.dumps(rec, default=str) + "\n")
    print("CONTROL17304", path, json.dumps({k: rec[k] for k in ("readd_commit", "expire", "file_still_exists", "detected")}),
          "A:", rec["A"]["A_violation"], rec["A"].get("read_error", ""), "B:", rec["B"]["B_violation"], "py:", rec["py"].get("A", {}).get("A_violation"), flush=True)
    return rec["detected"]

# ------------------------------------------------------------------ plan
def plan(scale=1.0):
    n = lambda k: max(1, round(k * scale))
    cells = []
    A_ = []   # uninterrupted arms first so dwell times are known
    for op, k in (("rewrite", 6), ("expire", 6), ("orphan", 4), ("chain", 4)): A_ += [dict(op=op, arm="a") for _ in range(n(k))]
    A_ += [dict(op="race", arm="a", kind="race") for _ in range(n(8))]
    A_ += [dict(op="ingest-only", arm="c") for _ in range(n(6))]
    B_ = []
    for op, phases, k in (("rewrite", PHASES["rewrite"], 8), ("expire", PHASES["expire"], 8), ("orphan", PHASES["orphan"], 4)):
        for ph in phases: B_ += [dict(op=op, arm="b", target=ph) for _ in range(n(k))]
    for ph in PHASES["expire"]: B_ += [dict(op="chain", arm="b", target=ph) for _ in range(n(3))]
    B_ += [dict(op="race", arm="b", kind="race-kill-rewrite") for _ in range(n(4))]
    for kind in ("pg", "minio"):
        for tg in ("rewrite-commit-window", "rewrite-writing", "ingest-steady", "expire-pre-commit"):
            B_ += [dict(op="infra", arm="b", kind=kind, target=tg)]
    random.Random(20261120).shuffle(B_)
    cells = A_ + B_
    for i, c in enumerate(cells): c["seed"] = 2000 + i
    return cells

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "control17202": sys.exit(0 if control17202() else 3)
    elif cmd == "control17304":
        d1 = control17304("core"); d2 = control17304("procedure")
    else:
        cells = plan(float(os.environ.get("SCALE", "1.0")))
        limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else len(cells)
        only = sys.argv[sys.argv.index("--only") + 1].split(",") if "--only" in sys.argv else None
        done = set()
        if os.path.exists(f"{RES}/interleavings.jsonl"):
            done = {json.loads(l)["cell"] for l in open(f"{RES}/interleavings.jsonl")}
        n = 0
        for i, spec in enumerate(cells):
            if only and spec["op"] not in only: continue
            nm = f"{spec['op']}_{spec['arm']}_{(spec.get('target') or spec.get('kind') or 'na')}_{i:03d}".replace("-", "_")
            if nm in done: continue
            if n >= limit: break
            try: run_cell(spec, i)
            except Exception as e:
                import traceback; traceback.print_exc()
                with open(f"{RES}/cell_errors.jsonl", "a") as f: f.write(json.dumps({"cell": nm, "spec": spec, "error": f"{type(e).__name__}: {str(e)[:500]}"}) + "\n")
            n += 1
