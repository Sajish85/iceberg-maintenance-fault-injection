"""Gated race re-run driver (RACE-RERUN-BRIEF.md). Does not modify frozen files; reuses seed/py-reader helpers from run_matrix
and the frozen checker. usage (via scripts/run.sh, ICE_VER and RUN_TAG=-race2 set): race2.py control|quota

Gate: rewrite JVM (maint.py rewrite_small_groups, same options as the study: partial-progress, max-commits 4, 40000-byte groups)
and expire JVM (maint.py expire) are both started. The expire JVM (chaos/maint_race2.py) prints PHASE call-start and then waits on a gate file; the gate is opened when the rewrite's FIRST group commit is seen in its log, so its metadata commit lands while rewrite commits are pending.
overlap_achieved = expire commit strictly between the rewrite's first commit and its last commit (observer timeline timestamps)."""
import subprocess, sys, os, json, time, random, signal, csv, shutil
sys.path.insert(0, "/Users/sajishkrishnan/b2-iceberg-chaos/chaos")
import run_matrix as rm
from run_matrix import *      # s, ck, seed, run_py_reader, LOADED, PIN_BATCH, ...

OUT = f"{B2}/results/race2"; os.makedirs(OUT, exist_ok=True)
COMMIT_MARK = "BaseRewriteFiles)"; FAIL_MARK = "Failure during rewrite commit process"; EXP_MARK = "RemoveSnapshots: Committed snapshot changes"

def table_name(idx):
    return f"t_race2_{VER.replace('.', '_')}_g3_c{idx:02d}"

def assert_prefix_empty(tbl):
    d = f"{WH}/{tbl}"
    left = []
    if os.path.isdir(d):
        for root, dirs, files in os.walk(d): left += [os.path.join(root, f) for f in files]
    exists = s.sql(f"show tables in ice.db like '{tbl}'").count()
    assert not left and exists == 0, f"store prefix/catalog not empty for {tbl}: {len(left)} objects, catalog rows {exists}"

def run_cell(idx):
    tbl = table_name(idx); name = f"race2_{VER}_c{idx:02d}"; rundir = f"{RUNS}/{name}"
    shutil.rmtree(rundir, ignore_errors=True); os.makedirs(rundir)
    t_cell = now()
    while free_bytes() < 6e9: time.sleep(20)
    assert_prefix_empty(tbl)
    rec = {"cell": name, "build": VER, "attempt": idx, "table": tbl, "ice_version": LOADED, "prefix_empty_before_seed": True,
           "free_gb_at_start": round(free_bytes() / 1e9, 1)}
    ing, pin = seed(tbl, rundir, "rewrite")
    F0 = table_files(s, tbl)
    rd = PinnedReader(s, tbl, pin, (PIN_BATCH + 1) * BATCH_ROWS, f"{rundir}/reader.jsonl")
    ing.start(); rd.start(); time.sleep(1.5)
    pr = spawn_maint("rewrite_small_groups", tbl, f"{rundir}/maint.json", f"{rundir}/maint.log"); obr = Observer("rewrite", f"{rundir}/maint.log")
    gate = f"{rundir}/GATE"
    pe = subprocess.Popen([f"{B2}/scripts/run.sh", f"{B2}/chaos/maint_race2.py", tbl, f"{rundir}/maint_expire.json", gate], stdout=open(f"{rundir}/maint_expire.log", "w"),
                          stderr=subprocess.STDOUT, start_new_session=True); obe = Observer("expire", f"{rundir}/maint_expire.log")
    T0 = now()
    t = dict(rewrite_start=None, commits=[], fails=[], expire_commit=None, rewrite_return=None, expire_hold=None, expire_release=None, expire_return=None)
    held = released = False
    while (pr.poll() is None or pe.poll() is None) and now() - T0 < 300:
        obr.poll(); obe.poll(); tn = now()
        if t["rewrite_start"] is None and "PHASE call-start" in obr.text: t["rewrite_start"] = tn
        n = obr.text.count(COMMIT_MARK)
        while len(t["commits"]) < n: t["commits"].append(tn)
        nf = obr.text.count(FAIL_MARK)
        while len(t["fails"]) < nf: t["fails"].append(tn)
        if t["rewrite_return"] is None and "PHASE call-end" in obr.text: t["rewrite_return"] = tn
        if t["expire_commit"] is None and EXP_MARK in obe.text: t["expire_commit"] = tn
        if t["expire_return"] is None and "PHASE call-end" in obe.text: t["expire_return"] = tn
        if not held and "PHASE call-start" in obe.text: held = True; t["expire_hold"] = tn      # expire JVM warm and waiting on the gate file
        if held and not released and (t["commits"] or pr.poll() is not None):
            open(gate, "w").close(); released = True; t["expire_release"] = tn
            rec["released_because"] = "first-rewrite-commit" if t["commits"] else "rewrite-ended-without-commit"
        time.sleep(0.003)
    for q in (pr, pe):
        if q.poll() is None: killpg(q, signal.SIGKILL); rec["hung_killed"] = True
    pr.wait(); pe.wait(); obr.poll(); obe.poll()
    c = t["commits"]; ec = t["expire_commit"]
    ev = sorted(c + t["fails"])      # commit events: successful commits + commit attempts that failed (logged by the commit service)
    rec["overlap_achieved"] = bool(c and ec is not None and c[0] < ec < ev[-1])     # expire commit strictly between first and last rewrite commit event
    rec["overlap_success_only"] = bool(len(c) >= 2 and ec is not None and c[0] < ec < c[-1])   # stricter: a rewrite commit SUCCEEDED after the expire commit
    rec["n_failed_commit_events"] = len(t["fails"])
    rec["expire_before_first_commit"] = bool(ec is not None and c and ec <= c[0]); rec["expire_after_last_commit"] = bool(ec is not None and c and ec >= c[-1])
    rec["n_rewrite_commits_seen"] = len(c)
    rec["epoch"] = {k: (v if not isinstance(v, list) else v) for k, v in t.items()}
    rec["rel_s"] = {k: ([round(x - T0, 3) for x in v] if isinstance(v, list) else (round(v - T0, 3) if v else None)) for k, v in t.items()}
    rec["rewrite_exit"] = pr.returncode; rec["expire_exit"] = pe.returncode
    res = json.load(open(f"{rundir}/maint.json")) if os.path.exists(f"{rundir}/maint.json") else {}
    res2 = json.load(open(f"{rundir}/maint_expire.json")) if os.path.exists(f"{rundir}/maint_expire.json") else {}
    time.sleep(3.0)
    ing.stop_ev.set(); rd.stop_ev.set(); ing.join(); rd.join()
    rec["A"] = ck.check_A(s, tbl, ing.ledger); rec["B"] = ck.check_B(s, tbl, ing.ledger, ing.tags)
    rec["B_pinned"] = ck.check_B_pinned(s, tbl, f"{rundir}/reader.jsonl", pin)
    rec["C"] = ck.check_C_rewrite(s, tbl, ing.ledger, res, obr.text, F0, race=True); rec["C_expire_raw"] = res2
    rec["py"] = run_py_reader(tbl, rundir); rec["reader_compare"] = ck.compare_readers(rec["A"], rec["B"], rec["py"])
    rec["reader_disagree"] = bool(rec["reader_compare"].get("disagree"))
    rec["secs"] = round(now() - t_cell, 1)
    viol = rec["A"]["A_violation"] or rec["B"]["B_violation"] or rec["B_pinned"]["B_pinned_violation"] or rec["C"]["C_violation"]
    rec["any_violation"] = bool(viol)
    json.dump(rec, open(f"{rundir}/result.json", "w"), indent=1, default=str)
    with open(f"{OUT}/interleavings.jsonl", "a") as f: f.write(json.dumps(rec, default=str) + "\n")
    row = dict(cell=name, build=VER, attempt=idx, table=tbl, overlap_achieved=rec["overlap_achieved"], overlap_success_only=rec["overlap_success_only"], n_failed_commit_events=len(t["fails"]), t_rewrite_failed_commit_events=json.dumps(t["fails"]),
               t_rewrite_start=t["rewrite_start"], n_rewrite_commits=len(c), t_rewrite_commits=json.dumps(c),
               t_rewrite_first_commit=c[0] if c else None, t_rewrite_last_commit=c[-1] if c else None,
               t_expire_release=t["expire_release"], t_expire_commit=ec, t_rewrite_return=t["rewrite_return"], t_expire_return=t["expire_return"],
               rel_expire_commit_minus_first=(round(ec - c[0], 3) if ec and c else None), rel_last_minus_expire_commit=(round(c[-1] - ec, 3) if ec and c else None),
               A=rec["A"]["A_violation"], B=rec["B"]["B_violation"], Bpin=rec["B_pinned"]["B_pinned_violation"], C_applicable=rec["C"]["C_applicable"],
               C=rec["C"]["C_violation"], C1=rec["C"].get("C1_success_zero_committed"), C1b=rec["C"].get("C1b_success_with_swallowed_group_failures"),
               C2=rec["C"].get("C2_report_ne_truth"), groups_ready=rec["C"].get("groups_ready"), groups_committed_log=rec["C"].get("groups_committed_log"),
               C_kinds=json.dumps(rec["C"].get("swallowed_exception_kinds", {})), py_A=rec["py"].get("A", {}).get("A_violation"),
               py_B=rec["py"].get("B", {}).get("B_violation"), reader_disagree=rec["reader_disagree"], secs=rec["secs"])
    newf = not os.path.exists(f"{OUT}/per_interleaving.csv")
    with open(f"{OUT}/per_interleaving.csv", "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row))
        if newf: w.writeheader()
        w.writerow(row)
    if not viol and not rec["reader_disagree"]:
        try: s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
        except Exception as e: print("drop error", e)
    print(name, "overlap" if rec["overlap_achieved"] else "no-overlap", "commits", len(c), "A" if rec["A"]["A_violation"] else "-", "B" if rec["B"]["B_violation"] else "-",
          "P" if rec["B_pinned"]["B_pinned_violation"] else "-", "C" if rec["C"]["C_violation"] else "-", "D" if rec["reader_disagree"] else "-",
          "rel", rec["rel_s"], f"{rec['secs']}s", flush=True)
    return rec

if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "control":
        for i in range(int(sys.argv[2]) if len(sys.argv) > 2 else 3): run_cell(i)
    else:
        ov = 0; att = 0; quota = 8; maxatt = 20
        while ov < quota and att < maxatt:
            r = run_cell(att); att += 1; ov += bool(r["overlap_achieved"])
            print(f"== {VER}: attempts={att} overlap={ov}", flush=True)
