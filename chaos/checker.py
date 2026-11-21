"""CHECKER: three classes scored separately.
 A INTEGRITY            every acked record readable in current snapshot (exact id-set vs ledger)
 B RETAINED-SNAPSHOT    every tag returns exactly its expected set; no dangling file refs in any live snapshot;
                        pinned-reader case reported SEPARATELY (B_pinned)
 C PROGRESS TRUTHFULNESS maintenance-reported success vs metadata-verified committed work
Ambiguous ingest outcomes are a separate class and never count as loss."""
import json, os, re
from lib import *

def ledger_sets(ledger):
    acked, amb = {}, []
    for r in read_ledger(ledger):
        if r["status"] == "acked": acked[r["batch"]] = r
        else: amb.append(r)
    return acked, amb

def ids_of(df):
    return [r[0] for r in df.select("rid").collect()]

def check_A(s, tbl, ledger):
    acked, amb = ledger_sets(ledger)
    try:
        ids = ids_of(s.table(f"ice.db.{tbl}"))
    except Exception as e:
        return {"A_violation": True, "read_error": f"{type(e).__name__}: {str(e)[:200]}", "acked_batches": len(acked), "acked_records": -1,
                "missing": -1, "unexplained_extra": -1, "duplicates": -1, "ambiguous_batches": len(amb), "ambiguous_present_in_table": -1}
    got = set(ids)
    exp = set(); amb_ids = set()
    for r in acked.values(): exp |= set(range(r["lo"], r["hi"]))
    for r in amb: amb_ids |= set(range(r["lo"], r["hi"]))
    missing = exp - got
    extra_unexplained = got - exp - amb_ids
    dup = len(ids) - len(got)
    return {"A_violation": bool(missing or extra_unexplained or dup), "acked_batches": len(acked), "acked_records": len(exp),
            "missing": len(missing), "unexplained_extra": len(extra_unexplained), "duplicates": dup,
            "ambiguous_batches": len(amb), "ambiguous_present_in_table": len(amb_ids & got)}

def check_B(s, tbl, ledger, tags_file):
    acked, amb = ledger_sets(ledger)
    amb_ids = set()
    for r in amb: amb_ids |= set(range(r["lo"], r["hi"]))
    refs = {r["name"]: r["snapshot_id"] for r in s.sql(f"select name,snapshot_id from ice.db.{tbl}.refs where type='TAG'").collect()}
    out = {"tags_expected": 0, "tags_missing": [], "tags_wrong": [], "tags_error": []}
    for line in (open(tags_file) if os.path.exists(tags_file) else []):
        tg = json.loads(line)
        if "create_error" in tg: continue
        out["tags_expected"] += 1
        if tg["tag"] not in refs: out["tags_missing"].append(tg["tag"]); continue
        try:
            got = set(ids_of(s.read.option("tag", tg["tag"]).table(f"ice.db.{tbl}")))
        except Exception as e:
            out["tags_error"].append(f'{tg["tag"]}: {str(e)[:150]}'); continue
        exp = set(range(tg["expected_hi"])) - amb_ids
        if (got - amb_ids) != exp: out["tags_wrong"].append(f'{tg["tag"]}: missing={len(exp-got)} extra={len(got-amb_ids-exp)}')
    # dangling refs: every object referenced by any live snapshot (data/delete files, manifests, manifest lists, metadata json) must exist
    try:
        refd = referenced_objects(s, tbl)
        dangling = [p for p in refd if not path_exists(p)]
    except Exception as e:
        dangling = [f"checker-error: {type(e).__name__}: {str(e)[:100]}"]
    out["dangling_refs"] = len(dangling); out["dangling_examples"] = dangling[:3]
    out["dangling_data_refs"] = len([p for p in dangling if "/data/" in p])
    out["B_violation"] = bool(out["tags_missing"] or out["tags_wrong"] or out["tags_error"] or dangling)
    return out

def check_B_pinned(s, tbl, reader_log, pinned_snap, excuse=None):
    recs = [json.loads(l) for l in open(reader_log)] if os.path.exists(reader_log) else []
    present = s.sql(f"select count(*) from ice.db.{tbl}.snapshots where snapshot_id={pinned_snap}").collect()[0][0] == 1
    ok = [r for r in recs if r["ok"]]; bad = [r for r in recs if not r["ok"]]
    # infra-kill cells: read errors inside the declared catalog/store outage window are expected, reported but not scored
    outage = [r for r in bad if excuse and excuse[0] <= r["t"] <= excuse[1]]
    bad = [r for r in bad if r not in outage]
    inexact = [r for r in ok if not r["exact"]]
    cats = {}
    for r in bad:
        e = r["err"]
        c = ("snapshot-not-found" if re.search(r"Cannot find snapshot|not found|Cannot find", e) and "FileNotFound" not in e and "NotFound" not in e.split(":")[0]
             else "file-not-found" if re.search(r"FileNotFound|NoSuchKey|does not exist|404", e) else "other")
        cats[c] = cats.get(c, 0) + 1
    # by design: once the (unretained) snapshot is expired, reads fail. Violation = wrong data, or errors while snapshot still live.
    viol = bool(inexact) or (present and bool(bad))
    return {"pinned_reads": len(recs), "pinned_ok_exact": len(ok) - len(inexact), "pinned_ok_inexact": len(inexact),
            "pinned_errors": len(bad), "pinned_errors_in_infra_outage_window": len(outage), "pinned_error_cats": cats, "pinned_snapshot_still_in_table": present,
            "B_pinned_violation": viol, "B_pinned_expired_by_design": (not present) and bool(bad)}

def check_C_rewrite(s, tbl, ledger, res, logtext, F0, race=False):
    acked, _ = ledger_sets(ledger)
    sids = {r["snapshot_id"] for r in acked.values() if r.get("snapshot_id")}
    Fnow = table_files(s, tbl)
    T1 = {r[0] for r in s.sql(f"select file_path from ice.db.{tbl}.all_data_files").collect()}
    truth_rewritten = len(T1 - Fnow)          # files ever reachable that are no longer current (no expire runs in rewrite arm)
    lower_bound = len(F0 - Fnow)
    ents = s.sql(f"select data_file.file_path, snapshot_id from ice.db.{tbl}.entries where status in (0,1)").collect()
    truth_added = len({p for p, sid in ents if sid not in sids})
    ready = logtext.count("Ready to be Committed")
    committed_log = logtext.count("BaseRewriteFiles)")
    commit_fail_log = ("Failure during rewrite commit process" in logtext)
    kinds = {}
    for m in re.findall(r"^([\w.]+(?:Exception|Error))(?::|\s)", logtext, re.M): kinds[m] = kinds.get(m, 0) + 1
    out = {"C_applicable": bool(res.get("returned")), "returned": res.get("returned"), "exception": res.get("exception"),
           "groups_ready": ready, "groups_committed_log": committed_log, "commit_failure_logged": commit_fail_log, "swallowed_exception_kinds": kinds,
           "truth_rewritten": truth_rewritten, "truth_added": truth_added, "checker_selfcheck_ok": lower_bound <= truth_rewritten}
    if res.get("returned"):
        row = res["rows"][0]
        out.update(reported_rewritten=row["rewritten_data_files_count"], reported_added=row["added_data_files_count"],
                   reported_failed=row.get("failed_data_files_count"))
        c1 = (ready > 0 and truth_rewritten == 0)                  # #17202 class: success, planned work, zero committed
        c1b = (commit_fail_log and ready > committed_log)          # partial: some planned groups lost to swallowed failures
        if race:   # a concurrent expire may have removed the replace snapshot: rewritten-files truth unavailable; use live outputs only
            c1 = (ready > 0 and truth_added == 0)
            c2 = (row["added_data_files_count"] != truth_added)
            out["race_mode_truth_added_only"] = True
        else:
            c2 = (row["rewritten_data_files_count"] != truth_rewritten or row["added_data_files_count"] != truth_added)
        out.update(C1_success_zero_committed=c1, C1b_success_with_swallowed_group_failures=c1b, C2_report_ne_truth=c2,
                   C_violation=bool(c1 or c1b or c2))
    else:
        out.update(C_violation=False, C_note="maintenance raised/was killed: no success claim made")
    return out

def check_C_expire(s, tbl, res, D0):
    out = {"C_applicable": bool(res.get("returned")), "returned": res.get("returned"), "exception": res.get("exception")}
    if res.get("returned"):
        row = res["rows"][0]
        gone = sum(1 for p in D0 if not path_exists(p))
        out.update(reported_deleted_data_files=row["deleted_data_files_count"], truth_data_files_gone=gone)
        out["C_violation"] = (row["deleted_data_files_count"] != gone)
    else:
        out.update(C_violation=False, C_note="maintenance raised/was killed: no success claim made")
    return out

def check_C_orphan(tbl, res, pre_orphans):   # pre_orphans = ALL table objects present at the cutoff instant (metadata files can also
    # become unreferenced during the run as the metadata log truncates, and are then legitimately removable)
    """Class C for remove_orphan_files: reported removed-count vs objects from the pre-run orphan set that are really gone."""
    out = {"C_applicable": bool(res.get("returned")), "returned": res.get("returned"), "exception": res.get("exception")}
    gone = sum(1 for p in pre_orphans if not path_exists(p))
    out.update(pre_orphans=len(pre_orphans), truth_gone=gone)
    if res.get("returned"):
        out.update(reported_orphans=res["n"])
        out["C_violation"] = (res["n"] != gone)
    else: out.update(C_violation=False, C_note="maintenance raised/was killed: no success claim made")
    return out

def orphan_data_files(s, tbl):
    ref = {os.path.basename(r[0]) for r in s.sql(f"select file_path from ice.db.{tbl}.all_files").collect()}
    try: onfs = set(os.listdir(f"{WH}/{tbl}/data"))
    except FileNotFoundError: onfs = set()
    return len(onfs - ref)   # observation only (expected after a kill); not scored

def compare_readers(spark_A, spark_B, py):
    """Independent-reader comparison. A disagreement is a finding; neither reader is silently preferred."""
    if "reader_error" in py: return {"py_ran": False, "reader_error": py["reader_error"], "disagree": None}
    d = []
    pa, pb = py.get("A", {}), py.get("B", {})
    if bool(spark_A["A_violation"]) != bool(pa.get("A_violation")): d.append("A_violation differs")
    elif spark_A["A_violation"] and (spark_A["missing"], spark_A["duplicates"]) != (pa.get("missing"), pa.get("duplicates")): d.append("A detail differs")
    if bool(spark_B["B_violation"]) != bool(pb.get("B_violation")): d.append("B_violation differs")
    elif len(spark_B["tags_wrong"]) != len(pb.get("tags_wrong", [])) or len(spark_B["tags_missing"]) != len(pb.get("tags_missing", [])): d.append("B detail differs")
    return {"py_ran": True, "disagree": bool(d), "why": d, "spark_A": spark_A["A_violation"], "py_A": pa.get("A_violation"),
            "spark_B": spark_B["B_violation"], "py_B": pb.get("B_violation"),
            "py_A_err": pa.get("read_error"), "py_dangling": py.get("dangling_refs"), "py_dangling_error": py.get("dangling_error")}
