"""Checker sensitivity self-test (classes A, B; both readers). Injects known faults into healthy tables; expects specific flags.
Saves FULL check dicts (not booleans). Includes one maintenance-produced fault (#17304 core-API expire deleting a live file)."""
import sys, os, json, time, shutil, subprocess
sys.path.insert(0, "/Users/sajishkrishnan/b2-iceberg-chaos/chaos")
from lib import *
import checker as ck
from pyspark.sql import SparkSession
s = SparkSession.builder.appName("selftest").getOrCreate(); s.sparkContext.setLogLevel("ERROR"); s.conf.set("spark.sql.session.timeZone", "UTC")
s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
os.makedirs(RES, exist_ok=True)
PYBIN = f"{B2}/work/venv/bin/python3.14"
out = {}
def mk(name, n=12):
    tbl = "t_self_" + name; d = f"{RUNS}/self_{name}"; shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
    s.sql(f"CREATE TABLE ice.db.{tbl} (batch_id int,rid long,payload string) USING iceberg")
    ing = Ingest(s, tbl, f"{d}/ledger.jsonl", f"{d}/tags.jsonl", 0, tag_from=2, tag_every=3)
    for _ in range(n): ing.one()
    return tbl, ing, d
def score(name, expect, tbl, ing, d):
    A = ck.check_A(s, tbl, ing.ledger); B = ck.check_B(s, tbl, ing.ledger, ing.tags)
    pj = f"{d}/py.json"
    subprocess.run([PYBIN, f"{B2}/chaos/pyreader.py", tbl, ing.ledger, ing.tags, pj], timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    py = json.load(open(pj)); cmp = ck.compare_readers(A, B, py)
    got = {"spark_A": A["A_violation"], "spark_B": B["B_violation"], "py_A": py.get("A", {}).get("A_violation"), "py_B": py.get("B", {}).get("B_violation")}
    ok = all(got[k] == v for k, v in expect.items())
    out[name] = {"expect": expect, "got": got, "match": ok, "reader_disagree": cmp.get("disagree"), "why": cmp.get("why"),
                 "spark_A_full": A, "spark_B_full": B, "py_full": py}
    print(name, "OK" if ok else "MISMATCH", got, flush=True)
T = dict(spark_A=True, py_A=True)
tbl, ing, d = mk("healthy"); score("healthy", dict(spark_A=False, spark_B=False, py_A=False, py_B=False), tbl, ing, d)
tbl, ing, d = mk("rowdelete"); s.sql(f"DELETE FROM ice.db.{tbl} WHERE rid < 100"); score("acked_rows_deleted", dict(spark_A=True, py_A=True), tbl, ing, d)
tbl, ing, d = mk("tagdrop"); s.sql(f"ALTER TABLE ice.db.{tbl} DROP TAG tag_b6"); score("retained_tag_dropped", dict(spark_B=True, py_B=True, spark_A=False, py_A=False), tbl, ing, d)
tbl, ing, d = mk("filegone"); f = sorted(table_files(s, tbl))[0]; shutil.rmtree(uri_to_fs(f)); score("live_data_file_removed", dict(spark_A=True, spark_B=True, py_A=True, py_B=True), tbl, ing, d)
tbl, ing, d = mk("dup"); s.range(0, 500).selectExpr("0 as batch_id", "id as rid", "'x' as payload").writeTo(f"ice.db.{tbl}").append(); score("duplicate_rows", dict(spark_A=True, py_A=True), tbl, ing, d)
tbl, ing, d = mk("extra"); s.range(10**7, 10**7 + 50).selectExpr("99 as batch_id", "id as rid", "'x' as payload").writeTo(f"ice.db.{tbl}").append(); score("unexplained_extra_rows", dict(spark_A=True, py_A=True), tbl, ing, d)
tbl, ing, d = mk("tagwrong")
tg = [json.loads(l) for l in open(ing.tags)]; other = [r for r in ck.ledger_sets(ing.ledger)[0].values()][0]["snapshot_id"]
s.sql(f"ALTER TABLE ice.db.{tbl} REPLACE TAG {tg[1]['tag']} AS OF VERSION {other}"); score("tag_points_to_wrong_snapshot", dict(spark_B=True, py_B=True, spark_A=False, py_A=False), tbl, ing, d)
tbl, ing, d = mk("manifestgone")
mf = s.sql(f"select path from ice.db.{tbl}.manifests").collect()[0][0]; shutil.rmtree(uri_to_fs(mf)); score("live_manifest_removed", dict(spark_B=True, py_B=True), tbl, ing, d)
# maintenance-produced fault (#17304, core-API expire after remove+re-add of one path)
tbl, ing, d = mk("m17304", n=10)
jv = s._jvm; jt = jv.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(s._jsparkSession, f"ice.db.{tbl}")
F = jt.newScan().planFiles().iterator().next().file(); h1 = jv.java.util.HashSet(); h1.add(F); h2 = jv.java.util.HashSet(); h2.add(F)
jt.newRewrite().rewriteFiles(h1, h2).commit(); time.sleep(1.2); ing.one(); time.sleep(1.2); jt.refresh()
jt.expireSnapshots().expireOlderThan(int(now() * 1000) - 500).commit()
score("maintenance_produced_17304_core_expire(informational: fault only exists on versions where #17304 reproduces)", {}, tbl, ing, d)
for t in os.listdir(RUNS): pass
for n in ("healthy", "rowdelete", "tagdrop", "filegone", "dup", "extra", "tagwrong", "manifestgone", "m17304"):
    try: s.sql(f"DROP TABLE IF EXISTS ice.db.t_self_{n} PURGE")
    except Exception: pass
out["_summary"] = {"all_match": all(v["match"] for k, v in out.items() if k != "_summary"), "checker_sha256": subprocess.run(["shasum", "-a", "256", f"{B2}/chaos/checker.py"], capture_output=True, text=True).stdout.split()[0], "ice_version": VER}
json.dump(out, open(f"{RES}/checker_selftest.json", "w"), indent=1, default=str)
print("SELFTEST all_match =", out["_summary"]["all_match"])
