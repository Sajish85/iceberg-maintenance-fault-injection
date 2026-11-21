import sys, time, os, json, re
sys.path.insert(0, "/Users/sajishkrishnan/b2-iceberg-chaos/chaos")
from lib import *
from pyspark.sql import SparkSession
op = sys.argv[1]; tbl = "probe2_" + op
s = SparkSession.builder.appName("probe2").getOrCreate()
s.sparkContext.setLogLevel("ERROR"); s.conf.set("spark.sql.session.timeZone","UTC")
s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
s.sql(f"CREATE TABLE ice.db.{tbl} (batch_id int,rid long,payload string) USING iceberg")
d = f"{B2}/work/probe2_{op}"; os.makedirs(d, exist_ok=True)
for f in os.listdir(d): os.remove(f"{d}/{f}")
ing = Ingest(s, tbl, f"{d}/ledger.jsonl", f"{d}/tags.jsonl", 0)
t=now()
for _ in range(int(sys.argv[2])): ing.one()
print("seed secs", now()-t, "files", len(table_files(s, tbl)), flush=True)
cutoff = None
if op == "orphan":
    p0 = spawn_maint("rewrite_small_groups", tbl, f"{d}/o.json", f"{d}/setup.log"); ob = Observer("rewrite", f"{d}/setup.log")
    while p0.poll() is None and ob.poll() != "during-writes": time.sleep(0.005)
    time.sleep(1.2); killpg(p0); p0.wait()
    print("orphans after killed rewrite", dircount(tbl,"data") , "files in table", len(table_files(s,tbl)))
    time.sleep(1.2); cutoff = int(now()*1000)
if op == "expire":
    s.sql(f"CALL ice.system.rewrite_data_files(table=>'db.{tbl}', options=>map('min-input-files','2'))").collect()
time.sleep(1.5)
log = f"{d}/maint.log"
ing.start()
p = spawn_maint(op, tbl, f"{d}/maint.json", log, cutoff)
t0 = now(); last = None; pos = 0; ev = []
while p.poll() is None and now()-t0 < 180:
    dd, m = dircount(tbl, "data"), dircount(tbl, "metadata")
    with open(log, "rb") as f:
        f.seek(pos); new = f.read(); pos += len(new); new = new.decode("utf8","ignore")
    for line in new.splitlines():
        if re.search(r"PHASE|Ready to be Committed|Committed snapshot|Successfully committed|Deleted|ExpireSnapshots|RewriteDataFiles|Starting job|Job \d+ finished|BaseCommitService|Orphan|orphan|Stage \d+ \(.*finished in|WARN|ERROR", line):
            ev.append((round(now()-t0,3), "LOG", line[:200]))
    if (dd, m) != last: ev.append((round(now()-t0,3), "DIR", f"data={dd} meta={m}")); last = (dd, m)
    time.sleep(0.01)
ing.stop_ev.set(); ing.join()
for e in ev: print(*e)
print("RESULT", open(f"{d}/maint.json").read()[:500])
print("FORMAT", s.sql(f"show tblproperties ice.db.{tbl}").collect())
