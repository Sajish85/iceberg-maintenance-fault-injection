import sys, time, os, json, re
sys.path.insert(0, "/Users/sajishkrishnan/b2-iceberg-chaos/chaos")
from lib import *
from pyspark.sql import SparkSession
op = sys.argv[1]; tbl = "probe_" + op
s = SparkSession.builder.appName("probe").getOrCreate()
s.sparkContext.setLogLevel("WARN")
s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
s.sql(f"DROP TABLE IF EXISTS ice.db.{tbl} PURGE")
s.sql(f"CREATE TABLE ice.db.{tbl} (batch_id int,rid long,payload string) USING iceberg")
ing = Ingest(s, tbl, f"{B2}/work/probe_ledger_{op}.jsonl", f"{B2}/work/probe_tags_{op}.jsonl", 0)
for f in (ing.ledger, ing.tags):
    if os.path.exists(f): os.remove(f)
t=now()
for _ in range(24): ing.one()
print("seed secs", now()-t, "files", len(table_files(s, tbl)), flush=True)
time.sleep(2)
log = f"{B2}/work/probe_{op}.log"
ing.start()
p = spawn_maint(op, tbl, f"{B2}/work/probe_{op}.json", log)
t0 = now(); last = None; pos = 0; ev = []
while p.poll() is None and now()-t0 < 120:
    d, m = dircount(tbl, "data"), dircount(tbl, "metadata")
    with open(log, "rb") as f:
        f.seek(pos); new = f.read().decode("utf8","ignore"); pos += len(new.encode())
    for line in new.splitlines():
        if re.search(r"PHASE|Ready to be Committed|Committed snapshot|Successfully committed|Deleted|ExpireSnapshots|RewriteDataFiles|Starting job|Job \d+ finished|BaseCommitService", line):
            ev.append((round(now()-t0,3), "LOG", line[:170]))
    if (d, m) != last:
        ev.append((round(now()-t0,3), "DIR", f"data={d} meta={m}")); last = (d, m)
    time.sleep(0.01)
ing.stop_ev.set(); ing.join()
for e in ev: print(*e)
print("RESULT", open(f"{B2}/work/probe_{op}.json").read()[:600])
