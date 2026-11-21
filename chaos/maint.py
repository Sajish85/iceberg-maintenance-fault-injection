"""Maintenance runner: one operation, one JVM. Usage: maint.py <op> <table> <out.json> [cutoff_ms]
ops: rewrite | rewrite_small_groups | expire | orphan   (orphan = remove_orphan_files via the Action API)
Writes PHASE markers to stdout (observer reads the log) and a result JSON ONLY if the call returns."""
import sys, json, time
from pyspark.sql import SparkSession
op, tbl, out = sys.argv[1], sys.argv[2], sys.argv[3]
arg = int(sys.argv[4]) if len(sys.argv) > 4 else None
s = SparkSession.builder.appName("maint-" + op).getOrCreate()
s.conf.set("spark.sql.session.timeZone","UTC")
t0 = time.time()
print("PHASE call-start", flush=True)
try:
    if op in ("rewrite", "rewrite_small_groups"):
        gsz = "1200000" if op == "rewrite" else "40000"
        r = s.sql(f"CALL ice.system.rewrite_data_files(table=>'db.{tbl}', options=>map("
                  "'partial-progress.enabled','true','partial-progress.max-commits','4',"
                  "'max-file-group-size-bytes','"+gsz+"','min-input-files','2','max-concurrent-file-group-rewrites','1'))").collect()
        res = {"returned": True, "rows": [x.asDict() for x in r], "secs": time.time() - t0}
    elif op == "expire":
        cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - 1))  # session tz is UTC
        r = s.sql(f"CALL ice.system.expire_snapshots(table=>'db.{tbl}', "
                  f"older_than=>TIMESTAMP '{cutoff}', retain_last=>1)").collect()
        res = {"returned": True, "rows": [x.asDict() for x in r], "secs": time.time() - t0}
    elif op == "orphan":
        # Action API: the SQL procedure refuses older_than < 24h on these versions.
        jv = s._jvm
        jt = jv.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(s._jsparkSession, "ice.db." + tbl)
        act = jv.org.apache.iceberg.spark.actions.SparkActions.get(s._jsparkSession).deleteOrphanFiles(jt).olderThan(arg)
        result = act.execute()
        it = result.orphanFileLocations().iterator(); locs = []
        while it.hasNext(): locs.append(str(it.next()))
        res = {"returned": True, "orphan_locations": locs, "n": len(locs), "secs": time.time() - t0}
    else: raise SystemExit("bad op")
except Exception as e:
    res = {"returned": False, "exception": f"{type(e).__name__}: {str(e)[:500]}", "secs": time.time() - t0}
json.dump(res, open(out, "w"))
print("PHASE call-end", flush=True)
