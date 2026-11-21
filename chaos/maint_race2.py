"""Expire for the gated race re-run. Same call as maint.py 'expire', except it prints PHASE call-start, then waits for a gate file
before computing the cutoff (now-1s) and issuing the CALL. usage: maint_race2.py <table> <out.json> <gatefile>"""
import sys, json, time, os
from pyspark.sql import SparkSession
tbl, out, gate = sys.argv[1], sys.argv[2], sys.argv[3]
s = SparkSession.builder.appName("maint-expire-gated").getOrCreate()
s.conf.set("spark.sql.session.timeZone", "UTC")
print("PHASE call-start", flush=True)
while not os.path.exists(gate): time.sleep(0.005)
t0 = time.time()
try:
    cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - 1))
    r = s.sql(f"CALL ice.system.expire_snapshots(table=>'db.{tbl}', older_than=>TIMESTAMP '{cutoff}', retain_last=>1)").collect()
    res = {"returned": True, "rows": [x.asDict() for x in r], "secs": time.time() - t0}
except Exception as e:
    res = {"returned": False, "exception": f"{type(e).__name__}: {str(e)[:500]}", "secs": time.time() - t0}
json.dump(res, open(out, "w"))
print("PHASE call-end", flush=True)
