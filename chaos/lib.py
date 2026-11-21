"""Shared harness code: ingest + pinned reader threads, kill observer, checker (classes A/B/C)."""
import re, os, json, time, random, signal, subprocess, threading

B2 = "/Users/sajishkrishnan/b2-iceberg-chaos"
VER = os.environ.get("ICE_VER", "1.7.1")
TAG = os.environ.get("RUN_TAG", "")
RES = f"{B2}/results/v{VER}{TAG}"; RUNS = f"{B2}/work/runs/v{VER}{TAG}"
WH = f"{B2}/work/minio-data/warehouse/wh/db"      # MinIO single-drive FS layout: one dir per object
BATCH_ROWS, FILES_PER_BATCH = 500, 4

def now(): return time.time()

# ------------------------------------------------------------------ ingest / ledger
class Ingest(threading.Thread):
    """Append-only stream. Ledger = JSONL written ONLY after append() returns (ack).
    Exceptions go to class 'ambiguous' (never counted as loss). Tags every Nth acked batch (retained snapshot)."""
    def __init__(self, s, tbl, ledger, tags, start_batch, interval=0.4, tag_every=4, tag_from=24):
        super().__init__(daemon=True)
        self.s, self.tbl, self.ledger, self.tags = s, tbl, ledger, tags
        self.b, self.interval, self.tag_every, self.tag_from = start_batch, interval, tag_every, tag_from
        self.stop_ev = threading.Event()
    def snap_for(self, b):
        r = self.s.sql(f"select snapshot_id from ice.db.{self.tbl}.snapshots where summary['batch_id']='{b}'").collect()
        return r[0][0] if r else None
    def one(self):
        b = self.b; self.b += 1
        lo, hi = b * BATCH_ROWS, (b + 1) * BATCH_ROWS
        t = now()
        try:
            (self.s.range(lo, hi).selectExpr(f"{b} as batch_id", "id as rid", "repeat('x',40) as payload")
               .repartition(FILES_PER_BATCH).writeTo(f"ice.db.{self.tbl}")
               .option("snapshot-property.batch_id", str(b)).append())
        except Exception as e:
            with open(self.ledger, "a") as f:
                f.write(json.dumps({"batch": b, "lo": lo, "hi": hi, "status": "ambiguous", "t": t,
                                    "err": f"{type(e).__name__}: {str(e)[:200]}"}) + "\n")
            return
        try: sid = self.snap_for(b)
        except Exception: sid = None          # infra outage after ack: ack stands, snapshot id unknown
        with open(self.ledger, "a") as f:
            f.write(json.dumps({"batch": b, "lo": lo, "hi": hi, "status": "acked", "snapshot_id": sid, "t": t, "t_ack": now()}) + "\n")
        if sid and b >= self.tag_from and b % self.tag_every == 0:
            try:
                self.s.sql(f"ALTER TABLE ice.db.{self.tbl} CREATE TAG tag_b{b} AS OF VERSION {sid} RETAIN 30 DAYS")
                with open(self.tags, "a") as f:
                    f.write(json.dumps({"tag": f"tag_b{b}", "snapshot_id": sid, "expected_hi": hi}) + "\n")
            except Exception as e:
                with open(self.tags, "a") as f:
                    f.write(json.dumps({"tag": f"tag_b{b}", "snapshot_id": sid, "expected_hi": hi, "create_error": str(e)[:200]}) + "\n")
    def run(self):
        while not self.stop_ev.is_set():
            self.one(); time.sleep(self.interval)

class PinnedReader(threading.Thread):
    """Long-running reader pinned to an older (UNtagged) snapshot; re-reads periodically."""
    def __init__(self, s, tbl, snap, expected_hi, log, interval=1.0):
        super().__init__(daemon=True)
        self.s, self.tbl, self.snap, self.hi, self.log, self.interval = s, tbl, snap, expected_hi, log, interval
        self.stop_ev = threading.Event()
    def run(self):
        while not self.stop_ev.is_set():
            rec = {"t": now(), "snapshot_id": self.snap}
            try:
                rows = self.s.read.option("snapshot-id", str(self.snap)).table(f"ice.db.{self.tbl}").select("rid").collect()
                ids = [r[0] for r in rows]
                rec.update(ok=True, n=len(ids), exact=(sorted(ids) == list(range(self.hi))))
            except Exception as e:
                rec.update(ok=False, err=f"{type(e).__name__}: {str(e)[:300]}")
            with open(self.log, "a") as f: f.write(json.dumps(rec) + "\n")
            time.sleep(self.interval)

# ------------------------------------------------------------------ table state helpers
def table_files(s, tbl):
    return {r[0] for r in s.sql(f"select file_path from ice.db.{tbl}.files").collect()}
def all_data_files(s, tbl):
    return {r[0] for r in s.sql(f"select file_path from ice.db.{tbl}.all_data_files").collect()}
def path_exists(p):   # s3://warehouse/wh/db/... -> MinIO FS dir
    return os.path.exists(f"{B2}/work/minio-data/warehouse/" + p.split("s3://warehouse/", 1)[1])
def dircount(tbl, sub):
    try: return len(os.listdir(f"{WH}/{tbl}/{sub}"))
    except FileNotFoundError: return 0
def read_ledger(path):
    return [json.loads(l) for l in open(path)] if os.path.exists(path) else []

# ------------------------------------------------------------------ kill observer
PHASES = {"rewrite": ["before-writes", "during-writes", "post-write-pre-commit", "during-commit"],
          "expire":  ["pre-commit", "post-commit-delete-planning", "during-deletes"],
          "orphan":  ["planning", "listing-compare", "deleting"]}

class Observer:
    """Classifies the maintenance JVM's phase from its own log markers only (inferred, see report).
    rewrite: counters of 'Starting job: save at SparkBinPackDataRewriter' (write job start), 'finished: save at
             SparkBinPackDataRewriter' (write job done), 'Ready to be Committed' (logged just before commit),
             'Committed snapshot .. BaseRewriteFiles' (commit done).
    expire:  'RemoveSnapshots: Committed snapshot changes' (metadata commit done), final
             'ResultStage .. finished' of collectAsList at ExpireSnapshotsSparkAction (delete-planning done),
             'Deleted N total files'."""
    def __init__(self, op, log):
        self.op, self.log, self.pos, self.text = op, log, 0, ""
    def poll(self):
        try:
            with open(self.log, "rb") as f:
                f.seek(self.pos); new = f.read(); self.pos += len(new)
            self.text += new.decode("utf8", "ignore")
        except FileNotFoundError: pass
        x = self.text
        if "PHASE call-start" not in x: return "jvm-startup"
        if "PHASE call-end" in x: return "finished"
        if self.op == "orphan":
            if "DeleteOrphanFilesSparkAction: Deleted" in x: return "after-deletes"
            if re.search(r"ResultStage \d+ \(collectAsList at DeleteOrphanFilesSparkAction\.java:\d+\) finished", x): return "deleting"
            if not re.search(r"Got (map stage )?job \d+ \([^)]*DeleteOrphanFilesSparkAction", x): return "planning"
            return "listing-compare"
        if self.op == "rewrite":
            starts = x.count("Starting job: save at SparkBinPack")
            fin = x.count("finished: save at SparkBinPack")
            ready = x.count("Ready to be Committed")
            comm = x.count("BaseRewriteFiles)")
            if starts == 0: return "before-writes"
            if starts > fin: return "during-writes"
            if fin > ready: return "post-write-pre-commit"
            if ready > comm: return "during-commit"
            return "between-groups"
        if "RemoveSnapshots: Committed snapshot changes" not in x: return "pre-commit"
        if "ExpireSnapshotsSparkAction: Deleted" in x: return "after-deletes"
        if re.search(r"ResultStage \d+ \(collectAsList at ExpireSnapshotsSparkAction[^)]*\) finished in", x): return "during-deletes"
        return "post-commit-delete-planning"

def spawn_maint(op, tbl, outjson, logfile, arg=None):
    lf = open(logfile, "w")
    env = dict(os.environ); 
    if op == "orphan": env["S3A"] = "1"
    p = subprocess.Popen([f"{B2}/scripts/run.sh", f"{B2}/chaos/maint.py", op, tbl, outjson] + ([str(arg)] if arg is not None else []),
                         stdout=lf, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    return p
def killpg(p, sig=signal.SIGKILL):
    try: os.killpg(os.getpgid(p.pid), sig)
    except ProcessLookupError: pass

# ------------------------------------------------------------------ store inspection (MinIO FS layout; see limits)
def uri_to_fs(p): return f"{B2}/work/minio-data/warehouse/" + p.split("s3://warehouse/", 1)[1]
def table_objects(tbl):
    """All object URIs under the table's data/ and metadata/ prefixes (MinIO single-drive FS listing)."""
    out = set()
    for sub in ("data", "metadata"):
        try:
            for n in os.listdir(f"{WH}/{tbl}/{sub}"): out.add(f"s3://warehouse/wh/db/{tbl}/{sub}/{n}")
        except FileNotFoundError: pass
    return out
def referenced_objects(s, tbl):
    """Every object a live table state references: data/delete files, manifests, manifest lists, metadata json files."""
    ref = set()
    for q in (f"select file_path from ice.db.{tbl}.all_files", f"select path from ice.db.{tbl}.all_manifests",
              f"select manifest_list from ice.db.{tbl}.snapshots", f"select file from ice.db.{tbl}.metadata_log_entries"):
        ref |= {r[0] for r in s.sql(q).collect() if r[0]}
    return ref
def free_bytes():
    import shutil; return shutil.disk_usage('/System/Volumes/Data').free
def iceberg_version(s):
    return str(s._jvm.org.apache.iceberg.IcebergBuild.version())
