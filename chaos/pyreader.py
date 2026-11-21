"""Independent read path (pyiceberg, no Spark/Iceberg-Java). Usage: pyreader.py <table> <ledger> <tags> <out.json>
Loads the table's current metadata_location straight from the JDBC catalog (Postgres) and reads data files via S3 API.
Computes: class A (id-set vs ledger), class B (each tag's id-set; dangling refs via S3 HEAD on data AND metadata files)."""
import sys, json, os, time
import psycopg2
from pyiceberg.table import StaticTable
tbl, ledger, tags_file, out = sys.argv[1:5]
PROPS = {"s3.endpoint": "http://127.0.0.1:9100", "s3.access-key-id": "chaos", "s3.secret-access-key": "chaos-pilot-pw",
         "s3.region": "us-east-1", "s3.force-virtual-addressing": "false"}
res = {"reader": "pyiceberg"}
try:
    import pyiceberg; res["pyiceberg_version"] = pyiceberg.__version__
    c = psycopg2.connect(host="127.0.0.1", port=5433, dbname="icecat", user="chaos", password="x")
    cur = c.cursor(); cur.execute("select metadata_location from iceberg_tables where table_name=%s and table_namespace='db'", (tbl,))
    loc = cur.fetchone()[0]; c.close(); res["metadata_location"] = loc
    t = StaticTable.from_metadata(loc, properties=PROPS)
    acked, amb = {}, []
    for l in open(ledger):
        r = json.loads(l)
        if r["status"] == "acked": acked[r["batch"]] = r
        else: amb.append(r)
    exp = set(); amb_ids = set()
    for r in acked.values(): exp |= set(range(r["lo"], r["hi"]))
    for r in amb: amb_ids |= set(range(r["lo"], r["hi"]))
    def ids(**kw):
        col = t.scan(selected_fields=("rid",), **kw).to_arrow().column("rid").to_pylist()
        return col
    try:
        col = ids(); got = set(col)
        missing = exp - got; extra = got - exp - amb_ids; dup = len(col) - len(got)
        res["A"] = {"A_violation": bool(missing or extra or dup), "missing": len(missing), "unexplained_extra": len(extra), "duplicates": dup, "rows": len(col)}
    except Exception as e:
        res["A"] = {"A_violation": True, "read_error": f"{type(e).__name__}: {str(e)[:250]}", "missing": -1, "unexplained_extra": -1, "duplicates": -1}
    # B: tags
    refs = {n: r for n, r in t.metadata.refs.items() if r.snapshot_ref_type.value == "tag"}
    wrong, missing_t, err = [], [], []
    ntags = 0
    for line in (open(tags_file) if os.path.exists(tags_file) else []):
        tg = json.loads(line)
        if "create_error" in tg: continue
        ntags += 1
        if tg["tag"] not in refs: missing_t.append(tg["tag"]); continue
        try:
            got = set(ids(snapshot_id=refs[tg["tag"]].snapshot_id))
        except Exception as e:
            err.append(f'{tg["tag"]}: {type(e).__name__}: {str(e)[:120]}'); continue
        e_ = set(range(tg["expected_hi"])) - amb_ids
        if (got - amb_ids) != e_: wrong.append(f'{tg["tag"]}: missing={len(e_-got)} extra={len(got-amb_ids-e_)}')
    # dangling: every file referenced by any live snapshot (manifest lists, manifests, data files) + metadata json must exist (S3 HEAD)
    io = t.io; paths = set(); dangling = []
    try:
        for snap in t.metadata.snapshots:
            paths.add(snap.manifest_list)
            for m in snap.manifests(io):
                paths.add(m.manifest_path)
                for e in m.fetch_manifest_entry(io, discard_deleted=True):
                    paths.add(e.data_file.file_path)
        for ml in t.metadata.metadata_log: paths.add(ml.metadata_file)
        for p in paths:
            if not io.new_input(p).exists(): dangling.append(p)
        res["dangling_refs"] = len(dangling); res["dangling_examples"] = dangling[:3]; res["paths_checked"] = len(paths)
    except Exception as e:
        res["dangling_error"] = f"{type(e).__name__}: {str(e)[:250]}"; res["dangling_refs"] = -1
        dangling = ["metadata-unreadable: " + res["dangling_error"]]   # an unreadable live manifest/list is a dangling reference
    res["B"] = {"tags_expected": ntags, "tags_missing": missing_t, "tags_wrong": wrong, "tags_error": err,
                "B_violation": bool(missing_t or wrong or err or dangling)}
except Exception as e:
    res["reader_error"] = f"{type(e).__name__}: {str(e)[:400]}"
json.dump(res, open(out, "w"), default=str)
