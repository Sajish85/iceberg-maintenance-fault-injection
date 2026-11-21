"""Aggregate results/v<VER>/interleavings.jsonl -> per_interleaving.csv, coverage_matrix.csv, violations.csv. usage: aggregate2.py <VER>"""
import json, csv, sys, collections
B2 = "/Users/sajishkrishnan/b2-iceberg-chaos"; V = sys.argv[1]; D = f"{B2}/results/v{V}"
rows = [json.loads(l) for l in open(f"{D}/interleavings.jsonl")]
out = []
for r in rows:
    C = r["C"]; py = r.get("py", {}); rc = r.get("reader_compare", {})
    out.append(dict(cell=r["cell"], op=r["op"], arm=r["arm"], target=r.get("target_phase") or r.get("kind") or "", phase_at_kill=r.get("observed_phase_at_signal", ""),
        reached_target=r.get("reached_target", ""), kill_miss=r.get("kill_miss", False), kill_delay_s=r.get("kill_delay_s", ""),
        acked=r["A"]["acked_batches"], ambiguous=r["A"]["ambiguous_batches"], amb_in_table=r["A"]["ambiguous_present_in_table"],
        A=r["A"]["A_violation"], A_missing=r["A"]["missing"], B=r["B"]["B_violation"], B_tags=r["B"]["tags_expected"], B_dangling=r["B"].get("dangling_refs"),
        Bpin=r["B_pinned"]["B_pinned_violation"], Bpin_expired=r["B_pinned"]["B_pinned_expired_by_design"],
        C_applicable=C["C_applicable"], C=C["C_violation"], C_kinds=json.dumps(C.get("swallowed_exception_kinds", "")),
        py_ran=rc.get("py_ran"), reader_disagree=r.get("reader_disagree"), disagree_why=";".join(rc.get("why", []) or []),
        py_A=py.get("A", {}).get("A_violation"), py_B=py.get("B", {}).get("B_violation"), orphans_obs=r["orphan_data_objects_observed"], secs=r["secs"]))
w = csv.DictWriter(open(f"{D}/per_interleaving.csv", "w"), fieldnames=list(out[0])); w.writeheader(); w.writerows(out)
cov = collections.OrderedDict()
for r, o in zip(rows, out):
    k = (r["op"], r["arm"], o["target"]); c = cov.setdefault(k, collections.Counter())
    c["attempted"] += 1
    ok = (r["arm"] != "b" or r["op"] in ("race", "infra") or bool(r.get("reached_target")))
    c["completed"] += ok
    for n, f in (("A", o["A"]), ("B", o["B"]), ("Bpin", o["Bpin"]), ("C", o["C"]), ("C_applicable", o["C_applicable"]), ("reader_disagree", o["reader_disagree"]), ("ambiguous_cells", o["ambiguous"] > 0)): c[n] += bool(f)
with open(f"{D}/coverage_matrix.csv", "w") as fh:
    w = csv.writer(fh); cols = ["attempted", "completed", "A", "B", "Bpin", "C_applicable", "C", "reader_disagree", "ambiguous_cells"]
    w.writerow(["version", "operation", "arm", "phase/kind"] + cols)
    for k, c in sorted(cov.items()): w.writerow([V, *k] + [c[x] for x in cols])
print(open(f"{D}/coverage_matrix.csv").read())
for o in out:
    if o["A"] or o["B"] or o["Bpin"] or o["C"] or o["reader_disagree"]: print("FLAG", {k: o[k] for k in ("cell", "A", "B", "Bpin", "C", "C_kinds", "reader_disagree", "disagree_why")})
