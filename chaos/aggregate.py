import json, csv, collections
rows=[json.loads(l) for l in open("/Users/sajishkrishnan/b2-iceberg-chaos/results/interleavings.jsonl")]
out=[]
for r in rows:
    out.append(dict(cell=r["cell"],op=r["op"],arm=r["arm"],target_phase=r.get("target_phase") or "",
      observed_phase_at_signal=r.get("observed_phase_at_signal",""),reached_target=r.get("reached_target",""),ambiguous_batches=r["A"]["ambiguous_batches"],killed=r.get("killed",""),kill_miss=r.get("kill_miss",False),
      maint_returned=(r["C"].get("returned") if r["arm"]!="c" else ""),
      A_viol=r["A"]["A_violation"],A_missing=r["A"]["missing"],A_dup=r["A"]["duplicates"],A_acked=r["A"]["acked_batches"],A_ambiguous=r["A"]["ambiguous_batches"],
      B_viol=r["B"]["B_violation"],B_tags=r["B"]["tags_expected"],B_dangling=r["B"]["dangling_data_refs"],
      Bpin_viol=r["B_pinned"]["B_pinned_violation"],Bpin_expired_by_design=r["B_pinned"]["B_pinned_expired_by_design"],Bpin_ok=r["B_pinned"]["pinned_ok_exact"],Bpin_err=r["B_pinned"]["pinned_errors"],
      C_applicable=r["C"]["C_applicable"],C_viol=r["C"]["C_violation"],C_detail=json.dumps({k:v for k,v in r["C"].items() if k not in("C_applicable","C_violation")}),
      orphans=r["orphan_data_objects_observed"],secs=r["secs"]))
w=csv.DictWriter(open("/Users/sajishkrishnan/b2-iceberg-chaos/results/per_interleaving.csv","w"),fieldnames=list(out[0])); w.writeheader(); w.writerows(out)
# matrix op x phase x arm
cells=collections.OrderedDict()
for r in rows:
    key=(r["op"],r["arm"],r.get("target_phase") or "-")
    c=cells.setdefault(key,dict(attempted=0,completed=0,miss=0,A=0,B=0,Bpin=0,C=0,pin_exp=0))
    c["attempted"]+=1
    if r["arm"]=="b":
        if r.get("killed") and r.get("reached_target"): c["completed"]+=1
        else: c["miss"]+=1
    else: c["completed"]+=1
    c["A"]+=r["A"]["A_violation"]; c["B"]+=r["B"]["B_violation"]; c["Bpin"]+=r["B_pinned"]["B_pinned_violation"]; c["C"]+=r["C"]["C_violation"]; c["pin_exp"]+=r["B_pinned"]["B_pinned_expired_by_design"]
fh=open("/Users/sajishkrishnan/b2-iceberg-chaos/results/coverage_matrix.csv","w"); w=csv.writer(fh)
w.writerow(["operation","arm","target_kill_phase","attempted","completed","kill_miss","A_viol","B_viol","B_pinned_viol","C_viol","pinned_expired_by_design"])
for k,c in sorted(cells.items()): w.writerow([*k,c["attempted"],c["completed"],c["miss"],c["A"],c["B"],c["Bpin"],c["C"],c["pin_exp"]])
fh.close()
print(open("/Users/sajishkrishnan/b2-iceberg-chaos/results/coverage_matrix.csv").read())
for o in out:
    if o["A_viol"] or o["B_viol"] or o["Bpin_viol"] or o["C_viol"] or o["kill_miss"]: print({k:o[k] for k in("cell","observed_phase_at_signal","kill_miss","A_viol","B_viol","Bpin_viol","C_viol","C_detail")})
