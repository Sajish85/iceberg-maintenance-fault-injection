# FULL-REPORT — B2: Iceberg maintenance-interleaving chaos, checker study
Run 20 Nov 2026 (00:2x–05:0x PT; `main` matrix completed 08:07–08:55 PT after the disk was freed). Local only: nothing posted, pushed, published, filed or transmitted. Only read-only GETs (GitHub API/raw, Maven Central, PyPI).

## 1. Summary (read first)

**Status: COMPLETE for all three builds.** 1.7.1, 1.12.0 and `main`@2d89aa4 each have controls, self-test and a full 123-cell matrix. The `main` matrix first ran 20 Nov 08:07–08:55 PT (earlier it was held by the >=6 GB disk guard, which was left unchanged; disk was then ~36 GB free).

| Version | Self-test | #17202 control | #17304 control | Matrix (123 cells) |
|---|---|---|---|---|
| 1.7.1 | 9/9 as expected | **detected** (class C) | core-API expire: **live file deleted, detected (class A, both readers)**; Spark procedure: file survives | complete |
| 1.12.0 (current stable, released 2026-09-30) | 9/9 | **detected** | same as 1.7.1 | complete |
| `main` @ 2d89aa4 (2026-11-20) | 9/9 | **detected** | same as 1.7.1 | complete (123 cells, 0 cell errors, 08:07–08:55 PT) |

Results per class (no merged number anywhere):
- **A (integrity):** 0 violations in 369 matrix cells (all three builds).
- **B (retained snapshots):** 0 in 369. **B-pinned:** 0 scored violations (reported apart, §6).
- **C (progress truthfulness):** **1.7.1: 8 of 8 uninterrupted race cells flagged**; every other cell clean. **1.12.0: 0 of 123 cells. `main`@2d89aa4: 0 of 123 cells** (0 of 8 uninterrupted race cells; 4 of those 8 logged a `CommitFailedException`, none flagged). All flagged cells map to existing issue **#17202** (no novel violation). Third category: none.
- **Independent reader (pyiceberg 0.12.0), all 369 cells + all controls:** **0 disagreements** with the Spark read for A or B.
- Ambiguous batches (never counted as loss): 84 total (1.7.1 30, 1.12.0 28, `main` 26), all in infra-kill cells; 0 ambiguous batches landed in a table (checked per cell via `amb_in_table`).

**What this does and does not show.** The checker detects (a) #17202 via class C on three builds, (b) a maintenance-produced live-file deletion (#17304 core API) via class A and B on three builds, (c) 8 synthetic faults. Across ~369 kill/interleave cells, no data was lost or made unreadable by rewrite, expire or remove_orphan_files on 1.7.1, 1.12.0 or `main`@2d89aa4. That is absence of evidence under this rig (§9), not proof of safety.

**Against the pre-set bars.** COMPLETE needs controls per version, matrix per §7, reader comparison and coverage matrix. Met for all three builds. Caveat: rewrite post-write-pre-commit reached its target phase in 3/8 (1.7.1), 7/8 (1.12.0) and 7/8 (`main`) cells (§5).

## 2. Architecture (what talks to what)
```mermaid
flowchart LR
  O[orchestrator JVM<br/>Spark+Iceberg under test] -->|append batches, tags| T[(Iceberg table)]
  O -->|ack -> JSONL| L[ledger + tags]
  O -->|spawn / SIGKILL / SIGSTOP| M[maintenance JVM(s)<br/>rewrite / expire / orphan]
  M -->|log markers| O
  M --> T
  T --- PG[(Postgres 16 JDBC catalog)]
  T --- S3[(MinIO local FS)]
  O -->|infra cells: SIGKILL + restart| PG
  O -->|infra cells: SIGKILL + restart| S3
  K[checker A/B/B-pinned/C<br/>Spark read] --> R[results/v*/]
  P[pyreader.py<br/>pyiceberg, reads catalog row + S3 API] --> R
  L --> K
  L --> P
```
Walkthrough: ingest appends 500-row, 4-file batches (~2.5/s) and writes a ledger line only after the append returns. Maintenance runs in separate JVMs so SIGKILL is a real process crash. The observer infers phase from the maintenance log. After each cell, two readers independently check the table against the ledger: Spark (the build under test) and pyiceberg (separate code path, reads metadata from the catalog row and data via S3). Phase inference is from log markers (inferred).

## 3. Environment (all versions)
macOS 15.5 arm64, 10 cores, 16 GiB; OpenJDK 21.0.13.1 (deviation: brief prefers 17); Spark 3.5.3 local[2]; Iceberg runtime/AWS bundle 1.7.1, 1.12.0, and `main`@2d89aa4f528e (built locally with gradle shadowJar); Postgres 16.16 JDBC catalog (cache disabled); MinIO RELEASE.2025-10-15 single-drive FS, S3FileIO path-style; pyiceberg 0.12.0 (py3.14 venv); hadoop-aws 3.3.4 + aws-sdk-bundle 1.12.262 (S3A) used **only** for orphan-file listing. Details and hashes: VERSION-MANIFEST.md.
Stack change vs pilot: tables seeded with **120 batches** (480 files) vs 24 (5x).

## 4. Controls per version (condition 2, 3)
Controls ran on the frozen checker before each matrix; the driver stops the version if the #17202 control is missed (never triggered).

**#17202** (SIGSTOP the rewrite JVM after its first write job, ingest advances, expire removes the starting snapshot, SIGCONT). All three builds: CALL returns success with rewritten=0/added=0/failed=0, 19 groups ready, 0 committed, swallowed failures logged; checker class C1 + C1b fire. Status of #17202 per pilot read: open; maintainer called it working as designed; fix PR #17204 closed unmerged (carried from the pilot report, **not re-fetched** in this run).

**#17304** (remove + re-add same path in one RewriteFiles commit, append, expire). Read this run via the GitHub API: closed 2026-07-24 (completed) by the reporter after maintainers pointed to the spec ("not recommended to add a deleted file back"). **Contested status: closed as spec-discouraged.** Result on all three builds (1.7.1, 1.12.0, main):
- core `ExpireSnapshots` API (the issue's reproducer): live file is deleted from storage; Spark read fails; **class A detected by both readers**.
- Spark `expire_snapshots` procedure: file survives; not detected (the procedure is not affected on these builds).
That the loss also reproduces on `main`@2d89aa4 follows the issue's own expectation; I did not check the code path.

**Self-test** (results/v*/checker_selftest.json, full check dicts saved): healthy, deleted rows, dropped tag, live data file removed, duplicate rows, unexplained extra rows, tag re-pointed to the wrong snapshot, live manifest removed, plus #17304 as informational. All expected flags raised by both readers on all three builds. One gap found and fixed before the freeze: the py reader did not flag an unreadable live manifest.

## 5. Coverage matrix (operation x phase x arm x version)
Full table: `results/coverage_matrix_all_versions.csv` (regenerated with the `main` rows; per version: `results/v*/coverage_matrix.csv`, per cell: `per_interleaving.csv`, raw: `interleavings.jsonl`). "completed" = SIGKILL landed while the observer saw the target phase (interrupted arms) or the cell ran to its end.

| op | arm / phase | 1.7.1 att/compl | 1.12.0 att/compl | main@2d89aa4 att/compl |
|---|---|---|---|---|
| rewrite | b before-writes | 8/8 | 8/8 | 8/8 |
| rewrite | b during-writes | 8/8 | 8/8 | 8/8 |
| rewrite | b post-write-pre-commit | 8/**3** | 8/7 | 8/7 |
| rewrite | b during-commit | 8/8 | 8/8 | 8/8 |
| rewrite | a uninterrupted | 6/6 | 6/6 | 6/6 |
| expire | b pre-commit / post-commit-delete-planning / during-deletes | 8/8 each | 8/8 each | 8/8 each |
| expire | a | 6/6 | 6/6 | 6/6 |
| orphan (Action API) | b planning / listing-compare / deleting | 4/4 each | 4/4 each | 4/4 each |
| orphan | a | 4/4 | 4/4 | 4/4 |
| chain (tags -> full rewrite -> expire) | b three expire phases | 3/3 each | 3/3 each | 3/3 each |
| chain | a | 4/4 | 4/4 | 4/4 |
| race (expire JVM vs multi-group rewrite JVM) | a / b(kill rewrite) | 8/8, 4/4 | 8/8, 4/4 | 8/8, 4/4 |
| infra (SIGKILL own Postgres / MinIO) | 4 targets x 2 stores | 8/8 | 8/8 | 8/8 |
| ingest-only | c | 6/6 | 6/6 | 6/6 |

Quotas (condition 7): >= 8 interrupted per rewrite/expire phase and >= 4 per orphan phase: **met in attempts for every phase; not met in "reached target" for rewrite post-write-pre-commit** (3/8 on 1.7.1, 7/8 on 1.12.0, 7/8 on `main`): that window lasts a few ms and the observer's log polling lags it. Uninterrupted and ingest-only arms: present on both versions. "Both matrices at ~5x pilot table size": yes (120 vs 24 batches).

## 6. Per-class results
**Class A.** 0/369 cells (Spark), 0/369 (pyiceberg).
**Class B.** 0/369. Dangling-reference scan now covers data and delete files, manifests, manifest lists and metadata json (pilot: data files only). Chain cells (tag -> rewrite replaces the tag's files -> expire) are the dangerous shape the vetting asked for: 10 cells per version (all three builds), no loss, tags returned exact sets.
**B-pinned (reported apart).** 0 scored violations. ~240 pinned-read errors per version are `snapshot not found` after expire removed the (untagged) pinned snapshot, excused by design. B-pinned still cannot detect an in-flight read racing file deletion (§9).
**Class C** (denominator = cells with a success claim, C_applicable: 28 per version by design; flagged 8 on 1.7.1, 0 on 1.12.0, 0 on `main`. For `main` the per-cell CSV counts 29 applicable: the 28 plus one rewrite post-write-pre-commit cell where the SIGKILL missed the window and the call returned. Recomputing the same CSV count gives 33 for 1.7.1 and 28 for 1.12.0; the 28 in the earlier text was the by-design figure and I did not reconcile the extra kill-missed cells for 1.7.1, so treat 28 as the design denominator).
- Uninterrupted rewrite (6/version): reported = truth (508/512 files), clean on both versions.
- remove_orphan_files: reported removed-count = objects really gone, 4/4 on both versions.
- Race cells, 1.7.1: **8/8 flagged (C1b)**: partial-progress rewrite returned success with `failed_data_files_count=0` while only 2–3 of 19 ready groups committed (e.g. 412 of ~512 files rewritten). Log shows `ValidationException: Cannot determine history between starting snapshot …` and a NullPointerException (`Table.snapshot(long)` is null) — the two layers in #17202. **Mapped to #17202.** Data intact (A/B clean).
- Race cells, 1.12.0: 0/8; all four group commits succeeded after the concurrent expire. I did not establish why (e.g. changed commit/validation path vs timing). Unverified.
- Race cells, `main`@2d89aa4: 0/8 flagged. 4 of the 8 logged a `CommitFailedException` among swallowed exceptions (cells race_a_race_022, 023, 026, 027) but the rewrite result and the checker's truth agreed, so no C violation. Why it differs from 1.7.1 is not established (unverified); the #17202 hold-and-expire control still detects the failure class on `main`.
- Uninterrupted rewrite/expire/orphan/chain cells on `main`: all clean (6, 6, 4, 4).
- Killed cells make no success claim, so class C is "not applicable" there (denominator fixed vs pilot: counted only where a success claim exists).
**Ambiguous outcomes (condition 8).** Done safely: SIGKILL of the project's own Postgres (PID from its pidfile) and own MinIO (matched by port 9100), restart after 2–5 s. 8 cells/version. Infra kills produced ambiguous ingest batches (26–30 per version, 5–6 cells, none present in the table, none counted as loss); MinIO kills mostly produced none (the S3 client retried). Rewrite/expire JVMs that lost the catalog still exited 0 and A/B stayed clean. Pinned-reader errors inside the declared outage window are reported, not scored.

## 7. Independent-reader comparison (condition 5)
pyiceberg 0.12.0 read **every** cell (not a sample) and every control, from the Postgres catalog row via S3 API, checking A (id set vs ledger), B (each tag) and a dangling scan. **0 disagreements on 369 cells + the controls on all three builds.** In the #17304 controls both readers flag the loss. Mean py read ≈5 s/cell. Caveat: both readers read the same MinIO store and same Parquet; agreement doesn't cover store-level faults that corrupt both views the same way.

## 8. Violations
No novel violation. Mapped: **#17202** (1.7.1 race cells x8; control on all builds). **#17304** control reproduces as described there (not a violation of the matrix; reproduced deliberately, contested/closed). No third category. Reproduction for the mapped item: `scripts/run.sh chaos/run_matrix.py control17202` with ICE_VER set; cell ids race_a_race_020…027 in results/v1.7.1/; class C1b; ledger clean.

### Side observation (condition 6) — **now verified from source**
Pilot WARN "partial-progress.enabled is true but 3 rewrite commits failed" on successful single-group rewrites. In `RewriteDataFilesSparkAction.java` at tag apache-iceberg-1.7.1 line 376: `failedCommits = maxCommits - commitService.succeededCommits()`, warned when > 0. With `max-commits=4` and one commit this logs "3 failed". In tag 1.12.0 line 355–356: `totalCommits = Math.min(plan.totalGroupCount(), maxCommits)`. Observed here: WARN present in 6/6 uninterrupted rewrites on 1.7.1, 0/6 on 1.12.0. So it is a log-only misreport of 1.7.1 (the source-level fix exists in 1.12.0; I did not look for the upstream issue/PR). It is not a real failure and not scored.

## 9. Limits and blockers (honest)
1. **`main` matrix: resolved.** Earlier blocked by the >=6 GB disk guard (4.1 GiB free). After disk was freed (~36 GB), `scripts/run_version.sh main` ran 20 Nov 08:07–08:55 PT with the guard unchanged: 123 cells, 0 cell errors. Spark temp/local dirs were redirected with `SPARK_LOCAL_DIRS=work/spark-local-main` (env var only; frozen files untouched). After the run: 96 `spark-*`/`blockmgr-*` dirs this run created in `$TMPDIR` and 192 entries (~13 GB) in `work/spark-local-main` were deleted; 261 pre-existing `blockmgr-*` dirs in `$TMPDIR` were left alone. Free disk 38,088,140 KB before, 37,229,524 KB after (~0.8 GB less: results, MinIO and Postgres data). Detail in STEPLOG.md.
2. **Rig is not production S3.** MinIO local FS; no S3 consistency/latency behaviour. Several checks read MinIO's directory layout (existence, orphan counts) as well as the S3 API. Catalog is Postgres JDBC only (no REST/Glue/Hive).
3. **Phase observation is inferred from logs**; two windows (rewrite post-write-pre-commit, expire during-deletes, orphan deleting) are milliseconds wide; for them kill delay is 0 and the phase at SIGKILL was recorded (`reached_target`). Wide phases use delay uniform(0, 0.9 x measured dwell) — wider than the pilot's 10 ms, but kills still start at a phase's first observation.
4. **B-pinned** never races an in-flight read with deletion; **B** tags are created by the ingest every 4th/10th batch only.
5. **#17202 reproduction inside the matrix** is race-timing dependent: 8/8 on 1.7.1, 0/8 on 1.12.0 (cause unknown). The hold-and-expire control remains the deterministic evidence.
6. Orphan listing uses S3A/Hadoop FS on both versions (1.7.1 has no `usePrefixListing`); deletes use S3FileIO. Orphan cutoff is fixed before ingest restarts so in-flight writes are never eligible.
7. **Observational orphan counter (`orphan_data_objects_observed`, `orphans_pre_maint`) is unreliable and unscored:** after expire it shows 480 on 1.7.1 vs 960 on 1.12.0, and `orphans_pre_maint` is nonzero in some 1.12.0 cells only. A standalone probe showed `files`/`all_files`/`all_data_files` agree on both versions, so the cause is not found. The pilot's 192-vs-96 orphan figure remains unexplained.
8. A `Spark read` is the build under test, so a bug in Iceberg metadata handling could in principle mislead both Iceberg-Java and the checker's Spark view; pyiceberg is the mitigation.
9. Pilot-era frozen-checker caveat: `run_matrix.py` changed once after the freeze (version assertion for `main`; diff in STEPLOG). 1.7.1/1.12.0 ran on the earlier hash; checker logic identical.
10. Smoke-test runs (before the freeze) are archived in results/smoke-archive and are **not** study data.
11. Pilot results at the top level of results/ are the pilot's (Iceberg 1.7.1, 24 batches), kept unchanged.

## 10. Binding conditions from ADDENDUM-FABLE-CONDITIONS.md (Muse, 20 Nov) — compliance
The addendum appeared in the folder at 00:21, during the build; I read it after the runs. Compliance, item by item:
1. **Maintenance-produced A/B positive control: MET.** #17304 core-API expire deletes a live file; classes A and B flag it, both readers, on 1.7.1, 1.12.0 and main.
2. **tag -> rewrite -> expire and expire-racing-rewrite cells: MET.** `chain` (10/version) and `race` (12/version) cells; race cells reproduce the #17202 failure class on 1.7.1.
3. **Kill times across the whole phase dwell: MET for wide phases** (delay uniform(0, 0.9 x measured dwell), observed 0.1–1.7 s); narrow phases are 0 delay (limit 3). **"Use orphan counts to confirm store state": PARTLY MET.** Orphan counts are recorded per cell, but the counter is inconsistent across versions (limit 7), so I did not use it as a confirmation of reached state; the observer's phase-at-SIGKILL (`reached_target`) is the confirmation used.
4. **Freeze + hash, self-test on that exact file, full output saved: MET** (chaos/FROZEN.sha256; results/v*/checker_selftest.json holds full dicts and the checker sha). One later edit (version assertion for `main`) is disclosed in §9.9.
5. **Explain 192-vs-96: NOT MET.** New evidence only: on 1.7.1 and 1.12.0, an uncontended compaction + expire probe (120 batches, no ingest, no kill) shows dir count = all_files count = 469 afterwards and zero orphans on both versions, so there is no doubling in the plain path. The matrix's 480 (1.7.1) / 960 (1.12.0) orphan figures (`main` not analysed for this) arise in cells with concurrent ingest and a kill; the mechanism is not identified. Also unexplained: the probe's expire left 468 files still referenced although only 1 is in the current snapshot. No verdict depends on it. Smallest next step: log per-file provenance (snapshot id that created each unreferenced file) in a kill-after-expire-commit cell.
6. **Class C true denominator: MET.** `C_applicable` is set only when the maintenance call returned (a success claim exists). Per-version denominators are in the coverage CSV (`C_applicable` column), e.g. 1.7.1: rewrite-a 6, expire-a 6, orphan-a 4, chain-a 4, race-a 8 = 28 of 123 cells; killed, infra and ingest-only cells are excluded. The pilot's 38-row "C=0" denominator is not used.

## 11. Files
FULL-REPORT.md, STEPLOG.md, VERSION-MANIFEST.md, results/v1.7.1/, results/v1.12.0/, results/vmain/ (controls, selftest, matrix, per_interleaving.csv, coverage_matrix.csv), results/coverage_matrix_all_versions.csv; code: chaos/{checker,lib,maint,pyreader,run_matrix,selftest,aggregate2}.py, scripts/{run.sh,run_version.sh,env.sh}, chaos/pilot_v1/ (pilot code + hashes).

## 12. Completion note
**Did:** Step 0 (VETTING read, applied as advisory); built pyiceberg reader, orphan Action-API op, chain/race/infra cell types; froze+hashed checker; ran selftest and both controls on 3 builds; ran full matrices on 1.7.1, 1.12.0 and `main`@2d89aa4 (123 cells each, 0 cell errors); verified the WARN from source; read #17304.
**Found:** no data-loss violation in A/B on any build; #17202-class C reproduced naturally in 1.7.1 race cells (8/8, mapped) but not on 1.12.0 or `main`; #17304 reproduces on all three builds through the core API only; Spark distribution had silently shadowed jars (fixed).
**Blocked:** nothing outstanding. Unverified: cause of the 1.7.1 vs 1.12.0/`main` race difference; orphan-counter anomaly; upstream issue for the WARN.

## 13. ADDENDUM 20 Nov (race re-run) — read before relying on §1, §6 or §9.5
The in-matrix race cells on 1.12.0 and `main` did not overlap (the expire commit landed after the rewrite returned in 16 of 16 cells), so the "0 of 8 on 1.12.0 / `main`" race results above do not support a comparison between builds. A gated re-run with real overlap is in **RACE-RERUN.md** (data: results/race2/per_interleaving.csv): 1.12.0 flagged 8 of 8, `main` 7 of 8, 1.7.1 control 3 of 3 (class C1b, A/B clean, readers agree). Original text above is unchanged.
