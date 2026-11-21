# iceberg-maintenance-fault-injection

A fault-injection (chaos engineering) checker for Apache Iceberg table maintenance, plus the full results of a 369-cell crash-interleaving study across three Iceberg builds.

The checker runs each maintenance operation — `rewrite_data_files` (compaction), `expire_snapshots` (snapshot expiry) and `remove_orphan_files` (orphan removal) — against a live ingest, kills or pauses the maintenance process at chosen phases, and then checks the table against a ledger of acknowledged writes. It scores three classes separately:

- **A** — data integrity (no acknowledged write lost, no live file deleted, no duplicate or resurrected rows)
- **B** — retained snapshots and tags still read their recorded row multisets
- **C** — a maintenance call's reported progress matches what actually committed

A second, independent reader (pyiceberg) repeats the A and B checks on every cell.

## Findings (summary)

- Classes A and B recorded **no violation in any of the 369 cells** (123 cells × Iceberg 1.7.1, 1.12.0 and `main` @ 2d89aa4), and the two readers never disagreed. No novel violation was found.
- The only class C flags map to the existing open issue [apache/iceberg#17202](https://github.com/apache/iceberg/issues/17202): a partial-progress rewrite can return success while only some of its commits land, after a concurrent expiry removes its starting snapshot. A gated re-run that forced the expiry commit inside the rewrite's commit window flagged **3/3 cells on 1.7.1, 8/8 on 1.12.0 and 7/8 on `main`**. Reported counts were exact in every flagged cell; only `failed_data_files_count = 0` misleads.
- A positive control for live-file deletion ([apache/iceberg#17304](https://github.com/apache/iceberg/issues/17304), closed as spec-discouraged and contested) is detected by classes A and B on all three builds through the core API, but does not occur through the Spark procedure or on a tagged table.

The negative result is narrow: an unpartitioned, append-only, tagged table, a Postgres JDBC catalog and single-node MinIO. Full method, per-cell results and limitations are in the paper and in `FULL-REPORT.md`.

## Layout

- `chaos/` — the checker (`checker.py`), maintenance drivers (`maint.py`, `maint_race2.py`), the independent reader (`pyreader.py`), the matrix runner (`run_matrix.py`), the gated race re-run (`race2.py`) and the checker self-tests (`selftest.py`)
- `scripts/` — environment and run scripts used for the study
- `results/` — per-cell result files for every build (`v1.7.1/`, `v1.12.0/`, `vmain/`), the gated race re-run (`race2/`), the coverage matrices (`coverage_matrix*.csv`, `per_interleaving.csv`) and the checker self-test output
- `FULL-REPORT.md` — the full study report
- `RACE-RERUN.md` — the gated race re-run report

## Running it

You need Python 3, a Spark distribution with the Iceberg runtime for the build under test, a Postgres JDBC catalog and an S3-compatible store (the study used single-node MinIO). See `scripts/env.sh` for the environment layout the study used, and `chaos/run_matrix.py` for the cell matrix. The checker self-tests (`chaos/selftest.py`) run without a cluster and verify the checker catches seeded violations of each class.

## Paper

Sajish Krishnan, *Fault Injection and Verification for Lakehouse Table Maintenance: Crash-Testing Apache Iceberg Compaction, Snapshot Expiry and Orphan Removal*, November 2026. DOI: [10.5281/zenodo.24412833](https://doi.org/10.5281/zenodo.24412833)

## How to cite

```bibtex
@misc{krishnan2026iceberg,
  author       = {Krishnan, Sajish},
  title        = {Fault Injection and Verification for Lakehouse Table Maintenance:
                  Crash-Testing Apache Iceberg Compaction, Snapshot Expiry and Orphan Removal},
  year         = {2026},
  publisher    = {Zenodo},
  doi          = {10.5281/zenodo.24412833},
  url          = {https://doi.org/10.5281/zenodo.24412833}
}
```

## Licence

Apache-2.0. See `LICENSE`.
