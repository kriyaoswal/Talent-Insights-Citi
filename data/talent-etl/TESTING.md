# talent-etl: test evidence

How the pipeline was tested against the **Testing Expectations** in
[docs/data-engineer.md](../../docs/data-engineer.md): the commands, what they
produced, and what we conclude. Results are from 2026-10-15 on the workshop VM
(Linux x86_64, 2 CPUs, Python 3.13, local PostgreSQL 18, LocalStack).

## Summary

| Area | Result |
|---|---|
| Automated tests | **203 passed, 0 failed, 0 skipped** (`pytest`, about 2 minutes) |
| Line coverage of `job.py` | **98%**; every coverage goal in the guide met (table below) |
| Functional tests 1–5 | all automated and passing |
| Performance tests 1–5 | all run; **SLA met**, time linear up to 20x, answers exact at every scale |
| Cloud readiness | job run inside the **actual EKS image** in cloud mode (S3 + empty database): Gold identical to local |

## How to run

From `data/talent-etl`, with PostgreSQL loaded (`./scripts/load_source_db.sh`):

```sh
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python job.py                                              # builds the local lake
.venv/bin/python -m pytest -q tests --cov=job --cov-report=term-missing
.venv/bin/python perf/run_perf.py --scales 1,5,10,20                 # about 5 minutes
```

Tests that need PostgreSQL, the local lake or the Jupyter kernel skip
themselves (with the reason) when those are unavailable. S3 tests use
**moto**, a local S3, so no AWS account is needed.

## Functional testing

| # | Guide's test | Where | What is checked | Result |
|---|---|---|---|---|
| 1 | Source ingestion validation | `test_pipeline_integration.py`, `test_cloud_mode.py`, `test_schema_models.py` | Bronze row counts equal the source for all 7 tables (12,173 rows); 783 event files copied with sizes verified; required source columns present (missing column stops the job) | ✅ |
| 2 | Transformation validation | `test_silver_tables.py` (35), `test_silver_events.py` (53), `test_schema_models.py` (25) | every Pydantic rule (types, nulls, allowed values, ranges, formats, dates, foreign keys, cross-field rules), strict dates and booleans, `notes` → `text` rename, duplicate removal, the date repair and its event-ID guardrail | ✅ |
| 3 | Aggregation validation | `test_gold.py` (65) | every Gold definition on a hand-built dataset with known answers; the 7 headline answers on the real data; the "rating thirds" split matches pandas `qcut`; **each of the 18 definition guards rejects a deliberately corrupted row and names the rule** | ✅ |
| 4 | Data quality validation | `test_pipeline_integration.py` | exact quarantine counts and reasons (13 `malformed_json`, 76 `null_employee_id`), 152 duplicates dropped, 53 dates repaired, 666 + 234 rows flagged; line accounting `read = written + quarantined + duplicates` | ✅ |
| 5 | Idempotency validation | `test_pipeline_integration.py`, `test_cloud_mode.py` | the job run **twice**, locally and on S3: Silver and Gold identical, no duplicates, 0 event files re-copied, source database never overwritten | ✅ |

**Key numbers verified on every run** (end-to-end test):

| Layer | Check | Value |
|---|---|---|
| Bronze | tables / event files | 12,173 rows / 783 files |
| Silver | events read → written + quarantined + duplicates | 29,868 → 29,627 + 89 + 152 |
| Gold | reconciliation checks against Silver | 15 / 15 PASS |
| Gold | Q2 critical gaps · Q3 tiers · Q5 Critical · Q6 on-time by third | 441 · 148/90/119 · 143 · 44% / 56% / 94% |

## Performance testing

`perf/run_perf.py` builds 1x, 5x, 10x and 20x copies of the sample data (new
IDs, same quirks), each in its own PostgreSQL database, and runs the job 3
times from scratch plus once incrementally. Full report:
[perf/results/PERFORMANCE.md](perf/results/PERFORMANCE.md) and
[scaling.png](perf/results/scaling.png).

| # | Guide's test | Result |
|---|---|---|
| 1 | Batch throughput | **1.3–1.9 million records per minute**. 1x (42,041 records): 1.9 s; 10x: 13.2 s; 20x (840,060): 30.6 s |
| 2 | Scale (small/medium/large) | Time grows **linearly or better** (fitted exponent 0.91 overall; no step above 1.0). **12/12 correctness checks pass at every scale**: e.g. 357 → 7,140 promotion candidates at 20x, on-time-by-third unchanged |
| 3 | Resource efficiency | CPU-bound on one core. Peak memory **280 MB (1x), 661 MB (5x), 1.13 GB (10x), 2.05 GB (20x)**; the Silver events step holds the parsed events in memory |
| 4 | External dependencies | `test_resilience.py`: 2 dropped database connections recovered with 1 s and 2 s backoff; a failed write in **each** layer recovered after 1 s; wrong password gives up after 1/2/4 s and reports failure; a Gold that breaks a rule leaves the previous Gold byte-for-byte unchanged. `test_retry.py`: permanent errors (file not found, access denied) fail at once |
| 5 | SLA (assumed: ≤ 60 s at 1x, ≤ 300 s at 10x; the brief sets none) | **PASS**: worst 2.8 s at 1x (22x headroom), 15.1 s at 10x (20x headroom). The end-to-end test also checks the 60 s target on every run |

**Capacity:** the EKS job is limited to 2 GiB (`infra/helm/values.yaml`).
Memory grows by about 93 MB per 1x of data and reached 2,052 MB at 20x, so
**about 20x today's volume is the limit**. Time is not the constraint.

## Coverage goals

Measured per area by mapping each function of `job.py` to the guide's categories.

| Guide's goal | Target | Statements | Coverage | Result |
|---|---|---|---|---|
| Core transformation functions | 80%+ | 325 | 95.1% | ✅ |
| Schema and validation rules | 90%+ | 173 | 99.4% | ✅ |
| Error-handling and retry paths | 85%+ | 100 | 96.0% | ✅ |
| Critical business metric calculations | 95%+ | 342 | 100.0% | ✅ |
| End-to-end smoke test, at least one flow | 100% | – | 2 flows: local, and cloud mode on S3 | ✅ |
| Whole file | – | 1,159 | 98% | – |

## Cloud readiness (beyond the guide's list)

| Check | How | Result |
|---|---|---|
| Runs in the EKS job image | `jupyter/all-spark-notebook:latest` (Python 3.11.6), same command as the Kubernetes Job (`pip install -r requirements.txt; python job.py`), `IS_LOCAL=false`, S3 on LocalStack, **empty** database | succeeded in 12.5 s; database loaded from S3 (12,173 rows); 15/15 Gold checks; **0** tracebacks or warnings; **all 11 Gold tables identical to the local run** |
| Cloud mode in CI | `test_cloud_mode.py`: moto S3, SSL to PostgreSQL, empty database seeded by the job | ✅ 4 tests, S3 Gold equal to local Gold |
| Infrastructure | `terraform validate`; Helm chart renders **byte-for-byte unchanged** for jobs without settings and with the Secret + env for ours; `helm lint`; data-lake bucket + source upload applied to LocalStack (783 + 8 files) | ✅ |

## Defects found by testing (and fixed)

| Found by | Defect | Fix |
|---|---|---|
| Gold row models | trend labels swapped: an unchanged rating (4 → 4) was labelled "declined" | condition order fixed; regression test |
| Failure injection | Silver and Gold writes had no retry (only Bronze did) | every storage read/write retries; permanent errors fail at once |
| Wheel check | `numpy==2.5.4` has no build for the EKS image's Python 3.11, so the job would not start | `numpy==2.3.5` (3.11 to 3.13) |
| EKS image run | the image's numexpr/bottleneck are built for NumPy 1 (`_ARRAY_API not found`) | NumPy-2 builds pinned |
| S3 rehearsal | Bronze made about 5 requests per event file on S3 (16 s) | one listing per side (6.6 s) |
| Coverage per area | business metrics at 94.4% (< 95%) | tests that corrupt each guarded field |
| Notebook test | a font warning printed into the saved notebook | font weight fixed; the test now fails on any warning |

## Known limitations

- **No run in real AWS yet**: EKS, Aurora and IAM were rehearsed with the EKS
  image, LocalStack and moto, but not on AWS (credentials expired at the time).
- **Simulated failures**: retry behaviour was proven by injecting errors in code,
  not with real network faults.
- **Scaled data is replicated sample data**: same distributions and error
  rates; skew and new kinds of bad data are not covered.
- **Local disk for performance**: S3 latency would add time to Bronze and to
  reading Silver; the SLA headroom (20x) covers it at today's volume.
- **SLA targets are assumptions**, since the brief defines none.
- **Wrong passwords are retried** (about 7 s before failing): the driver reports
  them like a dropped connection.

## Conclusion

The pipeline meets every functional and performance expectation in the guide,
with 203 automated tests, 98% coverage and each per-area coverage goal met. It
produces the same answers locally, in cloud mode on S3, and inside the EKS job
image, and it is safe to rerun. The capacity limit to watch is memory (about
20x today's volume within the 2 GiB EKS limit).
