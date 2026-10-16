# talent-etl: test evidence

How the pipeline was tested against the **Testing Expectations** in
[docs/data-engineer.md](../../docs/data-engineer.md): the commands, what they
produced, and what we conclude. Results are from 2026-10-15 on the workshop VM
(Linux x86_64, 2 CPUs, Python 3.13, local PostgreSQL 18, LocalStack), and from
the deployed job on AWS (EKS, Aurora PostgreSQL, S3 in `ap-south-1`).

## Summary

| Area | Result |
|---|---|
| Automated tests | **242 passed, 0 failed, 0 skipped** (`pytest`, about 2 minutes); raw output in [tests/results/](tests/results/) |
| Line coverage of `job.py` | **98%**; every coverage goal in the guide met (table below) |
| Functional tests 1–5 | all automated and passing |
| Performance tests 1–5 | all run; time linear up to 20x, answers exact at every scale; **SLA met** locally and on AWS reruns (12 s), **missed on the first AWS load** (75 s) |
| Cloud readiness | job run inside the **actual EKS image** in cloud mode (S3 + empty database): Gold identical to local |
| **Answers cross-checked** | all 7 answers recomputed with **plain SQL and plain Python** (no pipeline code): every Gold row matches |
| **Real AWS run** | deployed job on EKS succeeded twice: **15/15 Gold checks**, all 11 Gold tables in S3 **identical to local**, rerun copied 0 files (details below) |

## How to run

From `data/talent-etl`, with PostgreSQL loaded (`./scripts/load_source_db.sh`):

```sh
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python job.py                                              # builds the local lake
.venv/bin/python -m pytest -q tests --cov=job --cov-report=term-missing
.venv/bin/python perf/run_perf.py --scales 1,5,10,20                 # about 5 minutes
./scripts/test_evidence.sh                                           # full run, saves raw output to tests/results/
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
| 3 | Aggregation validation: **independent cross-check** | `test_answers_crosscheck.py` (9) | every answer recomputed a second way that shares no code with `job.py`: plain SQL on the source PostgreSQL tables (Q1–Q3, Q5–Q7) and plain `json.loads` over the raw event files (Q4). Compared **row by row**: trend, latest rating and risk tier for all 2,000 employees; critical flag on all 4,000 assessments; the 357 candidates and their tiers; courses and certifications per employee; rating third and on-time rate for the 27 teams; all 40 competencies; all 1,595 distribution rows | ✅ |
| – | Bronze immutability (guide's Bronze checklist) | `test_bronze_immutable.py` (5), `test_pipeline_integration.py` | after a rerun, **every Bronze file from the first run is byte-for-byte unchanged** and the rerun added only its own 7 table snapshots; an existing snapshot can't be replaced; a source file changed after landing leaves the landed file untouched and is counted (`files_changed_at_source`) | ✅ |

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
| Core transformation functions | 80%+ | 330 | 95.2% | ✅ |
| Schema and validation rules | 90%+ | 173 | 99.4% | ✅ |
| Error-handling and retry paths | 85%+ | 103 | 96.1% | ✅ |
| Critical business metric calculations | 95%+ | 342 | 100.0% | ✅ |
| End-to-end smoke test, at least one flow | 100% | – | 2 flows: local, and cloud mode on S3 | ✅ |
| Whole file | – | 1,264 | 98% | – |

## Cloud readiness (beyond the guide's list)

| Check | How | Result |
|---|---|---|
| Runs in the EKS job image | `jupyter/all-spark-notebook:latest` (Python 3.11.6), same command as the Kubernetes Job (`pip install -r requirements.txt; python job.py`), `IS_LOCAL=false`, S3 on LocalStack, **empty** database | succeeded in 12.5 s; database loaded from S3 (12,173 rows); 15/15 Gold checks; **0** tracebacks or warnings; **all 11 Gold tables identical to the local run** |
| Cloud mode in CI | `test_cloud_mode.py`: moto S3, SSL to PostgreSQL, empty database seeded by the job | ✅ 4 tests, S3 Gold equal to local Gold |
| Infrastructure | `terraform validate`; Helm chart renders **byte-for-byte unchanged** for jobs without settings and with the Secret + env for ours; `helm lint`; data-lake bucket + source upload applied to LocalStack (783 + 8 files) | ✅ |

## Real AWS run

Deployed with `./bin/deploy-backend.sh` (`TF_VAR_aws_eks_enabled=true`), as in
the guide. The job runs as a Kubernetes Job on EKS, reads Aurora over SSL, and
reads and writes `s3://coding-workshop-datalake-<participant>/`. Numbers below
are from the run records in `lake/_runs/run_date=2026-10-15/`, copied to
[tests/results/aws/](tests/results/aws/) together with the failed run's pod log.

| Step | First run `3f7327b5e4ab` (empty Aurora, empty lake) | Rerun `68f44d213e76` (same inputs) |
|---|---|---|
| `source.seed` | Aurora schema created, **12,173 rows** loaded from S3 (1.6 s) | data already present, **0 rows** loaded (0.04 s) |
| `bronze.postgres` | 12,173 rows, matches `count(*)` (0.4 s) | 12,173 rows, same date's snapshot overwritten (0.8 s; before Bronze became write-once, see below) |
| `bronze.learning_activity` | **783 files copied** (63.3 s) | 783 unchanged, **0 copied** (0.3 s) |
| `silver.tables` | 12,173 → 12,173, 0 quarantined (1.2 s) | same (1.1 s) |
| `silver.learning_activity` | 29,868 lines → 29,627 + 89 quarantined + 152 duplicates; 53 dates repaired, 39 `notes` renamed (5.6 s) | identical counts (6.4 s) |
| `gold` | 11 tables, **15/15 checks PASS** (2.9 s) | **15/15 PASS** (3.4 s) |
| **Whole job** | **75.0 s**, peak memory 334 MB, exit 0 | **12.0 s**, peak memory 336 MB, exit 0 |

**Gold on S3 equals Gold built locally.** All 11 tables were read back from
S3 and compared with the local run using `pandas.testing.assert_frame_equal`:
identical (e.g. 2,000 attrition rows, 357 promotion candidates, 27 teams). The
notebook also ran against S3 Gold with `LAKE_ROOT=s3://…/lake`: 0 errors, 0
warnings, 14 charts.

**What this adds to the earlier evidence:**

| Guide's test | Evidence on AWS |
|---|---|
| Source ingestion | every Bronze row count equals Aurora's `count(*)`; 783 event files copied with sizes checked |
| Idempotency | the rerun loaded 0 rows into Aurora, copied 0 files, and produced the same Silver counts and Gold |
| Resource efficiency | 336 MB peak, well within the job's 1 GiB request and 2 GiB limit |
| External dependencies | in the two failed runs below, a **real** network fault: the database connection was retried after 1 s, 2 s and 4 s, then the job gave up after 4 attempts (`retry.exhausted`) and ended with `status=failed` and exit code 1, as designed. The second run also wrote its run record; the first could not, because S3 access was the next problem |
| SLA (assumed ≤ 60 s at 1x) | **rerun: PASS (12 s). First run: missed (75 s)** because it copies all 783 event files to S3 one at a time. Later runs copy only new files, so daily runs meet the target |

**Problems found by deploying, and fixed:**

| Symptom | Cause | Fix |
|---|---|---|
| Runs `5c6ef5706f98`, `4d1be3727ace`: `ConnectionTimeout` to Aurora after 47 s of retries | Aurora's security group did not allow the EKS pods | Aurora also joins the EKS cluster security group (`infra/rds.tf`) |
| Job could not reach S3: `NoCredentialsError` | pods have no AWS role, and the node's metadata hop limit blocks the node role | EKS Pod Identity: the job's own role, allowed only on the data-lake bucket (`infra/eks.tf`) |
| First pod after creating Pod Identity: `NoCredentialsError` at 1.5 s; Kubernetes' retry succeeded | the pod started 2 s after the association, before credentials were being injected | wait 30 s after a new association before starting the job (`null_resource.pod_identity_wait`) |
| Deploy planned to remove a tag from Aurora | the account's managed-services monitoring adds the tag `ams:rt:ams-monitoring-policy` | Terraform ignores that tag (`lifecycle.ignore_changes`) |
| Notebook on S3: `No AWSAccessKey was presented` | the S3 library ignored `AWS_REGION` and sent requests to `us-east-1` | also set `AWS_DEFAULT_REGION=ap-south-1` (README) |

## Where the evidence is stored

| Evidence | Location |
|---|---|
| Every test by name with its result, line coverage | [tests/results/pytest_report.txt](tests/results/pytest_report.txt), [junit.xml](tests/results/junit.xml) |
| Coverage per area | [tests/results/coverage_by_area.md](tests/results/coverage_by_area.md) |
| AWS run records and the failed run's log | [tests/results/aws/](tests/results/aws/) |
| Performance report, raw timings, chart | [perf/results/](perf/results/) |
| The tests | [tests/](tests/) |

`./scripts/test_evidence.sh` reruns the suite and rewrites the test files.

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

- **First full load on AWS takes 75 s**, above the assumed 60 s target. It is
  a one-time cost (copying 783 small files to S3 one by one); copying them in
  parallel would shorten it.
- **Failures are only logged**: there is no alerting. A failed run is visible
  in the logs, the run record (`status=failed`) and the Kubernetes Job status.
- **Simulated failures**: retry behaviour was proven by injecting errors in
  code. On AWS we also saw it handle a real network fault (the Aurora timeouts
  above), but not S3 throttling.
- **Write-once Bronze is not deployed to AWS yet.** It was added after the AWS
  runs above and tested locally and on moto S3. It takes
  effect on AWS at the next deploy.
- **The 30 s Pod Identity wait was not exercised on AWS**: it only runs when
  the association is first created, which had already happened.
- **Scaled data is replicated sample data**: same distributions and error
  rates; skew and new kinds of bad data are not covered.
- **Local disk for performance**: S3 latency would add time to Bronze and to
  reading Silver; the SLA headroom (20x) covers it at today's volume.
- **SLA targets are assumptions**, since the brief defines none.
- **Wrong passwords are retried** (about 7 s before failing): the driver reports
  them like a dropped connection.

## Conclusion

The pipeline meets every functional and performance expectation in the guide,
with 242 automated tests, 98% coverage and each per-area coverage goal met. It
produces the same answers locally, in cloud mode on S3, inside the EKS job
image, and **deployed on AWS**, and it is safe to rerun. Two things to watch:
memory (about 20x today's volume within the 2 GiB EKS limit) and the first full
load on AWS (75 s against an assumed 60 s target; reruns take 12 s).
