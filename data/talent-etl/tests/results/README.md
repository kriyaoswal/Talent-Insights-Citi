# Test evidence (raw output)

Raw output behind [TESTING.md](../../TESTING.md). Regenerate the test files
with `./scripts/test_evidence.sh` from `data/talent-etl`.

| File | What it is |
|---|---|
| [pytest_report.txt](pytest_report.txt) | the full test run: every test by name with its result, then line coverage of `job.py` (first line: date, Python version, commit) |
| [junit.xml](junit.xml) | the same results in JUnit format, for CI tools |
| [coverage_by_area.md](coverage_by_area.md) | coverage per area of the guide's coverage goals (`scripts/coverage_by_area.py`) |
| [aws/run_3f7327b5e4ab.json](aws/run_3f7327b5e4ab.json) | run record of the **first successful AWS run** (EKS + Aurora + S3): per-step counts, durations, CPU, peak memory, the 15 Gold reconciliation checks and the headline answers |
| [aws/run_68f44d213e76.json](aws/run_68f44d213e76.json) | run record of the **AWS rerun**: 0 rows reloaded, 0 files copied, same answers (idempotency) |
| [aws/run_4d1be3727ace.json](aws/run_4d1be3727ace.json) | run record of a **failed AWS run** (Aurora not reachable) |
| [aws/run_4d1be3727ace_log.txt](aws/run_4d1be3727ace_log.txt) | its pod log: the database connection retried after 1 s, 2 s and 4 s, then the job stopped with `status=failed` |

The `aws/` files were downloaded from
`s3://coding-workshop-datalake-<participant>/lake/_runs/` and the job's
Kubernetes pod log on 2026-10-15. The database hostname was removed from the
log; the run records contain no hostnames or secrets.

Performance-test evidence is in [perf/results/](../../perf/results/).
