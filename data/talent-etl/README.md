# talent-etl

Batch pipeline for the **Performance & Talent Management** scenario: PostgreSQL
source tables and raw learning-activity events flow through Bronze → Silver →
Gold layers for analytics.

```
PostgreSQL (7 tables) ─┐
                       ├─► Bronze (raw) ─► Silver (clean) ─► Gold (curated) ─► Jupyter
Learning events JSONL ─┘
```

| Layer  | Status          |
|--------|-----------------|
| Bronze | Implemented     |
| Silver | Implemented     |
| Gold   | Implemented     |

## Quick start (local)

Requires PostgreSQL running on `localhost:5432` (see `bin/setup-environment.sh`)
and Python 3.11+. Run from this directory:

```sh
# 1. Load the sample CSVs into PostgreSQL (creates database talent_mgmt; safe to rerun)
./scripts/load_source_db.sh

# 2. Install dependencies
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

# 3. Run the pipeline
.venv/bin/python job.py 2>&1 | tee /tmp/talent-etl.log

# 4. Run the tests
.venv/bin/python -m pytest -q tests
```

No environment variables are needed locally. Defaults match the workshop's
local PostgreSQL (`postgres` / `postgres123`). See [.env.sample](.env.sample)
to override them.

## Configuration

| Variable            | Local default                        | Cloud (`IS_LOCAL=false`) |
|---------------------|--------------------------------------|--------------------------|
| `IS_LOCAL`          | `true`                               | `false`                  |
| `POSTGRES_HOST`     | `localhost`                          | required                 |
| `POSTGRES_PORT`     | `5432`                               | required                 |
| `POSTGRES_NAME`     | `talent_mgmt`                        | required                 |
| `POSTGRES_USER`     | `postgres`                           | required                 |
| `POSTGRES_PASS`     | `postgres123`                        | required                 |
| `LAKE_ROOT`         | `./lake` (next to `job.py`)          | required, e.g. `s3://…`  |
| `EVENTS_SOURCE_URI` | `sampleData/…/learning_activity`     | required, e.g. `s3://…`  |
| `SEED_SOURCE_URI`   | unset (local DB loaded by script)    | set by Terraform: `s3://…/source/postgres_seed` |
| `RUN_DATE`          | today (UTC)                          | optional                 |
| `LOG_LEVEL`         | `INFO`                               | optional                 |

Cloud mode connects to PostgreSQL with `sslmode=require` (Aurora). Local
paths and `s3://` URIs both go through fsspec, so cloud runs need no code
changes. In the cloud every variable is set by Terraform (`infra/eks.tf`,
`local.python_job_env`); the password is passed through a Kubernetes Secret.

**Loading Aurora (`SEED_SOURCE_URI`).** Aurora is private to the VPC, so the
job loads the sample tables itself when `SEED_SOURCE_URI` is set: it creates
the schema from `schema.sql` only if none of the tables exist, and loads only
empty tables, so a populated source system is never overwritten. A partially
created schema stops the job instead of guessing.

## Deploy to AWS (EKS)

As in [docs/data-engineer.md](../../docs/data-engineer.md):

```sh
# once: enable the EKS environment for data jobs
echo "export TF_VAR_aws_eks_enabled=true" >> ~/.bashrc && source ~/.bashrc

# from the repository root (needs valid AWS credentials)
./bin/deploy-backend.sh

# follow the run
kubectl logs -f job/coding-workshop-talent-etl
```

The deploy creates the data-lake bucket
`s3://coding-workshop-datalake-<participant>/`, uploads the sources to
`source/` (only when they change), and starts the job with every setting it
needs. The job then loads Aurora, writes `lake/bronze|silver|gold`, and records
a run manifest under `lake/_runs/`. The first deploy takes about 15–20 minutes
because it creates the EKS cluster. Read Gold from the notebook with
`LAKE_ROOT=s3://coding-workshop-datalake-<participant>/lake` (see
[Notebook](#notebook)).

**Deployed and verified on AWS** (details in [TESTING.md](TESTING.md#real-aws-run)):
the first run took 75 s, loaded the empty Aurora and passed all 15 Gold checks;
a rerun took 12 s and copied nothing. The Gold in S3 is identical to the local
Gold.

What the deploy sets up for the job, beyond the workshop defaults (`infra/`):

| Piece | Why |
|---|---|
| Data-lake bucket + source upload (`datalake.tf`) | the job's input and output; sources are uploaded only when they change |
| Job settings + Kubernetes Secret (`eks.tf`, Helm chart) | every variable in [Configuration](#configuration); the password never appears in a command line |
| **EKS Pod Identity** (`eks.tf`) | the job's own AWS role, allowed only on the data-lake bucket. Pods can't use the node's role, and the job doesn't need it |
| 30 s wait after a new Pod Identity association | a pod started sooner gets no credentials |
| Helm release reinstalled on every deploy | the Job is recreated, so every deploy runs the pipeline |
| Aurora in the EKS cluster security group (`rds.tf`) | lets the job's pods reach Aurora on port 5432 |
| Aurora ignores the `ams:rt:ams-monitoring-policy` tag (`rds.tf`) | the account's monitoring adds it; without this every deploy tries to remove it |

The job is a batch run, not a service: Kubernetes deletes the finished Job
after 100 s. Its results stay in S3 (`lake/gold/`) and its run record in
`lake/_runs/`.

## Lake layout

```
lake/
├── bronze/
│   ├── postgres/<table>/ingest_date=YYYY-MM-DD/data.parquet
│   └── learning_activity/year=YYYY/month=MM/day=DD/events-000.jsonl
├── silver/
│   ├── <table>/data.parquet
│   ├── learning_activity/event_year=YYYY/event_month=MM/data.parquet
│   └── _quarantine/<dataset>/run_date=YYYY-MM-DD/rejected.parquet   rejected rows + reject_reasons
├── gold/<table>/data.parquet       11 tables answering Q1–Q7 (see Gold)
└── _runs/run_date=YYYY-MM-DD/<run_id>.json    run manifest: per-step counts, durations, status
```

## Bronze

- **PostgreSQL tables** are snapshotted in full to Parquet with their source
  types. Each row also gets `_source`, `_ingested_at` and `_run_id` columns.
  Every snapshot is checked against `SELECT count(*)`. A rerun on the same
  `RUN_DATE` overwrites that date's snapshot, so there are never duplicates.
  A different date writes a new snapshot alongside it.
- **Learning events** are copied byte-for-byte, keeping the source
  `year=/month=/day=` partitions. A file already present with the same size is
  skipped, so reruns only copy new files. Sizes are checked after every copy.
  Malformed lines are *not* filtered here; that happens in Silver, so Bronze
  stays an exact raw copy.
- **Bronze contract:** each source table must still have every column its
  Silver model needs (`check_columns`). If one disappears, the job stops at
  ingestion with `SchemaDriftError`. Extra columns are landed and logged.
  Event files must follow the `year=YYYY/month=MM/day=DD/*.jsonl` layout.

## Schema validation

Following the brief's *"Pydantic or custom Python validators"*, the job uses
both, each for what it does best:

| Layer | Pydantic models (one record at a time) | Custom Python validators (whole dataset) |
|---|---|---|
| Bronze | – | `check_columns`: required source columns present; event file layout |
| Silver tables | `Team`, `Competency`, `Employee`, `Project`, `PerformanceReview`, `CompetencyAssessment`, `TalentReview` | unique primary / business keys, row reconciliation, soft-rule flags |
| Silver events | `LearningEvent` | duplicates, date repair + event-ID guardrail, line reconciliation |
| Gold | one model per table (`ReviewHistoryRow`, `SkillGapRow`, `PromotionCandidateRow`, …) that also re-checks each business definition per row | unique grain, 15 reconciliation checks against Silver |

The models live in `job.py` under **Schema contracts**. They encode types,
required fields, allowed values (`Literal`), ranges, formats, cross-field rules
(`model_validator`), dates not after `RUN_DATE`, and foreign keys. Foreign
keys and `RUN_DATE` are passed in through Pydantic's validation context, so
each model can also be used on its own.

Two deliberate tightenings of Pydantic's defaults:
- **Booleans** are rejected where a number is expected. By default Pydantic
  reads `True` as `1`.
- **Dates** must be a real date or a `YYYY-MM-DD` string. By default Pydantic
  also accepts epoch numbers.

**Reason codes.** Validation errors become stable codes stored in
`reject_reasons`:

| Code | Meaning |
|---|---|
| `null_<field>` | required value missing or blank |
| `missing_field_<field>` / `unexpected_field_<field>` | key absent / not in the contract (events) |
| `invalid_type_<field>` | wrong type or unparseable (e.g. `"abc"` as an ID, `55.5` as a score) |
| `not_allowed_<field>` | not one of the allowed values |
| `out_of_range_<field>` | outside its numeric range |
| `bad_format_<field>` | doesn't match its pattern (email, `YYYY-H1`, `EV` + 10 digits) |
| `future_<field>` | date after `RUN_DATE` |
| `unknown_<field>` | foreign key with no valid parent row |
| `duplicate_<key>` | primary or business key not unique |
| rule name | cross-field rule, e.g. `submitted_before_period_end`, `missing_quiz_score` |

## Silver

Silver is rebuilt from Bronze on every run (overwrite, so it's idempotent).
Every input record ends up in exactly one place: a Silver row, a quarantine
row, or the dropped-duplicate count. The job checks this and fails if the
numbers don't add up. A rejected row keeps its original values, all the reasons
it failed (`;`-separated), and for events its source file, line number and raw
line.

### Tables: schema contracts

The Pydantic models (registered in `TABLE_CONTRACTS` with each table's key
rules) restate `schema.sql`'s rules and add the ones it lacks:

| Rule type | Examples |
|---|---|
| Required / type | no nulls (except `manager_comment`); ints, decimals, `YYYY-MM-DD` dates cast strictly |
| Allowed values | `department` (10), `job_level` (7), `category`, `outcome_status`, talent ratings |
| Ranges | `rating`, proficiency/required levels 1–5; `outcome_score` 0–100 |
| Formats | `email`; `review_period` / `review_cycle` = `YYYY-H1`/`YYYY-H2` |
| Dates | `hire_date`, `submitted_date`, `assessed_date` not after `RUN_DATE`; review submitted after its period ends |
| Keys | unique primary key; unique `email`, team/competency `name`, (employee, period), (employee, competency) |
| Foreign keys | checked against the parent's *valid* Silver rows, so rejections cascade |

A missing contract column fails the job (schema drift). Extra columns are
dropped with a warning.

**Soft rules (flagged, kept).** Some rows are suspicious but plausible, so
they get a `flag_*` column instead of being quarantined:

| Flag | Rows | Meaning |
|---|---|---|
| `performance_reviews.flag_period_before_hire` | 666 | review period ended before the employee's hire date |
| `competency_assessments.flag_assessed_before_hire` | 234 | assessed before the employee's hire date |

Quarantining them would drop 17% of reviews. Gold should decide per metric
whether to exclude them.

### Learning events: rules

| # | Rule | Handling | Sample count |
|---|---|---|---|
| 1 | Line is a JSON object | quarantine `malformed_json` | 13 |
| 2 | Known drift `notes` → `text` | **rename**, keep row (quarantine if both present and different) | 39 |
| 3 | Exact duplicate records | **drop** extra copies | 152 |
| 3 | Same `event_id`, different content | quarantine all copies `conflicting_duplicate_event_id` | 0 |
| 4 | `LearningEvent` model: no unknown or missing fields | quarantine `unexpected_field_*` / `missing_field_*` | 0 |
| 4 | `employee_id` present, numeric, in Silver `employees` | quarantine `null_employee_id` / `invalid_type_employee_id` / `unknown_employee_id` | 76 / 0 / 0 |
| 4 | `event_id` = `EV` + 10 digits; known `event_type`; non-empty `course_name` | quarantine `bad_format_event_id` / `not_allowed_event_type` / `null_course_name` | 0 |
| 4 | `score` integer 0–100, required on quiz events, absent on others | quarantine `invalid_type_score` / `out_of_range_score` / `missing_quiz_score` / `unexpected_score` | 0 |
| 5 | `event_date` strict `YYYY-MM-DDTHH:MM:SS` on its partition day | quarantine `event_date_partition_mismatch` | 0 |
| 5 | `event_date` invalid | **repair and flag** (below) | 53 |

Empty files are logged and counted (`empty_files`), not treated as failures.

### Learning events: date repair and flag

Two independent signals agree on every bad-date event's **day**:

1. **Partition folder.** All 29,801 events with a valid date sit in the folder
   for their own day.
2. **Event ID.** IDs are issued in one contiguous block per day, with no
   overlap across 782 days. For example, 2023-09-05 is `EV…039–076`.

So an invalid `event_date` (`yesterday`, `""`, `N/A`, `2026-13-45`, …) is
repaired to **the partition day at 00:00**, and the row gets
`event_date_repaired = true` and `event_date_raw = <original value>`.

**Guardrail:** the repair only happens if the event's ID falls after every
trusted ID of earlier days and before every trusted ID of later days.
("Trusted" means events with a valid timestamp on their own partition day.)
Otherwise the row is quarantined as `unrepairable_event_date`.

The time of day can't be recovered, because timestamps within a file are
unordered. **Exclude `event_date_repaired` rows from any time-of-day
analysis.** Daily, monthly and team aggregates can use them.

54 bad-date lines → 53 repaired. One (`EV0000010344`) is an exact duplicate,
dropped by rule 3.

### Silver event schema

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | unique |
| `employee_id` | int64 | cast from string |
| `event_ts` | timestamp | 00:00 when repaired |
| `event_date` | date | |
| `event_type`, `course_name` | string | |
| `text` | string | includes values from legacy `notes` |
| `score` | Int64 | null except quiz events |
| `event_date_repaired` | bool | |
| `event_date_raw` | string | original value, only when repaired |
| `source_file`, `source_line` | string, int64 | lineage back to Bronze |

### Sample run results

| Dataset | Read | Silver | Quarantined | Duplicates dropped | Repaired | Flagged |
|---|---|---|---|---|---|---|
| 7 tables | 12,173 | 12,173 | 0 | – | – | 900 |
| Learning events | 29,868 lines | 29,627 | 89 (13 malformed, 76 no employee) | 152 | 53 | – |

## Gold

Gold answers the seven business questions. Each table is built only from
Silver by a pure function (`build_*` in `job.py`) and checked in two ways
before anything is written:

- **Row models.** Every row is validated against a Pydantic model that checks
  types and ranges and **re-checks the business definition on each row**. For
  example, `SkillGapRow` requires `is_critical == (shortfall >= 2 or (is_key and
  shortfall >= 1))`. These checks caught a real bug during development: trend
  labels were mismatched (a 4 → 4 rating was labelled "declined").
- **Reconciliation with Silver.** 15 checks: row counts, review, event and
  assessment totals, completions, candidates. All must match Silver exactly.

If either fails, the job stops and the previous Gold is left untouched.
Otherwise `gold/` is rebuilt from scratch (idempotent).

### Definitions

Constants at the top of the Gold section in `job.py`:

| Term | Definition |
|---|---|
| Valid review | not dated before the employee's hire (`flag_period_before_hire = false`). Only valid reviews feed ratings, trends and averages; Q1 history still shows all |
| Trend | latest period rating − previous period rating (2025-H1 vs 2024-H2), both valid; otherwise `insufficient_data` |
| Latest rating | most recent valid rating, any period |
| Key competency | required at level ≥ `KEY_REQUIRED_LEVEL` (4) |
| Critical gap | shortfall ≥ `CRITICAL_SHORTFALL` (2), or any shortfall on a key competency |
| Promotion tier | among potential High + Ready Now: **A** latest rating ≥ 4, **B** = 3, **C** otherwise (≤ 2 or none) |
| Retention priority | tier A and recorded attrition risk High |
| Performance signal | rating declined, or latest rating ≤ `LOW_RATING_MAX` (2) |
| Risk tier | **Critical** = signal + High recorded risk; **High** = signal + Medium, or High without signal; **Watch** = signal + Low; **Low** = rest |
| Low performer | person's average valid rating ≤ 2 |
| Rating band | teams with projects split into thirds by average rating |
| Project type | last word of the project name (Launch / Rollout / Migration / Revamp) |

### Tables and answers

| Q | Table | Grain (rows) | Headline answer (sample run) |
|---|---|---|---|
| 1 | `employee_review_history` | review (4,000) | every rating with comment, period, team; before-hire reviews flagged |
| 1 | `employee_performance_summary` | employee (2,000) | 721 improved, 692 stable, 191 declined, 396 insufficient data; average 3.03 → 3.42 |
| 2 | `employee_skill_gaps` | assessment (4,000) | **441 employees** (22%) have ≥ 1 critical gap (465 assessments). Worst: Stakeholder Management (18.5%) |
| 3 | `promotion_candidates` | candidate (357) | **148 tier A** (promote now), 90 B, 119 C; 18 tier A are also high attrition risk |
| 4 | `employee_learning` | employee (2,000) | **1,661** completed ≥ 1 course, **1,154** certified |
| 4 | `employee_course_completions` | employee × course (5,118) | each completion / certification with first date |
| 5 | `attrition_watchlist` | employee (2,000) | **143 Critical**, 294 High, 171 Watch, 1,392 Low; 42 Critical are high potential |
| 6 | `team_performance` | team with projects (27) | top-third teams **94% on time** vs bottom third **44%** |
| 6 | `project_performance` | project (33) | outcome, type and team rating per project; Migrations only 38% on time |
| 7 | `skill_distribution` | competency × department × level (1,595) | headcount at each proficiency level |
| 7 | `competency_summary` | competency (40) | 75% of assessments meet the requirement; weakest Stakeholder Management (avg 2.62) |

Headline numbers are also logged (`event=gold.highlights`) and stored in each
run manifest.

### Caveats per question

- **Q1/Q5:** only two review periods, so a trend is one change. 396 employees
  have no measurable trend because of before-hire reviews. Ratings rose in
  every department (+0.32 to +0.50), which suggests a change in how ratings
  were given.
- **Q2/Q7:** each employee is assessed on only 2 of 40 competencies (82–126
  assessments per competency), so this is a sample, not a full skills inventory.
- **Q3:** potential rating doesn't track performance (average rating is
  similar for High, Medium and Low potential), hence the rating-based tiers.
- **Q4:** completions and certifications are counted directly. Most have no
  matching enrolment or completion event, so "completion rate" is not
  meaningful. There is no data for development plans.
- **Q5:** recorded risk is corroborated by rating change (r = −0.30), but
  there is no data on who actually left.
- **Q6:** 27 teams; no record of who worked on which project and no project
  dates, so this shows association, not cause.

## Notebook

[`notebooks/talent_insights.ipynb`](notebooks/talent_insights.ipynb) answers all
seven questions for non-technical readers. It reads **only Gold** and is saved
with its outputs, so the charts and answers are visible without running it.

| Section | Contents |
|---|---|
| Executive summary | one-line answer per question, computed from Gold |
| Where the data comes from | Bronze → Silver → Gold counts, quarantine and repairs from the latest run manifest |
| Q1–Q7 | each: the answer in plain language, how it was worked out, charts, a table view, caveats |
| Appendix | definitions, data-quality notes, how to rerun |

Every number in the text is computed live, so it can't drift from the data.
`show_employee(id)` and `show_training(id)` give the per-employee view for Q1
and Q4. The charts use one validated palette: two shades of blue for the two
review periods, an ordinal blue ramp for tiers, and the status colours only for
risk tiers, always with a text label.

**Open it:** pick the kernel **"Python (talent-etl)"** (VS Code: *Select
Kernel → Jupyter Kernel*), or point VS Code at `.venv/bin/python`. One-time
setup:

```sh
.venv/bin/pip install -r requirements-notebook.txt
.venv/bin/python -m ipykernel install --user --name talent-etl --display-name "Python (talent-etl)"
```

**Rerun it after a pipeline run:**

```sh
.venv/bin/python job.py
~/.local/bin/jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=talent-etl notebooks/talent_insights.ipynb
```

**Read Gold from S3** instead of `lake/`: set `LAKE_ROOT` and the region before
starting Jupyter, from the repository root:

```sh
set -a && source ENVIRONMENT.config && set +a        # AWS credentials
export AWS_DEFAULT_REGION=ap-south-1 LAKE_ROOT=s3://coding-workshop-datalake-<participant>/lake
cd data/talent-etl/notebooks && ../.venv/bin/jupyter lab
```

`AWS_DEFAULT_REGION` is needed: the S3 library ignores `AWS_REGION`, sends the
request to `us-east-1` and fails with `No AWSAccessKey was presented`.

## Performance

Run `.venv/bin/python perf/run_perf.py --scales 1,5,10,20` (about 5 minutes). It
builds 1x, 5x, 10x and 20x copies of the sample data with new IDs, each in its own PostgreSQL database,
and runs the job 3 times from scratch plus once incrementally per size. Full
results: [perf/results/PERFORMANCE.md](perf/results/PERFORMANCE.md) and
[scaling.png](perf/results/scaling.png). Every run also records each step's CPU
time and peak memory in its run summary.

Results on this machine (2 CPUs, local disk), median of 3 runs (details in [TESTING.md](TESTING.md)):

| Test (from the data engineer guide) | Result |
|---|---|
| **Throughput** | about **1.3–1.9 million records per minute**; 1x (42,041 records) in 1.9 s, 10x (420,050) in 13.2 s, 20x (840,060) in 30.6 s |
| **Scale** | time grows **linearly or better** with volume, from 1x to 20x (fitted exponent 0.91 overall; no step above 1.0). Every answer scales **exactly** with the data, e.g. 357 → 7,140 promotion candidates at 20x (12 of 12 checks pass at every size) |
| **Resources** | CPU-bound on one core (CPU time ≈ wall time). Peak memory **280 MB at 1x, 661 MB at 5x, 1.13 GB at 10x, 2.05 GB at 20x**. The jump happens in the Silver events step, which holds every parsed event in memory at once |
| **External dependencies** | `tests/test_resilience.py`: the job recovers from 2 dropped database connections and from a failed write in each layer, with backoff (1 s, 2 s, 4 s). It gives up cleanly on permanent errors, and a failed Gold leaves the previous Gold untouched |
| **SLA** (assumed: ≤ 60 s at 1x, ≤ 300 s at 10x) | **PASS**: worst 2.8 s at 1x and 15.1 s at 10x (20-22x headroom). Even 20x finished within 40 s. The end-to-end test also checks the 60 s target on every run |

**Capacity limit: memory, not time (confirmed).** Memory grows by about 93 MB
per 1x of data. The EKS job is configured with 1 GiB requested and 2 GiB maximum
(`infra/helm/values.yaml`). At **20x the measured peak was 2,050-2,064 MB**,
right at the 2,048 MB limit, so in the cloud a 20x run would be stopped for
running out of memory while still finishing in about 30 s. It is also above the
1 GiB request from about 10x. Beyond that,
process events month by month (they're already partitioned that way) so only one
month is in memory at a time, or raise the memory limit.

## Data quality findings

From profiling the sample data. Each one is handled as described in
[Silver](#silver).

| Issue | Count |
|---|---|
| Truncated / invalid JSON lines | 13 |
| Duplicate `event_id` (exact copies, same file) | 152 |
| Null/empty `employee_id` | 76 |
| Invalid `event_date`: `yesterday` 15, `""` 15, `N/A` 13, impossible `2026-13-45` 11 | 54 |
| Schema drift: `notes` field instead of `text` | 39 |
| Empty file (0 bytes): `year=2025/month=09/day=03` | 1 |
| `employee_id` stored as a string | all |
| Review period ended before hire date | 666 |
| Competency assessed before hire date | 234 |

Every valid `event_date` falls on its file's `year/month/day` partition.
`score` is present only on `quiz_attempted` / `quiz_passed` events. The seven
PostgreSQL tables pass every `schema.sql` rule and every extra contract rule.

## Testing

```sh
.venv/bin/python -m pytest -q tests --cov=job --cov-report=term-missing
```

203 tests, 98% line coverage of `job.py`, and every per-area coverage goal in
the guide met. Commands, results and conclusions: **[TESTING.md](TESTING.md)**.

| File | Covers |
|---|---|
| `test_notebook.py` | the notebook runs top to bottom against Gold with no errors, 14+ charts, and an answer for every question. Skipped without a Gold layer or the `talent-etl` kernel |
| `test_gold.py` | every Gold definition on a hand-built dataset with known answers; each of the 18 definition guards rejects a deliberately corrupted row; grain and reconciliation failures |
| `test_schema_models.py` | the Pydantic models on their own: strict dates and booleans, whitespace, positive IDs, context-driven foreign keys and future dates, model-level rules, Bronze column contract |
| `test_silver_events.py` | every event rule, including the date repair and its guardrail |
| `test_silver_tables.py` | every contract rule type, cascading foreign keys, schema drift, soft flags |
| `test_config.py` | local defaults, required cloud variables, no password in logs |
| `test_resilience.py` | the real job under injected failures: dropped database connections, a failed write in each layer, permanent failure, failed Gold leaving previous Gold intact |
| `test_retry.py` | backoff timing, giving up after max attempts, no retry on non-transient errors |
| `test_cloud_mode.py` | cloud mode end to end on S3 (moto, no AWS): `IS_LOCAL=false`, SSL to PostgreSQL, an empty database the job loads from S3, Gold on S3 equal to local Gold, second run changes nothing, incomplete source schema refused |
| `test_pipeline_integration.py` | real PostgreSQL + sample events end to end, run **twice** (exact expected counts and headline answers, reconciliation, identical Silver and Gold output). Skipped if PostgreSQL is unreachable |

## Resilience and observability

- PostgreSQL connects and **every storage read/write in every layer** retry
  with exponential backoff (1s, 2s, 4s; 4 attempts). Permanent errors (file
  not found, access denied) fail immediately instead of being retried.
- Logs are structured `key=value` lines (`event=bronze.table table=teams rows=100`).
  Each step logs its record counts and duration, and the job logs start, end
  and final status.
- Exit code `0` means success and `1` means failure. A failed run still writes
  its manifest.
- **Lineage:** [LINEAGE.md](LINEAGE.md) maps every column from source to Gold
  and marks where it is renamed, cast, derived or aggregated. At run time,
  Bronze rows carry `_source` / `_run_id`, Silver events carry
  `source_file` / `source_line`, and the run manifest records every step's
  counts.

## Known limitations

- **Cloud deploy ships only `job.py` + `requirements.txt`.** See
  `infra/eks.tf` (`helm_python_job`), which mounts just those two files. This
  is why `job.py` is self-contained instead of split into modules.
- **The EKS image is Python 3.11** (`jupyter/all-spark-notebook:latest`), so
  packages are pinned to versions that install there, including NumPy-2 builds
  of numexpr and bottleneck. pip prints a notice that some preinstalled image
  packages (numba, scikit-learn, matplotlib) expect NumPy 1; the job doesn't use
  them, and each run is its own container.
- **The first full load on AWS takes 75 s**, above the assumed 60 s target:
  it copies 783 small event files to S3 one at a time. Reruns copy only new
  files and take 12 s. Copying in parallel would fix the first load.
- **No alerting.** Failures show up in the logs (`level=ERROR`), the run
  manifest (`status=failed`) and the Kubernetes Job status, but nobody is
  notified.
- **No S3 versioning.** Silver and Gold are rebuilt in place. A bad run can't
  overwrite Gold (it is validated first), but older Gold versions are not kept.
- **Memory caps the volume** at about 20x today's data within the 2 GiB EKS
  limit (see [Performance](#performance)).
- **SLA targets are assumptions.** The brief sets none.
- Retries also cover authentication errors. A wrong password costs about 7s
  before the job fails.

## Trade-offs and assumptions

- **Pandas, not Spark.** At 42,000 records a run finishes in seconds on one
  core, and the job runs well past 10x today's volume. Spark would add startup
  time and complexity for no gain at this size.
- **One file (`job.py`).** The deploy ships only `job.py` and
  `requirements.txt`, so shared helpers live in clearly separated sections of
  that file instead of modules.
- **Full rebuild of Silver and Gold** on every run instead of incremental
  merges: simpler, and idempotent by construction. Bronze is incremental for
  event files.
- **Flag, don't drop, suspicious rows** (reviews and assessments dated before
  the hire): quarantining them would remove 17% of reviews. Gold decides per
  metric.
- **Repair invalid event dates** only when two independent signals agree (the
  file's day and the event-ID block); otherwise quarantine.
- **Business definitions are our choices** (key competency ≥ 4, critical gap,
  tiers, the performance signal). They are constants at the top of the Gold
  section, so they are easy to change.
