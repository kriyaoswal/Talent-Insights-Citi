# talent-etl: source-to-target mapping

Where every column comes from and what happens to it on the way: **renamed**,
**cast**, **derived** or **aggregated**. The code references are functions in
[job.py](job.py). Storage paths are in the [README](README.md#lake-layout).

```
PostgreSQL table ──► bronze/postgres/<table>/ingest_date=…/run_id=…/ ──► silver/<table>/             ──┐
                     (as-is + 3 metadata columns)                (typed, validated, flagged)    ├─► gold/<table>/
Event JSONL file ──► bronze/learning_activity/year=…/…         ──► silver/learning_activity/   ──┘  (11 tables, Q1–Q7)
                     (byte-for-byte copy)                        (parsed, cleaned, repaired)
Rejected rows    ──────────────────────────────────────────────► silver/_quarantine/<dataset>/run_date=…/
Run record       ──────────────────────────────────────────────► _runs/run_date=…/<run_id>.json
```

Terms used below:

| Term | Meaning |
|---|---|
| as-is | value and name unchanged |
| cast | type changed, value unchanged (e.g. text `"444"` → integer `444`) |
| renamed | name changed, value unchanged |
| derived | new column computed from other columns in the same row |
| aggregated | computed across many rows (count, mean, min, …) |
| joined | looked up from another table by key |

## 1. Source → Bronze

### PostgreSQL tables (`ingest_postgres_to_bronze`)

All 7 tables (`teams`, `competencies`, `employees`, `projects`,
`performance_reviews`, `competency_assessments`, `talent_review`) are copied
**as-is**: every column keeps its source name and PostgreSQL type. Three
metadata columns are added:

| Target column | Source | Rule |
|---|---|---|
| *all source columns* | `<table>.<column>` | as-is |
| `_source` | job setting | derived: `postgres://<host>/<database>/<table>` |
| `_ingested_at` | job clock | the run's start time (UTC), the same for every table |
| `_run_id` | job | the run's ID, matching its run record in `_runs/` |

Checks: the row count must equal `SELECT count(*)`, and every column the Silver
model needs must be present (`check_columns`). Each run writes its own
write-once snapshot (`run_id=<run_id>/`); Silver reads that run's snapshot only.

### Learning events (`ingest_events_to_bronze`)

Files are copied **byte-for-byte** from `learning_activity/year=YYYY/month=MM/day=DD/*.jsonl`
to the same path under `bronze/learning_activity/`. Nothing is parsed or
renamed. Each copy's size is checked against the source. A file already in
Bronze is never replaced, even if its source changes later (logged as
`bronze.events.source_changed`).

## 2. Bronze → Silver

### Tables (`transform_tables_to_silver`, `validate_table`)

For every table:

- **Metadata dropped:** `_source`, `_ingested_at`, `_run_id` (and any column not in the contract, with a warning).
- **Cast:** each column to its Pydantic model type (`Team`, `Employee`, …). Whitespace is stripped from text.
- **Rejected rows** go to `silver/_quarantine/<table>/`: original values as text, plus `reject_reasons` and `_run_id`.

| Silver table | Column | Source (Bronze) | Rule |
|---|---|---|---|
| `teams` | `team_id` | `team_id` | cast to integer, > 0 |
| | `name`, `department` | same | as-is (text); `department` must be an allowed value |
| `competencies` | `competency_id` | same | cast to integer |
| | `name`, `category` | same | as-is (text) |
| `employees` | `employee_id`, `team_id` | same | cast to integer; `team_id` must exist in Silver `teams` |
| | `first_name`, `last_name`, `email`, `job_level` | same | as-is (text); email format and job level checked |
| | `hire_date` | `hire_date` | cast to date; not after the run date |
| `projects` | `project_id`, `team_id` | same | cast to integer |
| | `name`, `outcome_status` | same | as-is (text) |
| | `outcome_score` | `outcome_score` (NUMERIC 5,2) | **cast to float**, 0–100 |
| `performance_reviews` | `review_id`, `employee_id`, `rating` | same | cast to integer; rating 1–5 |
| | `review_period`, `manager_comment` | same | as-is; period must be `YYYY-H1`/`YYYY-H2`; comment may be null |
| | `submitted_date` | same | cast to date; must be after the period ends |
| | **`flag_period_before_hire`** | `review_period` + `employees.hire_date` | **derived** (`add_quality_flags`): period ended before the hire date |
| `competency_assessments` | `assessment_id`, `employee_id`, `competency_id`, `proficiency_level`, `required_level` | same | cast to integer; levels 1–5 |
| | `assessed_date` | same | cast to date |
| | **`flag_assessed_before_hire`** | `assessed_date` + `employees.hire_date` | **derived**: assessed before the hire date |
| `talent_review` | `employee_id` | same | cast to integer |
| | `review_cycle`, `potential_rating`, `promotion_readiness`, `attrition_risk` | same | as-is (text), allowed values checked |

### Learning events (`transform_events_to_silver`, `clean_events`)

Each JSON line becomes one Silver row, a quarantined line, or a dropped
exact duplicate. Silver is partitioned by `event_year=YYYY/event_month=MM`.

| Silver column | Source (JSON field) | Rule |
|---|---|---|
| `event_id` | `event_id` | as-is (text), must be `EV` + 10 digits |
| `employee_id` | `employee_id` (text, e.g. `"444"`) | **cast to integer**; must exist in Silver `employees` |
| `event_ts` | `event_date` | **cast** from `YYYY-MM-DDTHH:MM:SS` to timestamp; if invalid, **repaired** to the file's partition day at 00:00 (see the README's date repair rules) |
| `event_date` | `event_date` | **derived**: the date part of `event_ts` |
| `event_type`, `course_name` | same | as-is (text) |
| `text` | `text`, or **`notes` renamed** | **renamed** for old files that use `notes` (known schema drift) |
| `score` | `score` | cast to integer 0–100; only on quiz events |
| `event_date_repaired` | – | **derived**: `true` when `event_ts` was repaired |
| `event_date_raw` | `event_date` | the original bad value, only when repaired |
| `source_file`, `source_line` | file path, line number | **derived**: lineage back to the exact Bronze line |

Quarantined lines go to `silver/_quarantine/learning_activity/` with
`source_file`, `source_line`, `raw_line`, `reject_reasons`, `_run_id`.

## 3. Silver → Gold (`build_gold`)

Gold reads only Silver. **Valid reviews** below means
`performance_reviews` rows with `flag_period_before_hire = false`
(`_valid_reviews`).

**Employee context.** Many Gold tables start with the same 7 columns, built by
joining `employees` to `teams` (`_employee_directory`):

| Column | Source | Rule |
|---|---|---|
| `employee_id`, `first_name`, `last_name`, `team_id`, `job_level` | `employees` | as-is |
| `team_name` | `teams.name` | **joined** on `team_id`, **renamed** `name` → `team_name` |
| `department` | `teams.department` | **joined** on `team_id` |

### Q1 `employee_review_history`: one row per review

| Column | Source | Rule |
|---|---|---|
| employee context | see above | joined |
| `hire_date` | `employees.hire_date` | joined |
| `review_id`, `review_period`, `rating`, `manager_comment`, `submitted_date`, `flag_period_before_hire` | `performance_reviews` | as-is (all reviews, flagged ones included) |

### Q1 `employee_performance_summary`: one row per employee

| Column | Source | Rule |
|---|---|---|
| employee context | see above | joined |
| `reviews_total` | `performance_reviews` | **aggregated**: count per employee |
| `reviews_valid` | valid reviews | **aggregated**: count per employee |
| `avg_rating` | valid reviews `.rating` | **aggregated**: mean |
| `previous_period`, `latest_period` | `performance_reviews.review_period` | the two most recent periods in the data (2024-H2, 2025-H1) |
| `previous_rating`, `latest_period_rating` | valid reviews `.rating` | the employee's rating in that period (null if none) |
| `latest_rating` | valid reviews `.rating` | rating from the most recent period with a valid review |
| `rating_change` | – | **derived**: `latest_period_rating − previous_rating` |
| `trend` | – | **derived**: `improved` (> 0), `stable` (= 0), `declined` (< 0), else `insufficient_data` |

### Q2 `employee_skill_gaps`: one row per assessment

| Column | Source | Rule |
|---|---|---|
| employee context | see above | joined |
| `assessment_id`, `competency_id`, `proficiency_level`, `required_level`, `assessed_date`, `flag_assessed_before_hire` | `competency_assessments` | as-is |
| `competency_name` | `competencies.name` | **joined** on `competency_id`, **renamed** |
| `category` | `competencies.category` | joined |
| `shortfall` | – | **derived**: `max(0, required_level − proficiency_level)` |
| `is_key` | – | **derived**: `required_level ≥ 4` |
| `is_critical` | – | **derived**: `shortfall ≥ 2`, or `is_key` and `shortfall ≥ 1` |

### Q3 `promotion_candidates`: one row per high-potential, ready-now employee

Rows: `talent_review` where `potential_rating = High` and `promotion_readiness = Ready Now`.

| Column | Source | Rule |
|---|---|---|
| employee context, `potential_rating`, `promotion_readiness`, `attrition_risk` | `talent_review` + employee context | joined |
| `latest_rating`, `trend` | `employee_performance_summary` | joined on `employee_id` |
| `courses_completed`, `certifications` | `employee_learning` | joined on `employee_id` |
| `tier` | `latest_rating` | **derived**: A if ≥ 4, B if = 3, else C |
| `recommendation` | `tier` | **derived**: fixed text per tier |
| `retention_priority` | `tier`, `attrition_risk` | **derived**: tier A and attrition risk High |
| `rank` | – | **derived**: order by tier, latest rating, certifications, courses completed (best first), then `employee_id` |

### Q4 `employee_learning`: one row per employee

| Column | Source | Rule |
|---|---|---|
| employee context | see above | joined |
| `courses_enrolled`, `courses_completed`, `certifications` | `learning_activity` | **aggregated**: distinct `course_name` per employee for `enrollment` / `course_completed` / `certification_earned` events |
| `modules_completed`, `quizzes_attempted`, `quizzes_passed` | `learning_activity` | **aggregated**: count of `module_completed` / `quiz_attempted` / `quiz_passed` events |
| `avg_quiz_score` | `learning_activity.score` | **aggregated**: mean over quiz events |
| `total_events` | `learning_activity` | **aggregated**: count of all events |
| `first_activity_date`, `last_activity_date` | `learning_activity.event_date` | **aggregated**: min / max |

### Q4 `employee_course_completions`: one row per employee and course

| Column | Source | Rule |
|---|---|---|
| `employee_id`, `course_name` | `learning_activity` | grouping key |
| `completed_date` | `event_date` of `course_completed` events | **aggregated**: first (min) date |
| `certified_date` | `event_date` of `certification_earned` events | **aggregated**: first (min) date |
| `completed`, `certified` | – | **derived**: the matching date is present |
| `date_repaired` | `event_date_repaired` | **aggregated**: any contributing event had a repaired date |

### Q5 `attrition_watchlist`: one row per employee

| Column | Source | Rule |
|---|---|---|
| employee context, `previous_rating`, `latest_period_rating`, `latest_rating`, `rating_change`, `trend` | `employee_performance_summary` | as-is |
| `potential_rating` | `talent_review` | joined |
| `recorded_attrition_risk` | `talent_review.attrition_risk` | joined, **renamed** |
| `high_potential` | `potential_rating` | **derived**: `= High` |
| `performance_signal` | `rating_change`, `latest_rating` | **derived**: rating declined, or latest rating ≤ 2 |
| `signal_reason` | – | **derived**: plain-text reason for the signal |
| `risk_tier` | signal + recorded risk | **derived**: Critical = signal + High; High = signal + Medium, or High without signal; Watch = signal + Low; else Low |
| `tier_rank` | `risk_tier` | **derived**: 1 (Critical) to 4 (Low) |

### Q6 `team_performance`: one row per team that has projects

| Column | Source | Rule |
|---|---|---|
| `team_id`, `department` | `teams` | as-is |
| `team_name` | `teams.name` | **renamed** |
| `headcount` | `employees` | **aggregated**: employees in the team |
| `people_rated` | valid reviews | **aggregated**: employees in the team with a valid review |
| `avg_rating` | valid reviews `.rating` | **aggregated**: mean over the team's reviews |
| `high_rating_share` | valid reviews `.rating` | **aggregated**: share of reviews rated ≥ 4 |
| `low_performer_share` | valid reviews `.rating` | **aggregated**: share of rated people whose own average is ≤ 2 |
| `rating_band` | `avg_rating` | **derived across teams**: bottom / middle / top third by average rating |
| `projects`, `projects_on_time` | `projects` | **aggregated**: count, and count with `outcome_status = on-time` |
| `on_time_rate` | – | **derived**: `projects_on_time / projects` |
| `avg_outcome_score` | `projects.outcome_score` | **aggregated**: mean |

### Q6 `project_performance`: one row per project

| Column | Source | Rule |
|---|---|---|
| `project_id`, `team_id`, `outcome_status`, `outcome_score` | `projects` | as-is |
| `project_name` | `projects.name` | **renamed** |
| `project_type` | `projects.name` | **derived**: last word of the name (Launch / Rollout / Migration / Revamp) |
| `on_time` | `outcome_status` | **derived**: `= on-time` |
| `team_name`, `department` | `team_performance` | joined on `team_id` |
| `team_avg_rating`, `team_people_rated`, `team_low_performer_share`, `team_rating_band` | `team_performance` | joined, **renamed** with a `team_` prefix |

### Q7 `skill_distribution`: one row per competency, department and level

| Column | Source | Rule |
|---|---|---|
| `competency_id`, `competency_name`, `category`, `department`, `proficiency_level` | `employee_skill_gaps` | grouping key |
| `assessments` | `employee_skill_gaps` | **aggregated**: count |

### Q7 `competency_summary`: one row per competency

| Column | Source | Rule |
|---|---|---|
| `competency_id`, `competency_name`, `category` | `employee_skill_gaps` | grouping key |
| `assessed`, `employees` | `employee_skill_gaps` | **aggregated**: assessments, distinct employees |
| `avg_proficiency`, `avg_required_level`, `avg_shortfall` | `employee_skill_gaps` | **aggregated**: mean |
| `meets_required_share` | `shortfall` | **aggregated**: share with `shortfall = 0` |
| `critical_gaps` | `is_critical` | **aggregated**: count |
| `critical_gap_rate` | – | **derived**: `critical_gaps / assessed` |
| `level_1` … `level_5` | `proficiency_level` | **aggregated**: count of assessments at each level |

## 4. How lineage is recorded at run time

| Where | What it links |
|---|---|
| Bronze `_run_id`, `_ingested_at`, `_source` | each table row to the run and source table it came from |
| Silver events `source_file`, `source_line` | each event to its exact line in Bronze |
| Silver events `event_date_raw` | a repaired date to its original value |
| Quarantine `reject_reasons`, `_run_id` | each rejected row to why and when it was rejected |
| Run record `_runs/run_date=…/<run_id>.json` | per step: records read, written, quarantined, duration, CPU, peak memory, status; plus the 15 Gold reconciliation results and headline answers |
| Logs `event=…` | the same counts as structured `INFO` / `WARNING` / `ERROR` lines |
