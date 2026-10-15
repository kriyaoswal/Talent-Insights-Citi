"""End-to-end test: real PostgreSQL + sample events -> Bronze -> Silver, run twice.

Requires the source database loaded by ``scripts/load_source_db.sh``; skipped
when PostgreSQL is not reachable.
"""

import glob
import json
from pathlib import Path

import pandas as pd
import psycopg
import pytest

import job

RUN_DATE = "2026-10-14"


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    cfg = job.Config.from_env({})
    try:
        psycopg.connect(
            host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_name, user=cfg.pg_user, password=cfg.pg_pass,
            connect_timeout=3,
        ).close()  # fmt: skip
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    return tmp_path_factory.mktemp("lake")


def run_job(lake: Path, monkeypatch) -> dict:
    monkeypatch.setenv("LAKE_ROOT", str(lake))
    monkeypatch.setenv("RUN_DATE", RUN_DATE)
    assert job.main() == 0
    manifests = sorted(glob.glob(f"{lake}/_runs/run_date={RUN_DATE}/*.json"), key=lambda p: Path(p).stat().st_mtime)
    return json.loads(Path(manifests[-1]).read_text())


def lake_snapshot(lake: Path) -> dict[str, pd.DataFrame]:
    """Every Silver and Gold Parquet file, keyed by path (run ids dropped)."""
    files = sorted(f for layer in ("silver", "gold") for f in glob.glob(f"{lake}/{layer}/**/*.parquet", recursive=True))
    return {
        str(Path(f).relative_to(lake)): pd.read_parquet(f).drop(columns=["_run_id"], errors="ignore") for f in files
    }


def bronze_files(lake: Path) -> dict[str, bytes]:
    """Every Bronze file's bytes, keyed by path."""
    return {str(p.relative_to(lake)): p.read_bytes() for p in (lake / "bronze").rglob("*") if p.is_file()}


def step(manifest: dict, name: str) -> dict:
    return next(s for s in manifest["steps"] if s["step"] == name)


def test_pipeline_end_to_end_and_idempotent(lake, monkeypatch):
    first = run_job(lake, monkeypatch)
    snapshot = lake_snapshot(lake)
    first_bronze = bronze_files(lake)
    second = run_job(lake, monkeypatch)

    # --- Bronze: row counts match the source -----------------------------------
    assert step(first, "bronze.postgres")["details"] == {
        "teams": 100,
        "competencies": 40,
        "employees": 2000,
        "projects": 33,
        "performance_reviews": 4000,
        "competency_assessments": 4000,
        "talent_review": 2000,
    }
    assert step(first, "bronze.learning_activity")["details"]["files_found"] == 783
    assert step(second, "bronze.learning_activity")["details"]["files_copied"] == 0  # incremental

    # --- Bronze is write-once: the rerun added its own snapshots, changed nothing --
    rerun_bronze = bronze_files(lake)
    assert all(rerun_bronze[path] == data for path, data in first_bronze.items())
    added = sorted(set(rerun_bronze) - set(first_bronze))
    assert len(added) == 7 and all(f"/run_id={second['run_id']}/" in p for p in added)

    # --- Silver tables: clean source, soft flags only ----------------------------
    tables = step(first, "silver.tables")
    assert tables["records_read"] == tables["records_written"] == 12173
    assert tables["records_quarantined"] == 0
    assert tables["details"]["performance_reviews"]["flag_period_before_hire"] == 666
    assert tables["details"]["competency_assessments"]["flag_assessed_before_hire"] == 234

    # --- Silver events: every planted issue handled, counts reconcile ------------
    events = step(first, "silver.learning_activity")["details"]
    assert events["lines_read"] == 29868
    assert events["empty_files"] == 1
    assert events["duplicates_dropped"] == 152
    assert events["schema_drift_renamed"] == 39
    assert events["event_date_repaired"] == 53  # 54 bad-date lines, one is an exact duplicate
    assert events["quarantine_reasons"] == {"malformed_json": 13, "null_employee_id": 76}
    assert events["lines_quarantined"] == 89
    assert events["rows_written"] == 29627
    assert events["rows_written"] + events["lines_quarantined"] + events["duplicates_dropped"] == events["lines_read"]

    silver_events = pd.concat(v for k, v in snapshot.items() if k.startswith("silver/learning_activity/"))
    assert len(silver_events) == 29627
    assert not silver_events["event_id"].duplicated().any()
    repaired = silver_events[silver_events["event_date_repaired"]]
    assert (repaired["event_ts"].dt.strftime("%H:%M:%S") == "00:00:00").all()
    assert repaired.set_index("event_id").loc["EV0000000039", "event_date_raw"] == "yesterday"
    assert str(repaired.set_index("event_id").loc["EV0000000039", "event_date"]) == "2023-09-05"

    # --- Gold: every table validated, reconciled, and the headline answers -------
    gold = step(first, "gold")["details"]
    assert gold["tables"] == {
        "employee_review_history": 4000,
        "employee_performance_summary": 2000,
        "employee_skill_gaps": 4000,
        "promotion_candidates": 357,
        "employee_learning": 2000,
        "employee_course_completions": 5118,
        "attrition_watchlist": 2000,
        "team_performance": 27,
        "project_performance": 33,
        "skill_distribution": 1595,
        "competency_summary": 40,
    }
    assert len(gold["reconciliation"]) == 15 and all(v.startswith("PASS") for v in gold["reconciliation"].values())
    hl = gold["highlights"]
    assert hl["q1_trend"] == {"improved": 721, "stable": 692, "declined": 191, "insufficient_data": 396}
    assert hl["q2_employees_with_critical_gap"] == 441 and hl["q2_critical_assessments"] == 465
    assert hl["q3_candidates_by_tier"] == {"A": 148, "B": 90, "C": 119} and hl["q3_retention_priority"] == 18
    assert hl["q4_employees_completed_a_course"] == 1661 and hl["q4_employees_certified"] == 1154
    assert hl["q5_risk_tiers"] == {"Critical": 143, "High": 294, "Watch": 171, "Low": 1392}
    assert hl["q5_critical_high_potential"] == 42
    assert hl["q6_on_time_by_rating_band"] == {"bottom third": 0.444, "middle third": 0.556, "top third": 0.944}

    # --- SLA: a run at today's volume finishes well inside the 60 s target ----------
    assert first["duration_s"] < 60 and second["duration_s"] < 60
    assert all(s["peak_rss_mb"] is None or s["peak_rss_mb"] > 0 for s in first["steps"])

    # --- Idempotency: rerun produces identical Silver and Gold output -------------
    rerun = lake_snapshot(lake)
    assert rerun.keys() == snapshot.keys()
    for path, df in snapshot.items():
        pd.testing.assert_frame_equal(df, rerun[path], obj=path)
