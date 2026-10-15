"""Unit tests for the Pydantic schema models and the reason-code mapping."""

from datetime import date, datetime

import pandas as pd
import pytest

import job

TEAM = {"team_id": 1, "name": "Team A", "department": "Sales"}


def reasons(model, record, **context):
    obj, why = job.validate_record(model, record, context)
    assert (obj is None) == bool(why)
    return why


# --- shared field types ----------------------------------------------------- #


def test_whitespace_is_stripped_and_allowed_values_tolerate_it():
    obj, why = job.validate_record(job.Team, {**TEAM, "name": "  Team A ", "department": " Sales "}, {})
    assert why == [] and obj.name == "Team A" and obj.department == "Sales"


@pytest.mark.parametrize("value", [True, False])
def test_booleans_are_not_ids(value):
    # Pydantic alone would read True as 1.
    assert reasons(job.Team, {**TEAM, "team_id": value}) == ["invalid_type_team_id"]


@pytest.mark.parametrize("value", [0, -3])
def test_ids_must_be_positive(value):
    assert reasons(job.Team, {**TEAM, "team_id": value}) == ["out_of_range_team_id"]


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        (date(2024, 1, 15), True),
        ("2024-01-15", True),
        (datetime(2024, 1, 15), True),  # midnight timestamp, as pandas may hand back
        (pd.Timestamp("2024-01-15"), True),
        (datetime(2024, 1, 15, 9, 30), False),
        ("2024-01-15T09:30:00", False),
        (1705276800, False),  # epoch seconds: Pydantic alone would accept this
        ("15/01/2024", False),
        ("2024-02-30", False),
    ],
)
def test_dates_are_strict(value, ok):
    record = {
        "employee_id": 1,
        "first_name": "A",
        "last_name": "B",
        "email": "a@b.example",
        "team_id": 1,
        "job_level": "IC1",
        "hire_date": value,
    }
    why = reasons(job.Employee, record)
    assert why == ([] if ok else ["invalid_type_hire_date"])


def test_future_check_only_applies_with_run_date_in_context():
    record = {
        "employee_id": 1,
        "first_name": "A",
        "last_name": "B",
        "email": "a@b.example",
        "team_id": 1,
        "job_level": "IC1",
        "hire_date": "2030-01-01",
    }
    assert reasons(job.Employee, record) == []
    assert reasons(job.Employee, record, run_date=date(2026, 10, 14)) == ["future_hire_date"]


def test_foreign_key_only_checked_when_parent_keys_given():
    record = {"project_id": 1, "name": "P", "team_id": 7, "outcome_status": "on-time", "outcome_score": 50}
    assert reasons(job.Project, record) == []
    assert reasons(job.Project, record, parent_keys={"teams": {1, 2}}) == ["unknown_team_id"]
    assert reasons(job.Project, record, parent_keys={"teams": {7}}) == []


@pytest.mark.parametrize("score", [float("nan"), float("inf")])
def test_outcome_score_rejects_nan_and_inf(score):
    record = {"project_id": 1, "name": "P", "team_id": 1, "outcome_status": "on-time", "outcome_score": score}
    assert reasons(job.Project, record)


# --- model-level rules ---------------------------------------------------------- #


def test_model_rule_is_reported_by_its_own_code():
    record = {
        "review_id": 1,
        "employee_id": 1,
        "review_period": "2025-H1",
        "rating": 3,
        "submitted_date": "2025-06-30",
    }
    assert reasons(job.PerformanceReview, record) == ["submitted_before_period_end"]


def test_model_rules_run_only_when_fields_are_valid():
    record = {"review_id": 1, "employee_id": 1, "review_period": "bad", "rating": 3, "submitted_date": "2025-01-01"}
    assert reasons(job.PerformanceReview, record) == ["bad_format_review_period"]


def test_learning_event_forbids_unknown_fields_but_tables_ignore_them():
    event = {
        "event_id": "EV0000000001",
        "employee_id": "1",
        "event_date": "x",
        "event_type": "enrollment",
        "course_name": "SQL",
        "extra": 1,
    }
    assert reasons(job.LearningEvent, event) == ["unexpected_field_extra"]
    assert reasons(job.Team, {**TEAM, "extra": 1}) == []


def test_learning_event_requires_event_date_key_but_allows_null():
    event = {"event_id": "EV0000000001", "employee_id": "1", "event_type": "enrollment", "course_name": "SQL"}
    assert reasons(job.LearningEvent, event) == ["missing_field_event_date"]
    assert reasons(job.LearningEvent, {**event, "event_date": None}) == []


# --- every table has a model and the models match schema.sql ---------------------- #


def test_every_source_table_has_a_contract():
    assert set(job.TABLE_CONTRACTS) == set(job.SOURCE_TABLES)
    for table, contract in job.TABLE_CONTRACTS.items():
        assert contract.primary_key == job.SOURCE_TABLES[table]
        assert contract.primary_key in contract.columns


def test_only_manager_comment_is_optional():
    optional = {
        f"{table}.{name}"
        for table, contract in job.TABLE_CONTRACTS.items()
        for name, info in contract.model.model_fields.items()
        if not info.is_required()
    }
    assert optional == {"performance_reviews.manager_comment"}


# --- Bronze / Silver column contract ---------------------------------------------- #


def test_check_columns_raises_on_missing_and_reports_extra():
    df = pd.DataFrame(columns=["team_id", "name", "department", "region", "_source"])
    assert job.check_columns(df, job.TABLE_CONTRACTS["teams"].columns, "bronze.teams") == ["region"]
    with pytest.raises(job.SchemaDriftError, match=r"bronze\.teams: schema drift, missing columns \['department'\]"):
        job.check_columns(df.drop(columns=["department"]), job.TABLE_CONTRACTS["teams"].columns, "bronze.teams")
