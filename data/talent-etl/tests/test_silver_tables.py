"""Unit tests for the Silver table contracts (``validate_table`` and flags)."""

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

import job

RUN_DATE = date(2026, 10, 14)


def employees_df(**overrides) -> pd.DataFrame:
    row = {
        "employee_id": 1,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@acme.example",
        "team_id": 10,
        "job_level": "IC2",
        "hire_date": date(2024, 1, 15),
        "_source": "postgres://x",  # Bronze metadata columns are ignored
    }
    row.update(overrides)
    return pd.DataFrame([row])


def validate(table: str, df: pd.DataFrame, parents: dict[str, set[int]] | None = None):
    return job.validate_table(df, job.TABLE_CONTRACTS[table], parents or {}, RUN_DATE)


def only_reason(rejected: pd.DataFrame) -> str:
    assert len(rejected) == 1
    return rejected.iloc[0]["reject_reasons"]


# --- typing ------------------------------------------------------------------ #


def test_valid_row_is_typed_and_metadata_dropped():
    valid, rejected = validate("employees", employees_df(), {"teams": {10}})
    assert rejected.empty
    assert list(valid.columns) == list(job.Employee.model_fields)
    assert valid.iloc[0]["hire_date"] == date(2024, 1, 15)
    assert str(valid["employee_id"].dtype) == "Int64"


def test_text_values_are_cast():
    df = employees_df(employee_id="7", team_id=" 10 ", hire_date="2024-01-15")
    valid, rejected = validate("employees", df, {"teams": {10}})
    assert rejected.empty
    assert valid.iloc[0]["employee_id"] == 7
    assert valid.iloc[0]["hire_date"] == date(2024, 1, 15)


def test_decimal_is_cast_to_float():
    df = pd.DataFrame(
        [{"project_id": 1, "name": "P", "team_id": 10, "outcome_status": "on-time", "outcome_score": Decimal("56.87")}]
    )
    valid, _ = validate("projects", df, {"teams": {10}})
    assert valid.iloc[0]["outcome_score"] == pytest.approx(56.87)


# --- column rules ------------------------------------------------------------ #


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"first_name": None}, "null_first_name"),
        ({"first_name": "   "}, "null_first_name"),
        ({"employee_id": "abc"}, "invalid_type_employee_id"),
        ({"employee_id": 1.5}, "invalid_type_employee_id"),
        ({"hire_date": "15/01/2024"}, "invalid_type_hire_date"),
        ({"job_level": "Wizard"}, "not_allowed_job_level"),
        ({"email": "not-an-email"}, "bad_format_email"),
        ({"hire_date": date(2027, 1, 1)}, "future_hire_date"),
        ({"team_id": 99}, "unknown_team_id"),
    ],
)
def test_employee_column_rules(overrides, reason):
    _, rejected = validate("employees", employees_df(**overrides), {"teams": {10}})
    assert only_reason(rejected) == reason


def test_null_required_value_is_not_also_a_type_error():
    _, rejected = validate("employees", employees_df(hire_date=None), {"teams": {10}})
    assert only_reason(rejected) == "null_hire_date"


def test_hire_date_on_run_date_is_allowed():
    _, rejected = validate("employees", employees_df(hire_date=RUN_DATE), {"teams": {10}})
    assert rejected.empty


@pytest.mark.parametrize(("rating", "ok"), [(0, False), (1, True), (5, True), (6, False)])
def test_rating_range(rating, ok):
    df = pd.DataFrame(
        [
            {
                "review_id": 1,
                "employee_id": 1,
                "review_period": "2025-H1",
                "rating": rating,
                "manager_comment": None,  # nullable column
                "submitted_date": date(2025, 8, 1),
            }
        ]
    )
    _, rejected = validate("performance_reviews", df, {"employees": {1}})
    assert rejected.empty == ok
    if not ok:
        assert only_reason(rejected) == "out_of_range_rating"


@pytest.mark.parametrize(("score", "ok"), [(-0.01, False), (0, True), (100, True), (100.01, False)])
def test_outcome_score_range(score, ok):
    df = pd.DataFrame([{"project_id": 1, "name": "P", "team_id": 10, "outcome_status": "on-time", "outcome_score": score}])
    _, rejected = validate("projects", df, {"teams": {10}})
    assert rejected.empty == ok


def review(review_id=1, employee_id=1, period="2025-H1", submitted=date(2025, 8, 1)):
    return {
        "review_id": review_id,
        "employee_id": employee_id,
        "review_period": period,
        "rating": 3,
        "manager_comment": "Fine.",
        "submitted_date": submitted,
    }


@pytest.mark.parametrize("period", ["2025-H3", "2025H1", "25-H1"])
def test_review_period_format(period):
    _, rejected = validate("performance_reviews", pd.DataFrame([review(period=period)]), {"employees": {1}})
    assert "bad_format_review_period" in only_reason(rejected)


def test_review_submitted_before_period_end_is_rejected():
    df = pd.DataFrame([review(submitted=date(2025, 6, 30)), review(2, 2, submitted=date(2025, 7, 1))])
    _, rejected = validate("performance_reviews", df, {"employees": {1, 2}})
    assert only_reason(rejected) == "submitted_before_period_end"


# --- keys ----------------------------------------------------------------------- #


def test_duplicate_primary_key_rejects_all_copies():
    df = pd.concat([employees_df(), employees_df(email="other@acme.example")])
    valid, rejected = validate("employees", df, {"teams": {10}})
    assert valid.empty
    assert list(rejected["reject_reasons"]) == ["duplicate_primary_key"] * 2


def test_unique_business_key():
    df = pd.DataFrame([review(1, 1, "2025-H1"), review(2, 1, "2025-H1"), review(3, 1, "2024-H2", date(2025, 2, 1))])
    valid, rejected = validate("performance_reviews", df, {"employees": {1}})
    assert list(valid["review_id"]) == [3]
    assert set(rejected["reject_reasons"]) == {"duplicate_employee_id_review_period"}


def test_every_row_lands_in_exactly_one_output():
    df = pd.concat([employees_df(), employees_df(employee_id=2, email="b@acme.example", job_level="Wizard")])
    valid, rejected = validate("employees", df, {"teams": {10}})
    assert len(valid) + len(rejected) == len(df) == 2


def test_rejected_keeps_original_values_and_all_reasons():
    _, rejected = validate("employees", employees_df(job_level="Wizard", team_id=99), {"teams": {10}})
    assert rejected.iloc[0]["job_level"] == "Wizard"
    assert only_reason(rejected) == "unknown_team_id;not_allowed_job_level"  # in column order


# --- schema drift ------------------------------------------------------------ #


def test_missing_contract_column_raises():
    with pytest.raises(ValueError, match="missing columns \\['hire_date'\\]"):
        validate("employees", employees_df().drop(columns=["hire_date"]), {"teams": {10}})


def test_extra_column_is_dropped():
    valid, _ = validate("employees", employees_df(nickname="Ace"), {"teams": {10}})
    assert "nickname" not in valid.columns


# --- soft-rule flags ------------------------------------------------------------ #


def test_flag_period_before_hire():
    employees = pd.DataFrame({"employee_id": [1, 2], "hire_date": [date(2025, 1, 10), date(2024, 1, 1)]})
    reviews = pd.DataFrame({"employee_id": [1, 2], "review_period": ["2024-H2", "2024-H2"]})
    flagged = job.add_quality_flags("performance_reviews", reviews, employees)
    assert list(flagged["flag_period_before_hire"]) == [True, False]


def test_flag_assessed_before_hire():
    employees = pd.DataFrame({"employee_id": [1], "hire_date": [date(2025, 1, 10)]})
    assessments = pd.DataFrame({"employee_id": [1, 1], "assessed_date": [date(2025, 1, 9), date(2025, 1, 10)]})
    flagged = job.add_quality_flags("competency_assessments", assessments, employees)
    assert list(flagged["flag_assessed_before_hire"]) == [True, False]


def test_review_period_end():
    s = pd.Series(["2024-H1", "2024-H2", "bad"], dtype="string")
    assert list(job.review_period_end(s)) == [pd.Timestamp("2024-06-30"), pd.Timestamp("2024-12-31"), pd.NaT]
