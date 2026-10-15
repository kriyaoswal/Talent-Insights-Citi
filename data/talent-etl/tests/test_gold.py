"""Unit tests for the Gold layer: business definitions, row models and reconciliation.

Uses a small hand-built Silver dataset where every expected answer is known.
"""

import re
from datetime import date

import pandas as pd
import pytest

import job

D = date(2025, 1, 1)


def review(review_id, employee_id, period, rating, flagged=False):
    return {
        "review_id": review_id,
        "employee_id": employee_id,
        "review_period": period,
        "rating": rating,
        "manager_comment": "Fine.",
        "submitted_date": D,
        "flag_period_before_hire": flagged,
    }


def talent(employee_id, potential, readiness, risk):
    return {
        "employee_id": employee_id,
        "review_cycle": "2025-H1",
        "potential_rating": potential,
        "promotion_readiness": readiness,
        "attrition_risk": risk,
    }


def ev(employee_id, event_type, course, day=1, score=None, repaired=False):
    return {
        "event_id": f"EV{employee_id:04d}{day:02d}{len(event_type):04d}",
        "employee_id": employee_id,
        "event_date": date(2025, 1, day),
        "event_type": event_type,
        "course_name": course,
        "score": score,
        "event_date_repaired": repaired,
    }


@pytest.fixture
def silver() -> dict[str, list[dict]]:
    teams = pd.DataFrame(
        [
            {"team_id": 1, "name": "Team A", "department": "Sales"},
            {"team_id": 2, "name": "Team B", "department": "Design"},
            {"team_id": 3, "name": "Team C", "department": "Sales"},
        ]
    )
    employees = pd.DataFrame(
        [
            {"employee_id": i, "first_name": f"F{i}", "last_name": f"L{i}", "email": f"e{i}@x.example",
             "team_id": team, "job_level": "IC1", "hire_date": date(2020, 1, 1)}
            for i, team in [(1, 1), (2, 1), (3, 1), (4, 2), (5, 3), (6, 3)]
        ]
    )
    reviews = pd.DataFrame(
        [
            review(1, 1, "2024-H2", 3), review(2, 1, "2025-H1", 4),  # improved
            review(3, 2, "2024-H2", 4), review(4, 2, "2025-H1", 4),  # stable (regression: 4 -> 4)
            review(5, 3, "2024-H2", 4), review(6, 3, "2025-H1", 2),  # declined, low
            review(7, 4, "2024-H2", 3, flagged=True), review(8, 4, "2025-H1", 5),  # one valid review
            review(9, 5, "2024-H2", 2, flagged=True), review(10, 5, "2025-H1", 3, flagged=True),  # none valid
            review(11, 6, "2024-H2", 5), review(12, 6, "2025-H1", 1),  # declined, low
        ]
    )
    competencies = pd.DataFrame(
        [{"competency_id": 1, "name": "Python", "category": "Technical"},
         {"competency_id": 2, "name": "Coaching", "category": "Leadership"}]
    )
    # (employee, competency, proficiency, required)
    assessments = pd.DataFrame(
        [
            {"assessment_id": n, "employee_id": e, "competency_id": c, "proficiency_level": p, "required_level": r,
             "assessed_date": D, "flag_assessed_before_hire": False}
            for n, (e, c, p, r) in enumerate(
                [(1, 1, 3, 3), (2, 1, 1, 3), (3, 2, 3, 4), (4, 1, 2, 3), (5, 2, 5, 4), (6, 2, 2, 4)], start=1
            )
        ]
    )
    talent_review = pd.DataFrame(
        [
            talent(1, "High", "Ready Now", "Low"),  # A
            talent(2, "High", "Ready Now", "High"),  # A + retention priority
            talent(3, "High", "Ready Now", "Low"),  # C (latest 2)
            talent(4, "High", "1-2 Years", "Low"),  # not a candidate
            talent(5, "High", "Ready Now", "Low"),  # C (no valid rating)
            talent(6, "Medium", "Ready Now", "High"),  # not a candidate
        ]
    )
    projects = pd.DataFrame(
        [
            {"project_id": 1, "name": "Blue Launch", "team_id": 1, "outcome_status": "on-time", "outcome_score": 80.0},
            {"project_id": 2, "name": "Red Migration", "team_id": 1, "outcome_status": "delayed", "outcome_score": 50.0},
            {"project_id": 3, "name": "Gold Rollout", "team_id": 2, "outcome_status": "on-time", "outcome_score": 70.0},
        ]
    )
    events = pd.DataFrame(
        [
            ev(1, "enrollment", "C1", 1), ev(1, "course_completed", "C1", 2), ev(1, "certification_earned", "C1", 3),
            ev(1, "quiz_attempted", "C1", 4, score=60), ev(1, "quiz_passed", "C1", 5, score=80),
            ev(2, "course_completed", "C2", 6), ev(2, "course_completed", "C2", 9, repaired=True),
            ev(2, "certification_earned", "C3", 7),
        ]
    )
    return {
        "teams": teams,
        "competencies": competencies,
        "employees": employees,
        "projects": projects,
        "performance_reviews": reviews,
        "competency_assessments": assessments,
        "talent_review": talent_review,
        "learning_activity": events,
    }


@pytest.fixture
def gold(silver):
    return job.build_gold(silver)


def by_id(df, key="employee_id"):
    return df.set_index(key)


# --- Q1 ----------------------------------------------------------------------- #


def test_q1_history_keeps_every_review_with_context(gold):
    h = gold["employee_review_history"]
    assert len(h) == 12
    assert h.loc[h.review_id == 7, "flag_period_before_hire"].item()
    assert h.loc[h.review_id == 1, "team_name"].item() == "Team A"


@pytest.mark.parametrize(
    ("employee", "trend", "change", "latest"),
    [(1, "improved", 1, 4), (2, "stable", 0, 4), (3, "declined", -2, 2), (6, "declined", -4, 1)],
)
def test_q1_trend_from_valid_reviews(gold, employee, trend, change, latest):
    row = by_id(gold["employee_performance_summary"]).loc[employee]
    assert row.trend == trend and row.rating_change == change and row.latest_rating == latest


def test_q1_flagged_reviews_are_excluded_from_ratings(gold):
    s = by_id(gold["employee_performance_summary"])
    assert s.loc[4, "trend"] == "insufficient_data" and s.loc[4, "latest_rating"] == 5 and s.loc[4, "reviews_valid"] == 1
    assert s.loc[5, "reviews_valid"] == 0 and pd.isna(s.loc[5, "latest_rating"]) and pd.isna(s.loc[5, "avg_rating"])
    assert s.loc[5, "reviews_total"] == 2


# --- Q2 ----------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("assessment", "shortfall", "is_key", "is_critical"),
    [
        (1, 0, False, False),  # meets requirement
        (2, 2, False, True),  # 2 levels short: critical anywhere
        (3, 1, True, True),  # 1 short on a key competency (required 4)
        (4, 1, False, False),  # 1 short, not key
        (5, 0, True, False),  # key, but exceeds requirement
        (6, 2, True, True),  # key and 2 short
    ],
)
def test_q2_gap_definitions(gold, assessment, shortfall, is_key, is_critical):
    row = by_id(gold["employee_skill_gaps"], "assessment_id").loc[assessment]
    assert (row.shortfall, bool(row.is_key), bool(row.is_critical)) == (shortfall, is_key, is_critical)


# --- Q3 ----------------------------------------------------------------------- #


def test_q3_only_high_potential_ready_now(gold):
    assert set(gold["promotion_candidates"]["employee_id"]) == {1, 2, 3, 5}


def test_q3_tiers_rank_and_retention(gold):
    c = gold["promotion_candidates"]
    assert list(zip(c["rank"], c["employee_id"], c["tier"])) == [(1, 1, "A"), (2, 2, "A"), (3, 3, "C"), (4, 5, "C")]
    assert c.set_index("employee_id").loc[2, "retention_priority"]
    assert not c.set_index("employee_id").loc[1, "retention_priority"]


@pytest.mark.parametrize(("rating", "tier"), [(5, "A"), (4, "A"), (3, "B"), (2, "C"), (1, "C"), (None, "C")])
def test_q3_tier_rule(rating, tier):
    assert job._expected_promotion_tier(rating) == tier


# --- Q4 ----------------------------------------------------------------------- #


def test_q4_learning_counts_distinct_courses(gold):
    lr = by_id(gold["employee_learning"])
    e1, e2, e3 = lr.loc[1], lr.loc[2], lr.loc[3]
    assert (e1.courses_enrolled, e1.courses_completed, e1.certifications, e1.quizzes_passed, e1.avg_quiz_score) == (1, 1, 1, 1, 70)
    assert (e2.courses_completed, e2.certifications, e2.total_events) == (1, 1, 3)  # C2 completed twice counts once
    assert e3.total_events == 0 and pd.isna(e3.first_activity_date)


def test_q4_course_completions_detail(gold):
    cc = gold["employee_course_completions"].set_index(["employee_id", "course_name"])
    assert len(cc) == 3
    assert cc.loc[(1, "C1"), "completed"] and cc.loc[(1, "C1"), "certified"]
    assert cc.loc[(2, "C2"), "completed_date"] == date(2025, 1, 6)  # first completion
    assert cc.loc[(2, "C2"), "date_repaired"]
    assert not cc.loc[(2, "C3"), "completed"] and cc.loc[(2, "C3"), "certified"]  # certified without completion


# --- Q5 ----------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("employee", "signal", "tier"),
    [(1, False, "Low"), (2, False, "High"), (3, True, "Watch"), (4, False, "Low"), (5, False, "Low"), (6, True, "Critical")],
)
def test_q5_risk_tiers(gold, employee, signal, tier):
    row = by_id(gold["attrition_watchlist"]).loc[employee]
    assert (bool(row.performance_signal), row.risk_tier) == (signal, tier)


@pytest.mark.parametrize(
    ("signal", "recorded", "tier"),
    [(True, "High", "Critical"), (True, "Medium", "High"), (False, "High", "High"), (True, "Low", "Watch"),
     (False, "Medium", "Low"), (False, "Low", "Low")],
)
def test_q5_tier_rule(signal, recorded, tier):
    assert job._expected_risk_tier(signal, recorded) == tier


def test_q5_signal_reason(gold):
    w = by_id(gold["attrition_watchlist"])
    assert w.loc[3, "signal_reason"] == "rating declined; latest rating <= 2"
    assert w.loc[5, "signal_reason"] == "no rating trend available"


# --- Q6 ----------------------------------------------------------------------- #


def test_q6_only_teams_with_projects(gold):
    assert list(gold["team_performance"]["team_id"]) == [1, 2]


def test_q6_team_measures(gold):
    t = by_id(gold["team_performance"], "team_id")
    a = t.loc[1]
    assert (a.headcount, a.people_rated, a.projects, a.projects_on_time, a.on_time_rate) == (3, 3, 2, 1, 0.5)
    assert a.avg_rating == pytest.approx(21 / 6)  # ratings 3,4,4,4,4,2
    assert a.high_rating_share == pytest.approx(4 / 6) and a.low_performer_share == 0
    assert a.avg_outcome_score == 65
    assert t.loc[2, "people_rated"] == 1 and t.loc[2, "avg_rating"] == 5  # flagged review excluded


def test_q6_project_detail(gold):
    p = by_id(gold["project_performance"], "project_id")
    assert list(p["project_type"]) == ["Launch", "Migration", "Rollout"]
    assert p.loc[2, "on_time"] == False  # noqa: E712
    assert p.loc[1, "team_avg_rating"] == pytest.approx(21 / 6)


# --- Q7 ----------------------------------------------------------------------- #


def test_q7_distribution_and_summary(gold):
    dist, summ = gold["skill_distribution"], by_id(gold["competency_summary"], "competency_id")
    assert dist["assessments"].sum() == 6
    py = summ.loc[1]
    assert (py.assessed, py.critical_gaps, py.level_1, py.level_2, py.level_3) == (3, 1, 1, 1, 1)
    assert py.meets_required_share == pytest.approx(1 / 3)


# --- validation, reconciliation, grain --------------------------------------------- #


def test_all_gold_tables_pass_their_contracts(gold):
    for name, df in gold.items():
        rows = job.validate_gold_table(name, df)
        assert len(rows) == len(df)


def test_reconciliation_passes(silver, gold):
    checks = job.reconcile_gold(silver, gold)
    assert all(g == s for g, s in checks.values())


def test_row_model_names_the_broken_definition(gold):
    bad = gold["employee_performance_summary"].copy()
    bad.loc[bad.employee_id == 2, "trend"] = "declined"  # the bug the models caught: 4 -> 4 labelled declined
    with pytest.raises(job.GoldValidationError, match="trend follows the sign of rating_change"):
        job.validate_gold_table("employee_performance_summary", bad)


def test_skill_gap_model_rejects_wrong_critical_flag(gold):
    bad = gold["employee_skill_gaps"].copy()
    bad.loc[bad.assessment_id == 4, "is_critical"] = True
    with pytest.raises(job.GoldValidationError, match="is_critical"):
        job.validate_gold_table("employee_skill_gaps", bad)


def test_duplicate_grain_is_rejected(gold):
    df = pd.concat([gold["team_performance"], gold["team_performance"].head(1)])
    with pytest.raises(job.GoldValidationError, match="repeat the grain"):
        job.validate_gold_table("team_performance", df)


def test_reconciliation_fails_on_mismatch(silver, gold):
    gold = {**gold, "promotion_candidates": gold["promotion_candidates"].head(1)}
    with pytest.raises(job.GoldValidationError, match="Q3 candidates"):
        job.reconcile_gold(silver, gold)


def test_every_gold_table_has_a_builder_and_contract(gold):
    assert set(gold) == set(job.GOLD_CONTRACTS)


# --- every definition guard catches a corrupted row ------------------------------- #

GUARDS = [
    # (table, grain value(s), column, corrupt value, rule named in the error)
    ("employee_performance_summary", 1, "rating_change", 0, "rating_change = latest_period_rating - previous_rating"),
    ("employee_performance_summary", 1, "reviews_valid", 3, "reviews_valid <= reviews_total"),
    ("employee_performance_summary", 5, "latest_rating", 3, "latest_rating present iff a valid review exists"),
    ("employee_skill_gaps", 1, "shortfall", 1, "shortfall = max(0, required_level - proficiency_level)"),
    ("employee_skill_gaps", 1, "is_key", True, "is_key = required_level >= 4"),
    ("promotion_candidates", 1, "retention_priority", True, "retention_priority = tier A and attrition_risk High"),
    ("employee_learning", 3, "first_activity_date", date(2025, 1, 1), "activity dates present iff the employee has events"),
    ("employee_learning", 1, "first_activity_date", date(2025, 1, 31), "first_activity_date <= last_activity_date"),
    ("employee_course_completions", (2, "C3"), "completed", True, "completed/certified flags match their dates"),
    ("attrition_watchlist", 1, "performance_signal", True, "performance_signal = rating declined or latest_rating <= 2"),
    ("attrition_watchlist", 2, "risk_tier", "Low", "risk_tier combines performance_signal and recorded_attrition_risk"),
    ("attrition_watchlist", 1, "tier_rank", 1, "tier_rank / high_potential match their sources"),
    ("team_performance", 1, "on_time_rate", 0.9, "on_time_rate = projects_on_time / projects"),
    ("team_performance", 1, "people_rated", 10, "people_rated <= headcount"),
    ("project_performance", 2, "on_time", True, "on_time = outcome_status is on-time"),
    ("competency_summary", 1, "level_1", 5, "level counts sum to assessed"),
    ("competency_summary", 1, "critical_gaps", 4, "critical_gaps <= assessed"),
]


@pytest.mark.parametrize(("table", "key", "column", "value", "rule"), GUARDS, ids=[g[4][:40] for g in GUARDS])
def test_definition_guards_catch_corrupted_rows(gold, table, key, column, value, rule):
    df = gold[table].copy()
    grain = list(job.GOLD_CONTRACTS[table].grain)
    keys = key if isinstance(key, tuple) else (key,)
    mask = pd.Series(True, index=df.index)
    for col, k in zip(grain, keys):
        mask &= df[col] == k
    assert mask.sum() == 1
    df[column] = df[column].astype(object)
    df.loc[mask, column] = value
    with pytest.raises(job.GoldValidationError, match=re.escape(rule)):
        job.validate_gold_table(table, df)


def test_completion_row_needs_a_completion_or_certification(gold):
    df = gold["employee_course_completions"].copy().astype(object)
    mask = (df.employee_id == 2) & (df.course_name == "C3")
    df.loc[mask, ["certified", "certified_date"]] = [False, None]
    with pytest.raises(job.GoldValidationError, match="row has a completion or a certification"):
        job.validate_gold_table("employee_course_completions", df)


def test_trend_needs_two_review_periods(silver):
    one_period = {**silver, "performance_reviews": silver["performance_reviews"].query("review_period == '2025-H1'")}
    with pytest.raises(ValueError, match="at least two review periods"):
        job.build_performance_summary(one_period)
