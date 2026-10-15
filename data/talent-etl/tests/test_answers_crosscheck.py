"""Independent cross-check of the 7 business answers.

Gold is built by the pipeline (pandas). Here every answer is recomputed a
second way that shares no code with ``job.py``: plain SQL on the source
PostgreSQL tables (Q1-Q3, Q5-Q7) and plain-Python JSON parsing of the raw
event files (Q4). Each Gold table is then compared row by row, not only in
total. Skipped when PostgreSQL is not reachable.
"""

import json
from collections import defaultdict

import pandas as pd
import psycopg
import pytest

import job

RUN_DATE = "2026-10-14"
RATING_THIRDS = {1: "bottom third", 2: "middle third", 3: "top third"}

# Reviews that count: the review period did not end before the hire date.
# Summary per employee: rating in the previous and latest period, latest rating.
VALID_REVIEWS_SQL = """
WITH r AS (
  SELECT r.*, CASE WHEN right(review_period, 2) = 'H1'
                   THEN make_date(left(review_period, 4)::int, 6, 30)
                   ELSE make_date(left(review_period, 4)::int, 12, 31) END AS period_end
  FROM performance_reviews r),
valid AS (SELECT r.* FROM r JOIN employees e USING (employee_id) WHERE r.period_end >= e.hire_date),
periods AS (
  SELECT max(review_period) AS latest,
         (SELECT max(review_period) FROM performance_reviews
           WHERE review_period < (SELECT max(review_period) FROM performance_reviews)) AS previous
  FROM performance_reviews),
summ AS (
  SELECT e.employee_id,
         (SELECT v.rating FROM valid v, periods p WHERE v.employee_id = e.employee_id AND v.review_period = p.previous) AS prev,
         (SELECT v.rating FROM valid v, periods p WHERE v.employee_id = e.employee_id AND v.review_period = p.latest) AS latest_p,
         (SELECT v.rating FROM valid v WHERE v.employee_id = e.employee_id ORDER BY v.review_period DESC LIMIT 1) AS latest_rating
  FROM employees e)
"""

CRITICAL_SQL = "((required_level - proficiency_level >= 2) OR (required_level >= 4 AND required_level - proficiency_level >= 1))"


@pytest.fixture(scope="module")
def db():
    cfg = job.Config.from_env({})
    try:
        conn = psycopg.connect(
            host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_name, user=cfg.pg_user, password=cfg.pg_pass,
            connect_timeout=3,
        )  # fmt: skip
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    with conn:
        yield conn


@pytest.fixture(scope="module")
def gold(db, tmp_path_factory):
    """Run the pipeline once into a fresh lake and load its Gold tables."""
    lake = tmp_path_factory.mktemp("lake")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LAKE_ROOT", str(lake))
        mp.setenv("RUN_DATE", RUN_DATE)
        assert job.main() == 0
    return lambda table: pd.read_parquet(lake / "gold" / table / "data.parquet")


def sql(conn: psycopg.Connection, query: str) -> pd.DataFrame:
    cur = conn.execute(query)
    return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def sorted_by(df: pd.DataFrame, *keys: str) -> pd.DataFrame:
    return df.sort_values(list(keys)).reset_index(drop=True)


def test_q1_trend_and_latest_rating_for_every_employee(db, gold):
    mine = sql(db, VALID_REVIEWS_SQL + """
        SELECT employee_id, latest_rating,
               CASE WHEN prev IS NULL OR latest_p IS NULL THEN 'insufficient_data'
                    WHEN latest_p > prev THEN 'improved' WHEN latest_p = prev THEN 'stable' ELSE 'declined' END AS trend
        FROM summ ORDER BY employee_id""")  # fmt: skip
    g = sorted_by(gold("employee_performance_summary"), "employee_id")
    assert list(mine["trend"]) == list(g["trend"])
    assert list(mine["latest_rating"].astype("Float64")) == list(g["latest_rating"].astype("Float64"))
    assert mine["trend"].value_counts().to_dict() == {"improved": 721, "stable": 692, "insufficient_data": 396, "declined": 191}


def test_q1_history_keeps_every_review(db, gold):
    assert sql(db, "SELECT count(*) AS n FROM performance_reviews")["n"][0] == len(gold("employee_review_history")) == 4000


def test_q2_critical_gap_for_every_assessment(db, gold):
    mine = sql(db, f"SELECT assessment_id, employee_id, {CRITICAL_SQL} AS critical FROM competency_assessments ORDER BY 1")
    g = sorted_by(gold("employee_skill_gaps"), "assessment_id")
    assert list(mine["critical"]) == list(g["is_critical"])
    assert (mine.loc[mine["critical"], "employee_id"].nunique(), int(mine["critical"].sum())) == (441, 465)


def test_q3_candidates_tiers_and_retention(db, gold):
    mine = sql(db, VALID_REVIEWS_SQL + """
        SELECT t.employee_id, t.attrition_risk,
               CASE WHEN s.latest_rating >= 4 THEN 'A' WHEN s.latest_rating = 3 THEN 'B' ELSE 'C' END AS tier
        FROM talent_review t JOIN summ s USING (employee_id)
        WHERE t.potential_rating = 'High' AND t.promotion_readiness = 'Ready Now' ORDER BY employee_id""")  # fmt: skip
    g = sorted_by(gold("promotion_candidates"), "employee_id")
    assert list(mine["employee_id"]) == list(g["employee_id"])
    assert list(mine["tier"]) == list(g["tier"])
    retention = (mine["tier"] == "A") & (mine["attrition_risk"] == "High")
    assert list(retention) == list(g["retention_priority"])
    assert mine["tier"].value_counts().sort_index().to_dict() == {"A": 148, "B": 90, "C": 119} and retention.sum() == 18


def test_q4_training_completed_from_raw_event_files(db, gold):
    """Plain json.loads over the raw files: skip broken lines, missing/unknown employees and repeated event IDs."""
    employees = {row[0] for row in db.execute("SELECT employee_id FROM employees").fetchall()}
    seen: set[str] = set()
    completed: dict[int, set[str]] = defaultdict(set)
    certified: dict[int, set[str]] = defaultdict(set)
    for path in sorted(job.LOCAL_EVENTS_SOURCE.rglob("*.jsonl")):
        for line in path.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            emp = str(event.get("employee_id") or "").strip()
            if not emp.isdigit() or int(emp) not in employees or event["event_id"] in seen:
                continue
            seen.add(event["event_id"])
            if event["event_type"] == "course_completed":
                completed[int(emp)].add(event["course_name"])
            elif event["event_type"] == "certification_earned":
                certified[int(emp)].add(event["course_name"])

    g = gold("employee_learning").set_index("employee_id").sort_index()
    mine_completed = pd.Series({k: len(v) for k, v in completed.items()}).reindex(g.index, fill_value=0)
    mine_certified = pd.Series({k: len(v) for k, v in certified.items()}).reindex(g.index, fill_value=0)
    assert (mine_completed == g["courses_completed"]).all()
    assert (mine_certified == g["certifications"]).all()
    assert ((mine_completed > 0).sum(), (mine_certified > 0).sum()) == (1661, 1154)


def test_q5_risk_tier_for_every_employee(db, gold):
    mine = sql(db, VALID_REVIEWS_SQL + """
        SELECT s.employee_id, t.attrition_risk AS risk,
               COALESCE(s.latest_p < s.prev, false) OR COALESCE(s.latest_rating <= 2, false) AS signal
        FROM summ s JOIN talent_review t USING (employee_id) ORDER BY employee_id""")  # fmt: skip
    tiers = [
        ("Critical" if risk == "High" else "High" if risk == "Medium" else "Watch") if signal
        else ("High" if risk == "High" else "Low")
        for signal, risk in zip(mine["signal"], mine["risk"])
    ]  # fmt: skip
    g = sorted_by(gold("attrition_watchlist"), "employee_id")
    assert tiers == list(g["risk_tier"])
    assert pd.Series(tiers).value_counts().to_dict() == {"Low": 1392, "High": 294, "Watch": 171, "Critical": 143}


def test_q6_team_ratings_thirds_and_delivery(db, gold):
    mine = sql(db, VALID_REVIEWS_SQL + """,
        team_rating AS (SELECT e.team_id, avg(v.rating)::float AS avg_rating
                        FROM valid v JOIN employees e USING (employee_id) GROUP BY e.team_id),
        team_proj AS (SELECT team_id, count(*) AS projects, sum((outcome_status = 'on-time')::int) AS on_time
                      FROM projects GROUP BY team_id)
        SELECT p.team_id, r.avg_rating, p.on_time::float / p.projects AS on_time_rate,
               NTILE(3) OVER (ORDER BY r.avg_rating, p.team_id) AS third
        FROM team_proj p JOIN team_rating r USING (team_id) ORDER BY p.team_id""")  # fmt: skip
    g = sorted_by(gold("team_performance"), "team_id")
    assert list(mine["team_id"]) == list(g["team_id"]) and len(mine) == 27
    assert ((mine["avg_rating"] - g["avg_rating"]).abs() < 1e-9).all()
    assert list(mine["third"].map(RATING_THIRDS)) == list(g["rating_band"])
    assert ((mine["on_time_rate"] - g["on_time_rate"]).abs() < 1e-9).all()
    by_third = mine.groupby("third")["on_time_rate"].mean().round(3).tolist()
    assert by_third == [0.444, 0.556, 0.944]


def test_q7_competency_summary(db, gold):
    mine = sql(db, f"""
        SELECT competency_id, count(*) AS assessed, avg(proficiency_level)::float AS avg_proficiency,
               avg((proficiency_level >= required_level)::int)::float AS meets, sum({CRITICAL_SQL}::int) AS critical
        FROM competency_assessments GROUP BY competency_id ORDER BY competency_id""")  # fmt: skip
    g = sorted_by(gold("competency_summary"), "competency_id")
    assert list(mine["assessed"]) == list(g["assessed"]) and list(mine["critical"]) == list(g["critical_gaps"])
    assert ((mine["avg_proficiency"] - g["avg_proficiency"]).abs() < 1e-9).all()
    assert ((mine["meets"] - g["meets_required_share"]).abs() < 1e-9).all()


def test_q7_skill_distribution(db, gold):
    mine = sql(db, """
        SELECT a.competency_id, t.department, a.proficiency_level, count(*) AS n
        FROM competency_assessments a JOIN employees e USING (employee_id) JOIN teams t USING (team_id)
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")  # fmt: skip
    g = sorted_by(gold("skill_distribution"), "competency_id", "department", "proficiency_level")
    assert len(mine) == len(g) == 1595
    assert list(mine["n"]) == list(g["assessments"])
