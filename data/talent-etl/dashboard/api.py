"""Read-only JSON API over the Gold layer, for the React dashboard in ``web/``.

Reads Gold the same way the notebook does: ``LAKE_ROOT`` is a local folder or an
``s3://`` URI, defaulting to ``data/talent-etl/lake``. Gold is re-read at most
every 30 seconds, so after a pipeline run the dashboard shows the new answers on
the next refresh. It never writes anything.

    .venv/bin/python dashboard/api.py        # http://127.0.0.1:8510/api/source

Endpoints (all GET): /api/source, /api/summary, /api/q1 ... /api/q7,
/api/employee/<id>, /api/pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import fsspec
import pandas as pd

JOB_DIR = Path(__file__).resolve().parent.parent
CACHE_SECONDS = 30
GOLD_TABLES = (
    "employee_review_history",
    "employee_performance_summary",
    "employee_skill_gaps",
    "promotion_candidates",
    "employee_learning",
    "employee_course_completions",
    "attrition_watchlist",
    "team_performance",
    "project_performance",
    "skill_distribution",
    "competency_summary",
)
TRENDS = ["improved", "stable", "declined", "insufficient_data"]
RISK_TIERS = ["Critical", "High", "Watch", "Low"]
RATING_BANDS = ["bottom third", "middle third", "top third"]
PERSON = ["employee_id", "first_name", "last_name", "department", "team_name", "job_level"]

log = logging.getLogger("dashboard")


# --------------------------------------------------------------------------- #
# Reading Gold
# --------------------------------------------------------------------------- #


class Lake:
    """The Gold tables and run records under one lake root, cached briefly."""

    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")
        self.fs, self.path = fsspec.core.url_to_fs(self.root)
        self._lock = threading.Lock()
        self._gold: dict[str, pd.DataFrame] | None = None
        self._loaded_at = 0.0

    @property
    def is_cloud(self) -> bool:
        return self.root.startswith("s3://")

    def gold(self) -> dict[str, pd.DataFrame]:
        with self._lock:
            if self._gold is None or time.monotonic() - self._loaded_at > CACHE_SECONDS:
                self.fs.invalidate_cache()
                self._gold = {name: self._read_table(name) for name in GOLD_TABLES}
                self._loaded_at = time.monotonic()
            return self._gold

    def _read_table(self, name: str) -> pd.DataFrame:
        files = sorted(self.fs.glob(f"{self.path}/gold/{name}/*.parquet"))
        if not files:
            raise FileNotFoundError(f"no Gold table {name} under {self.root}/gold - run the pipeline first")
        frames = []
        for f in files:
            with self.fs.open(f, "rb") as fh:
                frames.append(pd.read_parquet(fh))
        return pd.concat(frames, ignore_index=True)

    def runs(self) -> list[dict]:
        """Every run record, newest first."""
        runs = []
        for f in self.fs.glob(f"{self.path}/_runs/run_date=*/*.json"):
            runs.append(json.loads(self.fs.cat_file(f)))
        return sorted(runs, key=lambda m: str(m.get("ended_at")), reverse=True)


def records(df: pd.DataFrame) -> list[dict]:
    """Rows as JSON-safe dicts: dates as ISO strings, missing values as null."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object or pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].map(lambda v: v.isoformat() if hasattr(v, "isoformat") else v)
    return json.loads(out.to_json(orient="records"))


def counts(series: pd.Series, order: list) -> list[dict]:
    vc = series.value_counts()
    return [{"label": str(k), "value": int(vc.get(k, 0))} for k in order]


def pct(x: float) -> str:
    return f"{x:.0%}"


# --------------------------------------------------------------------------- #
# One payload per page
# --------------------------------------------------------------------------- #


def source(lake: Lake) -> dict:
    g = lake.gold()
    ok = [r for r in lake.runs() if r.get("status") == "succeeded"]
    latest = ok[0] if ok else None
    run = None
    if latest:
        gold_step = next((s for s in latest["steps"] if s["step"] == "gold"), {})
        checks = gold_step.get("details", {}).get("reconciliation", {})
        run = {
            "run_id": latest["run_id"],
            "ended_at": latest["ended_at"],
            "duration_s": latest["duration_s"],
            "status": latest["status"],
            "checks_passed": sum(str(v).startswith("PASS") for v in checks.values()),
            "checks_total": len(checks),
        }
    return {
        "mode": "cloud" if lake.is_cloud else "local",
        "lake_root": lake.root,
        "latest_run": run,
        "tables": [{"label": n, "value": len(df)} for n, df in g.items()],
    }


def summary(g: dict[str, pd.DataFrame]) -> dict:
    s, gaps, cand = g["employee_performance_summary"], g["employee_skill_gaps"], g["promotion_candidates"]
    learn, watch, teams = g["employee_learning"], g["attrition_watchlist"], g["team_performance"]
    comp = g["competency_summary"]
    n = len(s)
    trend = s["trend"].value_counts()
    crit_emp = gaps.loc[gaps["is_critical"], "employee_id"].nunique()
    tiers = cand["tier"].value_counts()
    risk = watch["risk_tier"].value_counts()
    bands = teams.groupby("rating_band")["on_time_rate"].mean()
    meets = (comp["meets_required_share"] * comp["assessed"]).sum() / comp["assessed"].sum()
    weakest = comp.sort_values("avg_proficiency").iloc[0]
    return {
        "cards": [
            {"q": "Q1", "title": "Performance ratings", "value": f"{int(trend.get('improved', 0)):,} improved",
             "detail": f"{int(trend.get('stable', 0)):,} stable, {int(trend.get('declined', 0)):,} declined, "
                       f"{int(trend.get('insufficient_data', 0)):,} without a measurable trend"},
            {"q": "Q2", "title": "Critical skill gaps", "value": f"{crit_emp:,} employees",
             "detail": f"{crit_emp / n:.0%} of employees; {int(gaps['is_critical'].sum()):,} critical assessments"},
            {"q": "Q3", "title": "Ready for promotion", "value": f"{int(tiers.get('A', 0))} tier A",
             "detail": f"{len(cand)} high-potential, ready-now candidates; "
                       f"{int(cand['retention_priority'].sum())} are retention priorities"},
            {"q": "Q4", "title": "Training completed", "value": f"{int((learn['courses_completed'] > 0).sum()):,} employees",
             "detail": f"completed at least one course; {int((learn['certifications'] > 0).sum()):,} hold a certification"},
            {"q": "Q5", "title": "Attrition risk", "value": f"{int(risk.get('Critical', 0))} critical",
             "detail": f"{int(risk.get('High', 0))} high, {int(risk.get('Watch', 0))} watch; "
                       f"{int(((watch['risk_tier'] == 'Critical') & watch['high_potential']).sum())} critical are high potential"},
            {"q": "Q6", "title": "Teams and projects", "value": f"{bands.get('top third', 0):.0%} vs {bands.get('bottom third', 0):.0%}",
             "detail": "on-time delivery, top-rated vs bottom-rated third of teams"},
            {"q": "Q7", "title": "Skills distribution", "value": f"{meets:.0%} meet requirement",
             "detail": f"weakest skill: {weakest['competency_name']} (average level {weakest['avg_proficiency']:.2f})"},
        ]  # fmt: skip
    }


def q1(g: dict[str, pd.DataFrame]) -> dict:
    s, hist = g["employee_performance_summary"], g["employee_review_history"]
    valid = hist[~hist["flag_period_before_hire"].astype(bool)]
    by_period = valid.groupby("review_period")["rating"]
    dist = valid.groupby(["review_period", "rating"]).size().unstack(fill_value=0).reindex(columns=range(1, 6), fill_value=0)
    return {
        "trend": counts(s["trend"], TRENDS),
        "periods": [
            {"period": p, "avg_rating": round(float(r.mean()), 2), "reviews": int(r.size),
             "levels": [int(dist.loc[p, lvl]) for lvl in range(1, 6)]}
            for p, r in by_period
        ],  # fmt: skip
        "flagged_reviews": int(hist["flag_period_before_hire"].sum()),
        "employees": records(s[PERSON + ["previous_rating", "latest_period_rating", "rating_change", "trend", "reviews_valid"]]),
    }


def q2(g: dict[str, pd.DataFrame]) -> dict:
    gaps, comp = g["employee_skill_gaps"], g["competency_summary"]
    crit = gaps[gaps["is_critical"]]
    dept = gaps.groupby("department").apply(
        lambda d: d.loc[d["is_critical"], "employee_id"].nunique() / d["employee_id"].nunique(), include_groups=False
    )
    worst = comp.sort_values("critical_gap_rate", ascending=False).head(10)
    return {
        "employees": int(crit["employee_id"].nunique()),
        "assessments": int(len(crit)),
        "total_employees": int(gaps["employee_id"].nunique()),
        # flagged only because of the key rule: 1 level short on a key skill, never 2+ short anywhere
        "key_only": int(crit["employee_id"].nunique() - crit.loc[crit["shortfall"] >= 2, "employee_id"].nunique()),
        "by_competency": [{"label": r.competency_name, "value": float(r.critical_gap_rate)} for r in worst.itertuples()],
        "by_department": [{"label": k, "value": float(v)} for k, v in dept.sort_values(ascending=False).items()],
        "rows": records(crit[PERSON + ["competency_name", "category", "required_level", "proficiency_level", "shortfall", "is_key"]]),
    }


def q3(g: dict[str, pd.DataFrame]) -> dict:
    c = g["promotion_candidates"].sort_values("rank")
    return {
        "candidates": int(len(c)),
        "tiers": counts(c["tier"], ["A", "B", "C"]),
        "retention_priority": int(c["retention_priority"].sum()),
        "rows": records(c[["rank", *PERSON, "tier", "latest_rating", "trend", "courses_completed", "certifications",
                           "attrition_risk", "retention_priority", "recommendation"]]),
    }  # fmt: skip


def q4(g: dict[str, pd.DataFrame]) -> dict:
    learn, comp = g["employee_learning"], g["employee_course_completions"]
    cc = learn["courses_completed"]
    dept = (
        learn.groupby("department")
        .agg(
            employees=("employee_id", "size"),
            completed_a_course=("courses_completed", lambda s: (s > 0).mean()),
            certified=("certifications", lambda s: (s > 0).mean()),
            courses_per_employee=("courses_completed", "mean"),
        )
        .sort_values("certified", ascending=False)
        .reset_index()
    )
    top = comp[comp["completed"]].groupby("course_name").size().sort_values(ascending=False).head(10)
    hist = cc.value_counts().sort_index()
    return {
        "completed_any": int((cc > 0).sum()),
        "certified_any": int((learn["certifications"] > 0).sum()),
        "employees": int(len(learn)),
        "completions": int(comp["completed"].sum()),
        "certifications": int(comp["certified"].sum()),
        "courses": int(comp["course_name"].nunique()),
        "courses_histogram": [{"label": str(k), "value": int(v)} for k, v in hist.items()],
        "top_courses": [{"label": k, "value": int(v)} for k, v in top.items()],
        "departments": records(dept),
        "rows": records(learn[PERSON + ["courses_completed", "certifications", "quizzes_passed", "avg_quiz_score",
                                        "last_activity_date"]]),
    }  # fmt: skip


def q5(g: dict[str, pd.DataFrame]) -> dict:
    w = g["attrition_watchlist"]
    grid = pd.crosstab(w["performance_signal"], w["recorded_attrition_risk"]).reindex(
        index=[True, False], columns=["High", "Medium", "Low"], fill_value=0
    )
    return {
        "tiers": counts(w["risk_tier"], RISK_TIERS),
        "critical_high_potential": int(((w["risk_tier"] == "Critical") & w["high_potential"]).sum()),
        "reasons": [{"label": k, "value": int(v)} for k, v in w["signal_reason"].value_counts().items()],
        "grid": {"rows": ["Performance slipping", "No performance warning"], "columns": list(grid.columns),
                 "values": grid.values.tolist()},
        "rows": records(w[PERSON + ["previous_rating", "latest_period_rating", "trend", "signal_reason",
                                    "recorded_attrition_risk", "risk_tier", "high_potential"]]),
    }  # fmt: skip


def q6(g: dict[str, pd.DataFrame]) -> dict:
    t, p = g["team_performance"], g["project_performance"]
    thirds = (
        t.groupby("rating_band")
        .agg(teams=("team_id", "size"), avg_rating=("avg_rating", "mean"), projects=("projects", "sum"),
             on_time=("projects_on_time", "sum"), on_time_rate=("on_time_rate", "mean"))  # fmt: skip
        .reindex(RATING_BANDS)
        .reset_index()
    )
    types = p.groupby("project_type").agg(projects=("project_id", "size"), on_time=("on_time", "mean")).reset_index()
    return {
        "teams_with_projects": int(len(t)),
        "projects": int(len(p)),
        "thirds": records(thirds),
        "project_types": records(types.sort_values("on_time")),
        "teams": records(t[["team_name", "department", "headcount", "people_rated", "avg_rating", "low_performer_share",
                            "rating_band", "projects", "projects_on_time", "on_time_rate", "avg_outcome_score"]]
                         .sort_values("avg_rating", ascending=False)),
        "project_rows": records(p[["project_name", "project_type", "team_name", "department", "outcome_status",
                                   "outcome_score", "team_avg_rating", "team_rating_band"]]),
    }  # fmt: skip


def q7(g: dict[str, pd.DataFrame]) -> dict:
    comp, dist = g["competency_summary"], g["skill_distribution"]
    meets = (comp["meets_required_share"] * comp["assessed"]).sum() / comp["assessed"].sum()
    levels = comp[[f"level_{i}" for i in range(1, 6)]].sum()
    weighted = dist.assign(points=dist["proficiency_level"] * dist["assessments"])
    dept = weighted.groupby("department")[["points", "assessments"]].sum()
    cat = comp.groupby("category").apply(
        lambda d: (d["avg_proficiency"] * d["assessed"]).sum() / d["assessed"].sum(), include_groups=False
    )
    return {
        "assessments": int(comp["assessed"].sum()),
        "meets_share": float(meets),
        "levels": [{"label": f"Level {i}", "value": int(levels[f"level_{i}"])} for i in range(1, 6)],
        "by_department": [{"label": k, "value": round(float(v), 2)}
                          for k, v in (dept["points"] / dept["assessments"]).sort_values().items()],
        "by_category": [{"label": k, "value": round(float(v), 2)} for k, v in cat.items()],
        "rows": records(comp[["competency_name", "category", "assessed", "avg_proficiency", "avg_required_level",
                              "meets_required_share", "critical_gaps", "level_1", "level_2", "level_3", "level_4",
                              "level_5"]].sort_values("avg_proficiency")),
    }  # fmt: skip


def employee(g: dict[str, pd.DataFrame], employee_id: int) -> dict | None:
    s = g["employee_performance_summary"]
    me = s[s["employee_id"] == employee_id]
    if me.empty:
        return None

    def rows(table: str, cols: list[str] | None = None) -> list[dict]:
        df = g[table]
        df = df[df["employee_id"] == employee_id]
        return records(df[cols] if cols else df)

    return {
        "profile": records(me)[0],
        "reviews": rows("employee_review_history", ["review_period", "rating", "manager_comment", "submitted_date",
                                                    "flag_period_before_hire"]),
        "skills": rows("employee_skill_gaps", ["competency_name", "category", "required_level", "proficiency_level",
                                               "shortfall", "is_key", "is_critical"]),
        "learning": (rows("employee_learning") or [None])[0],
        "courses": rows("employee_course_completions", ["course_name", "completed", "completed_date", "certified",
                                                        "certified_date"]),
        "promotion": (rows("promotion_candidates", ["rank", "tier", "recommendation", "retention_priority"]) or [None])[0],
        "attrition": rows("attrition_watchlist", ["recorded_attrition_risk", "performance_signal", "signal_reason",
                                                  "risk_tier"])[0],
    }  # fmt: skip


def pipeline(lake: Lake) -> dict:
    runs = lake.runs()
    latest = next((r for r in runs if r.get("status") == "succeeded"), None)
    steps, checks = [], {}
    if latest:
        for s in latest["steps"]:
            steps.append({k: s.get(k) for k in ("step", "status", "records_read", "records_written",
                                                 "records_quarantined", "duration_s")})  # fmt: skip
            if s["step"] == "gold":
                checks = s.get("details", {}).get("reconciliation", {})
    return {
        "latest_run": latest and {k: latest[k] for k in ("run_id", "status", "started_at", "ended_at", "duration_s")},
        "steps": steps,
        "checks": [{"check": k, "result": v} for k, v in checks.items()],
        "runs": [{k: r.get(k) for k in ("run_id", "status", "ended_at", "duration_s", "error")} for r in runs[:10]],
    }


# --------------------------------------------------------------------------- #
# HTTP server
# --------------------------------------------------------------------------- #

PAGES = {"summary": summary, "q1": q1, "q2": q2, "q3": q3, "q4": q4, "q5": q5, "q6": q6, "q7": q7}


def handle(lake: Lake, path: str) -> tuple[int, object]:
    """Route a GET path to its payload: (HTTP status, JSON body)."""
    parts = [p for p in path.split("?")[0].split("/") if p]
    if len(parts) < 2 or parts[0] != "api":
        return 404, {"error": "not found"}
    name = parts[1]
    if name == "source":
        return 200, source(lake)
    if name == "pipeline":
        return 200, pipeline(lake)
    if name in PAGES and len(parts) == 2:
        return 200, PAGES[name](lake.gold())
    if name == "employee" and len(parts) == 3:
        if not parts[2].isdigit():
            return 400, {"error": "employee id must be a number"}
        found = employee(lake.gold(), int(parts[2]))
        return (200, found) if found else (404, {"error": f"no employee {parts[2]}"})
    return 404, {"error": "not found"}


def make_handler(lake: Lake) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - name required by BaseHTTPRequestHandler
            try:
                status, body = handle(lake, self.path)
            except Exception as exc:  # noqa: BLE001 - report any read error to the page
                log.exception("request failed: %s", self.path)
                status, body = 500, {"error": f"{type(exc).__name__}: {exc}"}
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt: str, *args: object) -> None:
            log.info("%s %s", self.address_string(), fmt % args)

    return Handler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    lake = Lake(os.environ.get("LAKE_ROOT") or str(JOB_DIR / "lake"))
    port = int(os.environ.get("DASHBOARD_PORT", "8510"))
    log.info("reading Gold from %s (%s)", lake.root, "cloud" if lake.is_cloud else "local")
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(lake))
    log.info("API on http://127.0.0.1:%d/api/source", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
