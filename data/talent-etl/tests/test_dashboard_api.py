"""The dashboard API serves the Gold answers correctly. Skipped without a local Gold layer."""

import json
import sys
import threading
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

JOB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JOB_DIR / "dashboard"))
import api  # noqa: E402


@pytest.fixture(scope="module")
def lake():
    if not (JOB_DIR / "lake" / "gold").exists():
        pytest.skip("no Gold layer under lake/ - run job.py first")
    return api.Lake(str(JOB_DIR / "lake"))


def get(lake, path):
    return api.handle(lake, path)


def test_summary_cards_match_the_answers(lake):
    status, body = get(lake, "/api/summary")
    values = {c["q"]: c["value"] for c in body["cards"]}
    assert status == 200
    assert values == {
        "Q1": "721 improved", "Q2": "441 employees", "Q3": "148 tier A", "Q4": "1,661 employees",
        "Q5": "143 critical", "Q6": "94% vs 44%", "Q7": "75% meet requirement",
    }  # fmt: skip


@pytest.mark.parametrize(
    ("path", "keys"),
    [
        ("/api/source", {"mode", "lake_root", "latest_run", "tables"}),
        ("/api/q1", {"trend", "periods", "employees"}),
        ("/api/q2", {"employees", "assessments", "key_only", "by_competency", "rows"}),
        ("/api/q3", {"candidates", "tiers", "retention_priority", "rows"}),
        ("/api/q4", {"completed_any", "certified_any", "top_courses", "departments", "rows"}),
        ("/api/q5", {"tiers", "grid", "rows"}),
        ("/api/q6", {"thirds", "teams", "project_rows"}),
        ("/api/q7", {"meets_share", "levels", "rows"}),
        ("/api/pipeline", {"latest_run", "steps", "checks", "runs"}),
        ("/api/employee/58", {"profile", "reviews", "skills", "courses", "attrition"}),
    ],
)
def test_every_page_has_its_data(lake, path, keys):
    status, body = get(lake, path)
    assert status == 200 and keys <= set(body)
    json.dumps(body)  # serialisable as sent


def test_page_numbers(lake):
    q1 = get(lake, "/api/q1")[1]
    assert [(p["period"], p["avg_rating"]) for p in q1["periods"]] == [("2024-H2", 3.03), ("2025-H1", 3.42)]
    q2 = get(lake, "/api/q2")[1]
    assert (q2["employees"], q2["assessments"], q2["key_only"]) == (441, 465, 124)
    q6 = get(lake, "/api/q6")[1]
    assert [round(t["on_time_rate"], 3) for t in q6["thirds"]] == [0.444, 0.556, 0.944]
    assert get(lake, "/api/source")[1]["latest_run"]["checks_passed"] == 15


@pytest.mark.parametrize(("path", "status"), [("/api/employee/99999", 404), ("/api/employee/x", 400), ("/api/nope", 404), ("/", 404)])
def test_bad_requests(lake, path, status):
    assert get(lake, path)[0] == status


def test_records_are_json_safe():
    df = pd.DataFrame({"d": [date(2025, 1, 2), None], "x": [1.5, float("nan")]})
    assert api.records(df) == [{"d": "2025-01-02", "x": 1.5}, {"d": None, "x": None}]


def test_served_over_http(lake):
    server = api.ThreadingHTTPServer(("127.0.0.1", 0), api.make_handler(lake))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urllib.request.urlopen(f"{base}/api/summary") as res:
            assert res.status == 200 and res.headers["Content-Type"] == "application/json"
            assert len(json.load(res)["cards"]) == 7
        with pytest.raises(urllib.error.HTTPError) as err:
            urllib.request.urlopen(f"{base}/api/employee/99999")
        assert err.value.code == 404
    finally:
        server.shutdown()
