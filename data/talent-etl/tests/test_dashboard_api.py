"""The dashboard data path: the pipeline's serving step, the Lambda that serves it, and the local server.

The pipeline is run once into a fresh lake (skipped when PostgreSQL is not
reachable); the Lambda's code (backend/talent-dashboard/function.py) is then
exercised against that lake on disk, on a local S3 (moto), and over HTTP.
"""

import json
import sys
import threading
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg
import pytest

import job

JOB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JOB_DIR.parents[1] / "backend" / "talent-dashboard"))
sys.path.insert(0, str(JOB_DIR / "dashboard"))
import api  # noqa: E402  - local server
import function  # noqa: E402  - the dashboard Lambda

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
    root = tmp_path_factory.mktemp("lake")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LAKE_ROOT", str(root))
        mp.setenv("RUN_DATE", RUN_DATE)
        assert job.main() == 0
    return root


@pytest.fixture(scope="module")
def store(lake):
    return function.LocalStore(lake)


def get(store, page):
    status, body = function.route(store, f"/api/talent-dashboard/{page}")
    return status, json.loads(body)


# --- the pipeline's serving step --------------------------------------------- #


def test_serving_step_writes_every_page(lake):
    files = sorted(p.relative_to(lake / "serving" / "dashboard").as_posix() for p in (lake / "serving" / "dashboard").rglob("*.json"))
    assert [f for f in files if "/" not in f] == sorted(f"{p}.json" for p in job.DASHBOARD_PAGES)
    assert len([f for f in files if f.startswith("employees/")]) == job.EMPLOYEE_PARTS == function.EMPLOYEE_PARTS
    manifest = json.loads(next((lake / "_runs").rglob("*.json")).read_text())
    step = next(s for s in manifest["steps"] if s["step"] == "serving.dashboard")
    assert step["records_written"] == len(files) and step["details"]["employee_pages"] == 2000


def test_records_are_json_safe():
    df = pd.DataFrame({"d": [date(2025, 1, 2), None], "x": [1.5, float("nan")]})
    assert job.records(df) == [{"d": "2025-01-02", "x": 1.5}, {"d": None, "x": None}]


# --- the Lambda's routes ------------------------------------------------------- #


def test_summary_cards_match_the_answers(store):
    status, body = get(store, "summary")
    assert status == 200
    assert {c["q"]: c["value"] for c in body["cards"]} == {
        "Q1": "721 improved", "Q2": "441 employees", "Q3": "148 tier A", "Q4": "1,661 employees",
        "Q5": "143 critical", "Q6": "94% vs 44%", "Q7": "75% meet requirement",
    }  # fmt: skip


@pytest.mark.parametrize(
    ("page", "keys"),
    [
        ("source", {"mode", "lake_root", "latest_run", "tables"}),
        ("pipeline", {"latest_run", "steps", "checks", "runs"}),
        ("q1", {"trend", "periods", "employees"}),
        ("q2", {"employees", "assessments", "key_only", "by_competency", "by_department", "rows"}),
        ("q3", {"candidates", "tiers", "retention_priority", "rows"}),
        ("q4", {"completed_any", "certified_any", "top_courses", "departments", "rows"}),
        ("q5", {"tiers", "grid", "reasons", "rows"}),
        ("q6", {"thirds", "project_types", "teams", "project_rows"}),
        ("q7", {"meets_share", "levels", "by_department", "by_category", "rows"}),
        ("employee/58", {"profile", "reviews", "skills", "learning", "courses", "promotion", "attrition"}),
    ],
)
def test_every_page_has_its_data(store, page, keys):
    status, body = get(store, page)
    assert status == 200 and keys <= set(body)


def test_page_numbers(store):
    q1 = get(store, "q1")[1]
    assert [(p["period"], p["avg_rating"]) for p in q1["periods"]] == [("2024-H2", 3.03), ("2025-H1", 3.42)]
    q2 = get(store, "q2")[1]
    assert (q2["employees"], q2["assessments"], q2["key_only"]) == (441, 465, 124)
    q6 = get(store, "q6")[1]
    assert [round(t["on_time_rate"], 3) for t in q6["thirds"]] == [0.444, 0.556, 0.944]
    source = get(store, "source")[1]
    assert (source["mode"], source["latest_run"]["checks_passed"]) == ("local", 15)
    steps = [s["step"] for s in get(store, "pipeline")[1]["steps"]]
    assert steps[-2:] == ["gold", "serving.dashboard"]
    events = next(s for s in get(store, "pipeline")[1]["steps"] if s["step"] == "silver.learning_activity")["details"]
    assert (events["duplicates_dropped"], events["event_date_repaired"], events["quarantine_reasons"]) == (
        152, 53, {"malformed_json": 13, "null_employee_id": 76})  # shown on the Data journey page
    eric = get(store, "employee/58")[1]
    assert eric["profile"]["first_name"] == "Eric" and eric["attrition"]["risk_tier"] == "Low"


@pytest.mark.parametrize(
    ("page", "status"),
    [("employee/99999", 404), ("employee/x", 400), ("nope", 404), ("q1/extra", 404), ("", 404)],
)
def test_bad_requests(store, page, status):
    assert get(store, page)[0] == status


def test_missing_data_says_to_run_the_pipeline(tmp_path):
    status, body = get(function.LocalStore(tmp_path), "summary")
    assert status == 404 and "run the pipeline" in body["error"]
    assert get(function.LocalStore(tmp_path), "source")[1]["latest_run"] is None


# --- the Lambda handler -------------------------------------------------------- #


def call(page, method="GET"):
    return function.handler({"rawPath": f"/api/talent-dashboard/{page}", "requestContext": {"http": {"method": method}}})


def test_handler_serves_pages(lake, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(lake))
    monkeypatch.setattr(function, "_store", None)
    res = call("q3")
    assert res["statusCode"] == 200 and res["headers"]["Content-Type"] == "application/json"
    assert json.loads(res["body"])["candidates"] == 357
    assert call("q3", method="POST")["statusCode"] == 405


def test_handler_reports_configuration_errors(monkeypatch):
    monkeypatch.delenv("LAKE_ROOT", raising=False)
    monkeypatch.delenv("DATA_LAKE_BUCKET", raising=False)
    monkeypatch.setattr(function, "_store", None)
    res = call("summary")
    assert res["statusCode"] == 500 and "LAKE_ROOT" in json.loads(res["body"])["error"]


def test_lake_root_from_bucket(monkeypatch):
    monkeypatch.delenv("LAKE_ROOT", raising=False)
    monkeypatch.setenv("DATA_LAKE_BUCKET", "my-lake")
    assert function.lake_root_from_env() == "s3://my-lake/lake"


def test_s3_store(lake, s3):
    """The same routes on S3 (moto), as the Lambda runs them."""
    s3.create_bucket(Bucket="dash-lake")
    for path in [*lake.joinpath("serving").rglob("*.json"), *lake.joinpath("_runs").rglob("*.json")]:
        s3.upload_file(str(path), "dash-lake", f"lake/{path.relative_to(lake).as_posix()}")
    s3_store = function.open_store("s3://dash-lake/lake")
    assert isinstance(s3_store, function.S3Store)
    assert get(s3_store, "summary")[1]["cards"][1]["value"] == "441 employees"
    assert get(s3_store, "employee/58")[1]["profile"]["employee_id"] == 58
    assert get(s3_store, "source")[1]["mode"] == "cloud"
    assert get(function.open_store("s3://dash-lake/empty"), "q1")[0] == 404


# --- the local server ------------------------------------------------------------ #


def test_local_server_over_http(store):
    server = api.ThreadingHTTPServer(("127.0.0.1", 0), api.make_handler(store))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}/api/talent-dashboard"
    try:
        with urllib.request.urlopen(f"{base}/summary") as res:
            assert res.status == 200 and len(json.load(res)["cards"]) == 7
        with pytest.raises(urllib.error.HTTPError) as err:
            urllib.request.urlopen(f"{base}/employee/99999")
        assert err.value.code == 404
    finally:
        server.shutdown()
