"""
Talent dashboard API: serves the data the talent-etl pipeline prepares for the React dashboard.

The pipeline (data/talent-etl/job.py, step ``serving.dashboard``) writes each
dashboard page as JSON under ``<lake>/serving/dashboard/``. This function only
reads those files and the pipeline's run records from S3, so it needs no pandas
and fits the workshop's 128 MB Lambda. It never writes anything.

Routes (CloudFront forwards /api/talent-dashboard/* here):
    GET /api/talent-dashboard/source          where the data is and which run produced it
    GET /api/talent-dashboard/pipeline        steps, Gold checks and recent runs
    GET /api/talent-dashboard/summary         one headline answer per question
    GET /api/talent-dashboard/q1 ... /q7      one page per question
    GET /api/talent-dashboard/employee/<id>   everything about one employee
"""

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SERVICE = "talent-dashboard"
PAGES = {"summary", "q1", "q2", "q3", "q4", "q5", "q6", "q7"}
EMPLOYEE_PARTS = 100  # must match job.py: employee pages are grouped by employee_id % 100
DASHBOARD = "serving/dashboard"


class S3Store:
    """Read-only access to the lake on S3 (``s3://bucket/prefix``)."""

    def __init__(self, root):
        import boto3  # provided by the Lambda runtime

        self.root = root.rstrip("/")
        self.bucket, _, prefix = self.root.removeprefix("s3://").partition("/")
        self.prefix = f"{prefix}/" if prefix else ""
        self.s3 = boto3.client("s3")

    def read(self, key):
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=self.prefix + key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def list(self, prefix):
        keys = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix + prefix):
            keys += [o["Key"][len(self.prefix):] for o in page.get("Contents", [])]
        return keys


class LocalStore:
    """Read-only access to a lake folder on disk (local development and tests)."""

    def __init__(self, root):
        self.root = str(root).rstrip("/")
        self.path = Path(self.root)

    def read(self, key):
        path = self.path / key
        return path.read_bytes() if path.is_file() else None

    def list(self, prefix):
        base = self.path / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.path)) for p in base.rglob("*") if p.is_file())


def open_store(root):
    return S3Store(root) if root.startswith("s3://") else LocalStore(root)


def lake_root_from_env():
    """LAKE_ROOT if set, else the data-lake bucket's lake/ folder."""
    root = os.getenv("LAKE_ROOT", "").strip()
    if root:
        return root
    bucket = os.getenv("DATA_LAKE_BUCKET", "").strip()
    if bucket:
        return f"s3://{bucket}/lake"
    raise RuntimeError("LAKE_ROOT (or DATA_LAKE_BUCKET) is not set")


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #


def runs(store):
    """Every run record, newest first."""
    records = [json.loads(store.read(k)) for k in store.list("_runs/") if k.endswith(".json")]
    return sorted(records, key=lambda r: str(r.get("ended_at")), reverse=True)


def gold_step(run):
    return next((s for s in run.get("steps", []) if s.get("step") == "gold"), {})


def source(store):
    ok = [r for r in runs(store) if r.get("status") == "succeeded"]
    latest = None
    tables = {}
    if ok:
        details = gold_step(ok[0]).get("details", {})
        checks = details.get("reconciliation", {})
        tables = details.get("tables", {})
        latest = {
            "run_id": ok[0]["run_id"],
            "ended_at": ok[0]["ended_at"],
            "duration_s": ok[0]["duration_s"],
            "status": ok[0]["status"],
            "checks_passed": sum(str(v).startswith("PASS") for v in checks.values()),
            "checks_total": len(checks),
        }
    return {
        "mode": "cloud" if store.root.startswith("s3://") else "local",
        "lake_root": store.root,
        "latest_run": latest,
        "tables": [{"label": k, "value": v} for k, v in tables.items()],
    }


def pipeline(store):
    all_runs = runs(store)
    latest = next((r for r in all_runs if r.get("status") == "succeeded"), None)
    steps = []
    checks = {}
    if latest:
        keys = ("step", "status", "records_read", "records_written", "records_quarantined", "duration_s", "details")
        steps = [{k: s.get(k) for k in keys} for s in latest["steps"]]
        checks = gold_step(latest).get("details", {}).get("reconciliation", {})
    return {
        "latest_run": latest and {k: latest[k] for k in ("run_id", "status", "started_at", "ended_at", "duration_s")},
        "steps": steps,
        "checks": [{"check": k, "result": v} for k, v in checks.items()],
        "runs": [{k: r.get(k) for k in ("run_id", "status", "ended_at", "duration_s", "error")} for r in all_runs[:10]],
    }


def route(store, path):
    """Answer a GET path: returns (HTTP status, JSON body as bytes)."""
    parts = [p for p in path.split("?")[0].split("/") if p]
    if parts[:1] == ["api"]:
        parts = parts[1:]
    if parts[:1] == [SERVICE]:
        parts = parts[1:]
    if not parts:
        return 404, _json({"error": "not found", "pages": sorted(PAGES | {"source", "pipeline", "employee/<id>"})})

    name = parts[0]
    if name == "source" and len(parts) == 1:
        return 200, _json(source(store))
    if name == "pipeline" and len(parts) == 1:
        return 200, _json(pipeline(store))
    if name in PAGES and len(parts) == 1:
        data = store.read(f"{DASHBOARD}/{name}.json")
        if data is None:
            return 404, _json({"error": f"no dashboard data under {store.root}/{DASHBOARD} - run the pipeline first"})
        return 200, data  # already JSON: passed through without parsing
    if name == "employee" and len(parts) == 2:
        if not parts[1].isdigit():
            return 400, _json({"error": "employee id must be a number"})
        part = store.read(f"{DASHBOARD}/employees/part-{int(parts[1]) % EMPLOYEE_PARTS:02d}.json")
        page = json.loads(part).get(str(int(parts[1]))) if part else None
        return (200, _json(page)) if page else (404, _json({"error": f"no employee {parts[1]}"}))
    return 404, _json({"error": "not found"})


def _json(obj):
    return json.dumps(obj).encode()


# --------------------------------------------------------------------------- #
# Lambda entry point
# --------------------------------------------------------------------------- #

_store = None


def handler(event=None, context=None):
    """Lambda function URL handler (payload format 2.0)."""
    global _store
    event = event or {}
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath") or event.get("path") or "/"
    try:
        if method not in ("GET", "HEAD"):
            status, body = 405, _json({"error": "read-only API: GET only"})
        else:
            if _store is None:
                _store = open_store(lake_root_from_env())
            status, body = route(_store, path)
    except Exception as e:  # report any failure as JSON, with the cause in the logs
        logger.exception("request failed: %s", path)
        status, body = 500, _json({"error": f"{type(e).__name__}: {e}"})
    logger.info("GET %s -> %s", path, status)
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "body": body.decode(),
    }
