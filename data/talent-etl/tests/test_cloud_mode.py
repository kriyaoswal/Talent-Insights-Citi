"""Cloud mode end to end on S3 (moto): what the EKS job does on its first deploy, without AWS.

IS_LOCAL=false (strict settings, SSL to PostgreSQL), sources and lake on S3, and
an empty database that the job loads itself from the S3 seed files (as it does
with Aurora). Requires local PostgreSQL; skipped otherwise.
"""

import glob
import json
from pathlib import Path

import pandas as pd
import psycopg
import pytest

import job

SAMPLE = Path(job.LOCAL_EVENTS_SOURCE).parent
BUCKET = "coding-workshop-datalake-test"
DB = "talent_cloud_mode_test"
RUN_DATE = "2026-10-14"
EXPECTED = {
    "q2_employees_with_critical_gap": 441,
    "q3_candidates_by_tier": {"A": 148, "B": 90, "C": 119},
    "q5_risk_tiers": {"Critical": 143, "High": 294, "Watch": 171, "Low": 1392},
    "q6_on_time_by_rating_band": {"bottom third": 0.444, "middle third": 0.556, "top third": 0.944},
}


def admin_conn(cfg: job.Config):
    return psycopg.connect(host=cfg.pg_host, port=cfg.pg_port, user=cfg.pg_user, password=cfg.pg_pass,
                           dbname="postgres", autocommit=True, connect_timeout=3)  # fmt: skip


@pytest.fixture(scope="session")
def s3_sources(moto_endpoint):
    """Upload the sample sources to moto once per session (the 'S3 Storage' data source)."""
    import boto3

    s3 = boto3.client("s3", endpoint_url=moto_endpoint, region_name="us-east-1",
                      aws_access_key_id="test", aws_secret_access_key="test")  # fmt: skip
    s3.create_bucket(Bucket=BUCKET)
    for path in sorted((SAMPLE / "learning_activity").rglob("*.jsonl")):
        s3.upload_file(str(path), BUCKET, f"source/learning_activity/{path.relative_to(SAMPLE / 'learning_activity')}")
    for path in [*SAMPLE.glob("*.csv"), SAMPLE / "schema.sql"]:
        s3.upload_file(str(path), BUCKET, f"source/postgres_seed/{path.name}")
    return BUCKET


@pytest.fixture
def cloud(s3, s3_sources, monkeypatch, request):
    """Cloud settings, a fresh empty database and a fresh lake prefix for each test."""
    local = job.Config.from_env({})
    try:
        with admin_conn(local) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{DB}"')
            conn.execute(f'CREATE DATABASE "{DB}"')
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    lake = f"lake-{request.node.name}"
    env = {
        "IS_LOCAL": "false", "POSTGRES_HOST": local.pg_host, "POSTGRES_PORT": str(local.pg_port), "POSTGRES_NAME": DB,
        "POSTGRES_USER": local.pg_user, "POSTGRES_PASS": local.pg_pass, "RUN_DATE": RUN_DATE,
        "LAKE_ROOT": f"s3://{BUCKET}/{lake}", "EVENTS_SOURCE_URI": f"s3://{BUCKET}/source/learning_activity",
        "SEED_SOURCE_URI": f"s3://{BUCKET}/source/postgres_seed",
    }  # fmt: skip
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    yield s3, lake

    with admin_conn(local) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{DB}"')
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{lake}/"):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=BUCKET, Key=obj["Key"])


def manifests(cloud) -> list[dict]:
    s3, lake = cloud
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{lake}/_runs/").get("Contents", [])]
    runs = [json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()) for k in keys]
    return sorted(runs, key=lambda m: m["ended_at"])


def step(m: dict, name: str) -> dict:
    return next(s for s in m["steps"] if s["step"] == name)


def gold_snapshot(cloud) -> dict[str, pd.DataFrame]:
    return {n: job.read_parquet_dataset(f"s3://{BUCKET}/{cloud[1]}/gold/{n}") for n in job.GOLD_CONTRACTS}


def test_first_cloud_run_seeds_the_database_and_answers(cloud):
    assert job.main() == 0
    (m,) = manifests(cloud)
    seed = step(m, "source.seed")
    assert seed["details"]["schema"] == "created" and seed["records_written"] == 12173
    assert step(m, "bronze.learning_activity")["details"]["files_copied"] == 783
    hl = step(m, "gold")["details"]["highlights"]
    assert {k: hl[k] for k in EXPECTED} == EXPECTED

    # The Gold written to S3 equals the Gold the local run produces (when the local lake exists).
    local_gold = Path(job.JOB_DIR) / "lake" / "gold"
    if local_gold.exists():
        for name, df in gold_snapshot(cloud).items():
            local = pd.concat(pd.read_parquet(p) for p in sorted(glob.glob(f"{local_gold}/{name}/*.parquet")))
            pd.testing.assert_frame_equal(df, local.reset_index(drop=True), obj=name)


def test_second_cloud_run_changes_nothing(cloud):
    assert job.main() == 0
    before = gold_snapshot(cloud)
    assert job.main() == 0
    second = manifests(cloud)[-1]
    assert step(second, "source.seed")["records_written"] == 0  # the source system is never overwritten
    assert all(v == "kept (already has data)" for k, v in step(second, "source.seed")["details"].items() if k != "schema")
    assert step(second, "bronze.learning_activity")["details"]["files_copied"] == 0
    for name, df in gold_snapshot(cloud).items():
        pd.testing.assert_frame_equal(df, before[name], obj=name)


def test_incomplete_source_schema_is_refused(cloud):
    cfg = job.Config.from_env()
    with psycopg.connect(host=cfg.pg_host, port=cfg.pg_port, user=cfg.pg_user, password=cfg.pg_pass,
                         dbname=DB, autocommit=True) as conn:  # fmt: skip
        conn.execute("CREATE TABLE teams (team_id integer)")
    assert job.main() == 1
    assert "source schema is incomplete" in manifests(cloud)[-1]["error"]


def test_cloud_mode_requires_its_settings(cloud, monkeypatch):
    monkeypatch.delenv("LAKE_ROOT")
    with pytest.raises(ValueError, match="LAKE_ROOT is required when IS_LOCAL=false"):
        job.Config.from_env()
