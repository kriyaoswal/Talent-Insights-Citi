"""External-dependency tests: the job under injected PostgreSQL and storage failures.

Each test runs the real job (real PostgreSQL, real files) into a temporary lake
and injects failures at the boundary: the database connect call or the file
system's ``open``. Sleeps are recorded instead of slept, so the backoff can be
checked without waiting. Real S3 failures are covered with moto separately.
Skipped when PostgreSQL is not reachable.
"""

import glob
import json
from pathlib import Path

import fsspec.implementations.local as local_fs
import psycopg
import pytest

import job

RUN_DATE = "2026-10-14"


@pytest.fixture
def env(tmp_path, monkeypatch):
    cfg = job.Config.from_env({})
    try:
        psycopg.connect(
            host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_name, user=cfg.pg_user, password=cfg.pg_pass,
            connect_timeout=3,
        ).close()  # fmt: skip
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    for key in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_NAME", "POSTGRES_USER", "POSTGRES_PASS", "IS_LOCAL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("RUN_DATE", RUN_DATE)
    sleeps: list[float] = []
    monkeypatch.setattr(job.time, "sleep", sleeps.append)
    return tmp_path / "lake", sleeps


def manifest(lake: Path) -> dict:
    files = glob.glob(f"{lake}/_runs/run_date={RUN_DATE}/*.json")
    return json.loads(Path(max(files, key=lambda f: Path(f).stat().st_mtime)).read_text())


def test_recovers_from_transient_postgres_failures(env, monkeypatch):
    lake, sleeps = env
    real_connect, calls = psycopg.connect, {"n": 0}

    def flaky_connect(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise psycopg.OperationalError("connection timeout (injected)")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(job.psycopg, "connect", flaky_connect)
    assert job.main() == 0
    assert sleeps[:2] == [1.0, 2.0]  # exponential backoff before the 3rd attempt succeeded
    m = manifest(lake)
    assert m["status"] == "succeeded"
    assert next(s for s in m["steps"] if s["step"] == "bronze.postgres")["records_read"] == 12173
    assert sum(next(s for s in m["steps"] if s["step"] == "gold")["details"]["tables"].values()) > 0


def test_recovers_from_transient_storage_failures_in_every_layer(env, monkeypatch):
    lake, sleeps = env
    real_open, failed = local_fs.LocalFileSystem.open, []

    def flaky_open(self, path, mode="rb", *args, **kwargs):
        # Fail the first write into each layer once, like a dropped S3 connection.
        layer = next((x for x in ("bronze", "silver", "gold") if f"/lake/{x}/" in str(path)), None)
        if "w" in mode and layer and layer not in failed:
            failed.append(layer)
            raise ConnectionResetError(f"connection reset writing {layer} (injected)")
        return real_open(self, path, mode, *args, **kwargs)

    monkeypatch.setattr(local_fs.LocalFileSystem, "open", flaky_open)
    assert job.main() == 0
    assert failed == ["bronze", "silver", "gold"]
    assert sleeps == [1.0, 1.0, 1.0]  # each recovered on its first retry
    m = manifest(lake)
    assert m["status"] == "succeeded"
    gold = next(s for s in m["steps"] if s["step"] == "gold")["details"]
    assert all(v.startswith("PASS") for v in gold["reconciliation"].values())


def test_gives_up_after_backoff_and_reports_failure(env, monkeypatch):
    lake, sleeps = env
    monkeypatch.setenv("POSTGRES_PASS", "wrong-password")
    assert job.main() == 1
    assert sleeps == [1.0, 2.0, 4.0]  # 4 attempts, then give up
    m = manifest(lake)
    assert m["status"] == "failed" and "OperationalError" in m["error"]
    assert not (lake / "gold").exists()  # nothing downstream was produced


def test_failed_gold_leaves_previous_gold_untouched(env, monkeypatch):
    lake, _ = env
    assert job.main() == 0
    before = {p: Path(p).read_bytes() for p in glob.glob(f"{lake}/gold/**/*.parquet", recursive=True)}
    assert before

    real_build = job.build_gold

    def broken_build(silver):
        gold = real_build(silver)
        gold["promotion_candidates"] = gold["promotion_candidates"].assign(tier="A")  # breaks the tier rule
        return gold

    monkeypatch.setattr(job, "build_gold", broken_build)
    assert job.main() == 1
    assert "tier follows latest_rating" in manifest(lake)["error"]
    after = {p: Path(p).read_bytes() for p in glob.glob(f"{lake}/gold/**/*.parquet", recursive=True)}
    assert after == before  # validation happens before any write
