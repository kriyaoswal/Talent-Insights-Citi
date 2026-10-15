"""Unit tests for environment-driven configuration."""

from datetime import date

import pytest

import job

CLOUD_ENV = {
    "IS_LOCAL": "false",
    "POSTGRES_HOST": "aurora.example",
    "POSTGRES_PORT": "5432",
    "POSTGRES_NAME": "talent",
    "POSTGRES_USER": "etl",
    "POSTGRES_PASS": "secret",
    "LAKE_ROOT": "s3://bucket/lake/",
    "EVENTS_SOURCE_URI": "s3://bucket/learning_activity",
    "RUN_DATE": "2026-06-30",
}


def test_local_defaults_apply_when_env_empty():
    cfg = job.Config.from_env({})
    assert cfg.is_local
    assert (cfg.pg_host, cfg.pg_port, cfg.pg_name) == ("localhost", 5432, "talent_mgmt")
    assert cfg.lake_root.endswith("talent-etl/lake")


def test_cloud_reads_all_values_and_strips_trailing_slash():
    cfg = job.Config.from_env(CLOUD_ENV)
    assert not cfg.is_local
    assert cfg.lake_root == "s3://bucket/lake"
    assert cfg.run_date == date(2026, 6, 30)


@pytest.mark.parametrize("missing", ["POSTGRES_HOST", "POSTGRES_PASS", "LAKE_ROOT", "EVENTS_SOURCE_URI"])
def test_cloud_requires_explicit_values(missing):
    env = {k: v for k, v in CLOUD_ENV.items() if k != missing}
    with pytest.raises(ValueError, match=missing):
        job.Config.from_env(env)


def test_password_is_not_in_repr():
    assert "secret" not in repr(job.Config.from_env(CLOUD_ENV))


def test_event_partition_layout():
    assert job.EVENT_PARTITION_RE.match("year=2024/month=02/day=09/events-000.jsonl")
    assert not job.EVENT_PARTITION_RE.match("2024/02/09/events-000.jsonl")
