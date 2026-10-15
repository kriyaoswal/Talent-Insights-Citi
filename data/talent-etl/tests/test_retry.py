"""Unit tests for ``with_retry`` exponential backoff."""

import psycopg
import pytest

import job


@pytest.fixture
def sleeps(monkeypatch):
    calls: list[float] = []
    monkeypatch.setattr(job.time, "sleep", calls.append)
    return calls


def flaky(failures: int, exc: Exception):
    state = {"calls": 0}

    def fn():
        state["calls"] += 1
        if state["calls"] <= failures:
            raise exc
        return "ok"

    return fn, state


def test_recovers_after_transient_failures(sleeps):
    fn, state = flaky(2, psycopg.OperationalError("connection reset"))
    assert job.with_retry(fn, what="test", attempts=4, base_delay_s=1.0) == "ok"
    assert state["calls"] == 3
    assert sleeps == [1.0, 2.0]  # exponential backoff


def test_gives_up_after_max_attempts(sleeps):
    fn, state = flaky(10, OSError("s3 timeout"))
    with pytest.raises(OSError, match="s3 timeout"):
        job.with_retry(fn, what="test", attempts=3, base_delay_s=0.5)
    assert state["calls"] == 3
    assert sleeps == [0.5, 1.0]


def test_non_transient_errors_are_not_retried(sleeps):
    fn, state = flaky(1, ValueError("bad data"))
    with pytest.raises(ValueError):
        job.with_retry(fn, what="test")
    assert state["calls"] == 1 and sleeps == []


@pytest.mark.parametrize("exc", [FileNotFoundError("missing"), PermissionError("access denied"), IsADirectoryError("dir")])
def test_permanent_os_errors_fail_immediately(sleeps, exc):
    fn, state = flaky(10, exc)
    with pytest.raises(type(exc)):
        job.with_retry(fn, what="test")
    assert state["calls"] == 1 and sleeps == []


def test_storage_helpers_do_not_retry_a_missing_file(sleeps, tmp_path):
    with pytest.raises(FileNotFoundError):
        job.read_parquet(str(tmp_path / "missing.parquet"))
    assert sleeps == []
