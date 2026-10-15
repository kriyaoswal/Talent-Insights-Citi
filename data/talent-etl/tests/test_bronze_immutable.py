"""Bronze is write-once: landed raw data is never replaced."""

import pandas as pd
import pytest

import job

LINE = '{"event_id": "EV0000000001", "employee_id": "1", "event_date": "2024-01-01T09:00:00", "event_type": "enrollment", "course_name": "SQL"}\n'
REL = "year=2024/month=01/day=01/events-000.jsonl"


@pytest.fixture
def sleeps(monkeypatch):
    calls: list[float] = []
    monkeypatch.setattr(job.time, "sleep", calls.append)
    return calls


@pytest.fixture
def paths(tmp_path):
    source, lake = tmp_path / "source", tmp_path / "lake"
    (source / REL).parent.mkdir(parents=True)
    (source / REL).write_text(LINE)
    cfg = job.Config.from_env({"LAKE_ROOT": str(lake), "EVENTS_SOURCE_URI": str(source)})
    return cfg, source, lake / "bronze" / "learning_activity"


def test_write_once_parquet_refuses_to_replace(tmp_path, sleeps):
    target = str(tmp_path / "t" / "data.parquet")
    job.write_parquet(pd.DataFrame({"a": [1]}), target, overwrite=False)
    with pytest.raises(FileExistsError, match="write-once"):
        job.write_parquet(pd.DataFrame({"a": [2]}), target, overwrite=False)
    assert job.read_parquet(target)["a"].tolist() == [1]
    assert sleeps == []  # a permanent error: not retried


def test_overwrite_is_still_the_default_for_silver_and_gold(tmp_path):
    target = str(tmp_path / "data.parquet")
    job.write_parquet(pd.DataFrame({"a": [1]}), target)
    job.write_parquet(pd.DataFrame({"a": [2]}), target)
    assert job.read_parquet(target)["a"].tolist() == [2]


def test_each_run_gets_its_own_table_snapshot(paths):
    cfg, _, _ = paths
    first, second = job.bronze_table_path(cfg, "teams", "run1"), job.bronze_table_path(cfg, "teams", "run2")
    assert first != second
    assert first.endswith(f"/bronze/postgres/teams/ingest_date={cfg.run_date}/run_id=run1/data.parquet")


def test_changed_source_file_does_not_replace_landed_file(paths):
    cfg, source, bronze = paths
    first = job.ingest_events_to_bronze(cfg)
    assert first.details == {"files_found": 1, "files_copied": 1, "files_unchanged": 0, "files_changed_at_source": 0}

    (source / REL).write_text(LINE + LINE.replace("EV0000000001", "EV0000000002"))  # source rewritten later
    second = job.ingest_events_to_bronze(cfg)
    assert second.details["files_changed_at_source"] == 1 and second.details["files_copied"] == 0
    assert (bronze / REL).read_text() == LINE  # the original raw file is kept


def test_new_files_are_still_landed(paths):
    cfg, source, bronze = paths
    job.ingest_events_to_bronze(cfg)
    new = "year=2024/month=01/day=02/events-000.jsonl"
    (source / new).parent.mkdir(parents=True)
    (source / new).write_text(LINE)
    m = job.ingest_events_to_bronze(cfg)
    assert (m.details["files_copied"], m.details["files_unchanged"]) == (1, 1)
    assert (bronze / new).read_text() == LINE
