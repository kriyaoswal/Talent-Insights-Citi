"""Unit tests for the Silver learning-activity rules (``clean_events`` and helpers)."""

import json
from datetime import date, datetime

import pandas as pd
import pytest

import job

DAY1 = "year=2024/month=02/day=08/events-000.jsonl"
DAY2 = "year=2024/month=02/day=09/events-000.jsonl"
DAY3 = "year=2024/month=02/day=10/events-000.jsonl"
EMPLOYEES = {1, 2, 3}


def event(num: int, day: str = "2024-02-09", **overrides) -> dict:
    """A valid event; ``overrides`` replace fields (value ``...`` removes the field)."""
    rec = {
        "event_id": f"EV{num:010d}",
        "employee_id": "1",
        "event_date": f"{day}T10:30:00",
        "event_type": "module_started",
        "course_name": "SQL Basics",
        "text": "Started Module 1 of SQL Basics.",
        "score": None,
    }
    rec.update(overrides)
    return {k: v for k, v in rec.items() if v is not ...}


def jsonl(*lines) -> bytes:
    return "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines).encode()


def background() -> dict[str, bytes]:
    """Three days of valid events with ID blocks 1-5, 10-12, 20-24 (gaps inside blocks)."""
    return {
        DAY1: jsonl(*(event(n, "2024-02-08") for n in (1, 3, 5))),
        DAY2: jsonl(*(event(n, "2024-02-09") for n in (10, 11, 12))),
        DAY3: jsonl(*(event(n, "2024-02-10") for n in (20, 22, 24))),
    }


def run(files: dict[str, bytes]) -> job.EventCleanResult:
    return job.clean_events(files, EMPLOYEES)


def reasons(result: job.EventCleanResult) -> list[str]:
    return list(result.rejected["reject_reasons"])


def assert_reconciles(result: job.EventCleanResult) -> None:
    s = result.stats
    assert s["lines_read"] == s["rows_written"] + s["lines_quarantined"] + s["duplicates_dropped"]


# --- happy path and typing -------------------------------------------------- #


def test_valid_events_are_typed():
    result = run(background())
    assert len(result.valid) == 9 and result.rejected.empty
    row = result.valid.iloc[0]
    assert row["event_id"] == "EV0000000001"
    assert row["employee_id"] == 1  # string "1" cast to int
    assert row["event_ts"] == datetime(2024, 2, 8, 10, 30)
    assert row["event_date"] == date(2024, 2, 8)
    assert not row["event_date_repaired"]
    assert str(result.valid["employee_id"].dtype) == "int64"
    assert_reconciles(result)


# --- parsing --------------------------------------------------------------- #


def test_malformed_json_is_quarantined_with_raw_line():
    files = {DAY2: jsonl(event(10), '{"event_id": "EV0000000011", "employee_id": "1", "event_date": "2024-02-09T1')}
    result = run(files)
    assert reasons(result) == ["malformed_json"]
    assert result.rejected.iloc[0]["source_line"] == 2
    assert result.rejected.iloc[0]["raw_line"].startswith('{"event_id": "EV0000000011"')
    assert_reconciles(result)


def test_non_object_json_is_malformed():
    assert reasons(run({DAY2: jsonl("[1, 2]")})) == ["malformed_json"]


def test_empty_file_is_counted_not_failed():
    files = {**background(), "year=2024/month=02/day=11/events-000.jsonl": b""}
    result = run(files)
    assert result.stats["empty_files"] == 1
    assert len(result.valid) == 9


# --- schema drift ------------------------------------------------------------ #


def test_notes_is_renamed_to_text():
    rec = event(10, text=...)
    rec["notes"] = "Legacy field name."
    result = run({DAY2: jsonl(rec)})
    assert result.valid.iloc[0]["text"] == "Legacy field name."
    assert "notes" not in result.valid.columns
    assert result.stats["schema_drift_renamed"] == 1


def test_notes_conflicting_with_text_is_quarantined():
    rec = event(10)
    rec["notes"] = "Something else."
    assert reasons(run({DAY2: jsonl(rec)})) == ["conflicting_notes_and_text"]


def test_unexpected_and_missing_fields_are_quarantined():
    rec = event(10, course_name=...)
    rec["colour"] = "blue"
    assert reasons(run({DAY2: jsonl(rec)})) == ["missing_field_course_name;unexpected_field_colour"]


# --- duplicates ------------------------------------------------------------ #


def test_exact_duplicates_are_dropped_once():
    result = run({DAY2: jsonl(event(10), event(10), event(11))})
    assert list(result.valid["event_id"]) == ["EV0000000010", "EV0000000011"]
    assert result.stats["duplicates_dropped"] == 1
    assert result.rejected.empty
    assert_reconciles(result)


def test_conflicting_duplicates_are_all_quarantined():
    result = run({DAY2: jsonl(event(10), event(10, course_name="Other"), event(11))})
    assert reasons(result) == ["conflicting_duplicate_event_id"] * 2
    assert list(result.valid["event_id"]) == ["EV0000000011"]
    assert_reconciles(result)


# --- field rules ----------------------------------------------------------- #


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"employee_id": None}, "null_employee_id"),
        ({"employee_id": ""}, "null_employee_id"),
        ({"employee_id": "  "}, "null_employee_id"),
        ({"employee_id": "abc"}, "invalid_type_employee_id"),
        ({"employee_id": True}, "invalid_type_employee_id"),
        ({"employee_id": "999"}, "unknown_employee_id"),
        ({"event_id": "EV12"}, "bad_format_event_id"),
        ({"event_type": "watched_video"}, "not_allowed_event_type"),
        ({"course_name": "  "}, "null_course_name"),
        ({"text": 42}, "invalid_type_text"),
        ({"event_type": "quiz_attempted", "score": None}, "missing_quiz_score"),
        ({"event_type": "quiz_attempted", "score": 101}, "out_of_range_score"),
        ({"event_type": "quiz_attempted", "score": -1}, "out_of_range_score"),
        ({"event_type": "quiz_attempted", "score": 55.5}, "invalid_type_score"),
        ({"event_type": "quiz_attempted", "score": "80"}, "invalid_type_score"),
        ({"event_type": "quiz_attempted", "score": True}, "invalid_type_score"),
        ({"event_type": "enrollment", "score": 80}, "unexpected_score"),
    ],
)
def test_field_rules(overrides, reason):
    result = run({DAY2: jsonl(event(10, **overrides))})
    assert reasons(result) == [reason]
    assert result.valid.empty


def test_quiz_score_boundaries_are_valid():
    files = {DAY2: jsonl(*(event(10 + i, event_type="quiz_passed", score=s) for i, s in enumerate((0, 100, 80.0))))}
    result = run(files)
    assert result.rejected.empty
    assert list(result.valid["score"]) == [0, 100, 80]


def test_multiple_reasons_are_all_reported():
    rec = event(10, employee_id=None, event_type="bogus")
    assert reasons(run({DAY2: jsonl(rec)})) == ["null_employee_id;not_allowed_event_type"]


# --- dates: strict parsing and partition check ---------------------------- #


def test_valid_date_on_wrong_partition_is_quarantined():
    files = {**background(), DAY2: jsonl(event(10, "2024-02-09"), event(11, "2024-02-15"))}
    assert reasons(run(files)) == ["event_date_partition_mismatch"]


@pytest.mark.parametrize("raw", ["2024-02-09", "2024-02-09 10:30:00", "2024-02-09T10:30:00+00:00"])
def test_non_strict_timestamp_formats_are_not_trusted(raw):
    # Not the source's format, so treated like any other bad value: repaired, flagged.
    files = {**background(), DAY2: jsonl(event(10), event(11, event_date=raw), event(12))}
    result = run(files)
    row = result.valid.set_index("event_id").loc["EV0000000011"]
    assert row["event_date_repaired"] and row["event_date_raw"] == raw


# --- dates: repair-and-flag ------------------------------------------------- #


@pytest.mark.parametrize("raw", ["yesterday", "", "N/A", "2026-13-45", "2024-02-30T10:00:00", None])
def test_bad_date_is_repaired_to_partition_day_at_midnight(raw):
    files = {**background(), DAY2: jsonl(event(10), event(11, event_date=raw), event(12))}
    result = run(files)
    assert result.rejected.empty
    row = result.valid.set_index("event_id").loc["EV0000000011"]
    assert row["event_ts"] == datetime(2024, 2, 9, 0, 0)
    assert row["event_date"] == date(2024, 2, 9)
    assert row["event_date_repaired"]
    assert row["event_date_raw"] == str(raw)
    assert result.stats["event_date_repaired"] == 1
    # untouched rows keep their timestamp and are not flagged
    assert result.valid["event_date_repaired"].sum() == 1
    assert pd.isna(result.valid.set_index("event_id").loc["EV0000000010", "event_date_raw"])


def test_repair_at_start_of_day_block():
    # Mirrors EV0000000039: the first ID of its day, adjacent to the previous day's last ID.
    files = {**background(), DAY2: jsonl(event(6, event_date="yesterday"), event(10), event(11))}
    result = run(files)
    row = result.valid.set_index("event_id").loc["EV0000000006"]
    assert row["event_date_repaired"] and row["event_date"] == date(2024, 2, 9)


@pytest.mark.parametrize("num", [2, 4, 21, 23, 25])
def test_repair_refused_when_id_belongs_to_another_day(num):
    # IDs up to 5 belong to Feb 8, from 20 on to Feb 10: a Feb 9 event can't have them.
    files = {**background(), DAY2: jsonl(event(10), event(num, event_date="N/A"), event(11))}
    result = run(files)
    assert reasons(result) == ["unrepairable_event_date"]
    assert_reconciles(result)


def test_repair_refused_when_event_id_is_malformed():
    files = {**background(), DAY2: jsonl(event(10), {**event(11, event_date="N/A"), "event_id": "X"})}
    assert reasons(run(files)) == ["bad_format_event_id;unrepairable_event_date"]


def test_repair_refused_when_partition_folder_is_not_a_real_date():
    bad_folder = "year=2024/month=02/day=31/events-000.jsonl"
    result = run({**background(), bad_folder: jsonl(event(15, event_date="N/A"))})
    assert reasons(result) == ["unrepairable_event_date"]


def test_repair_allowed_at_edges_of_history():
    # No trusted day before (or after), so only one bound applies.
    files = background()
    files["year=2024/month=02/day=07/events-000.jsonl"] = jsonl(event(0, event_date=""))
    files["year=2024/month=02/day=11/events-000.jsonl"] = jsonl(event(30, event_date=""))
    result = run(files)
    assert result.rejected.empty
    assert result.stats["event_date_repaired"] == 2


def test_rejected_events_do_not_count_as_repaired():
    files = {**background(), DAY2: jsonl(event(10), event(11, event_date="N/A", employee_id=None))}
    result = run(files)
    assert reasons(result) == ["null_employee_id"]
    assert result.stats["event_date_repaired"] == 0


def test_duplicate_bad_date_is_dropped_then_repaired_once():
    # Mirrors EV0000010344: an exact duplicate with an empty date.
    bad = event(11, event_date="")
    result = run({**background(), DAY2: jsonl(event(10), bad, bad)})
    assert result.stats["duplicates_dropped"] == 1
    assert result.stats["event_date_repaired"] == 1
    assert_reconciles(result)


# --- helpers ------------------------------------------------------------------ #


def test_build_day_bounds():
    ok = job.build_day_bounds([(date(2024, 1, 1), 1), (date(2024, 1, 1), 5), (date(2024, 1, 3), 10)])
    assert ok(date(2024, 1, 2), 6)  # between the two blocks
    assert not ok(date(2024, 1, 2), 5)  # inside Jan 1's block
    assert not ok(date(2024, 1, 2), 10)  # inside Jan 3's block
    assert ok(date(2024, 1, 1), 3)  # inside its own day's block
    assert ok(date(2023, 12, 31), 0) and not ok(date(2023, 12, 31), 1)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (DAY2, date(2024, 2, 9)),
        ("year=2024/month=02/day=30/events-000.jsonl", None),
        ("2024/02/09/events-000.jsonl", None),
    ],
)
def test_partition_date(path, expected):
    assert job.partition_date(path) == expected
