"""Performance & Talent Management ETL job.

Moves data through the Medallion layers:

    PostgreSQL source tables ─┐
                              ├─► Bronze (raw) ─► Silver (clean) ─► Gold (curated)
    Learning activity JSONL  ─┘

Runs unchanged locally and in the cloud: every location and credential comes
from environment variables (see ``Config.from_env``). Storage paths go through
fsspec, so ``LAKE_ROOT`` can be a local directory or an ``s3://`` URI.

This file is intentionally self-contained: the EKS deployment mounts only
``job.py`` and ``requirements.txt`` into the job container.
"""

from __future__ import annotations

import bisect
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Callable, Literal, TypeVar, get_args

try:
    import resource  # Unix only; used for peak-memory metrics
except ImportError:  # pragma: no cover - e.g. Windows
    resource = None

import fsspec
import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    model_validator,
)
from pydantic_core import PydanticCustomError

JOB_NAME = "talent-etl"
JOB_DIR = Path(__file__).resolve().parent
REPO_ROOT = JOB_DIR.parent.parent
LOCAL_EVENTS_SOURCE = (
    REPO_ROOT
    / "sampleData/data-engineering-sample-performance-scenario"
    / "data-engineering-performance-talent-management/learning_activity"
)

# Source tables and their primary keys, in foreign-key load order.
SOURCE_TABLES: dict[str, str] = {
    "teams": "team_id",
    "competencies": "competency_id",
    "employees": "employee_id",
    "projects": "project_id",
    "performance_reviews": "review_id",
    "competency_assessments": "assessment_id",
    "talent_review": "employee_id",
}

EVENT_PARTITION_RE = re.compile(r"^year=(\d{4})/month=(\d{2})/day=(\d{2})/[^/]+\.jsonl$")

log = logging.getLogger(JOB_NAME)
T = TypeVar("T")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Config:
    """Runtime configuration, resolved once from environment variables."""

    is_local: bool
    pg_host: str
    pg_port: int
    pg_name: str
    pg_user: str
    pg_pass: str = field(repr=False)
    lake_root: str
    events_source_uri: str
    run_date: date
    log_level: str
    seed_source_uri: str | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        """Build config from ``env`` (defaults to ``os.environ``).

        Local runs fall back to the workshop's local defaults; cloud runs
        (``IS_LOCAL=false``) must set every connection and location variable.
        """
        env = dict(os.environ if env is None else env)
        is_local = env.get("IS_LOCAL", "true").strip().lower() == "true"

        def get(name: str, local_default: str) -> str:
            value = env.get(name, "").strip()
            if value:
                return value
            if is_local:
                return local_default
            raise ValueError(f"Environment variable {name} is required when IS_LOCAL=false")

        run_date_raw = env.get("RUN_DATE", "").strip()
        return cls(
            is_local=is_local,
            pg_host=get("POSTGRES_HOST", "localhost"),
            pg_port=int(get("POSTGRES_PORT", "5432")),
            pg_name=get("POSTGRES_NAME", "talent_mgmt"),
            pg_user=get("POSTGRES_USER", "postgres"),
            pg_pass=get("POSTGRES_PASS", "postgres123"),
            lake_root=get("LAKE_ROOT", str(JOB_DIR / "lake")).rstrip("/"),
            events_source_uri=get("EVENTS_SOURCE_URI", str(LOCAL_EVENTS_SOURCE)).rstrip("/"),
            run_date=date.fromisoformat(run_date_raw) if run_date_raw else datetime.now(timezone.utc).date(),
            log_level=env.get("LOG_LEVEL", "INFO").upper(),
            seed_source_uri=env.get("SEED_SOURCE_URI", "").strip().rstrip("/") or None,
        )


# --------------------------------------------------------------------------- #
# Logging, metrics and retries
# --------------------------------------------------------------------------- #


def setup_logging(level: str) -> None:
    """Configure structured ``key=value`` logging to stdout."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s level=%(levelname)s job=%(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
        force=True,
    )


def log_event(level: int, event: str, **fields: object) -> None:
    """Log one structured event, e.g. ``event=bronze.table table=teams rows=100``."""
    parts = [f"event={event}"] + [f"{k}={v}" for k, v in fields.items()]
    log.log(level, " ".join(parts))


@dataclass
class StepMetrics:
    """Record counts and status for one pipeline step, persisted in the run manifest."""

    step: str
    status: str = "pending"
    records_read: int = 0
    records_written: int = 0
    records_quarantined: int = 0
    details: dict[str, object] = field(default_factory=dict)
    duration_s: float = 0.0
    cpu_s: float = 0.0
    peak_rss_mb: float | None = None


def peak_rss_mb() -> float | None:
    """Peak resident memory of this process so far, in MB (None where unsupported)."""
    if resource is None:
        return None
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KB on Linux, bytes on macOS
    return round(rss / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)


# OSError subclasses that retrying cannot fix (missing file, access denied): fail at once.
NON_TRANSIENT_ERRORS = (FileNotFoundError, FileExistsError, PermissionError, IsADirectoryError, NotADirectoryError)


def with_retry(fn: Callable[[], T], what: str, attempts: int = 4, base_delay_s: float = 1.0) -> T:
    """Call ``fn`` with exponential backoff on transient connection/IO errors.

    Waits 1s, 2s, 4s between attempts by default. Permanent errors
    (``NON_TRANSIENT_ERRORS``) and anything that isn't a connection/IO error
    are raised immediately.
    """
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except NON_TRANSIENT_ERRORS:
            raise
        except (psycopg.OperationalError, OSError) as exc:
            if attempt == attempts:
                log_event(logging.ERROR, "retry.exhausted", what=what, attempts=attempts, error=repr(exc))
                raise
            delay = base_delay_s * 2 ** (attempt - 1)
            log_event(logging.WARNING, "retry", what=what, attempt=attempt, delay_s=delay, error=repr(exc))
            time.sleep(delay)
    raise AssertionError("unreachable")


# --------------------------------------------------------------------------- #
# Storage and source connections
# --------------------------------------------------------------------------- #


def lake_path(cfg: Config, *parts: str) -> str:
    """Join a path under the lake root (local directory or s3:// URI)."""
    return "/".join([cfg.lake_root, *parts])


# Every storage helper below retries transient errors itself (``with_retry``),
# so all layers get the same resilience on S3, not just Bronze.


def write_parquet(df: pd.DataFrame, uri: str, overwrite: bool = True) -> None:
    """Write ``df`` to ``uri``.

    Silver and Gold overwrite (idempotent reruns). Bronze passes
    ``overwrite=False``: an existing file raises ``FileExistsError`` instead of
    being replaced. The check runs once, before the retried write, so a retry
    after a write whose response was lost doesn't trip over its own file.
    """
    fs, path = fsspec.core.url_to_fs(uri)
    if not overwrite and with_retry(lambda: fs.exists(path), what=f"check {uri}"):
        raise FileExistsError(f"{uri} already exists and is write-once")

    def write() -> None:
        fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        with fs.open(path, "wb") as fh:
            df.to_parquet(fh, index=False)

    with_retry(write, what=f"write {uri}")


def read_parquet(uri: str) -> pd.DataFrame:
    """Read one Parquet file from a local path or ``s3://`` URI."""
    fs, path = fsspec.core.url_to_fs(uri)

    def read() -> pd.DataFrame:
        with fs.open(path, "rb") as fh:
            return pd.read_parquet(fh)

    return with_retry(read, what=f"read {uri}")


def replace_dir(uri: str) -> None:
    """Remove ``uri`` recursively if it exists, so a layer can be rewritten from scratch."""
    fs, path = fsspec.core.url_to_fs(uri)

    def remove() -> None:
        if fs.exists(path):
            fs.rm(path, recursive=True)

    with_retry(remove, what=f"remove {uri}")


def read_bytes(uri: str) -> bytes:
    """Read a whole file (local path or ``s3://`` URI)."""
    fs, path = fsspec.core.url_to_fs(uri)
    return with_retry(lambda: fs.cat_file(path), what=f"read {uri}")


def write_json(obj: object, uri: str) -> None:
    """Write ``obj`` as pretty JSON to ``uri``."""
    fs, path = fsspec.core.url_to_fs(uri)

    def write() -> None:
        fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        with fs.open(path, "w") as fh:
            json.dump(obj, fh, indent=2, default=str)

    with_retry(write, what=f"write {uri}")


def pg_connect(cfg: Config) -> psycopg.Connection:
    """Open a PostgreSQL connection; Aurora (cloud) requires SSL, local does not."""
    return with_retry(
        lambda: psycopg.connect(
            host=cfg.pg_host,
            port=cfg.pg_port,
            dbname=cfg.pg_name,
            user=cfg.pg_user,
            password=cfg.pg_pass,
            sslmode="prefer" if cfg.is_local else "require",
            connect_timeout=10,
        ),
        what="postgres.connect",
    )


def read_table(conn: psycopg.Connection, table: str, order_by: str) -> pd.DataFrame:
    """Read a whole source table into a DataFrame, ordered by its primary key."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{table}" ORDER BY "{order_by}"')
        columns = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


# --------------------------------------------------------------------------- #
# Bronze: land raw data, unchanged, with ingestion metadata
# --------------------------------------------------------------------------- #


def seed_source_database(cfg: Config) -> StepMetrics:
    """Workshop bootstrap: load the sample source tables into an empty PostgreSQL.

    Runs only when ``SEED_SOURCE_URI`` is set: a folder (local or ``s3://``)
    holding the source ``schema.sql`` and one ``<table>.csv`` per source table.
    In the cloud the source database is Aurora, which is private to the VPC, so
    this job (running inside it) is the one place that can load it.

    It never overwrites the source system: tables are created from
    ``schema.sql`` only when none exist, and only empty tables are loaded.
    A partially created schema is an error rather than a guess.
    """
    m = StepMetrics(step="source.seed")
    root = cfg.seed_source_uri.rstrip("/")
    with pg_connect(cfg) as conn:
        existing = {t for t in SOURCE_TABLES if conn.execute("SELECT to_regclass(%s)", (t,)).fetchone()[0]}
        if not existing:
            conn.execute(read_bytes(f"{root}/schema.sql").decode("utf-8"))
            m.details["schema"] = "created"
        elif existing != set(SOURCE_TABLES):
            missing = sorted(set(SOURCE_TABLES) - existing)
            raise RuntimeError(f"source.seed: source schema is incomplete (missing {missing}); fix it by hand")
        else:
            m.details["schema"] = "already present"

        for table in SOURCE_TABLES:  # parents first, so foreign keys hold
            if conn.execute(f'SELECT EXISTS (SELECT 1 FROM "{table}")').fetchone()[0]:
                m.details[table] = "kept (already has data)"
                continue
            data = read_bytes(f"{root}/{table}.csv")
            with conn.cursor() as cur:
                with cur.copy(f'COPY "{table}" FROM STDIN WITH (FORMAT csv, HEADER true)') as copy:
                    copy.write(data)
            rows = conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            m.details[table] = rows
            m.records_written += rows
        conn.commit()
    log_event(logging.INFO, "source.seed", source=root, rows_loaded=m.records_written, **m.details)
    return m


def bronze_table_path(cfg: Config, table: str, run_id: str) -> str:
    """One run's write-once snapshot of a source table."""
    return lake_path(cfg, "bronze", "postgres", table, f"ingest_date={cfg.run_date}", f"run_id={run_id}", "data.parquet")


def ingest_postgres_to_bronze(cfg: Config, run_id: str, ingested_at: datetime) -> StepMetrics:
    """Snapshot every source table to ``bronze/postgres/<table>/ingest_date=<RUN_DATE>/run_id=<run_id>/``.

    Bronze is write-once: every run lands its own snapshot and never replaces
    an earlier one, so each run's raw input stays exactly as it was read.
    Silver reads only this run's snapshot, so reruns still never duplicate.
    Bronze contract: the source must still have every column its Silver model
    needs (``check_columns``), so schema drift stops the job at ingestion.
    """
    m = StepMetrics(step="bronze.postgres")
    with pg_connect(cfg) as conn:
        for table, pk in SOURCE_TABLES.items():
            df = read_table(conn, table, pk)
            check_columns(df, TABLE_CONTRACTS[table].columns, f"bronze.{table}")
            source_count = conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            if len(df) != source_count:
                raise RuntimeError(f"{table}: read {len(df)} rows but source has {source_count}")
            if df.empty:
                log_event(logging.WARNING, "bronze.table.empty", table=table)

            df["_source"] = f"postgres://{cfg.pg_host}/{cfg.pg_name}/{table}"
            df["_ingested_at"] = ingested_at
            df["_run_id"] = run_id
            target = bronze_table_path(cfg, table, run_id)
            write_parquet(df, target, overwrite=False)

            m.records_read += source_count
            m.records_written += len(df)
            m.details[table] = len(df)
            log_event(logging.INFO, "bronze.table", table=table, rows=len(df), target=target)
    return m


def ingest_events_to_bronze(cfg: Config) -> StepMetrics:
    """Copy raw learning-activity JSONL files byte-for-byte into Bronze.

    The source ``year=/month=/day=`` partition layout is preserved. Bronze is
    write-once: only files not yet landed are copied, so the step is
    incremental and safe to rerun. A landed file is never replaced: if its
    source has since changed size, the original is kept and the change is
    logged and counted (``files_changed_at_source``) for someone to review.
    Parsing (and quarantining malformed lines) is deliberately left to Silver
    so Bronze stays an exact raw copy.
    """
    m = StepMetrics(step="bronze.learning_activity")
    src_fs, src_root = fsspec.core.url_to_fs(cfg.events_source_uri)
    dst_fs, dst_root = fsspec.core.url_to_fs(lake_path(cfg, "bronze", "learning_activity"))

    def list_sizes(fs: fsspec.AbstractFileSystem, root: str) -> dict[str, int]:
        # One listing per side instead of per-file exists/size calls: on S3
        # that is a few requests in total rather than several per file.
        fs.invalidate_cache(root)
        found = with_retry(lambda: fs.find(root, detail=True), what=f"list {root}")
        return {p: int(info["size"]) for p, info in found.items() if p.endswith(".jsonl")}

    src_sizes = list_sizes(src_fs, src_root)
    files = sorted(src_sizes)
    if not files:
        raise RuntimeError(f"No .jsonl files found under {cfg.events_source_uri}")
    dst_sizes = list_sizes(dst_fs, dst_root)

    copied = skipped = changed = bad_layout = 0
    expected: dict[str, int] = {}
    for src in files:
        rel = src[len(src_root):].lstrip("/")
        if not EVENT_PARTITION_RE.match(rel):
            bad_layout += 1
            log_event(logging.WARNING, "bronze.events.bad_partition", file=rel)
            continue
        dst = f"{dst_root}/{rel}"
        if dst in dst_sizes:
            if dst_sizes[dst] == src_sizes[src]:
                skipped += 1
            else:
                changed += 1
                log_event(
                    logging.WARNING,
                    "bronze.events.source_changed",
                    file=rel,
                    landed_bytes=dst_sizes[dst],
                    source_bytes=src_sizes[src],
                    action="kept landed file (Bronze is write-once)",
                )
            continue
        expected[dst] = src_sizes[src]

        def copy() -> None:
            dst_fs.makedirs(dst.rsplit("/", 1)[0], exist_ok=True)
            dst_fs.pipe_file(dst, src_fs.cat_file(src))

        with_retry(copy, what=f"copy {rel}")
        copied += 1

    if copied:  # verify every Bronze file against its source size with one listing
        landed = list_sizes(dst_fs, dst_root)
        mismatched = [d for d, size in expected.items() if landed.get(d) != size]
        if mismatched:
            raise RuntimeError(f"Bronze copy incomplete: {len(mismatched)} files differ, e.g. {mismatched[:3]}")

    m.records_read = len(files)
    m.records_written = copied
    m.records_quarantined = bad_layout
    m.details = {
        "files_found": len(files),
        "files_copied": copied,
        "files_unchanged": skipped,
        "files_changed_at_source": changed,
    }
    log_event(
        logging.INFO,
        "bronze.events",
        files_found=len(files),
        copied=copied,
        unchanged=skipped,
        changed_at_source=changed,
        bad_layout=bad_layout,
    )
    return m


# --------------------------------------------------------------------------- #
# Schema contracts: Pydantic models (record level) + custom validators (dataset level)
# --------------------------------------------------------------------------- #
#
# Each dataset has a Pydantic model describing one valid record: types,
# required fields, allowed values, ranges, formats, cross-field rules, and
# foreign keys (checked against the validation context). Rules that need the
# whole dataset (primary/business key uniqueness, reconciliation, the event
# date-repair guardrail) are custom validators. Validation failures are turned
# into stable reason codes (``error_reasons``) that go to quarantine.

Department = Literal[
    "Customer Support",
    "Data & Analytics",
    "Design",
    "Engineering",
    "Finance",
    "Human Resources",
    "Marketing",
    "Operations",
    "Product",
    "Sales",
]
JobLevel = Literal["IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director"]
LowMediumHigh = Literal["Low", "Medium", "High"]
EventType = Literal[
    "enrollment",
    "module_started",
    "module_completed",
    "quiz_attempted",
    "quiz_passed",
    "course_completed",
    "certification_earned",
]
EVENT_TYPES = frozenset(get_args(EventType))
QUIZ_EVENT_TYPES = frozenset({"quiz_attempted", "quiz_passed"})
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
HALF_YEAR_PATTERN = r"^\d{4}-H[12]$"
EVENT_ID_PATTERN = r"^EV\d{10}$"


def _clean(value: Any) -> Any:
    """Strip strings and treat blanks as missing; reject booleans (Pydantic would read True as 1)."""
    if isinstance(value, bool):
        raise PydanticCustomError("invalid_type", "booleans are not accepted")
    if isinstance(value, str):
        return value.strip() or None
    return value


def _iso_date(value: Any) -> Any:
    """Accept only a date or a ``YYYY-MM-DD`` string (not epoch numbers or timestamps with a time)."""
    value = _clean(value)
    if isinstance(value, datetime):
        if value.time() != datetime.min.time():
            raise PydanticCustomError("invalid_type", "expected a date, got a timestamp")
        return value.date()
    if value is None or isinstance(value, date):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    raise PydanticCustomError("invalid_type", "expected a YYYY-MM-DD date")


def _not_after_run_date(value: date, info: ValidationInfo) -> date:
    """Reject dates after the run date passed in the validation context."""
    run_date = (info.context or {}).get("run_date")
    if run_date is not None and value > run_date:
        raise PydanticCustomError("future", "date is after the run date {run_date}", {"run_date": str(run_date)})
    return value


def _number_only(value: Any) -> Any:
    """Reject booleans and strings so a score must arrive as a JSON number."""
    if isinstance(value, (bool, str)):
        raise PydanticCustomError("invalid_type", "expected a number")
    return value


def references(parent: str) -> AfterValidator:
    """Foreign key: the value must be a key of ``parent`` in ``context['parent_keys']``.

    Skipped when the context has no keys for ``parent`` (e.g. validating a
    record on its own in a unit test).
    """

    def check(value: int, info: ValidationInfo) -> int:
        keys = (info.context or {}).get("parent_keys", {}).get(parent)
        if keys is not None and value not in keys:
            raise PydanticCustomError("unknown", "no {parent} row with this id", {"parent": parent})
        return value

    return AfterValidator(check)


def review_period_end_date(period: str) -> date:
    """Last day of a ``YYYY-H1``/``YYYY-H2`` period."""
    year = int(period[:4])
    return date(year, 6, 30) if period.endswith("H1") else date(year, 12, 31)


def review_period_end(period: pd.Series) -> pd.Series:
    """Vectorised ``review_period_end_date``; NaT when malformed."""
    year = period.str.slice(0, 4)
    month_day = period.str.slice(5).map({"H1": "-06-30", "H2": "-12-31"})
    return pd.to_datetime(year + month_day, format="%Y-%m-%d", errors="coerce")


Id = Annotated[int, BeforeValidator(_clean), Field(gt=0)]
Text = Annotated[str, BeforeValidator(_clean)]
OptionalText = Annotated[str | None, BeforeValidator(_clean)]
Level = Annotated[int, BeforeValidator(_clean), Field(ge=1, le=5)]
PastDate = Annotated[date, BeforeValidator(_iso_date), AfterValidator(_not_after_run_date)]
HalfYear = Annotated[str, BeforeValidator(_clean), StringConstraints(pattern=HALF_YEAR_PATTERN)]
TeamRef = Annotated[Id, references("teams")]
EmployeeRef = Annotated[Id, references("employees")]
CompetencyRef = Annotated[Id, references("competencies")]

# Allowed-value columns; surrounding whitespace is tolerated.
DepartmentChoice = Annotated[Department, BeforeValidator(_clean)]
CategoryChoice = Annotated[Literal["Technical", "Leadership", "Domain"], BeforeValidator(_clean)]
JobLevelChoice = Annotated[JobLevel, BeforeValidator(_clean)]
OutcomeStatusChoice = Annotated[Literal["on-time", "delayed", "at-risk", "cancelled"], BeforeValidator(_clean)]
ReadinessChoice = Annotated[Literal["Ready Now", "1-2 Years", "Not Ready"], BeforeValidator(_clean)]
LowMediumHighChoice = Annotated[LowMediumHigh, BeforeValidator(_clean)]
EventTypeChoice = Annotated[EventType, BeforeValidator(_clean)]


class SourceRecord(BaseModel):
    """Base for source-table records. Extra columns are ignored here and reported per dataset."""

    model_config = ConfigDict(extra="ignore", frozen=True)


class Team(SourceRecord):
    team_id: Id
    name: Text
    department: DepartmentChoice


class Competency(SourceRecord):
    competency_id: Id
    name: Text
    category: CategoryChoice


class Employee(SourceRecord):
    employee_id: Id
    first_name: Text
    last_name: Text
    email: Annotated[str, BeforeValidator(_clean), StringConstraints(pattern=EMAIL_PATTERN)]
    team_id: TeamRef
    job_level: JobLevelChoice
    hire_date: PastDate


class Project(SourceRecord):
    project_id: Id
    name: Text
    team_id: TeamRef
    outcome_status: OutcomeStatusChoice
    outcome_score: Annotated[float, BeforeValidator(_clean), Field(ge=0, le=100, allow_inf_nan=False)]


class PerformanceReview(SourceRecord):
    review_id: Id
    employee_id: EmployeeRef
    review_period: HalfYear
    rating: Level
    manager_comment: OptionalText = None
    submitted_date: PastDate

    @model_validator(mode="after")
    def _submitted_after_period_end(self) -> "PerformanceReview":
        if self.submitted_date <= review_period_end_date(self.review_period):
            raise PydanticCustomError("submitted_before_period_end", "review submitted before its period ended")
        return self


class CompetencyAssessment(SourceRecord):
    assessment_id: Id
    employee_id: EmployeeRef
    competency_id: CompetencyRef
    proficiency_level: Level
    required_level: Level
    assessed_date: PastDate


class TalentReview(SourceRecord):
    employee_id: EmployeeRef
    review_cycle: HalfYear
    potential_rating: LowMediumHighChoice
    promotion_readiness: ReadinessChoice
    attrition_risk: LowMediumHighChoice


class LearningEvent(BaseModel):
    """One learning-activity event, after known drift is mapped (``notes`` -> ``text``).

    ``event_date`` is required but kept raw: it is parsed, or repaired and
    flagged, by the dataset-level date rule in ``clean_events``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: Annotated[str, BeforeValidator(_clean), StringConstraints(pattern=EVENT_ID_PATTERN)]
    employee_id: EmployeeRef
    event_date: Any
    event_type: EventTypeChoice
    course_name: Text
    text: OptionalText = None
    score: Annotated[Annotated[int, Field(ge=0, le=100)] | None, BeforeValidator(_number_only)] = None

    @model_validator(mode="after")
    def _score_matches_event_type(self) -> "LearningEvent":
        if self.event_type in QUIZ_EVENT_TYPES and self.score is None:
            raise PydanticCustomError("missing_quiz_score", "quiz events must have a score")
        if self.event_type not in QUIZ_EVENT_TYPES and self.score is not None:
            raise PydanticCustomError("unexpected_score", "only quiz events have a score")
        return self


_ERROR_KINDS = {
    "missing": "missing_field",
    "extra_forbidden": "unexpected_field",
    "literal_error": "not_allowed",
    "greater_than": "out_of_range",
    "greater_than_equal": "out_of_range",
    "less_than": "out_of_range",
    "less_than_equal": "out_of_range",
    "string_pattern_mismatch": "bad_format",
}
_CUSTOM_FIELD_ERRORS = {"invalid_type", "future", "unknown"}


def error_reasons(exc: ValidationError) -> list[str]:
    """Turn a Pydantic ``ValidationError`` into stable reason codes.

    Field errors become ``<kind>_<field>`` (e.g. ``null_hire_date``,
    ``not_allowed_job_level``, ``unknown_team_id``); model-level rules keep
    their own code (e.g. ``submitted_before_period_end``).
    """
    reasons: list[str] = []
    for err in exc.errors():
        field_name = ".".join(str(p) for p in err["loc"])
        kind = err["type"]
        if not field_name:
            reason = kind
        elif kind in ("missing", "extra_forbidden"):
            reason = f"{_ERROR_KINDS[kind]}_{field_name}"
        elif err.get("input") is None:
            reason = f"null_{field_name}"
        elif kind in _ERROR_KINDS:
            reason = f"{_ERROR_KINDS[kind]}_{field_name}"
        elif kind in _CUSTOM_FIELD_ERRORS:
            reason = f"{kind}_{field_name}"
        else:  # int_parsing, date_from_datetime_parsing, string_type, ...
            reason = f"invalid_type_{field_name}"
        if reason not in reasons:
            reasons.append(reason)
    return reasons


ModelT = TypeVar("ModelT", bound=BaseModel)


def validate_record(model: type[ModelT], record: dict, context: dict) -> tuple[ModelT | None, list[str]]:
    """Validate one record; returns ``(instance, [])`` or ``(None, reasons)``."""
    try:
        return model.model_validate(record, context=context), []
    except ValidationError as exc:
        return None, error_reasons(exc)


@dataclass(frozen=True)
class TableContract:
    """A table's record model plus its dataset-level key rules."""

    model: type[SourceRecord]
    primary_key: str
    unique: tuple[tuple[str, ...], ...] = ()

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.model.model_fields)


TABLE_CONTRACTS: dict[str, TableContract] = {
    "teams": TableContract(Team, "team_id", unique=(("name",),)),
    "competencies": TableContract(Competency, "competency_id", unique=(("name",),)),
    "employees": TableContract(Employee, "employee_id", unique=(("email",),)),
    "projects": TableContract(Project, "project_id"),
    "performance_reviews": TableContract(
        PerformanceReview, "review_id", unique=(("employee_id", "review_period"),)
    ),
    "competency_assessments": TableContract(
        CompetencyAssessment, "assessment_id", unique=(("employee_id", "competency_id"),)
    ),
    "talent_review": TableContract(TalentReview, "employee_id"),
}


class SchemaDriftError(ValueError):
    """A dataset no longer has the columns its contract requires."""


def check_columns(df: pd.DataFrame, columns: tuple[str, ...], dataset: str) -> list[str]:
    """Fail on missing contract columns; return (and log) unexpected extra columns."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise SchemaDriftError(f"{dataset}: schema drift, missing columns {missing}")
    extra = [c for c in df.columns if c not in columns and not c.startswith("_")]
    if extra:
        log_event(logging.WARNING, "schema.extra_columns", dataset=dataset, columns=extra)
    return extra


def as_text(s: pd.Series) -> pd.Series:
    """Render any column as stripped text with blanks as NA (for quarantine output and key checks)."""
    if pd.api.types.is_datetime64_any_dtype(s):
        s = s.dt.strftime("%Y-%m-%d")
    text = s.astype("string").str.strip()
    return text.mask(text == "")


def validate_table(
    df: pd.DataFrame,
    contract: TableContract,
    parent_keys: dict[str, set[int]],
    run_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate a Bronze table against its contract.

    Every row is validated with the contract's Pydantic model (foreign keys
    against ``parent_keys``, dates against ``run_date``), then the dataset-level
    key rules are applied. Returns ``(valid, rejected)``: ``valid`` holds the
    typed contract columns; ``rejected`` holds the original values as text plus
    ``reject_reasons`` (``;``-separated). Every row lands in exactly one.
    """
    columns = list(contract.columns)
    check_columns(df, contract.columns, f"silver.{contract.model.__name__}")
    df = df.reset_index(drop=True)
    raw = df[columns].astype(object)
    records = raw.where(raw.notna(), None).to_dict("records")

    context = {"run_date": run_date, "parent_keys": parent_keys}
    validated: list[BaseModel | None] = []
    reasons: list[list[str]] = []
    for record in records:
        obj, why = validate_record(contract.model, record, context)
        validated.append(obj)
        reasons.append(why)

    # Dataset-level rules, on the raw text so invalid rows still count as duplicates.
    text = pd.DataFrame({c: as_text(df[c]) for c in columns})
    key_rules = [("duplicate_primary_key", (contract.primary_key,))] + [
        (f"duplicate_{'_'.join(cols)}", cols) for cols in contract.unique
    ]
    for reason, cols in key_rules:
        dup = text[list(cols)].notna().all(axis=1) & text.duplicated(subset=list(cols), keep=False)
        for i in dup[dup].index:
            reasons[i].append(reason)

    ok = [not r for r in reasons]
    valid = pd.DataFrame([obj.model_dump() for obj, good in zip(validated, ok) if good], columns=columns)
    valid = valid.convert_dtypes() if not valid.empty else valid
    rejected = text.loc[[not good for good in ok]].copy()
    rejected["reject_reasons"] = [";".join(r) for r in reasons if r]
    return valid, rejected.reset_index(drop=True)


def add_quality_flags(table: str, df: pd.DataFrame, employees: pd.DataFrame) -> pd.DataFrame:
    """Add soft-rule ``flag_*`` columns: suspicious but kept rows (not quarantined)."""
    hire = df["employee_id"].map(employees.set_index("employee_id")["hire_date"]) if len(df) else None
    if table == "performance_reviews":
        period_end = review_period_end(df["review_period"]).dt.date
        df["flag_period_before_hire"] = (period_end < hire).fillna(False).astype(bool)
    elif table == "competency_assessments":
        df["flag_assessed_before_hire"] = (df["assessed_date"] < hire).fillna(False).astype(bool)
    return df


def transform_tables_to_silver(cfg: Config, run_id: str) -> StepMetrics:
    """Validate this run's Bronze table snapshots into Silver.

    Tables are processed parents-first so foreign keys are checked against the
    *valid* parent rows; a rejected team therefore also rejects its employees.
    Silver tables are overwritten on every run (idempotent).
    """
    m = StepMetrics(step="silver.tables")
    parent_keys: dict[str, set[int]] = {}
    silver: dict[str, pd.DataFrame] = {}
    for table in SOURCE_TABLES:
        contract = TABLE_CONTRACTS[table]
        bronze = read_parquet(bronze_table_path(cfg, table, run_id))
        valid, rejected = validate_table(bronze, contract, parent_keys, cfg.run_date)
        if valid.empty:
            raise RuntimeError(f"silver.{table}: zero valid rows out of {len(bronze)}; halting")
        if len(valid) + len(rejected) != len(bronze):
            raise RuntimeError(f"silver.{table}: reconciliation failed")

        if "employee_id" in valid.columns and table != "employees":
            valid = add_quality_flags(table, valid, silver["employees"])
        parent_keys[table] = set(valid[contract.primary_key].astype(int))
        silver[table] = valid

        write_parquet(valid, lake_path(cfg, "silver", table, "data.parquet"))
        quarantine_dir = lake_path(cfg, "silver", "_quarantine", table, f"run_date={cfg.run_date}")
        replace_dir(quarantine_dir)
        if not rejected.empty:
            rejected["_run_id"] = run_id
            write_parquet(rejected, f"{quarantine_dir}/rejected.parquet")
            reasons = Counter(r for rs in rejected["reject_reasons"] for r in rs.split(";"))
            log_event(logging.WARNING, "silver.table.quarantined", table=table, rows=len(rejected), reasons=dict(reasons))

        flag_counts = {c: int(valid[c].sum()) for c in valid.columns if c.startswith("flag_")}
        for flag, count in flag_counts.items():
            if count:
                log_event(logging.WARNING, "silver.table.flagged", table=table, flag=flag, rows=count)
        m.records_read += len(bronze)
        m.records_written += len(valid)
        m.records_quarantined += len(rejected)
        m.details[table] = {"read": len(bronze), "written": len(valid), "quarantined": len(rejected), **flag_counts}
        log_event(logging.INFO, "silver.table", table=table, read=len(bronze), written=len(valid), quarantined=len(rejected))
    return m


# --------------------------------------------------------------------------- #
# Silver: learning activity events
# --------------------------------------------------------------------------- #

EVENT_FIELD_ALIASES = {"notes": "text"}  # known schema drift: old name -> current name
EVENT_ID_RE = re.compile(r"^EV(\d{10})$")
EVENT_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")

SILVER_EVENT_COLUMNS = (
    "event_id",
    "employee_id",
    "event_ts",
    "event_date",
    "event_type",
    "course_name",
    "text",
    "score",
    "event_date_repaired",
    "event_date_raw",
    "source_file",
    "source_line",
)


@dataclass
class EventCleanResult:
    """Output of ``clean_events``: Silver rows, quarantined lines and counters."""

    valid: pd.DataFrame
    rejected: pd.DataFrame
    stats: Counter


@dataclass
class ParsedEvent:
    """One parsed JSON line plus its lineage and any reasons found so far."""

    record: dict
    source_file: str
    source_line: int
    raw_line: str
    reasons: list[str] = field(default_factory=list)


def partition_date(source_file: str) -> date | None:
    """Return the calendar date encoded in ``year=/month=/day=``, or None if invalid."""
    m = EVENT_PARTITION_RE.match(source_file)
    if not m:
        return None
    try:
        return date(*map(int, m.groups()))
    except ValueError:
        return None


def parse_event_ts(raw: object) -> datetime | None:
    """Parse a strict ``YYYY-MM-DDTHH:MM:SS`` timestamp; anything else is None."""
    if not isinstance(raw, str) or not EVENT_TS_RE.match(raw):
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:  # e.g. 2026-13-45T08:00:00
        return None


def event_number(event_id: object) -> int | None:
    """Return the numeric part of ``EV0000000123``, or None if malformed."""
    m = EVENT_ID_RE.match(event_id) if isinstance(event_id, str) else None
    return int(m.group(1)) if m else None


def normalize_event(rec: dict) -> tuple[dict, list[str], bool]:
    """Map known schema drift (``notes`` -> ``text``) before model validation.

    Returns ``(record, reasons, renamed)``. Unknown or missing fields are left
    for the ``LearningEvent`` model to reject.
    """
    rec = dict(rec)
    reasons: list[str] = []
    renamed = False
    for old, new in EVENT_FIELD_ALIASES.items():
        if old in rec:
            old_value = rec.pop(old)
            if rec.get(new) in (None, ""):
                rec[new] = old_value
                renamed = True
            elif rec[new] != old_value:
                reasons.append(f"conflicting_{old}_and_{new}")
    return rec, reasons, renamed


def build_day_bounds(trusted: list[tuple[date, int]]) -> Callable[[date, int], bool]:
    """Build the repair guardrail from events whose timestamps are trustworthy.

    Event IDs are issued in one contiguous block per day. Given ``(day, id)``
    pairs from trusted events, the returned function says whether an ``id``
    sits strictly after every trusted ID of earlier days and strictly before
    every trusted ID of later days, i.e. whether the ID agrees with ``day``.
    """
    per_day: dict[date, tuple[int, int]] = {}
    for day, num in trusted:
        lo, hi = per_day.get(day, (num, num))
        per_day[day] = (min(lo, num), max(hi, num))
    days = sorted(per_day)
    prefix_max: list[int] = []
    for d in days:
        prefix_max.append(max(prefix_max[-1], per_day[d][1]) if prefix_max else per_day[d][1])
    suffix_min: list[int] = [0] * len(days)
    for i in range(len(days) - 1, -1, -1):
        suffix_min[i] = min(suffix_min[i + 1], per_day[days[i]][0]) if i + 1 < len(days) else per_day[days[i]][0]

    def id_matches_day(day: date, num: int) -> bool:
        before = bisect.bisect_left(days, day)  # days strictly earlier: [0, before)
        after = bisect.bisect_right(days, day)  # days strictly later: [after, len)
        if before > 0 and num <= prefix_max[before - 1]:
            return False
        if after < len(days) and num >= suffix_min[after]:
            return False
        return True

    return id_matches_day


def resolve_event_date(
    raw: object, source_file: str, event_id: object, id_matches_day: Callable[[date, int], bool]
) -> tuple[datetime | None, bool, str | None]:
    """Apply the event date rule. Returns ``(timestamp, repaired, reason)``.

    A strict timestamp must fall on the file's partition day. Anything else is
    repaired to the partition day at 00:00, but only if the event's ID agrees
    with that day (``id_matches_day``); otherwise the event is rejected.
    """
    day = partition_date(source_file)
    ts = parse_event_ts(raw)
    if ts is not None:
        if day is None or ts.date() != day:
            return None, False, "event_date_partition_mismatch"
        return ts, False, None
    num = event_number(event_id)
    if day is not None and num is not None and id_matches_day(day, num):
        return datetime(day.year, day.month, day.day), True, None
    return None, False, "unrepairable_event_date"


def clean_events(files: dict[str, bytes], employee_ids: set[int]) -> EventCleanResult:
    """Turn raw Bronze JSONL files into Silver event rows plus quarantined lines.

    ``files`` maps a partition-relative path (``year=…/month=…/day=…/x.jsonl``)
    to its bytes. Rules, in order:

    1. Each line must be a JSON object (else ``malformed_json``).
    2. Known drift ``notes`` is renamed to ``text``.
    3. Exact duplicate records are dropped (kept once); records sharing an
       ``event_id`` with different content are all rejected.
    4. The record must satisfy the ``LearningEvent`` Pydantic model (fields,
       types, allowed values, score rules, employee exists).
    5. ``event_date`` must be a strict timestamp on the file's partition day.
       An unparseable date is **repaired** to the partition day at 00:00 and
       flagged (``event_date_repaired``), but only if the event's ID also falls
       inside that day's ID block; otherwise it is rejected.

    Every input line ends up exactly once in valid, rejected or the dropped
    duplicate count.
    """
    stats: Counter = Counter()
    rejected: list[dict] = []
    parsed: list[ParsedEvent] = []

    def reject(source_file: str, source_line: int, raw_line: str, reasons: list[str]) -> None:
        rejected.append(
            {
                "source_file": source_file,
                "source_line": source_line,
                "raw_line": raw_line,
                "reject_reasons": ";".join(reasons),
            }
        )
        stats.update(reasons)

    for source_file in sorted(files):
        lines = files[source_file].decode("utf-8", errors="replace").splitlines()
        stats["files_read"] += 1
        if not lines:
            stats["empty_files"] += 1
            log_event(logging.WARNING, "silver.events.empty_file", file=source_file)
        for line_no, line in enumerate(lines, start=1):
            stats["lines_read"] += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                rec = None
            if not isinstance(rec, dict):
                reject(source_file, line_no, line, ["malformed_json"])
                continue
            rec, reasons, renamed = normalize_event(rec)
            stats["schema_drift_renamed"] += int(renamed)
            parsed.append(ParsedEvent(rec, source_file, line_no, line, reasons))

    # Deduplicate on event_id: exact copies collapse, conflicting versions are rejected.
    by_id: dict[str, list[ParsedEvent]] = {}
    unique: list[ParsedEvent] = []
    for pe in parsed:
        eid = pe.record.get("event_id")
        if isinstance(eid, str):
            by_id.setdefault(eid, []).append(pe)
        else:
            unique.append(pe)
    for versions in by_id.values():
        distinct = {json.dumps(v.record, sort_keys=True, default=str) for v in versions}
        if len(distinct) == 1:
            unique.append(versions[0])
            stats["duplicates_dropped"] += len(versions) - 1
        else:
            for v in versions:
                v.reasons.append("conflicting_duplicate_event_id")
                unique.append(v)

    # Guardrail for date repair, built only from events whose timestamp is trusted.
    trusted: list[tuple[date, int]] = []
    for pe in unique:
        ts = parse_event_ts(pe.record.get("event_date"))
        day = partition_date(pe.source_file)
        num = event_number(pe.record.get("event_id"))
        if ts and day and num is not None and ts.date() == day and not pe.reasons:
            trusted.append((day, num))
    id_matches_day = build_day_bounds(trusted)

    context = {"parent_keys": {"employees": employee_ids}}
    rows: list[dict] = []
    for pe in unique:
        event, model_reasons = validate_record(LearningEvent, pe.record, context)
        reasons = pe.reasons + model_reasons
        ts, repaired = None, False
        if "event_date" in pe.record:
            ts, repaired, date_reason = resolve_event_date(
                pe.record["event_date"], pe.source_file, pe.record.get("event_id"), id_matches_day
            )
            if date_reason:
                reasons.append(date_reason)

        if reasons or event is None or ts is None:
            reject(pe.source_file, pe.source_line, pe.raw_line, reasons)
            continue
        if repaired:
            stats["event_date_repaired"] += 1
            log_event(
                logging.WARNING,
                "silver.events.date_repaired",
                event_id=event.event_id,
                raw=repr(event.event_date),
                repaired_to=ts.date(),
                file=pe.source_file,
                line=pe.source_line,
            )
        rows.append(
            {
                "event_id": event.event_id,
                "employee_id": event.employee_id,
                "event_ts": ts,
                "event_date": ts.date(),
                "event_type": event.event_type,
                "course_name": event.course_name,
                "text": event.text,
                "score": event.score,
                "event_date_repaired": repaired,
                "event_date_raw": str(event.event_date) if repaired else None,
                "source_file": pe.source_file,
                "source_line": pe.source_line,
            }
        )

    valid = pd.DataFrame(rows, columns=list(SILVER_EVENT_COLUMNS))
    valid = valid.astype(
        {
            "event_id": "string",
            "employee_id": "int64",
            "event_ts": "datetime64[us]",
            "event_type": "string",
            "course_name": "string",
            "text": "string",
            "score": "Int64",
            "event_date_repaired": "bool",
            "event_date_raw": "string",
            "source_file": "string",
            "source_line": "int64",
        }
    )
    valid = valid.sort_values("event_id", kind="stable").reset_index(drop=True)
    rejected_df = pd.DataFrame(rejected, columns=["source_file", "source_line", "raw_line", "reject_reasons"])
    stats["rows_written"] = len(valid)
    stats["lines_quarantined"] = len(rejected_df)
    return EventCleanResult(valid=valid, rejected=rejected_df, stats=stats)


def transform_events_to_silver(cfg: Config, run_id: str) -> StepMetrics:
    """Clean all Bronze learning-activity files into Silver, partitioned by event month.

    The whole Silver events dataset is rebuilt each run (idempotent). Fails
    if the line counts do not reconcile or nothing valid is produced.
    """
    m = StepMetrics(step="silver.learning_activity")
    fs, root = fsspec.core.url_to_fs(lake_path(cfg, "bronze", "learning_activity"))
    paths = sorted(p for p in fs.find(root) if p.endswith(".jsonl"))
    contents = with_retry(lambda: fs.cat(paths), what="read bronze events") if paths else {}
    files = {p[len(root):].lstrip("/"): data for p, data in contents.items()}

    employees = read_parquet(lake_path(cfg, "silver", "employees", "data.parquet"))
    result = clean_events(files, set(employees["employee_id"].astype(int)))
    s = result.stats

    accounted = s["rows_written"] + s["lines_quarantined"] + s["duplicates_dropped"]
    if accounted != s["lines_read"]:
        raise RuntimeError(f"silver.events reconciliation failed: read {s['lines_read']} but accounted {accounted}")
    if result.valid.empty:
        raise RuntimeError("silver.events: zero valid rows; halting")

    target = lake_path(cfg, "silver", "learning_activity")
    replace_dir(target)
    ts = result.valid["event_ts"]
    for (year, month), part in result.valid.groupby([ts.dt.year, ts.dt.month], sort=True):
        write_parquet(part, f"{target}/event_year={year:04d}/event_month={month:02d}/data.parquet")

    quarantine_dir = lake_path(cfg, "silver", "_quarantine", "learning_activity", f"run_date={cfg.run_date}")
    replace_dir(quarantine_dir)
    if not result.rejected.empty:
        rejected = result.rejected.assign(_run_id=run_id)
        write_parquet(rejected, f"{quarantine_dir}/rejected.parquet")

    counters = (
        "files_read",
        "empty_files",
        "lines_read",
        "duplicates_dropped",
        "schema_drift_renamed",
        "event_date_repaired",
        "lines_quarantined",
        "rows_written",
    )
    reasons = {k: v for k, v in s.items() if k not in counters}
    m.records_read = s["lines_read"]
    m.records_written = s["rows_written"]
    m.records_quarantined = s["lines_quarantined"]
    m.details = {**{k: s[k] for k in counters}, "quarantine_reasons": reasons}
    log_event(logging.INFO, "silver.events", **{k: s[k] for k in counters})
    if reasons:
        log_event(logging.WARNING, "silver.events.quarantined", rows=s["lines_quarantined"], reasons=reasons)
    return m


# --------------------------------------------------------------------------- #
# Gold: curated datasets that answer the business questions
# --------------------------------------------------------------------------- #
#
# One Gold table per question (Q1-Q7). Business definitions are the constants
# below. Builders are pure functions of the Silver tables. Before anything is
# written, every Gold row is validated against its Pydantic model (which
# re-checks the definitions row by row) and every table is reconciled with
# Silver; on any failure the job stops and the previous Gold stays intact.

KEY_REQUIRED_LEVEL = 4  # Q2: a competency required at this level is "key"
CRITICAL_SHORTFALL = 2  # Q2: this many levels below required is critical for any competency
LOW_RATING_MAX = 2  # Q5/Q6: a rating at or below this is "low"
PROMOTE_NOW_MIN_RATING = 4  # Q3: tier A
PROMOTE_REVIEW_RATING = 3  # Q3: tier B

PROMOTION_TIERS = {
    "A": "Promote now: high potential, ready, rated 4-5",
    "B": "Promote after calibration: high potential, ready, rated 3",
    "C": "Hold: potential not backed by a recent rating of 3 or more",
}
RISK_TIERS = {"Critical": 1, "High": 2, "Watch": 3, "Low": 4}
RATING_BANDS = ("bottom third", "middle third", "top third")


# --- Gold row models ------------------------------------------------------- #

Count = Annotated[int, Field(ge=0)]
Positive = Annotated[int, Field(ge=1)]
Rate = Annotated[float, Field(ge=0, le=1)]
AvgRating = Annotated[float, Field(ge=1, le=5)]
Score100 = Annotated[float, Field(ge=0, le=100)]
OptionalLevel = Annotated[int, Field(ge=1, le=5)] | None
RatingChange = Annotated[int, Field(ge=-4, le=4)] | None
TrendChoice = Literal["improved", "stable", "declined", "insufficient_data"]
RatingBand = Literal["bottom third", "middle third", "top third"]


def _expected_trend(change: int | None) -> str:
    if change is None:
        return "insufficient_data"
    return "improved" if change > 0 else "declined" if change < 0 else "stable"


def _expected_promotion_tier(latest_rating: int | None) -> str:
    if latest_rating is not None and latest_rating >= PROMOTE_NOW_MIN_RATING:
        return "A"
    if latest_rating == PROMOTE_REVIEW_RATING:
        return "B"
    return "C"


def _expected_risk_tier(signal: bool, recorded: str) -> str:
    if signal and recorded == "High":
        return "Critical"
    if (signal and recorded == "Medium") or (not signal and recorded == "High"):
        return "High"
    if signal and recorded == "Low":
        return "Watch"
    return "Low"


def _inconsistent(rule: str) -> PydanticCustomError:
    return PydanticCustomError("inconsistent", "row breaks the definition: {rule}", {"rule": rule})


class GoldRow(BaseModel):
    """Base for Gold rows: exact columns only."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class EmployeeContext(GoldRow):
    employee_id: Id
    first_name: Text
    last_name: Text
    team_id: Id
    team_name: Text
    department: DepartmentChoice
    job_level: JobLevelChoice


class ReviewHistoryRow(EmployeeContext):
    """Q1 history: one row per review (all reviews, flagged ones marked)."""

    hire_date: date
    review_id: Id
    review_period: HalfYear
    rating: Level
    manager_comment: OptionalText = None
    submitted_date: date
    flag_period_before_hire: bool


class PerformanceSummaryRow(EmployeeContext):
    """Q1 summary: one row per employee; ratings use valid (not before-hire) reviews only."""

    reviews_total: Count
    reviews_valid: Count
    avg_rating: AvgRating | None
    previous_period: HalfYear
    previous_rating: OptionalLevel
    latest_period: HalfYear
    latest_period_rating: OptionalLevel
    latest_rating: OptionalLevel
    rating_change: RatingChange
    trend: TrendChoice

    @model_validator(mode="after")
    def _consistent(self) -> "PerformanceSummaryRow":
        both = self.previous_rating is not None and self.latest_period_rating is not None
        expected_change = self.latest_period_rating - self.previous_rating if both else None
        if self.rating_change != expected_change:
            raise _inconsistent("rating_change = latest_period_rating - previous_rating")
        if self.trend != _expected_trend(self.rating_change):
            raise _inconsistent("trend follows the sign of rating_change")
        if self.reviews_valid > self.reviews_total:
            raise _inconsistent("reviews_valid <= reviews_total")
        if (self.reviews_valid == 0) != (self.latest_rating is None):
            raise _inconsistent("latest_rating present iff a valid review exists")
        return self


class SkillGapRow(EmployeeContext):
    """Q2: one row per competency assessment with its shortfall and criticality."""

    assessment_id: Id
    competency_id: Id
    competency_name: Text
    category: CategoryChoice
    proficiency_level: Level
    required_level: Level
    shortfall: Annotated[int, Field(ge=0, le=4)]
    is_key: bool
    is_critical: bool
    assessed_date: date
    flag_assessed_before_hire: bool

    @model_validator(mode="after")
    def _consistent(self) -> "SkillGapRow":
        if self.shortfall != max(0, self.required_level - self.proficiency_level):
            raise _inconsistent("shortfall = max(0, required_level - proficiency_level)")
        if self.is_key != (self.required_level >= KEY_REQUIRED_LEVEL):
            raise _inconsistent(f"is_key = required_level >= {KEY_REQUIRED_LEVEL}")
        if self.is_critical != (self.shortfall >= CRITICAL_SHORTFALL or (self.is_key and self.shortfall >= 1)):
            raise _inconsistent(f"is_critical = shortfall >= {CRITICAL_SHORTFALL} or (is_key and shortfall >= 1)")
        return self


class PromotionCandidateRow(EmployeeContext):
    """Q3: one row per high-potential, ready-now employee, tiered by latest rating."""

    rank: Positive
    tier: Literal["A", "B", "C"]
    recommendation: Text
    potential_rating: Literal["High"]
    promotion_readiness: Literal["Ready Now"]
    latest_rating: OptionalLevel
    trend: TrendChoice
    courses_completed: Count
    certifications: Count
    attrition_risk: LowMediumHighChoice
    retention_priority: bool

    @model_validator(mode="after")
    def _consistent(self) -> "PromotionCandidateRow":
        if self.tier != _expected_promotion_tier(self.latest_rating):
            raise _inconsistent("tier follows latest_rating (A >= 4, B = 3, else C)")
        if self.retention_priority != (self.tier == "A" and self.attrition_risk == "High"):
            raise _inconsistent("retention_priority = tier A and attrition_risk High")
        return self


class EmployeeLearningRow(EmployeeContext):
    """Q4: one row per employee with completed training and activity counts."""

    courses_enrolled: Count
    courses_completed: Count
    certifications: Count
    modules_completed: Count
    quizzes_attempted: Count
    quizzes_passed: Count
    avg_quiz_score: Score100 | None
    total_events: Count
    first_activity_date: date | None
    last_activity_date: date | None

    @model_validator(mode="after")
    def _consistent(self) -> "EmployeeLearningRow":
        if (self.total_events == 0) != (self.first_activity_date is None):
            raise _inconsistent("activity dates present iff the employee has events")
        if self.first_activity_date and self.last_activity_date and self.first_activity_date > self.last_activity_date:
            raise _inconsistent("first_activity_date <= last_activity_date")
        return self


class CourseCompletionRow(GoldRow):
    """Q4 detail: one row per employee and course completed and/or certified."""

    employee_id: Id
    course_name: Text
    completed: bool
    completed_date: date | None
    certified: bool
    certified_date: date | None
    date_repaired: bool

    @model_validator(mode="after")
    def _consistent(self) -> "CourseCompletionRow":
        if self.completed != (self.completed_date is not None) or self.certified != (self.certified_date is not None):
            raise _inconsistent("completed/certified flags match their dates")
        if not (self.completed or self.certified):
            raise _inconsistent("row has a completion or a certification")
        return self


class AttritionWatchRow(EmployeeContext):
    """Q5: one row per employee with performance signal and combined risk tier."""

    potential_rating: LowMediumHighChoice
    high_potential: bool
    recorded_attrition_risk: LowMediumHighChoice
    previous_rating: OptionalLevel
    latest_period_rating: OptionalLevel
    latest_rating: OptionalLevel
    rating_change: RatingChange
    trend: TrendChoice
    performance_signal: bool
    signal_reason: Text
    risk_tier: Literal["Critical", "High", "Watch", "Low"]
    tier_rank: Annotated[int, Field(ge=1, le=4)]

    @model_validator(mode="after")
    def _consistent(self) -> "AttritionWatchRow":
        declined = self.rating_change is not None and self.rating_change < 0
        low = self.latest_rating is not None and self.latest_rating <= LOW_RATING_MAX
        if self.performance_signal != (declined or low):
            raise _inconsistent(f"performance_signal = rating declined or latest_rating <= {LOW_RATING_MAX}")
        if self.risk_tier != _expected_risk_tier(self.performance_signal, self.recorded_attrition_risk):
            raise _inconsistent("risk_tier combines performance_signal and recorded_attrition_risk")
        if self.tier_rank != RISK_TIERS[self.risk_tier] or self.high_potential != (self.potential_rating == "High"):
            raise _inconsistent("tier_rank / high_potential match their sources")
        return self


class TeamPerformanceRow(GoldRow):
    """Q6: one row per team that has projects."""

    team_id: Id
    team_name: Text
    department: DepartmentChoice
    headcount: Positive
    people_rated: Count
    avg_rating: AvgRating | None
    high_rating_share: Rate | None
    low_performer_share: Rate | None
    rating_band: RatingBand | None
    projects: Positive
    projects_on_time: Count
    on_time_rate: Rate
    avg_outcome_score: Score100

    @model_validator(mode="after")
    def _consistent(self) -> "TeamPerformanceRow":
        if self.projects_on_time > self.projects or abs(self.on_time_rate - self.projects_on_time / self.projects) > 1e-9:
            raise _inconsistent("on_time_rate = projects_on_time / projects")
        if self.people_rated > self.headcount:
            raise _inconsistent("people_rated <= headcount")
        return self


class ProjectPerformanceRow(GoldRow):
    """Q6 detail: one row per project with its team's performance."""

    project_id: Id
    project_name: Text
    project_type: Text
    team_id: Id
    team_name: Text
    department: DepartmentChoice
    outcome_status: OutcomeStatusChoice
    on_time: bool
    outcome_score: Score100
    team_avg_rating: AvgRating | None
    team_people_rated: Count
    team_low_performer_share: Rate | None
    team_rating_band: RatingBand | None

    @model_validator(mode="after")
    def _consistent(self) -> "ProjectPerformanceRow":
        if self.on_time != (self.outcome_status == "on-time"):
            raise _inconsistent("on_time = outcome_status is on-time")
        return self


class SkillDistributionRow(GoldRow):
    """Q7: assessment counts per competency, department and proficiency level."""

    competency_id: Id
    competency_name: Text
    category: CategoryChoice
    department: DepartmentChoice
    proficiency_level: Level
    assessments: Positive


class CompetencySummaryRow(GoldRow):
    """Q7 summary: one row per competency."""

    competency_id: Id
    competency_name: Text
    category: CategoryChoice
    assessed: Positive
    employees: Positive
    avg_proficiency: AvgRating
    avg_required_level: AvgRating
    meets_required_share: Rate
    critical_gaps: Count
    critical_gap_rate: Rate
    avg_shortfall: Annotated[float, Field(ge=0, le=4)]
    level_1: Count
    level_2: Count
    level_3: Count
    level_4: Count
    level_5: Count

    @model_validator(mode="after")
    def _consistent(self) -> "CompetencySummaryRow":
        if self.level_1 + self.level_2 + self.level_3 + self.level_4 + self.level_5 != self.assessed:
            raise _inconsistent("level counts sum to assessed")
        if self.critical_gaps > self.assessed:
            raise _inconsistent("critical_gaps <= assessed")
        return self


@dataclass(frozen=True)
class GoldContract:
    """A Gold table's row model, its grain (unique key) and the question it answers."""

    model: type[GoldRow]
    grain: tuple[str, ...]
    question: str

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.model.model_fields)


GOLD_CONTRACTS: dict[str, GoldContract] = {
    "employee_review_history": GoldContract(ReviewHistoryRow, ("review_id",), "Q1"),
    "employee_performance_summary": GoldContract(PerformanceSummaryRow, ("employee_id",), "Q1"),
    "employee_skill_gaps": GoldContract(SkillGapRow, ("assessment_id",), "Q2"),
    "promotion_candidates": GoldContract(PromotionCandidateRow, ("employee_id",), "Q3"),
    "employee_learning": GoldContract(EmployeeLearningRow, ("employee_id",), "Q4"),
    "employee_course_completions": GoldContract(CourseCompletionRow, ("employee_id", "course_name"), "Q4"),
    "attrition_watchlist": GoldContract(AttritionWatchRow, ("employee_id",), "Q5"),
    "team_performance": GoldContract(TeamPerformanceRow, ("team_id",), "Q6"),
    "project_performance": GoldContract(ProjectPerformanceRow, ("project_id",), "Q6"),
    "skill_distribution": GoldContract(
        SkillDistributionRow, ("competency_id", "department", "proficiency_level"), "Q7"
    ),
    "competency_summary": GoldContract(CompetencySummaryRow, ("competency_id",), "Q7"),
}


# --- Gold builders (pure functions of Silver) ------------------------------ #


def _employee_directory(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Employees with their team name and department."""
    teams = silver["teams"].rename(columns={"name": "team_name"})
    return silver["employees"].merge(teams, on="team_id", how="left")


def _valid_reviews(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Reviews that count toward ratings and trends (not dated before the hire)."""
    rev = silver["performance_reviews"]
    valid = rev[~rev["flag_period_before_hire"].astype(bool)].copy()
    valid["rating"] = valid["rating"].astype(float)
    return valid


def build_review_history(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q1 history: every review with employee and team context."""
    hist = silver["performance_reviews"].merge(_employee_directory(silver), on="employee_id", how="left")
    return hist.sort_values(["employee_id", "review_period"]).reset_index(drop=True)


def build_performance_summary(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q1 summary: previous vs latest period rating, change and trend per employee."""
    rev = silver["performance_reviews"]
    periods = sorted(rev["review_period"].dropna().unique())
    if len(periods) < 2:
        raise ValueError("gold: at least two review periods are needed to measure a trend")
    previous, latest = periods[-2], periods[-1]
    valid = _valid_reviews(silver)
    by_period = valid.pivot(index="employee_id", columns="review_period", values="rating")

    out = _employee_directory(silver).set_index("employee_id")
    idx = out.index
    nan = pd.Series(float("nan"), index=idx)
    out["reviews_total"] = rev.groupby("employee_id").size().reindex(idx, fill_value=0)
    out["reviews_valid"] = valid.groupby("employee_id").size().reindex(idx, fill_value=0)
    out["avg_rating"] = valid.groupby("employee_id")["rating"].mean().reindex(idx)
    out["previous_period"] = previous
    out["previous_rating"] = by_period[previous].reindex(idx) if previous in by_period else nan
    out["latest_period"] = latest
    out["latest_period_rating"] = by_period[latest].reindex(idx) if latest in by_period else nan
    out["latest_rating"] = valid.sort_values("review_period").groupby("employee_id")["rating"].last().reindex(idx)
    out["rating_change"] = out["latest_period_rating"] - out["previous_rating"]
    change = out["rating_change"]
    out["trend"] = np.select(
        [change > 0, change == 0, change < 0], ["improved", "stable", "declined"], "insufficient_data"
    )
    return out.reset_index()


def build_skill_gaps(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q2: shortfall, key and critical flags for every assessment."""
    comp = silver["competencies"].rename(columns={"name": "competency_name"})
    g = silver["competency_assessments"].merge(comp, on="competency_id", how="left")
    g = g.merge(_employee_directory(silver), on="employee_id", how="left")
    g["shortfall"] = (g["required_level"].astype(int) - g["proficiency_level"].astype(int)).clip(lower=0)
    g["is_key"] = g["required_level"].astype(int) >= KEY_REQUIRED_LEVEL
    g["is_critical"] = (g["shortfall"] >= CRITICAL_SHORTFALL) | (g["is_key"] & (g["shortfall"] >= 1))
    return g.sort_values("assessment_id").reset_index(drop=True)


def build_employee_learning(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q4: per-employee training completed (distinct courses) and activity counts."""
    ev = silver["learning_activity"]
    out = _employee_directory(silver).set_index("employee_id")
    idx = out.index

    def distinct_courses(event_type: str) -> pd.Series:
        return ev[ev["event_type"] == event_type].groupby("employee_id")["course_name"].nunique().reindex(idx, fill_value=0)

    def events(event_type: str) -> pd.Series:
        return (ev["event_type"] == event_type).groupby(ev["employee_id"]).sum().reindex(idx, fill_value=0)

    out["courses_enrolled"] = distinct_courses("enrollment")
    out["courses_completed"] = distinct_courses("course_completed")
    out["certifications"] = distinct_courses("certification_earned")
    out["modules_completed"] = events("module_completed")
    out["quizzes_attempted"] = events("quiz_attempted")
    out["quizzes_passed"] = events("quiz_passed")
    quiz = ev[ev["event_type"].isin(QUIZ_EVENT_TYPES)]
    out["avg_quiz_score"] = quiz.groupby("employee_id")["score"].mean().astype(float).reindex(idx)
    out["total_events"] = ev.groupby("employee_id").size().reindex(idx, fill_value=0)
    out["first_activity_date"] = ev.groupby("employee_id")["event_date"].min().reindex(idx)
    out["last_activity_date"] = ev.groupby("employee_id")["event_date"].max().reindex(idx)
    return out.reset_index()


def build_course_completions(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q4 detail: each course an employee completed and/or was certified in."""
    ev = silver["learning_activity"]
    keys = ["employee_id", "course_name"]

    def first(event_type: str, date_col: str, repaired_col: str) -> pd.DataFrame:
        sub = ev[ev["event_type"] == event_type]
        return sub.groupby(keys).agg(**{date_col: ("event_date", "min"), repaired_col: ("event_date_repaired", "any")})

    out = first("course_completed", "completed_date", "_repaired_c").join(
        first("certification_earned", "certified_date", "_repaired_x"), how="outer"
    )
    out["completed"] = out["completed_date"].notna()
    out["certified"] = out["certified_date"].notna()
    out["date_repaired"] = out["_repaired_c"].eq(True) | out["_repaired_x"].eq(True)
    return out.reset_index().sort_values(keys).reset_index(drop=True)


def build_promotion_candidates(
    silver: dict[str, pd.DataFrame], summary: pd.DataFrame, learning: pd.DataFrame
) -> pd.DataFrame:
    """Q3: high potential + ready now, tiered by latest valid rating and ranked."""
    tr = silver["talent_review"]
    c = tr[(tr["potential_rating"] == "High") & (tr["promotion_readiness"] == "Ready Now")]
    c = c.merge(summary.drop(columns=["reviews_total"]), on="employee_id", how="left")
    c = c.merge(learning[["employee_id", "courses_completed", "certifications"]], on="employee_id", how="left")
    rating = c["latest_rating"].astype(float)
    c["tier"] = np.select([rating >= PROMOTE_NOW_MIN_RATING, rating == PROMOTE_REVIEW_RATING], ["A", "B"], "C")
    c["recommendation"] = c["tier"].map(PROMOTION_TIERS)
    c["retention_priority"] = (c["tier"] == "A") & (c["attrition_risk"] == "High")
    c = c.sort_values(
        ["tier", "latest_rating", "certifications", "courses_completed", "employee_id"],
        ascending=[True, False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    c["rank"] = range(1, len(c) + 1)
    return c


def build_attrition_watchlist(silver: dict[str, pd.DataFrame], summary: pd.DataFrame) -> pd.DataFrame:
    """Q5: performance signal (declined or low rating) combined with recorded attrition risk."""
    tr = silver["talent_review"][["employee_id", "potential_rating", "attrition_risk"]]
    a = summary.merge(tr, on="employee_id", how="left").rename(columns={"attrition_risk": "recorded_attrition_risk"})
    declined = a["rating_change"] < 0
    low = a["latest_rating"] <= LOW_RATING_MAX
    a["performance_signal"] = declined | low
    a["signal_reason"] = np.select(
        [declined & low, declined, low, a["trend"] == "insufficient_data"],
        [f"rating declined; latest rating <= {LOW_RATING_MAX}", "rating declined", f"latest rating <= {LOW_RATING_MAX}",
         "no rating trend available"],
        "no performance signal",
    )
    signal, risk = a["performance_signal"], a["recorded_attrition_risk"]
    a["risk_tier"] = np.select(
        [signal & (risk == "High"), (signal & (risk == "Medium")) | (~signal & (risk == "High")), signal & (risk == "Low")],
        ["Critical", "High", "Watch"],
        "Low",
    )
    a["tier_rank"] = a["risk_tier"].map(RISK_TIERS)
    a["high_potential"] = a["potential_rating"] == "High"
    return a.sort_values(["tier_rank", "employee_id"]).reset_index(drop=True)


def build_team_performance(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q6: performance of the people in each team that has projects, next to project outcomes."""
    proj = silver["projects"].assign(on_time=lambda d: d["outcome_status"] == "on-time")
    project_teams = proj["team_id"].unique()
    people = silver["employees"][silver["employees"]["team_id"].isin(project_teams)]
    valid = _valid_reviews(silver).merge(people[["employee_id", "team_id"]], on="employee_id")
    person_avg = valid.groupby(["team_id", "employee_id"])["rating"].mean()

    t = silver["teams"][silver["teams"]["team_id"].isin(project_teams)].rename(columns={"name": "team_name"}).set_index("team_id")
    t["headcount"] = people.groupby("team_id").size().reindex(t.index, fill_value=0)
    t["people_rated"] = person_avg.groupby("team_id").size().reindex(t.index, fill_value=0)
    t["avg_rating"] = valid.groupby("team_id")["rating"].mean()
    t["high_rating_share"] = (valid["rating"] >= 4).groupby(valid["team_id"]).mean()
    t["low_performer_share"] = (person_avg <= LOW_RATING_MAX).groupby("team_id").mean()
    rated = t["avg_rating"].dropna()
    t["rating_band"] = pd.Series(
        pd.qcut(rated.rank(method="first"), 3, labels=list(RATING_BANDS)).astype(str), index=rated.index
    )
    p = proj.groupby("team_id").agg(
        projects=("project_id", "size"), projects_on_time=("on_time", "sum"), avg_outcome_score=("outcome_score", "mean")
    )
    t = t.join(p)
    t["on_time_rate"] = t["projects_on_time"] / t["projects"]
    return t.reset_index().sort_values("team_id").reset_index(drop=True)


def build_project_performance(silver: dict[str, pd.DataFrame], team_perf: pd.DataFrame) -> pd.DataFrame:
    """Q6 detail: each project with its type, outcome and its team's performance."""
    p = silver["projects"].rename(columns={"name": "project_name"}).copy()
    p["project_type"] = p["project_name"].str.split().str[-1]
    p["on_time"] = p["outcome_status"] == "on-time"
    team = team_perf[["team_id", "team_name", "department", "avg_rating", "people_rated", "low_performer_share", "rating_band"]]
    team = team.rename(
        columns={
            "avg_rating": "team_avg_rating",
            "people_rated": "team_people_rated",
            "low_performer_share": "team_low_performer_share",
            "rating_band": "team_rating_band",
        }
    )
    return p.merge(team, on="team_id", how="left").sort_values("project_id").reset_index(drop=True)


def build_skill_distribution(gaps: pd.DataFrame) -> pd.DataFrame:
    """Q7: how many assessments sit at each proficiency level, per competency and department."""
    keys = ["competency_id", "competency_name", "category", "department", "proficiency_level"]
    return gaps.groupby(keys).size().rename("assessments").reset_index()


def build_competency_summary(gaps: pd.DataFrame) -> pd.DataFrame:
    """Q7 summary: proficiency, requirement coverage and gaps per competency."""
    g = gaps.assign(meets=gaps["shortfall"] == 0)
    s = g.groupby(["competency_id", "competency_name", "category"]).agg(
        assessed=("assessment_id", "size"),
        employees=("employee_id", "nunique"),
        avg_proficiency=("proficiency_level", "mean"),
        avg_required_level=("required_level", "mean"),
        meets_required_share=("meets", "mean"),
        critical_gaps=("is_critical", "sum"),
        avg_shortfall=("shortfall", "mean"),
    )
    s["critical_gap_rate"] = s["critical_gaps"] / s["assessed"]
    levels = pd.crosstab(
        [g["competency_id"], g["competency_name"], g["category"]], g["proficiency_level"]
    ).reindex(columns=range(1, 6), fill_value=0)
    levels.columns = [f"level_{c}" for c in levels.columns]
    return s.join(levels).reset_index().sort_values("competency_id").reset_index(drop=True)


def build_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Build every Gold table from Silver (pure: no I/O)."""
    summary = build_performance_summary(silver)
    gaps = build_skill_gaps(silver)
    learning = build_employee_learning(silver)
    team_perf = build_team_performance(silver)
    return {
        "employee_review_history": build_review_history(silver),
        "employee_performance_summary": summary,
        "employee_skill_gaps": gaps,
        "promotion_candidates": build_promotion_candidates(silver, summary, learning),
        "employee_learning": learning,
        "employee_course_completions": build_course_completions(silver),
        "attrition_watchlist": build_attrition_watchlist(silver, summary),
        "team_performance": team_perf,
        "project_performance": build_project_performance(silver, team_perf),
        "skill_distribution": build_skill_distribution(gaps),
        "competency_summary": build_competency_summary(gaps),
    }


# --- Gold validation and reconciliation ---------------------------------------- #


class GoldValidationError(RuntimeError):
    """A Gold table broke its contract or does not reconcile with Silver."""


def validate_gold_table(name: str, df: pd.DataFrame) -> list[dict]:
    """Validate a Gold table against its contract; returns the typed rows.

    Checks the columns, that the grain is unique, and every row against the
    Pydantic model (types, ranges and the business definitions). Gold is
    derived data, so any failure is a bug and raises.
    """
    contract = GOLD_CONTRACTS[name]
    columns = list(contract.columns)
    missing = [c for c in columns if c not in df.columns]
    if missing:  # builders may carry extra helper columns; only the contract's are kept
        raise GoldValidationError(f"gold.{name}: builder is missing columns {missing}")
    dupes = int(df.duplicated(subset=list(contract.grain)).sum())
    if dupes:
        raise GoldValidationError(f"gold.{name}: {dupes} rows repeat the grain {contract.grain}")
    raw = df[columns].astype(object)
    rows: list[dict] = []
    failures: Counter = Counter()
    for record in raw.where(raw.notna(), None).to_dict("records"):
        try:
            rows.append(contract.model.model_validate(record).model_dump())
        except ValidationError as exc:
            # Name the field or the broken definition, e.g. "trend follows the sign of rating_change".
            failures.update(
                f"{'.'.join(map(str, e['loc'])) or 'row'}: {e['msg']}" for e in exc.errors()
            )
    if failures:
        raise GoldValidationError(f"gold.{name}: {sum(failures.values())} invalid rows: {dict(failures)}")
    return rows


def reconcile_gold(silver: dict[str, pd.DataFrame], gold: dict[str, pd.DataFrame]) -> dict[str, tuple[int, int]]:
    """Cross-check Gold totals against Silver. Returns {check: (gold, silver)}; raises on mismatch."""
    rev, ev, proj = silver["performance_reviews"], silver["learning_activity"], silver["projects"]
    tr, ca, emp = silver["talent_review"], silver["competency_assessments"], silver["employees"]

    def distinct_pairs(event_type: str) -> int:
        return len(ev.loc[ev["event_type"] == event_type, ["employee_id", "course_name"]].drop_duplicates())

    checks = {
        "Q1 review history rows = Silver reviews": (len(gold["employee_review_history"]), len(rev)),
        "Q1 summary rows = Silver employees": (len(gold["employee_performance_summary"]), len(emp)),
        "Q1 summary valid reviews = Silver valid reviews": (
            int(gold["employee_performance_summary"]["reviews_valid"].sum()),
            int((~rev["flag_period_before_hire"].astype(bool)).sum()),
        ),
        "Q2 skill gap rows = Silver assessments": (len(gold["employee_skill_gaps"]), len(ca)),
        "Q3 candidates = Silver High + Ready Now": (
            len(gold["promotion_candidates"]),
            int(((tr["potential_rating"] == "High") & (tr["promotion_readiness"] == "Ready Now")).sum()),
        ),
        "Q4 learning rows = Silver employees": (len(gold["employee_learning"]), len(emp)),
        "Q4 learning events = Silver events": (int(gold["employee_learning"]["total_events"].sum()), len(ev)),
        "Q4 completions = distinct Silver completions": (
            int(gold["employee_course_completions"]["completed"].sum()),
            distinct_pairs("course_completed"),
        ),
        "Q4 certifications = distinct Silver certifications": (
            int(gold["employee_course_completions"]["certified"].sum()),
            distinct_pairs("certification_earned"),
        ),
        "Q5 watchlist rows = Silver employees": (len(gold["attrition_watchlist"]), len(emp)),
        "Q6 team rows = Silver teams with projects": (len(gold["team_performance"]), proj["team_id"].nunique()),
        "Q6 team projects = Silver projects": (int(gold["team_performance"]["projects"].sum()), len(proj)),
        "Q6 project rows = Silver projects": (len(gold["project_performance"]), len(proj)),
        "Q7 distribution assessments = Silver assessments": (int(gold["skill_distribution"]["assessments"].sum()), len(ca)),
        "Q7 competency assessed = Silver assessments": (int(gold["competency_summary"]["assessed"].sum()), len(ca)),
    }
    failed = {k: v for k, v in checks.items() if v[0] != v[1]}
    if failed:
        raise GoldValidationError(f"gold reconciliation failed (gold, silver): {failed}")
    return checks


def gold_highlights(gold: dict[str, pd.DataFrame]) -> dict[str, object]:
    """Headline numbers per question, logged and stored in the run manifest."""
    summary, gaps = gold["employee_performance_summary"], gold["employee_skill_gaps"]
    cand, watch, teams = gold["promotion_candidates"], gold["attrition_watchlist"], gold["team_performance"]
    learning = gold["employee_learning"]
    return {
        "q1_trend": summary["trend"].value_counts().to_dict(),
        "q2_employees_with_critical_gap": int(gaps.loc[gaps["is_critical"], "employee_id"].nunique()),
        "q2_critical_assessments": int(gaps["is_critical"].sum()),
        "q3_candidates_by_tier": cand["tier"].value_counts().sort_index().to_dict(),
        "q3_retention_priority": int(cand["retention_priority"].sum()),
        "q4_employees_completed_a_course": int((learning["courses_completed"] > 0).sum()),
        "q4_employees_certified": int((learning["certifications"] > 0).sum()),
        "q5_risk_tiers": watch["risk_tier"].value_counts().reindex(list(RISK_TIERS), fill_value=0).to_dict(),
        "q5_critical_high_potential": int(((watch["risk_tier"] == "Critical") & watch["high_potential"]).sum()),
        "q6_on_time_by_rating_band": (
            teams.groupby("rating_band")["on_time_rate"].mean().round(3).reindex(list(RATING_BANDS)).to_dict()
        ),
    }


def read_parquet_dataset(uri: str) -> pd.DataFrame:
    """Read every Parquet file under ``uri`` (e.g. a month-partitioned dataset) into one frame."""
    fs, root = fsspec.core.url_to_fs(uri)
    paths = sorted(p for p in fs.find(root) if p.endswith(".parquet"))
    if not paths:
        raise FileNotFoundError(f"No Parquet files under {uri}")
    def read(path: str) -> pd.DataFrame:
        with fs.open(path, "rb") as fh:
            return pd.read_parquet(fh)

    return pd.concat([with_retry(lambda: read(p), what=f"read {p}") for p in paths], ignore_index=True)


def write_rows(rows: list[dict], columns: list[str], uri: str) -> None:
    """Write validated rows as Parquet; Arrow keeps nullable ints and dates exact."""
    table = pa.Table.from_pylist(rows) if rows else pa.Table.from_pandas(pd.DataFrame(columns=columns))
    fs, path = fsspec.core.url_to_fs(uri)

    def write() -> None:
        fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        with fs.open(path, "wb") as fh:
            pq.write_table(table, fh)

    with_retry(write, what=f"write {uri}")


def aggregate_to_gold(cfg: Config) -> StepMetrics:
    """Build, validate, reconcile and write every Gold table from Silver.

    Nothing is written unless every table passes validation and
    reconciliation; then ``gold/`` is rebuilt from scratch (idempotent).
    """
    m = StepMetrics(step="gold")
    silver = {t: read_parquet(lake_path(cfg, "silver", t, "data.parquet")) for t in SOURCE_TABLES}
    silver["learning_activity"] = read_parquet_dataset(lake_path(cfg, "silver", "learning_activity"))
    m.records_read = sum(len(df) for df in silver.values())

    gold = build_gold(silver)
    validated = {name: validate_gold_table(name, df) for name, df in gold.items()}
    checks = reconcile_gold(silver, gold)

    replace_dir(lake_path(cfg, "gold"))
    for name, rows in validated.items():
        target = lake_path(cfg, "gold", name, "data.parquet")
        write_rows(rows, list(GOLD_CONTRACTS[name].columns), target)
        m.records_written += len(rows)
        log_event(logging.INFO, "gold.table", table=name, question=GOLD_CONTRACTS[name].question, rows=len(rows))

    highlights = gold_highlights(gold)
    m.details = {
        "tables": {name: len(rows) for name, rows in validated.items()},
        "reconciliation": {k: f"PASS ({g})" for k, (g, _) in checks.items()},
        "highlights": highlights,
    }
    log_event(logging.INFO, "gold.reconciled", checks=len(checks), status="PASS")
    log_event(logging.INFO, "gold.highlights", **highlights)
    return m


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def run_step(fn: Callable[[], StepMetrics]) -> StepMetrics:
    """Run one step: wall time, CPU time and peak memory, marked succeeded unless it set its own status."""
    started, cpu_started = time.monotonic(), time.process_time()
    metrics = fn()
    metrics.duration_s = round(time.monotonic() - started, 3)
    metrics.cpu_s = round(time.process_time() - cpu_started, 3)
    metrics.peak_rss_mb = peak_rss_mb()
    if metrics.status == "pending":
        metrics.status = "succeeded"
    log_event(
        logging.INFO,
        "step.end",
        step=metrics.step,
        status=metrics.status,
        duration_s=metrics.duration_s,
        cpu_s=metrics.cpu_s,
        peak_rss_mb=metrics.peak_rss_mb,
    )
    return metrics


def main() -> int:
    """Run Bronze → Silver → Gold and write a run manifest; returns the exit code."""
    cfg = Config.from_env()
    setup_logging(cfg.log_level)

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc)
    log_event(logging.INFO, "job.start", run_id=run_id, run_date=cfg.run_date, config=cfg)

    steps: list[StepMetrics] = []
    status, error = "succeeded", None
    try:
        if cfg.seed_source_uri:
            steps.append(run_step(lambda: seed_source_database(cfg)))
        steps.append(run_step(lambda: ingest_postgres_to_bronze(cfg, run_id, started_at)))
        steps.append(run_step(lambda: ingest_events_to_bronze(cfg)))
        steps.append(run_step(lambda: transform_tables_to_silver(cfg, run_id)))
        steps.append(run_step(lambda: transform_events_to_silver(cfg, run_id)))
        steps.append(run_step(lambda: aggregate_to_gold(cfg)))
    except Exception as exc:  # noqa: BLE001 - top-level boundary: log, record, exit non-zero
        status, error = "failed", repr(exc)
        log.exception("event=job.error error=%r", exc)

    ended_at = datetime.now(timezone.utc)
    duration_s = round((ended_at - started_at).total_seconds(), 3)
    manifest = {
        "job": JOB_NAME,
        "run_id": run_id,
        "run_date": cfg.run_date,
        "status": status,
        "error": error,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_s": duration_s,
        "cpu_s": round(time.process_time(), 3),
        "peak_rss_mb": peak_rss_mb(),
        "steps": [asdict(s) for s in steps],
    }
    try:
        write_json(manifest, lake_path(cfg, "_runs", f"run_date={cfg.run_date}", f"{run_id}.json"))
    except Exception as exc:  # noqa: BLE001 - manifest is best-effort; never mask the job status
        log_event(logging.ERROR, "manifest.write_failed", error=repr(exc))

    log_event(
        logging.INFO if status == "succeeded" else logging.ERROR,
        "job.end",
        run_id=run_id,
        status=status,
        duration_s=duration_s,
    )
    return 0 if status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
