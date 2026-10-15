"""Performance tests for talent-etl: throughput, scale, resource use and SLA.

For each scale k (default 1x, 5x, 10x) this builds k replicas of the sample
data with new IDs, loads them into their own PostgreSQL database, and runs
``job.py`` several times from an empty lake (cold) plus once more on the same
lake (warm, i.e. incremental). It reports per-step wall time, CPU time, peak
memory and throughput; fits how time grows with volume to spot non-linear
steps; checks that every answer scales exactly with the data (correctness at
volume); and evaluates the SLA.

Replicas keep every property the pipeline relies on: foreign keys, unique
names/emails, the planted data-quality issues, and event IDs that stay in one
contiguous block per day (needed by the date-repair guardrail).

Usage (from data/talent-etl):
    .venv/bin/python perf/run_perf.py [--scales 1,5,10] [--repeats 3] [--keep]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

JOB_DIR = Path(__file__).resolve().parents[1]
SOURCE_DIR = (
    JOB_DIR.parents[1]
    / "sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management"
)
WORK_DIR = JOB_DIR / "perf" / "work"
RESULTS_DIR = JOB_DIR / "perf" / "results"
RUN_DATE = "2026-10-14"

TABLES = [
    "teams",
    "competencies",
    "employees",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
]
ID_OFFSET = 1_000_000  # replica i adds i * ID_OFFSET to every entity ID
OFFSET_COLUMNS = {
    "teams": ["team_id"],
    "competencies": [],  # a shared dimension: not replicated
    "employees": ["employee_id", "team_id"],
    "projects": ["project_id", "team_id"],
    "performance_reviews": ["review_id", "employee_id"],
    "competency_assessments": ["assessment_id", "employee_id"],
    "talent_review": ["employee_id"],
}
STEPS = ["bronze.postgres", "bronze.learning_activity", "silver.tables", "silver.learning_activity", "gold"]

# Service level for a daily batch (assumed; the brief doesn't set one).
SLA_SECONDS = {1: 60.0, 10: 300.0}
NONLINEAR_EXPONENT = 1.2  # time ~ volume^b; b above this is flagged


# --------------------------------------------------------------------------- data


def pg_params(dbname: str) -> dict:
    return {
        "host": os.environ.get("POSTGRES_HOST") or "localhost",
        "port": int(os.environ.get("POSTGRES_PORT") or 5432),
        "user": os.environ.get("POSTGRES_USER") or "postgres",
        "password": os.environ.get("POSTGRES_PASS") or "postgres123",
        "dbname": dbname,
    }


def scale_tables(k: int) -> dict[str, pd.DataFrame]:
    """k replicas of every source table, IDs offset per replica, names/emails kept unique."""
    out = {}
    for table in TABLES:
        src = pd.read_csv(SOURCE_DIR / f"{table}.csv", dtype=str, keep_default_na=False)
        if not OFFSET_COLUMNS[table]:
            out[table] = src
            continue
        parts = []
        for i in range(k):
            rep = src.copy()
            for col in OFFSET_COLUMNS[table]:
                rep[col] = (rep[col].astype(int) + i * ID_OFFSET).astype(str)
            if i and table == "teams":
                rep["name"] = rep["name"] + f" r{i}"
            if i and table == "employees":
                rep["email"] = rep["email"].str.replace("@", f"+r{i}@", n=1, regex=False)
            parts.append(rep)
        out[table] = pd.concat(parts, ignore_index=True)
    return out


def load_database(dbname: str, tables: dict[str, pd.DataFrame]) -> float:
    """(Re)create ``dbname`` from schema.sql and COPY the tables in. Returns seconds taken."""
    started = time.perf_counter()
    with psycopg.connect(**pg_params("postgres"), autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}"')
    with psycopg.connect(**pg_params(dbname)) as conn:
        conn.execute((SOURCE_DIR / "schema.sql").read_text())
        with conn.cursor() as cur:
            for table in TABLES:
                with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                    copy.write(tables[table].to_csv(index=False))
        conn.commit()
    return time.perf_counter() - started


def scale_events(k: int, dest: Path) -> int:
    """Write k replicas of every event file (one file per replica per day). Returns lines written.

    Event numbers become ``n * k + i``, so each day's IDs stay one contiguous
    block across replicas; employee IDs get the replica's offset. Lines that
    aren't JSON objects (the planted malformed ones) are copied unchanged.
    """
    shutil.rmtree(dest, ignore_errors=True)
    src_root = SOURCE_DIR / "learning_activity"
    written = 0
    for src in sorted(src_root.rglob("*.jsonl")):
        lines = src.read_text(encoding="utf-8").splitlines()
        out_dir = dest / src.parent.relative_to(src_root)
        out_dir.mkdir(parents=True, exist_ok=True)
        for i in range(k):
            out = []
            for line in lines:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    out.append(line)
                    continue
                if isinstance(rec, dict):
                    m = re.fullmatch(r"EV(\d{10})", str(rec.get("event_id", "")))
                    if m:
                        rec["event_id"] = f"EV{int(m.group(1)) * k + i:010d}"
                    emp = rec.get("employee_id")
                    if isinstance(emp, str) and emp.strip().isdigit():
                        rec["employee_id"] = str(int(emp) + i * ID_OFFSET)
                    line = json.dumps(rec)
                out.append(line)
            (out_dir / f"events-{i:03d}.jsonl").write_text("\n".join(out) + ("\n" if out else ""), encoding="utf-8")
            written += len(out)
    return written


# --------------------------------------------------------------------------- runs


def run_job(dbname: str, events: Path, lake: Path, fresh: bool) -> tuple[float, dict]:
    """Run job.py as a separate process; returns (wall seconds incl. startup, run manifest)."""
    if fresh:
        shutil.rmtree(lake, ignore_errors=True)
    env = {
        **os.environ,
        "IS_LOCAL": "true",
        "POSTGRES_NAME": dbname,
        "LAKE_ROOT": str(lake),
        "EVENTS_SOURCE_URI": str(events),
        "RUN_DATE": RUN_DATE,
    }
    started = time.perf_counter()
    proc = subprocess.run([sys.executable, "job.py"], cwd=JOB_DIR, env=env, capture_output=True, text=True)
    wall = time.perf_counter() - started
    if proc.returncode != 0:
        raise RuntimeError(f"job.py failed (exit {proc.returncode}):\n{proc.stdout[-3000:]}")
    manifests = [json.loads(p.read_text()) for p in (lake / "_runs").rglob("*.json")]
    return wall, max(manifests, key=lambda m: m["ended_at"])


def summarise(k: int, mode: str, rep: int, wall: float, m: dict) -> dict:
    steps = {s["step"]: s for s in m["steps"]}
    table_rows = steps["bronze.postgres"]["records_read"]
    event_lines = steps["silver.learning_activity"]["details"]["lines_read"]
    row = {
        "scale": k,
        "mode": mode,
        "rep": rep,
        "wall_s": round(wall, 3),
        "job_s": m["duration_s"],
        "cpu_s": m["cpu_s"],
        "peak_rss_mb": m["peak_rss_mb"],
        "source_records": table_rows + event_lines,
        "table_rows": table_rows,
        "event_lines": event_lines,
    }
    for name in STEPS:
        s = steps[name]
        row[f"{name}.s"] = s["duration_s"]
        row[f"{name}.cpu_s"] = s["cpu_s"]
        row[f"{name}.rss_mb"] = s["peak_rss_mb"]
        row[f"{name}.read"] = s["records_read"]
    return row


def correctness_counts(m: dict) -> dict[str, object]:
    """Counts that must grow exactly k-fold with k replicas, plus ratios that must not change."""
    steps = {s["step"]: s for s in m["steps"]}
    ev = steps["silver.learning_activity"]["details"]
    hl = steps["gold"]["details"]["highlights"]
    return {
        "event lines read": ev["lines_read"],
        "Silver events written": ev["rows_written"],
        "event lines quarantined": ev["lines_quarantined"],
        "duplicates dropped": ev["duplicates_dropped"],
        "dates repaired": ev["event_date_repaired"],
        "empty files": ev["empty_files"],
        "Q2 employees with critical gap": hl["q2_employees_with_critical_gap"],
        "Q3 promotion candidates": sum(hl["q3_candidates_by_tier"].values()),
        "Q4 employees certified": hl["q4_employees_certified"],
        "Q5 Critical attrition risk": hl["q5_risk_tiers"]["Critical"],
        "Q6 teams with projects": steps["gold"]["details"]["tables"]["team_performance"],
        "Q6 on-time by rating band (ratio)": hl["q6_on_time_by_rating_band"],
    }


# --------------------------------------------------------------------------- report


def growth_exponent(scales: list[int], times: list[float]) -> float | None:
    """b in time ~ a * volume^b (log-log fit). ~1 is linear; >1 grows faster than the data."""
    pts = [(s, t) for s, t in zip(scales, times) if t > 0]
    if len(pts) < 2:
        return None
    b, _ = np.polyfit(np.log([p[0] for p in pts]), np.log([p[1] for p in pts]), 1)
    return round(float(b), 2)


def build_report(rows: list[dict], checks: dict, load_s: dict, env_info: dict) -> tuple[str, dict]:
    df = pd.DataFrame(rows)
    cold = df[df["mode"] == "cold"]
    med = cold.groupby("scale").median(numeric_only=True)
    warm = df[df["mode"] == "warm"].set_index("scale")
    scales = sorted(med.index)
    out = []

    out.append("## Environment\n")
    out.append("| | |\n|---|---|")
    for k, v in env_info.items():
        out.append(f"| {k} | {v} |")

    out.append("\n## 1. Throughput (median of cold runs)\n")
    out.append("| Scale | Source records | Job time (s) | Wall time incl. startup (s) | Records / minute | Event lines / minute |")
    out.append("|---|---|---|---|---|---|")
    for k in scales:
        r = med.loc[k]
        out.append(
            f"| {k}x | {int(r.source_records):,} | {r.job_s:.2f} | {r.wall_s:.2f} | "
            f"{r.source_records / r.job_s * 60:,.0f} | {r['silver.learning_activity.read'] / r['silver.learning_activity.s'] * 60:,.0f} |"
        )

    out.append("\n## 2. Scale: time per step and growth\n")
    out.append("`b` is the fitted exponent in *time ∝ volume^b*: 1.0 = linear, below 1 = fixed overhead dominates, "
               f"above {NONLINEAR_EXPONENT} is flagged as non-linear.\n")
    out.append("| Step | " + " | ".join(f"{k}x (s)" for k in scales) + " | growth b | verdict |")
    out.append("|---|" + "---|" * len(scales) + "---|---|")
    growth = {}
    for name in STEPS + ["job"]:
        col = "job_s" if name == "job" else f"{name}.s"
        times = [float(med.loc[k, col]) for k in scales]
        b = growth_exponent(scales, times)
        growth[name] = b
        verdict = "-" if b is None else ("NON-LINEAR" if b > NONLINEAR_EXPONENT else "linear or better")
        label = "**whole job**" if name == "job" else name
        out.append(f"| {label} | " + " | ".join(f"{t:.3f}" for t in times) + f" | {b} | {verdict} |")

    out.append("\n## 3. Resources (median of cold runs)\n")
    out.append("CPU time includes all threads. Peak memory is the process's maximum resident memory, "
               "recorded after each step (so it only grows).\n")
    out.append("| Scale | Step | Time (s) | CPU (s) | CPU / time | Peak memory (MB) |")
    out.append("|---|---|---|---|---|---|")
    for k in scales:
        for name in STEPS:
            t, c, rss = med.loc[k, f"{name}.s"], med.loc[k, f"{name}.cpu_s"], med.loc[k, f"{name}.rss_mb"]
            out.append(f"| {k}x | {name} | {t:.3f} | {c:.3f} | {c / t if t else 0:.0%} | {rss:.0f} |")
        out.append(f"| {k}x | **whole job** | {med.loc[k, 'job_s']:.3f} | {med.loc[k, 'cpu_s']:.3f} | "
                   f"{med.loc[k, 'cpu_s'] / med.loc[k, 'job_s']:.0%} | **{med.loc[k, 'peak_rss_mb']:.0f}** |")

    out.append("\n## 4. Incremental rerun (warm) vs full load (cold)\n")
    out.append("A warm run reuses the lake: Bronze skips event files it already has; Silver and Gold are rebuilt.\n")
    out.append("| Scale | Cold job (s) | Warm job (s) | Bronze events cold (s) | Bronze events warm (s) |")
    out.append("|---|---|---|---|---|")
    for k in scales:
        if k in warm.index:
            out.append(f"| {k}x | {med.loc[k, 'job_s']:.2f} | {warm.loc[k, 'job_s']:.2f} | "
                       f"{med.loc[k, 'bronze.learning_activity.s']:.3f} | {warm.loc[k, 'bronze.learning_activity.s']:.3f} |")

    out.append("\n## 5. Run-to-run variation (cold job time, s)\n")
    out.append("| Scale | Runs | Min | Median | Max |")
    out.append("|---|---|---|---|---|")
    for k in scales:
        t = cold[cold.scale == k]["job_s"]
        out.append(f"| {k}x | {len(t)} | {t.min():.2f} | {t.median():.2f} | {t.max():.2f} |")

    out.append("\n## 6. Correctness at scale\n")
    out.append("Each count must be exactly k × the 1x value; ratios must be unchanged.\n")
    out.append("| Check | " + " | ".join(f"{k}x" for k in scales) + " | Result |")
    out.append("|---|" + "---|" * len(scales) + "---|")
    all_ok = True
    base = checks.get(1)
    for name in (base or {}):
        vals = [checks[k][name] for k in scales]
        if isinstance(base[name], dict):
            ok = all(checks[k][name] == base[name] for k in scales)
            cells = ["same" if checks[k][name] == base[name] else str(checks[k][name]) for k in scales]
            cells[0] = ", ".join(f"{v:.0%}" for v in base[name].values())
        else:
            ok = all(checks[k][name] == base[name] * k for k in scales)
            cells = [f"{v:,}" for v in vals]
        all_ok &= ok
        out.append(f"| {name} | " + " | ".join(cells) + f" | {'PASS' if ok else 'FAIL'} |")

    out.append("\n## 7. SLA\n")
    out.append("Assumed service level for a daily batch (the brief sets none): **≤ 60 s at today's volume (1x)** "
               "and **≤ 300 s at 10x**. Measured as wall time including Python startup, worst of the cold runs.\n")
    out.append("| Scale | Target (s) | Worst cold wall time (s) | Headroom | Result |")
    out.append("|---|---|---|---|---|")
    sla = {}
    for k, target in SLA_SECONDS.items():
        if k in scales:
            worst = float(cold[cold.scale == k]["wall_s"].max())
            sla[k] = worst <= target
            out.append(f"| {k}x | {target:.0f} | {worst:.2f} | {target / worst:.0f}x | {'PASS' if sla[k] else 'FAIL'} |")

    out.append("\n## Data load (setup, not part of the job)\n")
    out.append("| Scale | PostgreSQL load (s) | Event files written |")
    out.append("|---|---|---|")
    for k in scales:
        out.append(f"| {k}x | {load_s[k]['db_s']:.2f} | {load_s[k]['event_files']:,} files, {load_s[k]['event_lines']:,} lines |")

    verdicts = {
        "correctness_at_scale": all_ok,
        "sla": sla,
        "nonlinear_steps": [s for s, b in growth.items() if b is not None and b > NONLINEAR_EXPONENT],
        "growth": growth,
    }
    return "\n".join(out) + "\n", verdicts


def plot(rows: list[dict], path: Path) -> None:
    """Stacked time per step at each scale, plus throughput (validated palette)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    surface, ink2, axis, grid = "#fcfcfb", "#52514e", "#c3c2b7", "#e1e0d9"
    colors = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]  # categorical slots 1-5, adjacent-safe
    df = pd.DataFrame(rows)
    med = df[df["mode"] == "cold"].groupby("scale").median(numeric_only=True)
    scales = list(med.index)
    plt.rcParams.update({"figure.facecolor": surface, "axes.facecolor": surface, "axes.edgecolor": axis,
                         "axes.spines.top": False, "axes.spines.right": False, "xtick.color": axis,
                         "ytick.color": axis, "xtick.labelcolor": ink2, "ytick.labelcolor": ink2,
                         "axes.labelcolor": ink2, "grid.color": grid, "font.size": 10})
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2), constrained_layout=True)
    x = np.arange(len(scales))
    bottom = np.zeros(len(scales))
    for name, color in zip(STEPS, colors):
        vals = med[f"{name}.s"].to_numpy()
        a1.bar(x, vals, bottom=bottom, width=0.5, color=color, edgecolor=surface, linewidth=2, label=name)
        bottom += vals
    for xi, total in zip(x, bottom):
        a1.text(xi, total, f"{total:.1f} s", ha="center", va="bottom", fontsize=9, color=ink2)
    a1.set_xticks(x, [f"{k}x\n({int(med.loc[k, 'source_records']):,} records)" for k in scales])
    a1.set_ylabel("Seconds (median of cold runs)")
    a1.grid(axis="y"); a1.set_axisbelow(True)
    a1.legend(loc="upper left", frameon=False, fontsize=8)
    a1.set_title("Where the time goes, by step", loc="left", fontweight="bold")

    thr = (med["source_records"] / med["job_s"] * 60).to_numpy()
    a2.bar(x, thr / 1000, width=0.5, color=colors[0])
    for xi, v in zip(x, thr):
        a2.text(xi, v / 1000, f"{v / 1000:,.0f}K", ha="center", va="bottom", fontsize=9, color=ink2)
    a2.set_xticks(x, [f"{k}x" for k in scales])
    a2.set_ylabel("Thousand records per minute")
    a2.grid(axis="y"); a2.set_axisbelow(True)
    a2.set_title("Throughput", loc="left", fontweight="bold")
    fig.savefig(path, dpi=110)
    plt.close(fig)


# --------------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scales", default="1,5,10", help="comma-separated replica counts (must include 1)")
    parser.add_argument("--repeats", type=int, default=3, help="cold runs per scale")
    parser.add_argument("--keep", action="store_true", help="keep perf databases and work files afterwards")
    args = parser.parse_args()
    scales = sorted({int(s) for s in args.scales.split(",")})
    if 1 not in scales:
        parser.error("--scales must include 1 (the baseline for correctness checks)")

    rows, checks, load_s = [], {}, {}
    try:
        for k in scales:
            db = f"talent_perf_x{k}"
            events = WORK_DIR / f"events_x{k}"
            lake = WORK_DIR / f"lake_x{k}"
            print(f"[{k}x] preparing data ...", flush=True)
            db_s = load_database(db, scale_tables(k))
            lines = scale_events(k, events)
            load_s[k] = {"db_s": db_s, "event_files": sum(1 for _ in events.rglob("*.jsonl")), "event_lines": lines}
            for rep in range(1, args.repeats + 1):
                wall, m = run_job(db, events, lake, fresh=True)
                rows.append(summarise(k, "cold", rep, wall, m))
                checks.setdefault(k, correctness_counts(m))
                print(f"[{k}x] cold run {rep}: job {m['duration_s']:.2f}s, wall {wall:.2f}s, "
                      f"peak {m['peak_rss_mb']:.0f} MB", flush=True)
            wall, m = run_job(db, events, lake, fresh=False)
            rows.append(summarise(k, "warm", 1, wall, m))
            print(f"[{k}x] warm run: job {m['duration_s']:.2f}s", flush=True)
    finally:
        if not args.keep:
            for k in scales:
                with psycopg.connect(**pg_params("postgres"), autocommit=True) as admin:
                    admin.execute(f'DROP DATABASE IF EXISTS "talent_perf_x{k}"')
            shutil.rmtree(WORK_DIR, ignore_errors=True)

    env_info = {
        "Date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "Machine": f"{platform.system()} {platform.machine()}, {os.cpu_count()} CPUs",
        "Python / pandas / pyarrow": f"{platform.python_version()} / {pd.__version__} / "
                                     f"{__import__('pyarrow').__version__}",
        "Storage": "local disk (LAKE_ROOT = local folder)",
        "Runs": f"{args.repeats} cold per scale (fresh lake) + 1 warm (same lake)",
    }
    report, verdicts = build_report(rows, checks, load_s, env_info)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "perf_results.json").write_text(
        json.dumps({"environment": env_info, "verdicts": verdicts, "runs": rows, "checks": checks,
                    "load": load_s}, indent=2, default=str)
    )
    header = ("# Performance test results\n\nGenerated by `perf/run_perf.py`. "
              "See [scaling.png](scaling.png) for the chart.\n\n")
    (RESULTS_DIR / "PERFORMANCE.md").write_text(header + report)
    plot(rows, RESULTS_DIR / "scaling.png")
    print(report)
    print("VERDICTS:", json.dumps(verdicts, default=str))
    ok = verdicts["correctness_at_scale"] and all(verdicts["sla"].values())
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
