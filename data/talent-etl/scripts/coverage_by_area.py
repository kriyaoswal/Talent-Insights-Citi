"""Coverage of ``job.py`` per area of the data engineer guide's coverage goals.

Reads a coverage JSON report (``coverage json``) and maps each top-level
function or class of ``job.py`` to one of the guide's areas. Prints a
Markdown table and exits 1 if an area is below its target.

    python scripts/coverage_by_area.py <coverage.json> [job.py]
"""

import ast
import json
import sys
from pathlib import Path

AREAS: dict[str, tuple[int, list[str]]] = {
    "Core transformation functions": (80, [
        "seed_source_database", "bronze_table_path", "ingest_postgres_to_bronze", "ingest_events_to_bronze",
        "transform_tables_to_silver", "validate_table", "add_quality_flags", "normalize_event", "build_day_bounds",
        "resolve_event_date", "clean_events", "transform_events_to_silver", "partition_date", "parse_event_ts",
        "event_number",
    ]),
    "Schema and validation rules": (90, [
        "_clean", "_iso_date", "_not_after_run_date", "_number_only", "references", "error_reasons",
        "validate_record", "check_columns", "Team", "Competency", "Employee", "Project", "PerformanceReview",
        "CompetencyAssessment", "TalentReview", "LearningEvent", "validate_gold_table", "Config",
    ]),
    "Error-handling and retry paths": (85, [
        "with_retry", "write_parquet", "read_parquet", "replace_dir", "read_bytes", "write_json", "pg_connect",
        "write_rows", "read_parquet_dataset", "run_step", "main",
    ]),
    "Critical business metric calculations": (95, [
        "build_review_history", "build_performance_summary", "build_skill_gaps", "build_employee_learning",
        "build_course_completions", "build_promotion_candidates", "build_attrition_watchlist",
        "build_team_performance", "build_project_performance", "build_skill_distribution",
        "build_competency_summary", "reconcile_gold", "gold_highlights", "PerformanceSummaryRow", "SkillGapRow",
        "PromotionCandidateRow", "EmployeeLearningRow", "CourseCompletionRow", "AttritionWatchRow",
        "TeamPerformanceRow", "ProjectPerformanceRow", "CompetencySummaryRow", "_expected_trend",
        "_expected_promotion_tier", "_expected_risk_tier",
    ]),
}  # fmt: skip


def main() -> int:
    report = json.loads(Path(sys.argv[1]).read_text())
    source = Path(sys.argv[2] if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent / "job.py")
    lines = next(v for k, v in report["files"].items() if k.endswith("job.py"))
    executed, missing = set(lines["executed_lines"]), set(lines["missing_lines"])
    statements = executed | missing

    tree = ast.parse(source.read_text())
    spans = {
        node.name: set(range(node.lineno, node.end_lineno + 1))
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef))
    }

    print("| Guide's goal | Target | Statements | Coverage | Result |")
    print("|---|---|---|---|---|")
    ok = True
    for area, (target, members) in AREAS.items():
        unknown = [m for m in members if m not in spans]
        if unknown:
            raise SystemExit(f"{area}: not found in {source.name}: {unknown}")
        area_statements = set().union(*(spans[m] for m in members)) & statements
        pct = 100 * len(area_statements & executed) / len(area_statements)
        ok &= pct >= target
        print(f"| {area} | {target}%+ | {len(area_statements)} | {pct:.1f}% | {'PASS' if pct >= target else 'BELOW TARGET'} |")
    print(f"| Whole file | - | {len(statements)} | {100 * len(executed) / len(statements):.1f}% | - |")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
