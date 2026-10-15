#!/usr/bin/env bash
# Runs the full test suite and saves its raw output as evidence in tests/results/.
#
# Produces:
#   tests/results/pytest_report.txt     every test by name with pass/fail, plus line coverage
#   tests/results/junit.xml             the same results, machine-readable
#   tests/results/coverage_by_area.md   coverage per area of the guide's coverage goals
#
# Needs the local PostgreSQL loaded (scripts/load_source_db.sh) and the
# "talent-etl" Jupyter kernel; tests that need them are skipped otherwise.
# AWS credentials are removed for the run: cloud tests use a local S3 (moto).
set -euo pipefail

JOB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="${JOB_DIR}/tests/results"
PY="${JOB_DIR}/.venv/bin/python"
mkdir -p "${RESULTS}"
cd "${JOB_DIR}"

status=0
{
  echo "# talent-etl test run: $(date -u '+%Y-%m-%d %H:%M UTC'), $(${PY} --version), commit $(git rev-parse --short HEAD)"
  env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN -u AWS_ENDPOINT_URL \
    "${PY}" -m pytest tests -v -p no:cacheprovider \
      --cov=job --cov-report=term-missing --cov-report="json:${RESULTS}/.coverage.json" \
      --junitxml="${RESULTS}/junit.xml"
} 2>&1 | tee "${RESULTS}/pytest_report.txt" || status=$?

"${PY}" scripts/coverage_by_area.py "${RESULTS}/.coverage.json" | tee "${RESULTS}/coverage_by_area.md" || status=$?
rm -f "${RESULTS}/.coverage.json" .coverage
exit "${status}"
