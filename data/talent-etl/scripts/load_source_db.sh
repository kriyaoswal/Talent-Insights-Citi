#!/usr/bin/env bash
# Loads the Performance & Talent Management sample CSVs into PostgreSQL.
#
# Re-runnable wrapper around the provided sampleData schema.sql + CSVs: it
# creates the database if missing and drops the source tables before
# recreating them, so every run ends in the same state. The provided files
# are used as-is and never modified.
#
# Uses the workshop's standard connection env vars (see docs/data-engineer.md):
# IS_LOCAL, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_NAME, POSTGRES_USER,
# POSTGRES_PASS.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SOURCE_DIR="${SOURCE_DIR:-${REPO_ROOT}/sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management}"

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_NAME="${POSTGRES_NAME:-talent_mgmt}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
export PGPASSWORD="${POSTGRES_PASS:-postgres123}"
export PGOPTIONS="-c client_min_messages=warning"

SSLMODE=""
if [ "${IS_LOCAL:-true}" != "true" ]; then
    SSLMODE="sslmode=require"
fi

# Load order matters because of foreign keys (parents first).
TABLES=(teams competencies employees projects performance_reviews competency_assessments talent_review)

conn() {
    echo "host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=$1 user=${POSTGRES_USER} ${SSLMODE}"
}

if [ ! -f "${SOURCE_DIR}/schema.sql" ]; then
    echo "ERROR: schema.sql not found in ${SOURCE_DIR}" >&2
    exit 1
fi

echo "Target: ${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_NAME}"

if [ "$(psql "$(conn postgres)" -tAc "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_NAME}'")" != "1" ]; then
    echo "Creating database ${POSTGRES_NAME} ..."
    psql "$(conn postgres)" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${POSTGRES_NAME}\""
fi

DB="$(conn "${POSTGRES_NAME}")"

echo "Dropping existing source tables ..."
for (( i=${#TABLES[@]}-1; i>=0; i-- )); do
    psql "$DB" -q -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS ${TABLES[$i]} CASCADE"
done

echo "Creating schema ..."
psql "$DB" -q -v ON_ERROR_STOP=1 -f "${SOURCE_DIR}/schema.sql"

cd "${SOURCE_DIR}"
for table in "${TABLES[@]}"; do
    psql "$DB" -q -v ON_ERROR_STOP=1 -c "\copy ${table} FROM '${table}.csv' WITH (FORMAT csv, HEADER true)"
    rows="$(psql "$DB" -tAc "SELECT count(*) FROM ${table}")"
    csv_rows="$(( $(wc -l < "${table}.csv") - 1 ))"
    if [ "$rows" != "$csv_rows" ]; then
        echo "ERROR: ${table}: loaded ${rows} rows but CSV has ${csv_rows}" >&2
        exit 1
    fi
    printf "  %-24s %6s rows (matches CSV)\n" "${table}" "${rows}"
done

echo "Done."
