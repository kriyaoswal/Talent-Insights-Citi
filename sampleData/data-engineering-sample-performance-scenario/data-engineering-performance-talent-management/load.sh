#!/usr/bin/env bash
# Loads the data-eng-001 Performance & Talent Management CSVs into
# PostgreSQL using the workshop's standard connection env vars (see
# docs/data-engineer.md -> "Database Environment Variables"): IS_LOCAL,
# POSTGRES_HOST, POSTGRES_PORT, POSTGRES_NAME, POSTGRES_USER, POSTGRES_PASS.
set -euo pipefail
cd "$(dirname "$0")"

SSLMODE=""
if [ "${IS_LOCAL:-true}" != "true" ]; then
    SSLMODE="sslmode=require"
fi

CONN="host=${POSTGRES_HOST:-localhost} port=${POSTGRES_PORT:-5432} dbname=${POSTGRES_NAME} user=${POSTGRES_USER} password=${POSTGRES_PASS} ${SSLMODE}"

psql "$CONN" -v ON_ERROR_STOP=1 -f schema.sql

for table in teams competencies employees projects performance_reviews competency_assessments talent_review; do
    echo "Loading ${table}.csv ..."
    psql "$CONN" -v ON_ERROR_STOP=1 -c "\copy ${table} FROM '${table}.csv' WITH (FORMAT csv, HEADER true)"
done

echo "Done."
