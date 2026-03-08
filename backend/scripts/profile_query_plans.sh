#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_PATH="${1:-$ROOT_DIR/output/qa/query_plan_profile.txt}"
CITY="${CITY:-delhi}"
WARD_TABLE="${WARD_TABLE:-${CITY}_wards_normalized}"

mkdir -p "$(dirname "$OUT_PATH")"

psql -d urbanmor -v city="$CITY" -v ward_table="$WARD_TABLE" -f "$ROOT_DIR/backend/scripts/profile_query_plans.sql" >"$OUT_PATH"

echo "Query plan output written to: $OUT_PATH"
