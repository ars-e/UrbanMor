#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/ars-e/projects/Morph"
LOG="$ROOT/output/qa/ward_cache_refresh.log"
mkdir -p "$ROOT/output/qa"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] start ward cache refresh" >> "$LOG"
for c in ahmedabad bengaluru chandigarh chennai delhi kolkata mumbai; do
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] city=$c begin" >> "$LOG"
  /usr/bin/time -p psql -d urbanmor -Atc "SELECT city, wards_seen, inserted_rows, updated_rows FROM metrics.refresh_ward_cache(EXTRACT(YEAR FROM CURRENT_DATE)::int, '$c');" >> "$LOG" 2>&1
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] city=$c end" >> "$LOG"
done

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] done ward cache refresh" >> "$LOG"
