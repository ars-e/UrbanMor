#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
REPORT_PATH="${1:-$ROOT_DIR/output/qa/response_time_targets.md}"
BASE_URL="${BASE_URL:-http://127.0.0.1:18000}"
RUNS="${RUNS:-12}"

mkdir -p "$(dirname "$REPORT_PATH")"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"; if [[ -n "${SERVER_PID:-}" ]]; then kill "$SERVER_PID" 2>/dev/null || true; fi' EXIT

if ! curl -sf "$BASE_URL/health" >/dev/null 2>&1; then
  (
    cd "$BACKEND_DIR"
    uvicorn app.main:app --host 127.0.0.1 --port 18000 >"$TMP_DIR/uvicorn.log" 2>&1 &
    echo $! >"$TMP_DIR/server.pid"
  )
  SERVER_PID="$(cat "$TMP_DIR/server.pid")"

  for _ in $(seq 1 80); do
    if curl -sf "$BASE_URL/health" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
fi

CITY="$(psql -d urbanmor -Atc "SELECT city FROM metrics.ward_cache ORDER BY city LIMIT 1;")"
WARD_ID="$(psql -d urbanmor -Atc "SELECT ward_id FROM metrics.ward_cache WHERE city='${CITY}' ORDER BY ward_id LIMIT 1;")"

GEOM_JSON="$(psql -d urbanmor -Atc "WITH sample AS (
  SELECT ST_Transform(
    ST_Envelope(ST_Buffer(ST_Transform(ST_Centroid(geom), 3857), 180)),
    4326
  ) AS geom
  FROM boundaries.${CITY}_wards_normalized
  LIMIT 1
)
SELECT ST_AsGeoJSON(
  ST_Translate(geom, (random() - 0.5) * 0.0006, (random() - 0.5) * 0.0006)
)
FROM sample;")"

PAYLOAD_FILE="$TMP_DIR/custom_payload.json"
cat >"$PAYLOAD_FILE" <<JSON
{"mode":"custom_polygon","city":"$CITY","geometry":$GEOM_JSON,"run_async":false}
JSON

TIMINGS_WARD="$TMP_DIR/ward_timings.txt"
for _ in $(seq 1 "$RUNS"); do
  curl -s -o /dev/null -w '%{time_total}\n' "$BASE_URL/cities/$CITY/wards/$WARD_ID" >>"$TIMINGS_WARD"
done

FIRST_HIT_TIME="$(curl -s -o /dev/null -w '%{time_total}' -X POST "$BASE_URL/analyse" -H 'Content-Type: application/json' --data @"$PAYLOAD_FILE")"

TIMINGS_REPEAT="$TMP_DIR/repeat_timings.txt"
for _ in $(seq 1 "$RUNS"); do
  curl -s -o /dev/null -w '%{time_total}\n' -X POST "$BASE_URL/analyse" -H 'Content-Type: application/json' --data @"$PAYLOAD_FILE" >>"$TIMINGS_REPEAT"
done

python3 - <<'PY' "$TIMINGS_WARD" "$TIMINGS_REPEAT" "$FIRST_HIT_TIME" "$REPORT_PATH" "$CITY" "$WARD_ID"
import pathlib
import statistics
import sys

ward_path = pathlib.Path(sys.argv[1])
repeat_path = pathlib.Path(sys.argv[2])
first_hit = float(sys.argv[3])
report_path = pathlib.Path(sys.argv[4])
city = sys.argv[5]
ward = sys.argv[6]


def read_values(path: pathlib.Path):
    values = [float(line.strip()) for line in path.read_text().splitlines() if line.strip()]
    return values


def p95(values):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    idx = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
    return ordered[idx]

ward_vals = read_values(ward_path)
repeat_vals = read_values(repeat_path)

ward_avg = statistics.mean(ward_vals)
ward_p95 = p95(ward_vals)
repeat_avg = statistics.mean(repeat_vals)
repeat_p95 = p95(repeat_vals)

ward_pass = ward_p95 < 0.5
first_hit_pass = first_hit <= 9.0
repeat_pass = repeat_p95 < 0.5

lines = [
    "# Response-Time Profiling",
    "",
    f"City sample: `{city}`",
    f"Ward sample: `{ward}`",
    "",
    "## Targets",
    "",
    "- Ward cached request: `< 500 ms`",
    "- Custom polygon first hit: `<= 9 s`",
    "- Repeated custom polygon: `< 500 ms`",
    "",
    "## Measured",
    "",
    f"- Ward cached avg: `{ward_avg*1000:.1f} ms`, p95: `{ward_p95*1000:.1f} ms` ({'PASS' if ward_pass else 'FAIL'})",
    f"- Custom polygon first hit: `{first_hit:.3f} s` ({'PASS' if first_hit_pass else 'FAIL'})",
    f"- Custom polygon repeated avg: `{repeat_avg*1000:.1f} ms`, p95: `{repeat_p95*1000:.1f} ms` ({'PASS' if repeat_pass else 'FAIL'})",
    "",
    "## Notes",
    "",
    "- First hit uses synchronous `POST /analyse` with `run_async=false`.",
    "- Repeated hit uses identical geometry to trigger cache hit behavior.",
]

report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("Wrote", report_path)
PY

echo "Report written to: $REPORT_PATH"
