\set ON_ERROR_STOP on
\timing on

-- Override with: psql -v city='delhi' -v ward_table='delhi_wards_normalized' -f ...
\if :{?city}
\else
\set city 'delhi'
\endif

\if :{?ward_table}
\else
\set ward_table 'delhi_wards_normalized'
\endif

\echo '=== 1) Cached ward lookup path ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
  city,
  ward_id,
  ward_uid,
  ward_name,
  vintage_year,
  metrics_json,
  quality_summary,
  computed_at
FROM metrics.ward_cache
WHERE city = :'city'
  AND ward_id = (SELECT ward_id::text FROM boundaries.:"ward_table" ORDER BY ward_id::text LIMIT 1)
ORDER BY vintage_year DESC, computed_at DESC
LIMIT 1;

\echo '=== 2) City aggregate path (latest per ward) ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH latest AS (
  SELECT DISTINCT ON (ward_id)
    ward_id,
    metrics_json
  FROM metrics.ward_cache
  WHERE city = :'city'
  ORDER BY ward_id, vintage_year DESC, computed_at DESC
),
src AS (
  SELECT metrics_json->'all_metrics' AS all_metrics
  FROM latest
),
flat AS (
  SELECT
    kv.key AS metric_id,
    (kv.value::text)::double precision AS value
  FROM src
  CROSS JOIN LATERAL jsonb_each(src.all_metrics) kv
  WHERE jsonb_typeof(kv.value)='number'
)
SELECT
  metric_id,
  AVG(value)::double precision AS avg_value,
  MIN(value)::double precision AS min_value,
  MAX(value)::double precision AS max_value,
  COUNT(*)::int AS sample_count
FROM flat
GROUP BY metric_id
ORDER BY metric_id;

\echo '=== 3) Family-level compute plans (sample ward geom) ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT metrics.analyse_roads(
  :'city',
  (SELECT geom FROM boundaries.:"ward_table" LIMIT 1)
);

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT metrics.analyse_buildings(
  :'city',
  (SELECT geom FROM boundaries.:"ward_table" LIMIT 1)
);

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT metrics.analyse_landuse(
  :'city',
  (SELECT geom FROM boundaries.:"ward_table" LIMIT 1)
);

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT metrics.analyse_topography(
  :'city',
  (SELECT geom FROM boundaries.:"ward_table" LIMIT 1)
);

\echo '=== 4) Custom polygon cache function: first + second call ==='
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH sample AS (
  SELECT ST_Transform(
    ST_Envelope(
      ST_Buffer(ST_Transform(ST_Centroid(geom), 3857), 250)
    ),
    4326
  ) AS geom
  FROM boundaries.:"ward_table"
  LIMIT 1
)
SELECT *
FROM metrics.get_or_compute_custom_cache(
  :'city',
  (SELECT geom FROM sample),
  EXTRACT(YEAR FROM CURRENT_DATE)::int
);

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH sample AS (
  SELECT ST_Transform(
    ST_Envelope(
      ST_Buffer(ST_Transform(ST_Centroid(geom), 3857), 250)
    ),
    4326
  ) AS geom
  FROM boundaries.:"ward_table"
  LIMIT 1
)
SELECT *
FROM metrics.get_or_compute_custom_cache(
  :'city',
  (SELECT geom FROM sample),
  EXTRACT(YEAR FROM CURRENT_DATE)::int
);
