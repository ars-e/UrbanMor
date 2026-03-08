\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f 004_ward_cache.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f 004_ward_cache.sql

\if :{?db_name}
\connect :db_name
\endif

SET search_path = public, metrics, boundaries, meta;

CREATE SCHEMA IF NOT EXISTS metrics;

-- Compact cache-level quality rollup derived from metrics.analyse_polygon payload.
CREATE OR REPLACE FUNCTION metrics._metric_quality_summary(p_metrics_json jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
WITH all_metrics AS (
  SELECT COALESCE(p_metrics_json -> 'all_metrics', '{}'::jsonb) AS j
),
flat AS (
  SELECT e.key, e.value, jsonb_typeof(e.value) AS vtype
  FROM all_metrics a
  CROSS JOIN LATERAL jsonb_each(a.j) e
),
agg AS (
  SELECT
    COUNT(*)::int AS total_metrics,
    COUNT(*) FILTER (WHERE vtype = 'null')::int AS null_metrics,
    COUNT(*) FILTER (WHERE vtype = 'number')::int AS numeric_metrics,
    COUNT(*) FILTER (WHERE vtype = 'object')::int AS object_metrics,
    COUNT(*) FILTER (WHERE vtype = 'array')::int AS array_metrics,
    COUNT(*) FILTER (WHERE vtype = 'boolean')::int AS boolean_metrics,
    COUNT(*) FILTER (WHERE vtype = 'string')::int AS string_metrics,
    COUNT(*) FILTER (WHERE vtype <> 'null')::int AS computed_non_null_metrics,
    COUNT(*) FILTER (
      WHERE vtype = 'number'
        AND abs((value::text)::numeric) <= 0.000000000001
    )::int AS zero_metrics
  FROM flat
),
families AS (
  SELECT COALESCE(p_metrics_json -> 'families', '{}'::jsonb) AS j
)
SELECT jsonb_build_object(
  'has_error', (p_metrics_json ? 'error'),
  'error_code', p_metrics_json ->> 'error',
  'total_metrics', COALESCE(a.total_metrics, 0),
  'computed_non_null_metrics', COALESCE(a.computed_non_null_metrics, 0),
  'null_metrics', COALESCE(a.null_metrics, 0),
  'numeric_metrics', COALESCE(a.numeric_metrics, 0),
  'object_metrics', COALESCE(a.object_metrics, 0),
  'array_metrics', COALESCE(a.array_metrics, 0),
  'boolean_metrics', COALESCE(a.boolean_metrics, 0),
  'string_metrics', COALESCE(a.string_metrics, 0),
  'zero_metrics', COALESCE(a.zero_metrics, 0),
  'completeness_ratio',
    CASE
      WHEN COALESCE(a.total_metrics, 0) = 0 THEN NULL
      ELSE ROUND((a.computed_non_null_metrics::numeric / a.total_metrics::numeric), 6)
    END,
  'families_present', jsonb_build_object(
    'roads', COALESCE((f.j ? 'roads'), false),
    'buildings', COALESCE((f.j ? 'buildings'), false),
    'landuse', COALESCE((f.j ? 'landuse'), false),
    'topography', COALESCE((f.j ? 'topography'), false),
    'composites', COALESCE((f.j ? 'composites'), false)
  )
)
FROM agg a
CROSS JOIN families f;
$$;

CREATE TABLE IF NOT EXISTS metrics.ward_cache (
  city text NOT NULL,
  ward_id text NOT NULL,
  ward_uid text,
  ward_name text,
  vintage_year integer NOT NULL,
  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  quality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  computed_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ward_cache_pk PRIMARY KEY (city, ward_id, vintage_year),
  CONSTRAINT ward_cache_city_not_blank_chk CHECK (btrim(city) <> ''),
  CONSTRAINT ward_cache_ward_id_not_blank_chk CHECK (btrim(ward_id) <> ''),
  CONSTRAINT ward_cache_vintage_year_chk CHECK (vintage_year BETWEEN 1900 AND 2100)
);

CREATE INDEX IF NOT EXISTS idx_ward_cache_city_vintage
  ON metrics.ward_cache (city, vintage_year, ward_id);

CREATE INDEX IF NOT EXISTS idx_ward_cache_computed_at
  ON metrics.ward_cache (computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_ward_cache_metrics_json_gin
  ON metrics.ward_cache
  USING GIN (metrics_json jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_ward_cache_quality_summary_gin
  ON metrics.ward_cache
  USING GIN (quality_summary jsonb_path_ops);

CREATE OR REPLACE VIEW metrics.ward_cache_latest AS
SELECT DISTINCT ON (city, ward_id)
  city,
  ward_id,
  ward_uid,
  ward_name,
  vintage_year,
  metrics_json,
  quality_summary,
  computed_at
FROM metrics.ward_cache
ORDER BY city, ward_id, vintage_year DESC, computed_at DESC;

-- Recompute + upsert ward cache for all or one city.
CREATE OR REPLACE FUNCTION metrics.refresh_ward_cache(
  p_vintage_year integer DEFAULT EXTRACT(YEAR FROM CURRENT_DATE)::integer,
  p_city_filter text DEFAULT NULL
)
RETURNS TABLE (
  city text,
  wards_seen integer,
  inserted_rows integer,
  updated_rows integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;
  v_city_filter text;
  v_sql text;
BEGIN
  IF p_vintage_year < 1900 OR p_vintage_year > 2100 THEN
    RAISE EXCEPTION 'p_vintage_year out of range (1900-2100): %', p_vintage_year;
  END IF;

  IF p_city_filter IS NOT NULL AND btrim(p_city_filter) <> '' THEN
    v_city_filter := metrics._normalize_city(p_city_filter);
  END IF;

  IF to_regprocedure('metrics.analyse_polygon(text,geometry)') IS NULL THEN
    RAISE EXCEPTION 'metrics.analyse_polygon(text,geometry) is required before refresh_ward_cache';
  END IF;

  FOR r IN
    SELECT
      t.table_name,
      regexp_replace(t.table_name, '_wards_normalized$', '') AS city
    FROM information_schema.tables t
    WHERE t.table_schema = 'boundaries'
      AND t.table_type = 'BASE TABLE'
      AND t.table_name LIKE '%\_wards\_normalized' ESCAPE '\'
      AND t.table_name NOT LIKE '%\_source\_%' ESCAPE '\'
      AND (v_city_filter IS NULL OR regexp_replace(t.table_name, '_wards_normalized$', '') = v_city_filter)
    ORDER BY t.table_name
  LOOP
    v_sql := format($SQL$
      WITH source_wards AS (
        SELECT
          %L::text AS city,
          w.ward_id::text AS ward_id,
          w.ward_uid::text AS ward_uid,
          w.ward_name::text AS ward_name,
          w.geom::geometry AS geom
        FROM boundaries.%I w
        WHERE w.geom IS NOT NULL
          AND NOT ST_IsEmpty(w.geom)
          AND w.ward_id IS NOT NULL
          AND btrim(w.ward_id::text) <> ''
      ),
      computed AS (
        SELECT
          sw.city,
          sw.ward_id,
          sw.ward_uid,
          sw.ward_name,
          metrics.analyse_polygon(sw.city, sw.geom) AS metrics_json
        FROM source_wards sw
      ),
      upserted AS (
        INSERT INTO metrics.ward_cache (
          city,
          ward_id,
          ward_uid,
          ward_name,
          vintage_year,
          metrics_json,
          quality_summary,
          computed_at
        )
        SELECT
          c.city,
          c.ward_id,
          c.ward_uid,
          c.ward_name,
          $1::integer AS vintage_year,
          c.metrics_json,
          metrics._metric_quality_summary(c.metrics_json) AS quality_summary,
          now() AS computed_at
        FROM computed c
        ON CONFLICT (city, ward_id, vintage_year)
        DO UPDATE
          SET ward_uid = EXCLUDED.ward_uid,
              ward_name = EXCLUDED.ward_name,
              metrics_json = EXCLUDED.metrics_json,
              quality_summary = EXCLUDED.quality_summary,
              computed_at = EXCLUDED.computed_at
        RETURNING (xmax = 0) AS inserted
      )
      SELECT
        %L::text AS city,
        (SELECT COUNT(*) FROM source_wards)::int AS wards_seen,
        COALESCE((SELECT COUNT(*) FROM upserted WHERE inserted), 0)::int AS inserted_rows,
        COALESCE((SELECT COUNT(*) FROM upserted WHERE NOT inserted), 0)::int AS updated_rows;
    $SQL$, r.city, r.table_name, r.city);

    RETURN QUERY EXECUTE v_sql USING p_vintage_year;
  END LOOP;
END;
$$;

