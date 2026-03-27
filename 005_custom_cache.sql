\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f 005_custom_cache.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f 005_custom_cache.sql

\if :{?db_name}
\connect :db_name
\endif

SET search_path = public, metrics;

CREATE SCHEMA IF NOT EXISTS metrics;

DO $$
BEGIN
  IF to_regprocedure('metrics.analyse_polygon(text,geometry)') IS NULL THEN
    RAISE EXCEPTION 'metrics.analyse_polygon(text,geometry) is required before custom cache setup';
  END IF;

  IF to_regprocedure('metrics._metric_quality_summary(jsonb)') IS NULL THEN
    RAISE EXCEPTION 'metrics._metric_quality_summary(jsonb) is required; run 004_ward_cache.sql first';
  END IF;

  IF to_regprocedure('metrics._city_input_signature(text)') IS NULL THEN
    RAISE EXCEPTION 'metrics._city_input_signature(text) is required; run 004_ward_cache.sql first';
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION metrics._normalized_geom_hash(p_geom geometry)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_geom geometry(MultiPolygon, 4326);
BEGIN
  v_geom := metrics._normalize_polygon_geom(p_geom);
  IF v_geom IS NULL THEN
    RETURN NULL;
  END IF;

  RETURN md5(encode(ST_AsBinary(ST_Normalize(v_geom)), 'hex'));
END;
$$;

CREATE TABLE IF NOT EXISTS metrics.custom_geometry_cache (
  city text NOT NULL,
  geom_hash text NOT NULL,
  vintage_year integer NOT NULL,
  geom geometry(MultiPolygon, 4326) NOT NULL,
  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  quality_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  input_signature timestamptz,
  computed_at timestamptz NOT NULL DEFAULT now(),
  last_accessed_at timestamptz NOT NULL DEFAULT now(),
  hit_count bigint NOT NULL DEFAULT 0,
  CONSTRAINT custom_geometry_cache_pk PRIMARY KEY (city, geom_hash, vintage_year),
  CONSTRAINT custom_geometry_cache_city_not_blank_chk CHECK (btrim(city) <> ''),
  CONSTRAINT custom_geometry_cache_geom_hash_not_blank_chk CHECK (btrim(geom_hash) <> ''),
  CONSTRAINT custom_geometry_cache_vintage_year_chk CHECK (vintage_year BETWEEN 1900 AND 2100),
  CONSTRAINT custom_geometry_cache_hit_count_chk CHECK (hit_count >= 0)
);

ALTER TABLE IF EXISTS metrics.custom_geometry_cache
  ADD COLUMN IF NOT EXISTS input_signature timestamptz;

CREATE INDEX IF NOT EXISTS idx_custom_cache_city_vintage
  ON metrics.custom_geometry_cache (city, vintage_year, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_custom_cache_last_accessed
  ON metrics.custom_geometry_cache (last_accessed_at DESC);

CREATE INDEX IF NOT EXISTS idx_custom_cache_input_signature
  ON metrics.custom_geometry_cache (city, input_signature DESC, computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_custom_cache_geom_gist
  ON metrics.custom_geometry_cache
  USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_custom_cache_metrics_json_gin
  ON metrics.custom_geometry_cache
  USING GIN (metrics_json jsonb_path_ops);

CREATE OR REPLACE FUNCTION metrics.get_or_compute_custom_cache(
  p_city text,
  p_geom geometry,
  p_vintage_year integer DEFAULT EXTRACT(YEAR FROM CURRENT_DATE)::integer
)
RETURNS TABLE (
  city text,
  geom_hash text,
  vintage_year integer,
  cache_hit boolean,
  metrics_json jsonb,
  quality_summary jsonb,
  computed_at timestamptz
)
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  v_city text;
  v_geom geometry(MultiPolygon, 4326);
  v_hash text;
  v_input_signature timestamptz;
  v_cached_signature timestamptz;
  v_metrics jsonb;
  v_quality jsonb;
  v_computed_at timestamptz;
BEGIN
  IF p_vintage_year < 1900 OR p_vintage_year > 2100 THEN
    RAISE EXCEPTION 'p_vintage_year out of range (1900-2100): %', p_vintage_year;
  END IF;

  v_city := metrics._normalize_city(p_city);
  v_geom := metrics._normalize_polygon_geom(p_geom);

  IF v_geom IS NULL THEN
    RAISE EXCEPTION 'Invalid polygon geometry for custom cache';
  END IF;

  v_hash := metrics._normalized_geom_hash(v_geom);
  v_input_signature := metrics._city_input_signature(v_city);

  SELECT
    c.metrics_json,
    c.quality_summary,
    c.input_signature,
    c.computed_at
  INTO
    v_metrics,
    v_quality,
    v_cached_signature,
    v_computed_at
  FROM metrics.custom_geometry_cache c
  WHERE c.city = v_city
    AND c.geom_hash = v_hash
    AND c.vintage_year = p_vintage_year;

  IF FOUND
    AND (
      v_input_signature IS NULL
      OR (v_cached_signature IS NOT NULL AND v_cached_signature >= v_input_signature)
    ) THEN
    UPDATE metrics.custom_geometry_cache c
    SET last_accessed_at = now(),
        hit_count = c.hit_count + 1
    WHERE c.city = v_city
      AND c.geom_hash = v_hash
      AND c.vintage_year = p_vintage_year;

    RETURN QUERY
    SELECT
      v_city,
      v_hash,
      p_vintage_year,
      true,
      v_metrics,
      v_quality,
      v_computed_at;
    RETURN;
  END IF;

  v_metrics := metrics.analyse_polygon(v_city, v_geom);
  v_quality := metrics._metric_quality_summary(v_metrics);

  INSERT INTO metrics.custom_geometry_cache (
    city,
    geom_hash,
    vintage_year,
    geom,
    metrics_json,
    quality_summary,
    input_signature,
    computed_at,
    last_accessed_at,
    hit_count
  )
  VALUES (
    v_city,
    v_hash,
    p_vintage_year,
    v_geom,
    v_metrics,
    v_quality,
    v_input_signature,
    now(),
    now(),
    0
  )
  ON CONFLICT ON CONSTRAINT custom_geometry_cache_pk
  DO UPDATE
    SET geom = EXCLUDED.geom,
        metrics_json = EXCLUDED.metrics_json,
        quality_summary = EXCLUDED.quality_summary,
        input_signature = EXCLUDED.input_signature,
        computed_at = EXCLUDED.computed_at,
        last_accessed_at = now(),
        hit_count = metrics.custom_geometry_cache.hit_count + 1
  RETURNING
    metrics.custom_geometry_cache.computed_at,
    metrics.custom_geometry_cache.metrics_json,
    metrics.custom_geometry_cache.quality_summary
  INTO
    v_computed_at,
    v_metrics,
    v_quality;

  RETURN QUERY
  SELECT
    v_city,
    v_hash,
    p_vintage_year,
    false,
    v_metrics,
    v_quality,
    v_computed_at;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.purge_custom_geometry_cache(
  p_city text DEFAULT NULL,
  p_vintage_year integer DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  v_city text;
  v_deleted integer;
BEGIN
  IF p_city IS NOT NULL AND btrim(p_city) <> '' THEN
    v_city := metrics._normalize_city(p_city);
  END IF;

  DELETE FROM metrics.custom_geometry_cache c
  WHERE (v_city IS NULL OR c.city = v_city)
    AND (p_vintage_year IS NULL OR c.vintage_year = p_vintage_year);

  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END;
$$;
