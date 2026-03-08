-- sql/000_metric_helpers.sql
-- Canonical shared helpers for all metric family SQL files.
-- Run this before any metric SQL file.
-- Individual metric files also define these (idempotent CREATE OR REPLACE) for standalone execution.

SET search_path = public, metrics;

CREATE SCHEMA IF NOT EXISTS metrics;

CREATE OR REPLACE FUNCTION metrics._normalize_city(p_city TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_city TEXT;
BEGIN
  v_city := lower(trim(COALESCE(p_city, '')));
  v_city := regexp_replace(v_city, '[^a-z0-9_]+', '', 'g');
  IF v_city = '' THEN
    RAISE EXCEPTION 'Invalid city identifier: %', p_city;
  END IF;
  RETURN v_city;
END;
$$;

CREATE OR REPLACE FUNCTION metrics._to_4326(p_geom geometry)
RETURNS geometry
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT
    CASE
      WHEN p_geom IS NULL THEN NULL
      WHEN ST_SRID(p_geom) = 4326 THEN p_geom
      WHEN ST_SRID(p_geom) = 0 THEN ST_SetSRID(p_geom, 4326)
      ELSE ST_Transform(p_geom, 4326)
    END;
$$;

CREATE OR REPLACE FUNCTION metrics._to_3857(p_geom geometry)
RETURNS geometry
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT
    CASE
      WHEN p_geom IS NULL THEN NULL
      WHEN ST_SRID(p_geom) = 3857 THEN p_geom
      WHEN ST_SRID(p_geom) = 0 THEN ST_Transform(ST_SetSRID(p_geom, 4326), 3857)
      ELSE ST_Transform(p_geom, 3857)
    END;
$$;

CREATE OR REPLACE FUNCTION metrics._normalize_polygon_geom(p_geom geometry)
RETURNS geometry(MultiPolygon, 4326)
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_geom geometry;
BEGIN
  IF p_geom IS NULL THEN
    RETURN NULL;
  END IF;

  v_geom := metrics._to_4326(p_geom);
  v_geom := ST_CollectionExtract(ST_MakeValid(v_geom), 3);

  IF v_geom IS NULL OR ST_IsEmpty(v_geom) THEN
    RETURN NULL;
  END IF;

  RETURN ST_Multi(v_geom)::geometry(MultiPolygon, 4326);
END;
$$;

-- Area in square kilometres using the WGS84 spheroid (accurate; avoids Web Mercator distortion).
CREATE OR REPLACE FUNCTION metrics._area_sqkm(p_geom geometry)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_area_m2   double precision;
BEGIN
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;

  v_area_m2 := ST_Area(v_geom_4326::geography);
  IF v_area_m2 <= 0 THEN
    RETURN NULL;
  END IF;

  RETURN v_area_m2 / 1_000_000.0;
END;
$$;
