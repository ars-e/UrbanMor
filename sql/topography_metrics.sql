-- sql/topography_metrics.sql
-- Topography metric family functions.

SET search_path = public, metrics, dem, green;

CREATE SCHEMA IF NOT EXISTS metrics;

-- ------------------------------------------------------------
-- Shared helpers (idempotent)
-- ------------------------------------------------------------

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

-- ------------------------------------------------------------
-- Raster helpers
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics._raster_summary(
  p_schema text,
  p_table text,
  p_geom geometry
)
RETURNS TABLE(cnt bigint, min_val double precision, max_val double precision, mean_val double precision)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
BEGIN
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN;
  END IF;

  IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN
    RETURN;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Clip(r.rast, ST_Transform($1, ST_SRID(r.rast)), true) AS rast
      FROM %I.%I r
      WHERE ST_Intersects(r.rast, ST_Transform($1, ST_SRID(r.rast)))
    ), s AS (
      SELECT ST_SummaryStatsAgg(rast, 1, true) AS st
      FROM clipped
    )
    SELECT
      COALESCE((st).count, 0)::bigint AS cnt,
      (st).min::double precision AS min_val,
      (st).max::double precision AS max_val,
      (st).mean::double precision AS mean_val
    FROM s
  $SQL$, p_schema, p_table);

  RETURN QUERY EXECUTE v_sql USING v_geom_4326;
END;
$$;

-- Returns % of pixels with value > p_threshold.
CREATE OR REPLACE FUNCTION metrics._raster_pct_above(
  p_schema text,
  p_table text,
  p_geom geometry,
  p_threshold double precision
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_true_cnt double precision;
  v_all_cnt double precision;
BEGIN
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Clip(r.rast, ST_Transform($1, ST_SRID(r.rast)), true) AS rast
      FROM %I.%I r
      WHERE ST_Intersects(r.rast, ST_Transform($1, ST_SRID(r.rast)))
    ), vc AS (
      SELECT
        (x).value::double precision AS v,
        SUM((x).count)::double precision AS cnt
      FROM clipped c
      CROSS JOIN LATERAL ST_ValueCount(c.rast, 1, true) x
      GROUP BY (x).value
    )
    SELECT
      COALESCE(SUM(CASE WHEN v > $2 THEN cnt ELSE 0 END), 0)::double precision,
      COALESCE(SUM(cnt), 0)::double precision
    FROM vc
  $SQL$, p_schema, p_table);

  EXECUTE v_sql INTO v_true_cnt, v_all_cnt USING v_geom_4326, p_threshold;

  IF COALESCE(v_all_cnt, 0.0) <= 0 THEN
    RETURN NULL;
  END IF;

  RETURN (v_true_cnt / v_all_cnt) * 100.0;
END;
$$;

-- Returns % of pixels with value < p_threshold.
CREATE OR REPLACE FUNCTION metrics._raster_pct_below(
  p_schema text,
  p_table text,
  p_geom geometry,
  p_threshold double precision
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_true_cnt double precision;
  v_all_cnt double precision;
BEGIN
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Clip(r.rast, ST_Transform($1, ST_SRID(r.rast)), true) AS rast
      FROM %I.%I r
      WHERE ST_Intersects(r.rast, ST_Transform($1, ST_SRID(r.rast)))
    ), vc AS (
      SELECT
        (x).value::double precision AS v,
        SUM((x).count)::double precision AS cnt
      FROM clipped c
      CROSS JOIN LATERAL ST_ValueCount(c.rast, 1, true) x
      GROUP BY (x).value
    )
    SELECT
      COALESCE(SUM(CASE WHEN v < $2 THEN cnt ELSE 0 END), 0)::double precision,
      COALESCE(SUM(cnt), 0)::double precision
    FROM vc
  $SQL$, p_schema, p_table);

  EXECUTE v_sql INTO v_true_cnt, v_all_cnt USING v_geom_4326, p_threshold;

  IF COALESCE(v_all_cnt, 0.0) <= 0 THEN
    RETURN NULL;
  END IF;

  RETURN (v_true_cnt / v_all_cnt) * 100.0;
END;
$$;

-- Returns % of pixels with value = p_value.
CREATE OR REPLACE FUNCTION metrics._raster_pct_equal(
  p_schema text,
  p_table text,
  p_geom geometry,
  p_value double precision
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_true_cnt double precision;
  v_all_cnt double precision;
BEGIN
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Clip(r.rast, ST_Transform($1, ST_SRID(r.rast)), true) AS rast
      FROM %I.%I r
      WHERE ST_Intersects(r.rast, ST_Transform($1, ST_SRID(r.rast)))
    ), vc AS (
      SELECT
        (x).value::double precision AS v,
        SUM((x).count)::double precision AS cnt
      FROM clipped c
      CROSS JOIN LATERAL ST_ValueCount(c.rast, 1, true) x
      GROUP BY (x).value
    )
    SELECT
      COALESCE(SUM(CASE WHEN v = $2 THEN cnt ELSE 0 END), 0)::double precision,
      COALESCE(SUM(cnt), 0)::double precision
    FROM vc
  $SQL$, p_schema, p_table);

  EXECUTE v_sql INTO v_true_cnt, v_all_cnt USING v_geom_4326, p_value;

  IF COALESCE(v_all_cnt, 0.0) <= 0 THEN
    RETURN NULL;
  END IF;

  RETURN (v_true_cnt / v_all_cnt) * 100.0;
END;
$$;

-- ------------------------------------------------------------
-- Topography metrics
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.compute_topo_mean_elevation(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_rec record;
BEGIN
  v_city := metrics._normalize_city(p_city);
  SELECT * INTO v_rec FROM metrics._raster_summary('dem', v_city || '_dem_normalized', p_geom);
  IF COALESCE(v_rec.cnt, 0) = 0 THEN
    RETURN NULL;
  END IF;
  RETURN v_rec.mean_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_topo_elevation_range(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_rec record;
BEGIN
  v_city := metrics._normalize_city(p_city);
  SELECT * INTO v_rec FROM metrics._raster_summary('dem', v_city || '_dem_normalized', p_geom);
  IF COALESCE(v_rec.cnt, 0) = 0 OR v_rec.min_val IS NULL OR v_rec.max_val IS NULL THEN
    RETURN NULL;
  END IF;
  RETURN (v_rec.max_val - v_rec.min_val)::double precision;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_topo_mean_slope(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_rec record;
BEGIN
  v_city := metrics._normalize_city(p_city);
  SELECT * INTO v_rec FROM metrics._raster_summary('dem', v_city || '_slope_deg_normalized', p_geom);
  IF COALESCE(v_rec.cnt, 0) = 0 THEN
    RETURN NULL;
  END IF;
  RETURN v_rec.mean_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_topo_steep_area_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  RETURN metrics._raster_pct_above('dem', v_city || '_slope_deg_normalized', p_geom, 15.0);
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_topo_flat_area_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  RETURN metrics._raster_pct_below('dem', v_city || '_slope_deg_normalized', p_geom, 3.0);
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_topo_flood_risk_proxy(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  RETURN metrics._raster_pct_equal('dem', v_city || '_flood_risk_proxy', p_geom, 1.0);
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_topo_natural_constraint_index(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
-- Weights: steep_slope(0.7) + water_body(0.3).
-- These are proxy weights and are not scientifically calibrated.
-- Factors may overlap (e.g. steep riverbanks counted in both components).
-- Treat as indicative ordering only.
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_steep double precision;
  v_water_pct double precision := 0.0;
  v_water_m2 double precision := 0.0;
  v_sql text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  IF ST_Area(v_geom_4326::geography) <= 0 THEN RETURN NULL; END IF;

  v_steep := COALESCE(metrics.compute_topo_steep_area_pct(v_city, v_geom_4326), 0.0);

  IF to_regclass(format('%I.%I', 'green', v_city || '_water_bodies_canonical')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH clipped AS (
        SELECT ST_Area(ST_Intersection(w.geom, $1)::geography)::double precision AS a
        FROM %I.%I w
        WHERE w.geom IS NOT NULL
          AND NOT ST_IsEmpty(w.geom)
          AND ST_Intersects(w.geom, $1)
      )
      SELECT COALESCE(SUM(a), 0)::double precision
      FROM clipped
      WHERE a > 0
    $SQL$, 'green', v_city || '_water_bodies_canonical');

    EXECUTE v_sql INTO v_water_m2 USING v_geom_4326;
    v_water_pct := (COALESCE(v_water_m2, 0.0) / ST_Area(v_geom_4326::geography)) * 100.0;
  END IF;

  -- Weighted 0-100 index of natural constraints.
  RETURN LEAST(100.0, GREATEST(0.0, (0.7 * v_steep) + (0.3 * v_water_pct)));
END;
$$;

-- ------------------------------------------------------------
-- Family aggregator
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.analyse_topography(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'topo.mean_elevation', metrics.compute_topo_mean_elevation(p_city, p_geom),
    'topo.elevation_range', metrics.compute_topo_elevation_range(p_city, p_geom),
    'topo.mean_slope', metrics.compute_topo_mean_slope(p_city, p_geom),
    'topo.steep_area_pct', metrics.compute_topo_steep_area_pct(p_city, p_geom),
    'topo.flat_area_pct', metrics.compute_topo_flat_area_pct(p_city, p_geom),
    'topo.natural_constraint_index', metrics.compute_topo_natural_constraint_index(p_city, p_geom)
  );
$$;
