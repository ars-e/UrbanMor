-- sql/landuse_metrics.sql
-- Land-use and open-space metric family functions.

SET search_path = public, metrics, lulc, green, buildings, transport;

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
-- Raster extraction helpers
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics._lulc_class_area_m2(
  p_city text,
  p_geom geometry
)
RETURNS TABLE(class_value int, area_m2 double precision)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);

  IF v_geom_4326 IS NULL THEN
    RETURN;
  END IF;

  IF to_regclass(format('%I.%I', 'lulc', v_city || '_lulc_normalized')) IS NULL THEN
    RETURN;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Clip(r.rast, ST_Transform($1, ST_SRID(r.rast)), true) AS rast
      FROM %I.%I r
      WHERE ST_Intersects(r.rast, ST_Transform($1, ST_SRID(r.rast)))
    ), polys AS (
      SELECT
        (dp).val::int AS class_value,
        (dp).geom AS geom
      FROM clipped c
      CROSS JOIN LATERAL ST_DumpAsPolygons(c.rast, 1, true) dp
      WHERE (dp).geom IS NOT NULL
        AND NOT ST_IsEmpty((dp).geom)
    )
    SELECT
      class_value,
      SUM(ST_Area(ST_Transform(geom, 4326)::geography))::double precision AS area_m2
    FROM polys
    GROUP BY class_value
  $SQL$, 'lulc', v_city || '_lulc_normalized');

  RETURN QUERY EXECUTE v_sql USING v_geom_4326;
END;
$$;

CREATE OR REPLACE FUNCTION metrics._lulc_flag_pct(
  p_city text,
  p_geom geometry,
  p_flag_col text
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_m2 double precision;
  v_val_m2 double precision;
  v_sql text;
BEGIN
  IF p_flag_col NOT IN (
    'is_water',
    'is_green',
    'is_open_surface',
    'is_built_up',
    'is_residential_proxy',
    'is_vacant_candidate'
  ) THEN
    RAISE EXCEPTION 'Unsupported LULC class-map flag: %', p_flag_col;
  END IF;

  v_area_m2 := COALESCE(metrics._area_sqkm(p_geom), 0.0) * 1_000_000.0;
  IF v_area_m2 <= 0 THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    SELECT COALESCE(SUM(a.area_m2), 0.0)::double precision
    FROM metrics._lulc_class_area_m2($1, $2) a
    JOIN lulc.lulc_class_map m
      ON m.class_value = a.class_value
    WHERE m.%I IS TRUE
  $SQL$, p_flag_col);

  EXECUTE v_sql INTO v_val_m2 USING p_city, p_geom;
  RETURN LEAST(100.0, GREATEST(0.0, (COALESCE(v_val_m2, 0.0) / v_area_m2) * 100.0));
END;
$$;

CREATE OR REPLACE FUNCTION metrics._lulc_canonical_pct(
  p_city text,
  p_geom geometry,
  p_canonical text
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_m2 double precision;
  v_val_m2 double precision;
BEGIN
  v_area_m2 := COALESCE(metrics._area_sqkm(p_geom), 0.0) * 1_000_000.0;
  IF v_area_m2 <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT COALESCE(SUM(a.area_m2), 0.0)::double precision
  INTO v_val_m2
  FROM metrics._lulc_class_area_m2(p_city, p_geom) a
  JOIN lulc.lulc_class_map m
    ON m.class_value = a.class_value
  WHERE lower(m.canonical_class) = lower(p_canonical);

  RETURN LEAST(100.0, GREATEST(0.0, (COALESCE(v_val_m2, 0.0) / v_area_m2) * 100.0));
END;
$$;

CREATE OR REPLACE FUNCTION metrics._binary_raster_pct(
  p_schema text,
  p_table text,
  p_geom geometry,
  p_true_value integer DEFAULT 1
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_area_m2 double precision;
  v_sql text;
  v_true_area_m2 double precision;
BEGIN
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;
  v_area_m2 := COALESCE(metrics._area_sqkm(v_geom_4326), 0.0) * 1_000_000.0;
  IF v_area_m2 <= 0 THEN
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
    ), polys AS (
      SELECT
        (dp).val::int AS v,
        (dp).geom AS geom
      FROM clipped c
      CROSS JOIN LATERAL ST_DumpAsPolygons(c.rast, 1, true) dp
      WHERE (dp).geom IS NOT NULL
        AND NOT ST_IsEmpty((dp).geom)
    )
    SELECT
      COALESCE(SUM(ST_Area(metrics._to_4326(geom)::geography)), 0)::double precision
    FROM polys
    WHERE v = $2
  $SQL$, p_schema, p_table);

  EXECUTE v_sql INTO v_true_area_m2 USING v_geom_4326, p_true_value;
  RETURN LEAST(100.0, GREATEST(0.0, (COALESCE(v_true_area_m2, 0.0) / v_area_m2) * 100.0));
END;
$$;

-- ------------------------------------------------------------
-- LULC metrics
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.compute_lulc_green_cover_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN metrics._lulc_flag_pct(p_city, p_geom, 'is_green');
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_lulc_mix_index(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_h double precision;
  v_total_m2 double precision;
BEGIN
  SELECT
    SUM(area_m2)::double precision
  INTO v_total_m2
  FROM metrics._lulc_class_area_m2(p_city, p_geom)
  WHERE area_m2 > 0;

  IF v_total_m2 IS NULL OR v_total_m2 <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT
    -SUM((area_m2 / v_total_m2) * LN(area_m2 / v_total_m2))
  INTO v_h
  FROM metrics._lulc_class_area_m2(p_city, p_geom)
  WHERE area_m2 > 0;

  RETURN v_h;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_lulc_residential_cover_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_m2 double precision;
  v_has_distinct_residential_proxy boolean;
  v_residential_m2 double precision;
BEGIN
  v_area_m2 := COALESCE(metrics._area_sqkm(p_geom), 0.0) * 1_000_000.0;
  IF v_area_m2 <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM lulc.lulc_class_map
    WHERE is_residential_proxy
      AND NOT is_built_up
  )
  INTO v_has_distinct_residential_proxy;

  -- Avoid returning a misleading duplicate of generic built-up cover when the
  -- class map does not distinguish residential land use from built area.
  IF NOT COALESCE(v_has_distinct_residential_proxy, false) THEN
    RETURN NULL;
  END IF;

  SELECT COALESCE(SUM(a.area_m2), 0.0)::double precision
  INTO v_residential_m2
  FROM metrics._lulc_class_area_m2(p_city, p_geom) a
  JOIN lulc.lulc_class_map m
    ON m.class_value = a.class_value
  WHERE m.is_residential_proxy
    AND NOT m.is_built_up;

  RETURN LEAST(100.0, GREATEST(0.0, (COALESCE(v_residential_m2, 0.0) / v_area_m2) * 100.0));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_lulc_agriculture_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN metrics._lulc_canonical_pct(p_city, p_geom, 'agriculture');
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_lulc_water_coverage_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_lulc_pct double precision;
  v_poly_pct double precision := NULL;
  v_sql text;
  v_poly_m2 double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  IF ST_Area(v_geom_4326::geography) <= 0 THEN RETURN NULL; END IF;

  v_lulc_pct := metrics._lulc_flag_pct(v_city, v_geom_4326, 'is_water');

  IF to_regclass(format('%I.%I', 'green', v_city || '_water_bodies_canonical')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH clipped AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(w.geom, $1)),
            3
          ) AS geom
        FROM %I.%I w
        WHERE w.geom IS NOT NULL
          AND NOT ST_IsEmpty(w.geom)
          AND ST_Intersects(w.geom, $1)
      ), unioned AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM clipped
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
      )
      SELECT
        COALESCE(
          ST_Area(ST_CollectionExtract((SELECT geom FROM unioned), 3)::geography),
          0
        )::double precision
    $SQL$, 'green', v_city || '_water_bodies_canonical');
    EXECUTE v_sql INTO v_poly_m2 USING v_geom_4326;
    v_poly_pct := (COALESCE(v_poly_m2, 0.0) / ST_Area(v_geom_4326::geography)) * 100.0;
  END IF;

  IF v_poly_pct IS NOT NULL THEN
    RETURN LEAST(100.0, GREATEST(0.0, v_poly_pct));
  END IF;
  IF v_lulc_pct IS NULL THEN
    RETURN NULL;
  END IF;
  RETURN LEAST(100.0, GREATEST(0.0, v_lulc_pct));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_lulc_impervious_ratio(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_geom_3857 geometry;
  v_area_m2 double precision;
  v_lulc_built_m2 double precision := 0.0;
  v_vector_impervious_m2 double precision := 0.0;
  v_sql text;
  v_has_bldg boolean;
  v_has_roads boolean;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  v_geom_3857 := metrics._to_3857(v_geom_4326);
  v_area_m2 := ST_Area(v_geom_4326::geography);
  IF v_area_m2 <= 0 THEN RETURN NULL; END IF;

  SELECT COALESCE(SUM(a.area_m2), 0.0)::double precision
  INTO v_lulc_built_m2
  FROM metrics._lulc_class_area_m2(v_city, v_geom_4326) a
  JOIN lulc.lulc_class_map m
    ON m.class_value = a.class_value
  WHERE m.is_built_up IS TRUE;

  v_has_bldg := to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NOT NULL;
  v_has_roads := to_regclass(format('%I.%I', 'transport', v_city || '_roads_normalized')) IS NOT NULL;

  IF v_has_bldg AND v_has_roads THEN
    v_sql := format($SQL$
      WITH b AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(b.geom), $1)),
            3
          ) AS geom
        FROM %I.%I b
        WHERE b.geom IS NOT NULL
          AND NOT ST_IsEmpty(b.geom)
          AND ST_Intersects(metrics._to_3857(b.geom), $1)
      ),
      rb AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(
              ST_Intersection(
                ST_Buffer(
                  ST_CollectionExtract(
                    ST_MakeValid(ST_Intersection(metrics._to_3857(r.geom), $1)),
                    2
                  ),
                  4.0
                ),
                $1
              )
            ),
            3
          ) AS geom
        FROM %I.%I r
        WHERE r.geom IS NOT NULL
          AND NOT ST_IsEmpty(r.geom)
          AND ST_Intersects(metrics._to_3857(r.geom), $1)
      ),
      geoms AS (
        SELECT geom FROM b WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
        UNION ALL
        SELECT geom FROM rb WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      ),
      unioned AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM geoms
      )
      SELECT
        COALESCE(
          ST_Area(
            ST_Transform(
              ST_CollectionExtract((SELECT geom FROM unioned), 3),
              4326
            )::geography
          ),
          0
        )::double precision
    $SQL$, 'buildings', v_city || '_buildings_normalized', 'transport', v_city || '_roads_normalized');
    EXECUTE v_sql INTO v_vector_impervious_m2 USING v_geom_3857;
  ELSIF v_has_bldg THEN
    v_sql := format($SQL$
      WITH b AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(b.geom), $1)),
            3
          ) AS geom
        FROM %I.%I b
        WHERE b.geom IS NOT NULL
          AND NOT ST_IsEmpty(b.geom)
          AND ST_Intersects(metrics._to_3857(b.geom), $1)
      ),
      u AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM b
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      )
      SELECT
        COALESCE(
          ST_Area(
            ST_Transform(
              ST_CollectionExtract((SELECT geom FROM u), 3),
              4326
            )::geography
          ),
          0
        )::double precision
    $SQL$, 'buildings', v_city || '_buildings_normalized');
    EXECUTE v_sql INTO v_vector_impervious_m2 USING v_geom_3857;
  ELSIF v_has_roads THEN
    v_sql := format($SQL$
      WITH rb AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(
              ST_Intersection(
                ST_Buffer(
                  ST_CollectionExtract(
                    ST_MakeValid(ST_Intersection(metrics._to_3857(r.geom), $1)),
                    2
                  ),
                  4.0
                ),
                $1
              )
            ),
            3
          ) AS geom
        FROM %I.%I r
        WHERE r.geom IS NOT NULL
          AND NOT ST_IsEmpty(r.geom)
          AND ST_Intersects(metrics._to_3857(r.geom), $1)
      ),
      u AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM rb
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      )
      SELECT
        COALESCE(
          ST_Area(
            ST_Transform(
              ST_CollectionExtract((SELECT geom FROM u), 3),
              4326
            )::geography
          ),
          0
        )::double precision
    $SQL$, 'transport', v_city || '_roads_normalized');
    EXECUTE v_sql INTO v_vector_impervious_m2 USING v_geom_3857;
  END IF;

  RETURN (LEAST(v_area_m2, GREATEST(COALESCE(v_lulc_built_m2, 0.0), COALESCE(v_vector_impervious_m2, 0.0))) / v_area_m2) * 100.0;
END;
$$;

-- ------------------------------------------------------------
-- Open-space metrics
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.compute_open_bare_ground_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN metrics._lulc_canonical_pct(p_city, p_geom, 'bare_ground');
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_open_park_green_space_density(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_area_sqkm double precision;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_park_area_m2 double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_area_sqkm := metrics._area_sqkm(p_geom);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);

  IF v_geom_4326 IS NULL OR v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', 'green', v_city || '_open_spaces_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT
        ST_CollectionExtract(
          ST_MakeValid(ST_Intersection(g.geom, $1)),
          3
        ) AS geom
      FROM %I.%I g
      WHERE g.geom IS NOT NULL
        AND NOT ST_IsEmpty(g.geom)
        AND g.source_layer IN ('green_parks_vegetation', 'sports_play_open')
        AND ST_Intersects(g.geom, $1)
    ), unioned AS (
      SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
      FROM clipped
      WHERE geom IS NOT NULL
        AND NOT ST_IsEmpty(geom)
    )
    SELECT
      COALESCE(
        ST_Area(ST_CollectionExtract((SELECT geom FROM unioned), 3)::geography),
        0
      )::double precision
  $SQL$, 'green', v_city || '_open_spaces_normalized');

  EXECUTE v_sql INTO v_park_area_m2 USING v_geom_4326;
  RETURN (COALESCE(v_park_area_m2, 0.0) / 10000.0) / v_area_sqkm;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_open_distance_to_nearest_park(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_geom_3857 geometry;
  v_sql text;
  v_val double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;
  v_geom_3857 := metrics._to_3857(v_geom_4326);

  IF to_regclass(format('%I.%I', 'green', v_city || '_open_spaces_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH g AS (
      SELECT
        $1::geometry(MultiPolygon, 4326) AS geom,
        $2::geometry(MultiPolygon, 3857) AS geom_3857
    ),
    parks AS (
      SELECT p.geom::geometry AS geom
      FROM %I.%I p
      WHERE p.geom IS NOT NULL
        AND NOT ST_IsEmpty(p.geom)
        AND p.source_layer IN ('green_parks_vegetation', 'sports_play_open')
    ),
    park_count AS (
      SELECT COUNT(*)::bigint AS n
      FROM parks
    ), samples AS (
      SELECT
        ST_Transform(
          ST_PointOnSurface(ST_Intersection(sg.geom, g.geom_3857)),
          4326
        )::geometry(Point, 4326) AS geom
      FROM g,
           LATERAL ST_SquareGrid(300.0, g.geom_3857) AS sg
      WHERE ST_Intersects(sg.geom, g.geom_3857)
      LIMIT 800
    ), fallback_sample AS (
      SELECT ST_PointOnSurface(g.geom)::geometry(Point, 4326) AS geom
      FROM g
      WHERE NOT EXISTS (SELECT 1 FROM samples)
    ), all_samples AS (
      SELECT geom FROM samples
      UNION ALL
      SELECT geom FROM fallback_sample
    ), nearest AS (
      SELECT (
        SELECT ST_Distance(s.geom::geography, p.geom::geography)::double precision
        FROM (
          SELECT p.geom
          FROM parks p
          ORDER BY s.geom <-> p.geom
          LIMIT 8
        ) p
        ORDER BY ST_Distance(s.geom::geography, p.geom::geography)
        LIMIT 1
      ) AS nearest_m
      FROM all_samples s
    )
    SELECT
      CASE
        WHEN (SELECT n FROM park_count) = 0 THEN NULL
        ELSE AVG(nearest_m)::double precision
      END
    FROM nearest
    WHERE nearest_m IS NOT NULL
  $SQL$, 'green', v_city || '_open_spaces_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_4326, v_geom_3857;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_open_vacant_land_pct(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_pct double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);

  IF to_regclass(format('%I.%I', 'green', v_city || '_vacant_land')) IS NOT NULL THEN
    v_pct := metrics._binary_raster_pct('green', v_city || '_vacant_land', p_geom, 1);
    IF v_pct IS NOT NULL THEN
      RETURN v_pct;
    END IF;
  END IF;

  RETURN metrics._lulc_flag_pct(v_city, p_geom, 'is_vacant_candidate');
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_open_riparian_buffer_integrity(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_num_m2 double precision;
  v_den_m2 double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  IF to_regclass(format('%I.%I', 'green', v_city || '_riparian_buffers')) IS NULL THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', 'green', v_city || '_open_surfaces')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH r AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(x.geom, $1)),
            3
          ) AS geom
        FROM %I.%I x
        WHERE x.geom IS NOT NULL
          AND NOT ST_IsEmpty(x.geom)
          AND ST_Intersects(x.geom, $1)
      ), ru AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM r
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      ), o AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(s.geom, $1)),
            3
          ) AS geom
        FROM %I.%I s
        WHERE s.geom IS NOT NULL
          AND NOT ST_IsEmpty(s.geom)
          AND ST_Intersects(s.geom, $1)
      ), ou AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM o
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      )
      SELECT
        COALESCE(ST_Area((SELECT geom FROM ru)::geography), 0)::double precision AS den_m2,
        COALESCE(ST_Area(ST_Intersection((SELECT geom FROM ru), (SELECT geom FROM ou))::geography), 0)::double precision AS num_m2
    $SQL$, 'green', v_city || '_riparian_buffers', 'green', v_city || '_open_surfaces');
  ELSE
    v_sql := format($SQL$
      WITH r AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(x.geom, $1)),
            3
          ) AS geom
        FROM %I.%I x
        WHERE x.geom IS NOT NULL
          AND NOT ST_IsEmpty(x.geom)
          AND ST_Intersects(x.geom, $1)
      ), ru AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM r
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      ), o AS (
        SELECT
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(s.geom, $1)),
            3
          ) AS geom
        FROM %I.%I s
        WHERE s.geom IS NOT NULL
          AND NOT ST_IsEmpty(s.geom)
          AND s.source_layer IN ('green_parks_vegetation', 'sports_play_open', 'open_space_master', 'other_open_landuse')
          AND ST_Intersects(s.geom, $1)
      ), ou AS (
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM o
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
      )
      SELECT
        COALESCE(ST_Area((SELECT geom FROM ru)::geography), 0)::double precision AS den_m2,
        COALESCE(ST_Area(ST_Intersection((SELECT geom FROM ru), (SELECT geom FROM ou))::geography), 0)::double precision AS num_m2
    $SQL$, 'green', v_city || '_riparian_buffers', 'green', v_city || '_open_spaces_normalized');
  END IF;

  EXECUTE v_sql INTO v_den_m2, v_num_m2 USING v_geom_4326;

  IF COALESCE(v_den_m2, 0.0) <= 0 THEN
    RETURN NULL;
  END IF;

  RETURN LEAST(100.0, GREATEST(0.0, (COALESCE(v_num_m2, 0.0) / v_den_m2) * 100.0));
END;
$$;

-- ------------------------------------------------------------
-- Family aggregator
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.analyse_landuse(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'lulc.green_cover_pct', metrics.compute_lulc_green_cover_pct(p_city, p_geom),
    'lulc.impervious_ratio', metrics.compute_lulc_impervious_ratio(p_city, p_geom),
    'lulc.mix_index', metrics.compute_lulc_mix_index(p_city, p_geom),
    'lulc.residential_cover_pct', metrics.compute_lulc_residential_cover_pct(p_city, p_geom),
    'lulc.agriculture_pct', metrics.compute_lulc_agriculture_pct(p_city, p_geom),
    'lulc.water_coverage_pct', metrics.compute_lulc_water_coverage_pct(p_city, p_geom),
    'open.bare_ground_pct', metrics.compute_open_bare_ground_pct(p_city, p_geom),
    'open.park_green_space_density', metrics.compute_open_park_green_space_density(p_city, p_geom),
    'open.distance_to_nearest_park', metrics.compute_open_distance_to_nearest_park(p_city, p_geom),
    'open.vacant_land_pct', metrics.compute_open_vacant_land_pct(p_city, p_geom),
    'open.riparian_buffer_integrity', metrics.compute_open_riparian_buffer_integrity(p_city, p_geom)
  );
$$;
