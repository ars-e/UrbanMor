-- sql/building_metrics.sql
-- Building metric family functions (geometry-first; reusable for wards and drawn polygons).

SET search_path = public, metrics, buildings, transport;

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
-- Building metrics
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.compute_bldg_bcr(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_area_m2 double precision;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_geom_3857 geometry;
  v_sql text;
  v_bldg_area double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;
  v_geom_3857 := metrics._to_3857(v_geom_4326);
  v_area_m2 := ST_Area(v_geom_4326::geography);
  IF v_area_m2 <= 0 THEN RETURN NULL; END IF;

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Area(ST_Intersection(b.geom, $2)::geography)::double precision AS a
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(metrics._to_3857(b.geom), $1)
    )
    SELECT COALESCE(SUM(a), 0)::double precision
    FROM clipped
    WHERE a > 0
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_bldg_area USING v_geom_3857, v_geom_4326;
  RETURN (COALESCE(v_bldg_area, 0.0) / v_area_m2) * 100.0;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_density_per_ha(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_area_ha double precision;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_cnt bigint;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  v_area_ha := ST_Area(v_geom_4326::geography) / 10000.0;
  IF v_area_ha <= 0 THEN RETURN NULL; END IF;

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    SELECT COUNT(*)::bigint
    FROM %I.%I b
    WHERE b.geom IS NOT NULL
      AND NOT ST_IsEmpty(b.geom)
      AND b.geom && $1
      AND ST_Intersects(b.geom, $1)
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_cnt USING v_geom_4326;
  RETURN COALESCE(v_cnt, 0)::double precision / v_area_ha;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_avg_footprint_size(
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

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT ST_Area(ST_Intersection(b.geom, $2)::geography)::double precision AS a
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(metrics._to_3857(b.geom), $1)
    )
    SELECT AVG(a)::double precision
    FROM clipped
    WHERE a > 1.0
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_3857, v_geom_4326;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_size_distribution(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_geom_3857 geometry;
  v_sql text;
  v_out jsonb;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;
  v_geom_3857 := metrics._to_3857(v_geom_4326);

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH sizes AS (
      SELECT ST_Area(ST_Intersection(b.geom, $2)::geography)::double precision AS a
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(metrics._to_3857(b.geom), $1)
    ), s AS (
      SELECT a FROM sizes WHERE a > 1.0
    )
    SELECT jsonb_build_object(
      'variance_m2', VAR_POP(a),
      'p50_m2', percentile_cont(0.5) WITHIN GROUP (ORDER BY a),
      'p90_m2', percentile_cont(0.9) WITHIN GROUP (ORDER BY a)
    )
    FROM s
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_out USING v_geom_3857, v_geom_4326;
  RETURN v_out;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_clustering_coeff(
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
  v_val double precision;
  v_centroids_rel text;
  v_link_threshold_m double precision := 60.0;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_building_centroids')) IS NOT NULL THEN
    v_centroids_rel := v_city || '_building_centroids';
    v_sql := format($SQL$
      WITH pts AS (
        SELECT centroid_id::bigint AS id, metrics._to_4326(c.geom)::geometry(Point, 4326) AS geom
        FROM %I.%I c
        WHERE c.geom IS NOT NULL
          AND NOT ST_IsEmpty(c.geom)
          AND ST_Intersects(metrics._to_4326(c.geom), $1)
        ORDER BY centroid_id
        LIMIT 400
      ),
      undirected_edges AS (
        SELECT a.id AS a_id, b.id AS b_id
        FROM pts a
        JOIN pts b
          ON a.id < b.id
         AND ST_DWithin(a.geom::geography, b.geom::geography, $2)
      ),
      adjacency AS (
        SELECT a_id AS id, b_id AS nbr FROM undirected_edges
        UNION ALL
        SELECT b_id AS id, a_id AS nbr FROM undirected_edges
      ),
      neighbor_pairs AS (
        SELECT a.id, a.nbr AS nbr_lo, b.nbr AS nbr_hi
        FROM adjacency a
        JOIN adjacency b
          ON a.id = b.id
         AND a.nbr < b.nbr
      ),
      closed_pairs AS (
        SELECT
          np.id,
          COUNT(*) FILTER (WHERE ue.a_id IS NOT NULL)::double precision AS closed_cnt,
          COUNT(*)::double precision AS total_pairs
        FROM neighbor_pairs np
        LEFT JOIN undirected_edges ue
          ON ue.a_id = np.nbr_lo
         AND ue.b_id = np.nbr_hi
        GROUP BY np.id
      ),
      local_coeff AS (
        SELECT
          p.id,
          CASE
            WHEN cp.total_pairs IS NULL OR cp.total_pairs = 0 THEN NULL
            ELSE cp.closed_cnt / cp.total_pairs
          END AS coeff
        FROM pts p
        LEFT JOIN closed_pairs cp
          ON cp.id = p.id
      )
      SELECT
        CASE
          WHEN (SELECT COUNT(*) FROM pts) < 3 THEN NULL
          ELSE AVG(coeff) FILTER (WHERE coeff IS NOT NULL) * 100.0
        END::double precision
      FROM local_coeff
    $SQL$, 'buildings', v_centroids_rel);
  ELSIF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH pts AS (
        SELECT id::bigint AS id, ST_PointOnSurface(metrics._to_4326(b.geom))::geometry(Point, 4326) AS geom
        FROM %I.%I b
        WHERE b.geom IS NOT NULL
          AND NOT ST_IsEmpty(b.geom)
          AND ST_Intersects(metrics._to_4326(b.geom), $1)
        ORDER BY id
        LIMIT 400
      ),
      undirected_edges AS (
        SELECT a.id AS a_id, b.id AS b_id
        FROM pts a
        JOIN pts b
          ON a.id < b.id
         AND ST_DWithin(a.geom::geography, b.geom::geography, $2)
      ),
      adjacency AS (
        SELECT a_id AS id, b_id AS nbr FROM undirected_edges
        UNION ALL
        SELECT b_id AS id, a_id AS nbr FROM undirected_edges
      ),
      neighbor_pairs AS (
        SELECT a.id, a.nbr AS nbr_lo, b.nbr AS nbr_hi
        FROM adjacency a
        JOIN adjacency b
          ON a.id = b.id
         AND a.nbr < b.nbr
      ),
      closed_pairs AS (
        SELECT
          np.id,
          COUNT(*) FILTER (WHERE ue.a_id IS NOT NULL)::double precision AS closed_cnt,
          COUNT(*)::double precision AS total_pairs
        FROM neighbor_pairs np
        LEFT JOIN undirected_edges ue
          ON ue.a_id = np.nbr_lo
         AND ue.b_id = np.nbr_hi
        GROUP BY np.id
      ),
      local_coeff AS (
        SELECT
          p.id,
          CASE
            WHEN cp.total_pairs IS NULL OR cp.total_pairs = 0 THEN NULL
            ELSE cp.closed_cnt / cp.total_pairs
          END AS coeff
        FROM pts p
        LEFT JOIN closed_pairs cp
          ON cp.id = p.id
      )
      SELECT
        CASE
          WHEN (SELECT COUNT(*) FROM pts) < 3 THEN NULL
          ELSE AVG(coeff) FILTER (WHERE coeff IS NOT NULL) * 100.0
        END::double precision
      FROM local_coeff
    $SQL$, 'buildings', v_city || '_buildings_normalized');
  ELSE
    RETURN NULL;
  END IF;

  EXECUTE v_sql INTO v_val USING v_geom_4326, v_link_threshold_m;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_avg_interbuilding_distance(
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
  v_val double precision;
  v_centroids_rel text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_building_centroids')) IS NOT NULL THEN
    v_centroids_rel := v_city || '_building_centroids';
    v_sql := format($SQL$
      WITH pts AS (
        SELECT centroid_id::bigint AS id, metrics._to_4326(c.geom)::geometry(Point, 4326) AS geom
        FROM %I.%I c
        WHERE c.geom IS NOT NULL
          AND NOT ST_IsEmpty(c.geom)
          AND ST_Intersects(metrics._to_4326(c.geom), $1)
        ORDER BY centroid_id
        LIMIT 600
      ), nearest AS (
        SELECT (
          SELECT ST_Distance(a.geom::geography, b.geom::geography)::double precision
          FROM (
            SELECT id, geom
            FROM pts b
            WHERE b.id <> a.id
            ORDER BY a.geom <-> b.geom
            LIMIT 8
          ) b
          ORDER BY ST_Distance(a.geom::geography, b.geom::geography)
          LIMIT 1
        ) AS d
        FROM pts a
      )
      SELECT CASE WHEN (SELECT COUNT(*) FROM pts) < 2 THEN NULL ELSE AVG(d) END::double precision
      FROM nearest
      WHERE d IS NOT NULL
    $SQL$, 'buildings', v_centroids_rel);
  ELSIF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH pts AS (
        SELECT id::bigint AS id, ST_PointOnSurface(metrics._to_4326(b.geom))::geometry(Point, 4326) AS geom
        FROM %I.%I b
        WHERE b.geom IS NOT NULL
          AND NOT ST_IsEmpty(b.geom)
          AND ST_Intersects(metrics._to_4326(b.geom), $1)
        ORDER BY id
        LIMIT 600
      ), nearest AS (
        SELECT (
          SELECT ST_Distance(a.geom::geography, b.geom::geography)::double precision
          FROM (
            SELECT id, geom
            FROM pts b
            WHERE b.id <> a.id
            ORDER BY a.geom <-> b.geom
            LIMIT 8
          ) b
          ORDER BY ST_Distance(a.geom::geography, b.geom::geography)
          LIMIT 1
        ) AS d
        FROM pts a
      )
      SELECT CASE WHEN (SELECT COUNT(*) FROM pts) < 2 THEN NULL ELSE AVG(d) END::double precision
      FROM nearest
      WHERE d IS NOT NULL
    $SQL$, 'buildings', v_city || '_buildings_normalized');
  ELSE
    RETURN NULL;
  END IF;

  EXECUTE v_sql INTO v_val USING v_geom_4326;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_elongation(
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

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH sample AS (
      SELECT b.geom
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(metrics._to_3857(b.geom), $1)
      LIMIT 2000
    ),
    poly AS (
      SELECT
        (ST_Dump(
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(s.geom), $1)),
            3
          )
        )).geom::geometry(Polygon, 3857) AS geom
      FROM sample s
    ), env AS (
      SELECT ST_OrientedEnvelope(geom) AS env
      FROM poly
      WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
    ), lens AS (
      SELECT
        ST_Distance(ST_PointN(ST_ExteriorRing(env), 1), ST_PointN(ST_ExteriorRing(env), 2))::double precision AS l1,
        ST_Distance(ST_PointN(ST_ExteriorRing(env), 2), ST_PointN(ST_ExteriorRing(env), 3))::double precision AS l2
      FROM env
      WHERE GeometryType(env) = 'POLYGON'
    ), ratio AS (
      SELECT
        CASE
          WHEN LEAST(l1, l2) <= 0 THEN NULL
          ELSE GREATEST(l1, l2) / LEAST(l1, l2)
        END::double precision AS r
      FROM lens
    )
    SELECT AVG(r)::double precision
    FROM ratio
    WHERE r IS NOT NULL
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_3857;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_orientation(
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

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH sample AS (
      SELECT b.geom
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(metrics._to_3857(b.geom), $1)
      LIMIT 2000
    ),
    poly AS (
      SELECT
        (ST_Dump(
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(s.geom), $1)),
            3
          )
        )).geom::geometry(Polygon, 3857) AS geom
      FROM sample s
    ), env AS (
      SELECT ST_OrientedEnvelope(geom) AS env
      FROM poly
      WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
    ), edges AS (
      SELECT
        ST_MakeLine(ST_PointN(ST_ExteriorRing(env), 1), ST_PointN(ST_ExteriorRing(env), 2)) AS e1,
        ST_MakeLine(ST_PointN(ST_ExteriorRing(env), 2), ST_PointN(ST_ExteriorRing(env), 3)) AS e2
      FROM env
      WHERE GeometryType(env) = 'POLYGON'
    ), major AS (
      SELECT
        CASE WHEN ST_Length(e1) >= ST_Length(e2) THEN e1 ELSE e2 END AS e
      FROM edges
      WHERE ST_Length(e1) > 0 AND ST_Length(e2) > 0
    ), theta AS (
      SELECT
        MOD(
          (DEGREES(ST_Azimuth(ST_StartPoint(e), ST_EndPoint(e))) + 180.0)::numeric,
          180.0::numeric
        )::double precision AS t_deg
      FROM major
    ), circ AS (
      SELECT
        SUM(SIN(RADIANS(2.0 * t_deg)))::double precision AS s,
        SUM(COS(RADIANS(2.0 * t_deg)))::double precision AS c
      FROM theta
    )
    SELECT
      CASE
        WHEN c IS NULL OR s IS NULL THEN NULL
        WHEN c = 0 AND s = 0 THEN NULL
        ELSE MOD((DEGREES(ATAN2(s, c)) / 2.0 + 180.0)::numeric, 180.0::numeric)::double precision
      END AS mean_axial_orientation_deg
    FROM circ
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_3857;
  RETURN v_val;
END;
$$;

-- Isoperimetric quotient (4πA/P²), scaled to 0..100.
CREATE OR REPLACE FUNCTION metrics.compute_bldg_footprint_regularity(
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

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH poly AS (
      SELECT
        (ST_Dump(
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(b.geom), $1)),
            3
          )
        )).geom::geometry(Polygon, 3857) AS geom
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(metrics._to_3857(b.geom), $1)
    ), m AS (
      SELECT
        ST_Area(geom)::double precision AS a,
        ST_Perimeter(geom)::double precision AS p
      FROM poly
      WHERE geom IS NOT NULL
        AND NOT ST_IsEmpty(geom)
        AND ST_Area(geom) > 1.0
        AND ST_Perimeter(geom) > 0
    )
    SELECT AVG((4.0 * pi() * a) / (p * p))::double precision * 100.0
    FROM m
  $SQL$, 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_3857;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_edge_coverage(
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
  v_val double precision;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;
  IF to_regclass(format('%I.%I', 'transport', v_city || '_roads_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH poly AS (
      SELECT $1::geometry(MultiPolygon, 4326) AS geom
    ),
    roads AS (
      SELECT
        (ST_Dump(
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(r.geom, p.geom)),
            2
          )
        )).geom::geometry(LineString, 4326) AS geom
      FROM %I.%I r
      CROSS JOIN poly p
      WHERE r.geom IS NOT NULL
        AND NOT ST_IsEmpty(r.geom)
        AND ST_Intersects(r.geom, p.geom)
    ),
    candidate_buildings AS (
      SELECT b.geom::geometry AS geom
      FROM %I.%I b
      CROSS JOIN poly p
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND (
          ST_Intersects(b.geom, p.geom)
          OR ST_DWithin(b.geom::geography, p.geom::geography, 20.0)
        )
    ),
    road_samples AS (
      SELECT
        ST_LineInterpolatePoint(
          r.geom,
          CASE
            WHEN steps.n = 0 THEN 0.5
            ELSE LEAST(1.0, gs.i::double precision / steps.n::double precision)
          END
        )::geometry(Point, 4326) AS geom
      FROM (
        SELECT
          geom,
          ST_Length(geom::geography)::double precision AS length_m
        FROM roads
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
      ) r
      CROSS JOIN LATERAL (
        SELECT GREATEST(1, CEIL(COALESCE(r.length_m, 0.0) / 20.0)::int) AS n
      ) steps
      CROSS JOIN LATERAL generate_series(0, steps.n) AS gs(i)
      WHERE COALESCE(r.length_m, 0.0) > 0.0
    ),
    agg AS (
      SELECT
        COUNT(*)::double precision AS total_samples,
        COUNT(*) FILTER (
          WHERE EXISTS (
            SELECT 1
            FROM candidate_buildings b
            WHERE b.geom && ST_Expand(rs.geom, 0.0004)
              AND ST_DWithin(b.geom::geography, rs.geom::geography, 15.0)
          )
        )::double precision AS frontage_samples
      FROM road_samples rs
    )
    SELECT
      CASE
        WHEN COALESCE(total_samples, 0.0) = 0.0 THEN NULL
        ELSE LEAST(100.0, (frontage_samples / total_samples) * 100.0)
      END::double precision
    FROM agg
  $SQL$, 'transport', v_city || '_roads_normalized', 'buildings', v_city || '_buildings_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_4326;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_far_proxy(
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
  v_area_m2 double precision;
  v_sql text;
  v_val double precision;
  v_rel text;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN RETURN NULL; END IF;
  v_area_m2 := ST_Area(v_geom_4326::geography);
  IF v_area_m2 <= 0 THEN RETURN NULL; END IF;

  IF to_regclass(format('%I.%I', 'buildings', v_city || '_building_levels_enriched')) IS NOT NULL THEN
    v_rel := v_city || '_building_levels_enriched';
  ELSIF to_regclass(format('%I.%I', 'buildings', v_city || '_buildings_normalized')) IS NOT NULL THEN
    v_rel := v_city || '_buildings_normalized';
  ELSE
    RETURN NULL;
  END IF;

  IF v_rel = v_city || '_building_levels_enriched' THEN
    v_sql := format($SQL$
      WITH clipped AS (
        SELECT
          COALESCE(ST_Area(ST_Intersection(b.geom, $1)::geography), 0.0)::double precision AS clipped_area_m2,
          COALESCE(NULLIF(b.footprint_area_m2, 0.0), ST_Area(b.geom::geography))::double precision AS footprint_area_m2,
          COALESCE(b.floor_area_proxy_m2, 0.0)::double precision AS floor_area_proxy_m2
        FROM %I.%I b
        WHERE b.geom IS NOT NULL
          AND NOT ST_IsEmpty(b.geom)
          AND ST_Intersects(b.geom, $1)
      )
      SELECT
        (
          COALESCE(
            SUM(
              CASE
                WHEN footprint_area_m2 <= 0.0 OR clipped_area_m2 <= 0.0 THEN 0.0
                ELSE floor_area_proxy_m2 * LEAST(1.0, clipped_area_m2 / footprint_area_m2)
              END
            ),
            0.0
          ) / $2
        )::double precision
      FROM clipped
    $SQL$, 'buildings', v_rel);
  ELSE
    v_sql := format($SQL$
      SELECT
        (
          COALESCE(
            SUM(
              COALESCE(ST_Area(ST_Intersection(b.geom, $1)::geography), 0.0)
            ),
            0.0
          ) / $2
        )::double precision
      FROM %I.%I b
      WHERE b.geom IS NOT NULL
        AND NOT ST_IsEmpty(b.geom)
        AND ST_Intersects(b.geom, $1)
    $SQL$, 'buildings', v_rel);
  END IF;

  EXECUTE v_sql INTO v_val USING v_geom_4326, v_area_m2;

  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_bldg_growth_rate(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE sql
STABLE
AS $$
  SELECT NULL::double precision;
$$;

-- ------------------------------------------------------------
-- Family aggregator
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.analyse_buildings(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'bldg.bcr', metrics.compute_bldg_bcr(p_city, p_geom),
    'bldg.density_per_ha', metrics.compute_bldg_density_per_ha(p_city, p_geom),
    'bldg.avg_footprint_size', metrics.compute_bldg_avg_footprint_size(p_city, p_geom),
    'bldg.size_distribution', metrics.compute_bldg_size_distribution(p_city, p_geom),
    'bldg.clustering_coeff', metrics.compute_bldg_clustering_coeff(p_city, p_geom),
    'bldg.avg_interbuilding_distance', metrics.compute_bldg_avg_interbuilding_distance(p_city, p_geom),
    'bldg.elongation', metrics.compute_bldg_elongation(p_city, p_geom),
    'bldg.orientation', metrics.compute_bldg_orientation(p_city, p_geom),
    'bldg.footprint_regularity', metrics.compute_bldg_footprint_regularity(p_city, p_geom),
    'bldg.edge_coverage', metrics.compute_bldg_edge_coverage(p_city, p_geom),
    'bldg.far_proxy', metrics.compute_bldg_far_proxy(p_city, p_geom),
    -- Explicitly returned as NULL until temporal snapshots are ingested.
    'bldg.growth_rate', metrics.compute_bldg_growth_rate(p_city, p_geom)
  );
$$;
