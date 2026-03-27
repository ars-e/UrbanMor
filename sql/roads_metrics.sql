-- roads_metrics.sql
-- Geometry-first road-network metric functions for wards and user-drawn polygons.
--
-- Usage:
--   psql -d urbanmor -v ON_ERROR_STOP=1 -f roads_metrics.sql
--
-- Primary entrypoint:
--   SELECT metrics.analyse_roads('ahmedabad', geom)
--   FROM boundaries.ahmedabad_wards_normalized
--   LIMIT 1;

SET search_path = public, metrics, transport, boundaries;

CREATE SCHEMA IF NOT EXISTS metrics;

-- ------------------------------------------------------------
-- Core helpers
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

-- Motorized-road filter for graph-based road metrics.
-- Explicit allow-list avoids pulling pedestrian-only links into connectivity metrics.
CREATE OR REPLACE FUNCTION metrics._is_motorized_road(p_highway text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT COALESCE(lower(trim(p_highway)), '') IN (
    'motorway', 'motorway_link',
    'trunk', 'trunk_link',
    'primary', 'primary_link',
    'secondary', 'secondary_link',
    'tertiary', 'tertiary_link',
    'unclassified', 'residential',
    'living_street', 'service',
    'road', 'busway'
  );
$$;

-- Shared clipped-road graph derivation:
-- - clips roads to input polygon (projected),
-- - nodes edges, then derives node degree and orientation entropy.
-- - also returns optional block-size stats from _network_blocks (NULL if table absent).
CREATE OR REPLACE FUNCTION metrics._road_graph_stats(
  p_city text,
  p_geom geometry
)
RETURNS TABLE (
  edge_count              bigint,
  node_count              bigint,
  connected_node_count    bigint,
  intersection_count      bigint,
  culdesac_count          bigint,
  edge_length_m           double precision,
  orientation_entropy_bits double precision,
  avg_block_size_m2       double precision,
  block_size_variance_m2  double precision
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_geom_3857 geometry;
  v_sql text;
  v_blocks_table text;
  v_blocks_sql text;
  v_avg_block double precision;
  v_var_block double precision;
  v_has_block_method boolean := false;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  v_geom_3857 := metrics._to_3857(v_geom_4326);

  IF v_geom_4326 IS NULL OR v_geom_3857 IS NULL THEN
    RETURN QUERY
    SELECT 0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint,
           0::double precision, NULL::double precision,
           NULL::double precision, NULL::double precision;
    RETURN;
  END IF;

  IF to_regclass(format('%I.%I', 'transport', v_city || '_roads_normalized')) IS NULL THEN
    RETURN QUERY
    SELECT 0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint,
           0::double precision, NULL::double precision,
           NULL::double precision, NULL::double precision;
    RETURN;
  END IF;

  v_sql := format($SQL$
    WITH clipped_raw AS (
      SELECT
        (ST_Dump(
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(r.geom), $1)),
            2
          )
        )).geom::geometry(LineString, 3857) AS geom
      FROM %I.%I r
      WHERE r.geom IS NOT NULL
        AND NOT ST_IsEmpty(r.geom)
        AND metrics._is_motorized_road(r.highway)
        AND ST_Intersects(metrics._to_3857(r.geom), $1)
    ),
    clipped AS (
      SELECT geom
      FROM clipped_raw
      WHERE geom IS NOT NULL
        AND NOT ST_IsEmpty(geom)
        AND ST_Length(geom) > 1.0
    ),
    noded_union AS (
      SELECT ST_Node(ST_UnaryUnion(ST_Collect(geom))) AS geom
      FROM clipped
    ),
    noded_edges AS (
      SELECT
        (ST_Dump(ST_CollectionExtract(geom, 2))).geom::geometry(LineString, 3857) AS geom
      FROM noded_union
      WHERE geom IS NOT NULL
    ),
    edges AS (
      SELECT
        geom,
        ST_Length(ST_Transform(geom, 4326)::geography)::double precision AS length_m
      FROM noded_edges
      WHERE geom IS NOT NULL
        AND NOT ST_IsEmpty(geom)
        AND ST_Length(geom) > 1.0
    ),
    endpoints AS (
      SELECT md5(encode(ST_AsEWKB(ST_SnapToGrid(ST_StartPoint(geom), 0.5)), 'hex')) AS node_key
      FROM edges
      UNION ALL
      SELECT md5(encode(ST_AsEWKB(ST_SnapToGrid(ST_EndPoint(geom), 0.5)), 'hex')) AS node_key
      FROM edges
    ),
    node_degree AS (
      SELECT node_key, COUNT(*)::int AS degree
      FROM endpoints
      GROUP BY node_key
    ),
    bearings AS (
      SELECT
        MOD(
          (DEGREES(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) + 180.0)::numeric,
          180.0::numeric
        )::double precision AS bearing_deg,
        length_m
      FROM edges
      WHERE ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) > 0.01
    ),
    bins AS (
      SELECT FLOOR(bearing_deg / 10.0)::int AS bin_id, SUM(length_m) AS bin_length_m
      FROM bearings
      GROUP BY 1
    ),
    probs AS (
      SELECT
        bin_length_m / NULLIF(SUM(bin_length_m) OVER (), 0)::double precision AS p
      FROM bins
    ),
    entropy AS (
      SELECT
        CASE WHEN COUNT(*) = 0 THEN NULL
             ELSE -SUM(p * (LN(p) / LN(2.0)))
        END::double precision AS h_bits
      FROM probs
      WHERE p > 0
    )
    SELECT
      COALESCE((SELECT COUNT(*) FROM edges), 0)::bigint AS edge_count,
      COALESCE((SELECT COUNT(*) FROM node_degree), 0)::bigint AS node_count,
      COALESCE((SELECT COUNT(*) FROM node_degree WHERE degree >= 2), 0)::bigint AS connected_node_count,
      COALESCE((SELECT COUNT(*) FROM node_degree WHERE degree >= 3), 0)::bigint AS intersection_count,
      COALESCE((SELECT COUNT(*) FROM node_degree WHERE degree = 1), 0)::bigint AS culdesac_count,
      COALESCE((SELECT SUM(length_m) FROM edges), 0)::double precision AS edge_length_m,
      (SELECT h_bits FROM entropy)::double precision AS orientation_entropy_bits,
      NULL::double precision AS avg_block_size_m2,
      NULL::double precision AS block_size_variance_m2
  $SQL$, 'transport', v_city || '_roads_normalized');

  EXECUTE v_sql
  INTO edge_count, node_count, connected_node_count, intersection_count, culdesac_count,
       edge_length_m, orientation_entropy_bits, avg_block_size_m2, block_size_variance_m2
  USING v_geom_3857;

  -- Use precomputed road-derived block table only when it is explicitly marked.
  -- Older grid-based derivations lacked derivation metadata and are ignored.
  v_blocks_table := format('%I.%I', 'transport', v_city || '_network_blocks');
  IF to_regclass(v_blocks_table) IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema='transport'
        AND table_name = v_city || '_network_blocks'
        AND column_name='derivation_method'
    )
    INTO v_has_block_method;

    IF v_has_block_method THEN
      v_blocks_sql := format($SQL$
        WITH clipped AS (
          SELECT
            ST_Area(ST_Intersection(b.geom, $2)::geography)::double precision AS area_m2
          FROM %I.%I b
          WHERE b.geom IS NOT NULL
            AND NOT ST_IsEmpty(b.geom)
            AND b.derivation_method = 'road_polygonize'
            AND b.geom && $2
            AND ST_Intersects(b.geom, $2)
        )
        SELECT
          AVG(area_m2)::double precision,
          VAR_POP(area_m2)::double precision
        FROM clipped
        WHERE area_m2 > 1.0
      $SQL$, 'transport', v_city || '_network_blocks');
      EXECUTE v_blocks_sql INTO v_avg_block, v_var_block USING v_geom_3857, v_geom_4326;
      avg_block_size_m2 := v_avg_block;
      block_size_variance_m2 := v_var_block;
    END IF;
  END IF;

  RETURN NEXT;
END;
$$;

-- ------------------------------------------------------------
-- Road metrics
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.compute_road_intersection_density(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_sqkm double precision;
  v_stats record;
BEGIN
  v_area_sqkm := metrics._area_sqkm(p_geom);
  IF v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  RETURN COALESCE(v_stats.intersection_count, 0)::double precision / v_area_sqkm;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_cnr(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_stats record;
BEGIN
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  IF COALESCE(v_stats.intersection_count, 0) + COALESCE(v_stats.culdesac_count, 0) = 0 THEN
    RETURN NULL;
  END IF;

  RETURN (v_stats.intersection_count::double precision /
    (v_stats.intersection_count + v_stats.culdesac_count)::double precision) * 100.0;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_node_density(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_sqkm double precision;
  v_stats record;
BEGIN
  v_area_sqkm := metrics._area_sqkm(p_geom);
  IF v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  RETURN COALESCE(v_stats.node_count, 0)::double precision / v_area_sqkm;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_edge_density(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_sqkm double precision;
  v_stats record;
BEGIN
  v_area_sqkm := metrics._area_sqkm(p_geom);
  IF v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  RETURN (COALESCE(v_stats.edge_length_m, 0.0) / 1000.0) / v_area_sqkm;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_avg_block_size(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_stats record;
BEGIN
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  RETURN v_stats.avg_block_size_m2;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_block_size_variance(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_stats record;
BEGIN
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  RETURN v_stats.block_size_variance_m2;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_street_connectivity_index(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_stats record;
  v_raw double precision;
BEGIN
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  IF COALESCE(v_stats.node_count, 0) = 0 THEN
    RETURN NULL;
  END IF;

  v_raw := v_stats.edge_count::double precision / v_stats.node_count::double precision;
  -- Planar street grids typically approach ~2 links/node (E/V ~= 2 for 4-way meshes).
  RETURN LEAST(100.0, GREATEST(0.0, (v_raw / 2.0) * 100.0));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_culdesac_ratio(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_stats record;
BEGIN
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  IF COALESCE(v_stats.node_count, 0) = 0 THEN
    RETURN NULL;
  END IF;

  RETURN (v_stats.culdesac_count::double precision / v_stats.node_count::double precision) * 100.0;
END;
$$;

-- NOTE: measures per-segment sinuosity (path/chord), not OD route circuity.
CREATE OR REPLACE FUNCTION metrics.compute_road_circuity(
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
  v_rel text;
  v_highway_filter text := '';
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;
  v_geom_3857 := metrics._to_3857(v_geom_4326);

  IF to_regclass(format('%I.%I', 'transport', v_city || '_routing_graph_edges')) IS NOT NULL THEN
    v_rel := v_city || '_routing_graph_edges';
  ELSIF to_regclass(format('%I.%I', 'transport', v_city || '_roads_normalized')) IS NOT NULL THEN
    v_rel := v_city || '_roads_normalized';
    v_highway_filter := 'AND metrics._is_motorized_road(r.highway)';
  ELSE
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped_raw AS (
      SELECT
        (ST_Dump(
          ST_CollectionExtract(
            ST_MakeValid(ST_Intersection(metrics._to_3857(r.geom), $1)),
            2
          )
        )).geom::geometry(LineString, 3857) AS geom
      FROM %I.%I r
      WHERE r.geom IS NOT NULL
        AND NOT ST_IsEmpty(r.geom)
        %s
        AND ST_Intersects(metrics._to_3857(r.geom), $1)
    ),
    seg AS (
      SELECT
        ST_Length(geom)::double precision AS path_m,
        ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::double precision AS chord_m
      FROM clipped_raw
      WHERE geom IS NOT NULL
        AND NOT ST_IsEmpty(geom)
        AND ST_Length(geom) > 1.0
        AND ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) > 0.5
    ),
    ratios AS (
      SELECT
        path_m,
        (path_m / NULLIF(chord_m, 0.0))::double precision AS ratio
      FROM seg
    )
    SELECT
      CASE
        WHEN COALESCE(SUM(path_m), 0.0) = 0.0 THEN NULL
        ELSE (SUM(ratio * path_m) / SUM(path_m))::double precision
      END AS weighted_avg_ratio
    FROM ratios
  $SQL$, 'transport', v_rel, v_highway_filter);

  EXECUTE v_sql INTO v_val USING v_geom_3857;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_orientation_entropy(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_stats record;
BEGIN
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);
  RETURN v_stats.orientation_entropy_bits;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_network_density_by_type(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_area_sqkm double precision;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_sql text;
  v_val jsonb;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_area_sqkm := metrics._area_sqkm(p_geom);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);

  IF v_geom_4326 IS NULL OR v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    RETURN NULL;
  END IF;

  IF to_regclass(format('%I.%I', 'transport', v_city || '_roads_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH clipped AS (
      SELECT
        COALESCE(NULLIF(lower(r.highway), ''), 'unknown') AS highway_class,
        COALESCE((
          SELECT SUM(ST_Length((d).geom::geography))
          FROM ST_Dump(
            ST_CollectionExtract(
              ST_MakeValid(ST_Intersection(r.geom, $1)),
              2
            )
          ) AS d
        ), 0.0)::double precision / 1000.0 AS length_km
      FROM %I.%I r
      WHERE r.geom IS NOT NULL
        AND NOT ST_IsEmpty(r.geom)
        AND metrics._is_motorized_road(r.highway)
        AND ST_Intersects(r.geom, $1)
    ),
    agg AS (
      SELECT highway_class, SUM(length_km)::double precision AS length_km
      FROM clipped
      WHERE length_km > 0
      GROUP BY highway_class
    ),
    dens AS (
      SELECT
        highway_class,
        (length_km / NULLIF($2, 0.0))::double precision AS km_per_sqkm
      FROM agg
    )
    SELECT jsonb_build_object(
      'total_km_per_sq_km',
      COALESCE((SELECT SUM(length_km) / NULLIF($2, 0.0) FROM agg), 0.0),
      'by_highway',
      COALESCE((SELECT jsonb_object_agg(highway_class, km_per_sqkm ORDER BY highway_class) FROM dens), '{}'::jsonb)
    )
  $SQL$, 'transport', v_city || '_roads_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_4326, v_area_sqkm;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_road_pedestrian_infra_ratio(
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
  v_ped_table_exists boolean;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;

  v_ped_table_exists := to_regclass(format('%I.%I', 'transport', v_city || '_roads_pedestrian_enriched')) IS NOT NULL;

  IF v_ped_table_exists THEN
    v_sql := format($SQL$
      WITH clipped AS (
        SELECT
          COALESCE((
            SELECT SUM(ST_Length((d).geom::geography))
            FROM ST_Dump(
              ST_CollectionExtract(
                ST_MakeValid(ST_Intersection(r.geom, $1)),
                2
              )
            ) AS d
          ), 0.0)::double precision AS length_m,
          COALESCE(r.is_pedestrian_link, FALSE) AS is_ped
        FROM %I.%I r
        WHERE r.geom IS NOT NULL
          AND NOT ST_IsEmpty(r.geom)
          AND ST_Intersects(r.geom, $1)
      ),
      agg AS (
        SELECT
          SUM(length_m)::double precision AS total_m,
          SUM(CASE WHEN is_ped THEN length_m ELSE 0 END)::double precision AS ped_m
        FROM clipped
        WHERE length_m > 0
      )
      SELECT
        CASE
          WHEN COALESCE(total_m, 0.0) = 0.0 THEN NULL
          ELSE (ped_m / total_m) * 100.0
        END::double precision
      FROM agg
    $SQL$, 'transport', v_city || '_roads_pedestrian_enriched');
  ELSIF to_regclass(format('%I.%I', 'transport', v_city || '_roads_normalized')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH clipped AS (
        SELECT
          COALESCE((
            SELECT SUM(ST_Length((d).geom::geography))
            FROM ST_Dump(
              ST_CollectionExtract(
                ST_MakeValid(ST_Intersection(r.geom, $1)),
                2
              )
            ) AS d
          ), 0.0)::double precision AS length_m,
          CASE
            WHEN r.source_layer = 'walkability_access' THEN TRUE
            WHEN lower(COALESCE(r.highway, '')) IN ('footway', 'pedestrian', 'path', 'steps', 'living_street', 'cycleway', 'track') THEN TRUE
            WHEN lower(COALESCE(r.foot, '')) IN ('yes', 'designated', 'permissive') THEN TRUE
            WHEN lower(COALESCE(r.bicycle, '')) IN ('yes', 'designated') THEN TRUE
            ELSE FALSE
          END AS is_ped
        FROM %I.%I r
        WHERE r.geom IS NOT NULL
          AND NOT ST_IsEmpty(r.geom)
          AND ST_Intersects(r.geom, $1)
      ),
      agg AS (
        SELECT
          SUM(length_m)::double precision AS total_m,
          SUM(CASE WHEN is_ped THEN length_m ELSE 0 END)::double precision AS ped_m
        FROM clipped
        WHERE length_m > 0
      )
      SELECT
        CASE
          WHEN COALESCE(total_m, 0.0) = 0.0 THEN NULL
          ELSE (ped_m / total_m) * 100.0
        END::double precision
      FROM agg
    $SQL$, 'transport', v_city || '_roads_normalized');
  ELSE
    RETURN NULL;
  END IF;

  EXECUTE v_sql INTO v_val USING v_geom_4326;
  RETURN v_val;
END;
$$;

-- ------------------------------------------------------------
-- Transit metrics (road-network family companion)
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.compute_transit_stop_density(
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
  v_geom_3857 geometry;
  v_sql text;
  v_count bigint;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_area_sqkm := metrics._area_sqkm(p_geom);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);

  IF v_geom_4326 IS NULL OR v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    RETURN NULL;
  END IF;
  v_geom_3857 := metrics._to_3857(v_geom_4326);

  IF to_regclass(format('%I.%I', 'transport', v_city || '_transit_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH pts AS (
      SELECT
        ST_PointOnSurface(metrics._to_3857(t.geom))::geometry(Point, 3857) AS geom
      FROM %I.%I t
      WHERE t.geom IS NOT NULL
        AND NOT ST_IsEmpty(t.geom)
        AND t.source_layer IN (
          'public_transport_stops',
          'public_transport_platforms',
          'public_transport_stations',
          'public_transport_shelters',
          'metro_stations',
          'rail_stations',
          'metro_entrances'
        )
    )
    SELECT COUNT(*)::bigint
    FROM pts
    WHERE ST_Intersects(geom, $1)
  $SQL$, 'transport', v_city || '_transit_normalized');

  EXECUTE v_sql INTO v_count USING v_geom_3857;
  RETURN COALESCE(v_count, 0)::double precision / v_area_sqkm;
END;
$$;

-- Shared sampled nearest-distance helper for transit access metrics.
-- Uses precomputed city transit points when available for significantly faster refreshes.
CREATE OR REPLACE FUNCTION metrics._compute_transit_distance_sampled(
  p_city text,
  p_geom geometry,
  p_layers text[],
  p_sample_count integer DEFAULT 160
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom_4326 geometry(MultiPolygon, 4326);
  v_geom_3857 geometry(MultiPolygon, 3857);
  v_sql text;
  v_val double precision;
  v_samples integer;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom_4326 := metrics._normalize_polygon_geom(p_geom);
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;
  IF p_layers IS NULL OR array_length(p_layers, 1) IS NULL THEN
    RETURN NULL;
  END IF;

  v_geom_3857 := metrics._to_3857(v_geom_4326)::geometry(MultiPolygon, 3857);
  v_samples := LEAST(400, GREATEST(24, COALESCE(p_sample_count, 160)));

  IF to_regclass(format('%I.%I', 'transport', v_city || '_transit_points')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH g AS (
        SELECT
          $1::geometry(MultiPolygon, 4326) AS geom,
          $2::geometry(MultiPolygon, 3857) AS geom_3857
      ),
      targets AS (
        SELECT t.geom::geometry(Point, 4326) AS geom
        FROM %I.%I t
        WHERE t.geom IS NOT NULL
          AND NOT ST_IsEmpty(t.geom)
          AND t.source_layer = ANY($3)
      ),
      samples AS (
        SELECT
          ST_Transform((dp.geom)::geometry(Point, 3857), 4326)::geometry(Point, 4326) AS geom
        FROM g,
             LATERAL ST_Dump(ST_GeneratePoints(g.geom_3857, $4, 20260326)) AS dp
        LIMIT $4
      ),
      fallback_sample AS (
        SELECT ST_PointOnSurface(g.geom)::geometry(Point, 4326) AS geom
        FROM g
        WHERE NOT EXISTS (SELECT 1 FROM samples)
      ),
      all_samples AS (
        SELECT geom FROM samples
        UNION ALL
        SELECT geom FROM fallback_sample
      ),
      nearest AS (
        SELECT
          (
            SELECT ST_Distance(s.geom::geography, t.geom::geography)::double precision
            FROM targets t
            ORDER BY s.geom <-> t.geom
            LIMIT 1
          ) AS nearest_m
        FROM all_samples s
      )
      SELECT AVG(nearest_m)::double precision
      FROM nearest
      WHERE nearest_m IS NOT NULL
    $SQL$, 'transport', v_city || '_transit_points');
  ELSIF to_regclass(format('%I.%I', 'transport', v_city || '_transit_normalized')) IS NOT NULL THEN
    v_sql := format($SQL$
      WITH g AS (
        SELECT
          $1::geometry(MultiPolygon, 4326) AS geom,
          $2::geometry(MultiPolygon, 3857) AS geom_3857
      ),
      targets AS (
        SELECT
          ST_PointOnSurface(metrics._to_4326(t.geom))::geometry(Point, 4326) AS geom
        FROM %I.%I t
        WHERE t.geom IS NOT NULL
          AND NOT ST_IsEmpty(t.geom)
          AND t.source_layer = ANY($3)
      ),
      samples AS (
        SELECT
          ST_Transform((dp.geom)::geometry(Point, 3857), 4326)::geometry(Point, 4326) AS geom
        FROM g,
             LATERAL ST_Dump(ST_GeneratePoints(g.geom_3857, $4, 20260326)) AS dp
        LIMIT $4
      ),
      fallback_sample AS (
        SELECT ST_PointOnSurface(g.geom)::geometry(Point, 4326) AS geom
        FROM g
        WHERE NOT EXISTS (SELECT 1 FROM samples)
      ),
      all_samples AS (
        SELECT geom FROM samples
        UNION ALL
        SELECT geom FROM fallback_sample
      ),
      nearest AS (
        SELECT
          (
            SELECT ST_Distance(s.geom::geography, t.geom::geography)::double precision
            FROM targets t
            ORDER BY s.geom <-> t.geom
            LIMIT 1
          ) AS nearest_m
        FROM all_samples s
      )
      SELECT AVG(nearest_m)::double precision
      FROM nearest
      WHERE nearest_m IS NOT NULL
    $SQL$, 'transport', v_city || '_transit_normalized');
  ELSE
    RETURN NULL;
  END IF;

  EXECUTE v_sql INTO v_val USING v_geom_4326, v_geom_3857, p_layers, v_samples;
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_transit_distance_to_metro_or_rail(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_val double precision;
BEGIN
  v_val := metrics._compute_transit_distance_sampled(
    p_city,
    p_geom,
    ARRAY['metro_stations', 'rail_stations', 'metro_entrances']::text[],
    120
  );
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_transit_distance_to_bus_stop(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_val double precision;
BEGIN
  v_val := metrics._compute_transit_distance_sampled(
    p_city,
    p_geom,
    ARRAY[
      'public_transport_stops',
      'public_transport_platforms',
      'public_transport_shelters',
      'public_transport_stations'
    ]::text[],
    160
  );
  RETURN v_val;
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_transit_coverage_500m(
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
  IF v_geom_4326 IS NULL THEN
    RETURN NULL;
  END IF;
  v_geom_3857 := metrics._to_3857(v_geom_4326);

  IF to_regclass(format('%I.%I', 'transport', v_city || '_transit_normalized')) IS NULL THEN
    RETURN NULL;
  END IF;

  v_sql := format($SQL$
    WITH g AS (
      SELECT $1::geometry(MultiPolygon, 3857) AS geom,
             $2::geometry(MultiPolygon, 4326) AS geom_4326
    ),
    stops AS (
      SELECT
        ST_PointOnSurface(metrics._to_3857(t.geom))::geometry(Point, 3857) AS geom
      FROM %I.%I t
      WHERE t.geom IS NOT NULL
        AND NOT ST_IsEmpty(t.geom)
        AND t.source_layer IN (
          'public_transport_stops',
          'public_transport_platforms',
          'public_transport_stations',
          'metro_stations',
          'rail_stations'
        )
    ),
    buffered AS (
      SELECT ST_Buffer(geom, 500.0) AS geom
      FROM stops
    ),
    unioned AS (
      SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
      FROM buffered
    )
    SELECT
      CASE
        WHEN ST_Area((SELECT geom_4326 FROM g)::geography) <= 0 THEN NULL
        WHEN (SELECT geom FROM unioned) IS NULL THEN 0.0
        ELSE (
          ST_Area(
            ST_Intersection(
              ST_Transform((SELECT geom FROM unioned), 4326),
              (SELECT geom_4326 FROM g)
            )::geography
          ) /
          ST_Area((SELECT geom_4326 FROM g)::geography)
        ) * 100.0
      END::double precision AS coverage_pct
    FROM g
    LIMIT 1
  $SQL$, 'transport', v_city || '_transit_normalized');

  EXECUTE v_sql INTO v_val USING v_geom_3857, v_geom_4326;
  RETURN v_val;
END;
$$;

-- ------------------------------------------------------------
-- Aggregator
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.analyse_roads(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_area_sqkm double precision;
  v_stats record;
  v_intersection_density double precision;
  v_cnr double precision;
  v_node_density double precision;
  v_edge_density double precision;
  v_connectivity_idx double precision;
  v_culdesac_ratio double precision;
BEGIN
  v_area_sqkm := metrics._area_sqkm(p_geom);
  SELECT * INTO v_stats FROM metrics._road_graph_stats(p_city, p_geom);

  IF v_area_sqkm IS NULL OR v_area_sqkm <= 0 THEN
    v_intersection_density := NULL;
    v_node_density := NULL;
    v_edge_density := NULL;
  ELSE
    v_intersection_density := COALESCE(v_stats.intersection_count, 0)::double precision / v_area_sqkm;
    v_node_density := COALESCE(v_stats.node_count, 0)::double precision / v_area_sqkm;
    v_edge_density := (COALESCE(v_stats.edge_length_m, 0.0) / 1000.0) / v_area_sqkm;
  END IF;

  IF COALESCE(v_stats.intersection_count, 0) + COALESCE(v_stats.culdesac_count, 0) = 0 THEN
    v_cnr := NULL;
  ELSE
    v_cnr := (v_stats.intersection_count::double precision /
      (v_stats.intersection_count + v_stats.culdesac_count)::double precision) * 100.0;
  END IF;

  IF COALESCE(v_stats.node_count, 0) = 0 THEN
    v_connectivity_idx := NULL;
    v_culdesac_ratio := NULL;
  ELSE
    v_connectivity_idx := LEAST(
      100.0,
      GREATEST(0.0, ((v_stats.edge_count::double precision / v_stats.node_count::double precision) / 2.0) * 100.0)
    );
    v_culdesac_ratio := (v_stats.culdesac_count::double precision / v_stats.node_count::double precision) * 100.0;
  END IF;

  RETURN jsonb_build_object(
    'road.intersection_density', v_intersection_density,
    'road.cnr', v_cnr,
    'road.node_density', v_node_density,
    'road.edge_density', v_edge_density,
    'road.avg_block_size', v_stats.avg_block_size_m2,
    'road.block_size_variance', v_stats.block_size_variance_m2,
    'road.street_connectivity_index', v_connectivity_idx,
    'road.culdesac_ratio', v_culdesac_ratio,
    'road.circuity', metrics.compute_road_circuity(p_city, p_geom),
    'road.orientation_entropy', v_stats.orientation_entropy_bits,
    'road.network_density_by_type', metrics.compute_road_network_density_by_type(p_city, p_geom),
    'road.pedestrian_infra_ratio', metrics.compute_road_pedestrian_infra_ratio(p_city, p_geom),
    'transit.stop_density', metrics.compute_transit_stop_density(p_city, p_geom),
    'transit.distance_to_metro_or_rail', metrics.compute_transit_distance_to_metro_or_rail(p_city, p_geom),
    'transit.distance_to_bus_stop', metrics.compute_transit_distance_to_bus_stop(p_city, p_geom),
    'transit.coverage_500m', metrics.compute_transit_coverage_500m(p_city, p_geom)
  );
END;
$$;
