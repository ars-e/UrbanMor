-- sql/composite_metrics.sql
-- Composite metrics derived from primary road/building/landuse/topography metrics.

SET search_path = public, metrics;

CREATE SCHEMA IF NOT EXISTS metrics;

CREATE OR REPLACE FUNCTION metrics._clamp_0_100(p_val double precision)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT CASE
    WHEN p_val IS NULL THEN NULL
    WHEN p_val < 0 THEN 0.0
    WHEN p_val > 100 THEN 100.0
    ELSE p_val
  END;
$$;

CREATE OR REPLACE FUNCTION metrics._weighted_score(
  p_scores double precision[],
  p_weights double precision[]
)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  WITH idx AS (
    SELECT
      s.i AS i
    FROM generate_subscripts(p_scores, 1) AS s(i)
    WHERE s.i <= COALESCE(array_length(p_weights, 1), 0)
  ),
  usable AS (
    SELECT
      p_scores[i] AS score,
      p_weights[i] AS weight
    FROM idx
    WHERE p_scores[i] IS NOT NULL
      AND p_weights[i] IS NOT NULL
      AND p_weights[i] > 0
  )
  SELECT
    CASE
      WHEN COALESCE(SUM(weight), 0.0) <= 0.0 THEN NULL
      ELSE SUM(score * weight) / SUM(weight)
    END::double precision
  FROM usable;
$$;

CREATE OR REPLACE FUNCTION metrics._json_metric_number(
  p_metrics jsonb,
  p_metric_id text
)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT CASE
    WHEN p_metrics ? p_metric_id THEN NULLIF(p_metrics ->> p_metric_id, '')::double precision
    ELSE NULL::double precision
  END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_walkability_index(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_intersection_density double precision;
  v_cnr double precision;
  v_ped_ratio double precision;
  v_transit_coverage double precision;
  v_transit_distance_m double precision;
  v_intersection_norm double precision;
  v_transit_distance_score double precision;
BEGIN
  v_intersection_density := metrics.compute_road_intersection_density(p_city, p_geom);
  v_cnr := metrics.compute_road_cnr(p_city, p_geom);
  v_ped_ratio := metrics.compute_road_pedestrian_infra_ratio(p_city, p_geom);
  v_transit_coverage := metrics.compute_transit_coverage_500m(p_city, p_geom);
  v_transit_distance_m := metrics.compute_transit_distance_to_metro_or_rail(p_city, p_geom);

  v_intersection_norm := CASE
    WHEN v_intersection_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_intersection_density / 120.0) * 100.0)
  END;
  v_transit_distance_score := CASE
    WHEN v_transit_distance_m IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 / (1.0 + (GREATEST(0.0, v_transit_distance_m) / 500.0)))
  END;

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_intersection_norm, v_cnr, v_ped_ratio, v_transit_coverage, v_transit_distance_score],
    ARRAY[0.25, 0.25, 0.20, 0.20, 0.10]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_informality_index(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_bldg_density double precision;
  v_culdesac_ratio double precision;
  v_green_cover double precision;
  v_vacant_pct double precision;
  v_density_norm double precision;
  v_greenness_inv double precision;
BEGIN
  v_bldg_density := metrics.compute_bldg_density_per_ha(p_city, p_geom);
  v_culdesac_ratio := metrics.compute_road_culdesac_ratio(p_city, p_geom);
  v_green_cover := metrics.compute_lulc_green_cover_pct(p_city, p_geom);
  v_vacant_pct := metrics.compute_open_vacant_land_pct(p_city, p_geom);

  v_density_norm := CASE
    WHEN v_bldg_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_bldg_density / 250.0) * 100.0)
  END;
  v_greenness_inv := CASE
    WHEN v_green_cover IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 - v_green_cover)
  END;

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_density_norm, v_culdesac_ratio, v_greenness_inv, v_vacant_pct],
    ARRAY[0.35, 0.25, 0.20, 0.20]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_heat_island_proxy(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_impervious double precision;
  v_green_cover double precision;
  v_flat_area double precision;
  v_greenness_inv double precision;
BEGIN
  v_impervious := metrics.compute_lulc_impervious_ratio(p_city, p_geom);
  v_green_cover := metrics.compute_lulc_green_cover_pct(p_city, p_geom);
  v_flat_area := metrics.compute_topo_flat_area_pct(p_city, p_geom);
  v_greenness_inv := CASE
    WHEN v_green_cover IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 - v_green_cover)
  END;

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_impervious, v_greenness_inv, v_flat_area],
    ARRAY[0.50, 0.35, 0.15]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_development_pressure(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_vacant_pct double precision;
  v_bldg_density double precision;
  v_edge_density double precision;
  v_density_norm double precision;
  v_low_density_score double precision;
  v_edge_norm double precision;
BEGIN
  v_vacant_pct := metrics.compute_open_vacant_land_pct(p_city, p_geom);
  v_bldg_density := metrics.compute_bldg_density_per_ha(p_city, p_geom);
  v_edge_density := metrics.compute_road_edge_density(p_city, p_geom);

  v_density_norm := CASE
    WHEN v_bldg_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_bldg_density / 250.0) * 100.0)
  END;
  v_low_density_score := CASE
    WHEN v_density_norm IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 - v_density_norm)
  END;
  v_edge_norm := CASE
    WHEN v_edge_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_edge_density / 30.0) * 100.0)
  END;

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_vacant_pct, v_low_density_score, v_edge_norm],
    ARRAY[0.50, 0.20, 0.30]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_topographic_constraint_expansion(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_natural_constraint double precision;
  v_steep_pct double precision;
BEGIN
  v_natural_constraint := metrics.compute_topo_natural_constraint_index(p_city, p_geom);
  v_steep_pct := metrics.compute_topo_steep_area_pct(p_city, p_geom);

  IF v_natural_constraint IS NOT NULL THEN
    RETURN metrics._clamp_0_100(v_natural_constraint);
  END IF;

  -- Fallback when water-body support for natural-constraint is missing.
  RETURN metrics._clamp_0_100(v_steep_pct);
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_green_accessibility(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_distance_to_park_m double precision;
  v_park_density double precision;
  v_green_cover double precision;
  v_distance_score double precision;
  v_park_density_norm double precision;
BEGIN
  v_distance_to_park_m := metrics.compute_open_distance_to_nearest_park(p_city, p_geom);
  v_park_density := metrics.compute_open_park_green_space_density(p_city, p_geom);
  v_green_cover := metrics.compute_lulc_green_cover_pct(p_city, p_geom);

  v_distance_score := CASE
    WHEN v_distance_to_park_m IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 / (1.0 + (GREATEST(0.0, v_distance_to_park_m) / 300.0)))
  END;
  v_park_density_norm := CASE
    WHEN v_park_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_park_density / 80.0) * 100.0)
  END;

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_distance_score, v_park_density_norm, v_green_cover],
    ARRAY[0.40, 0.30, 0.30]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_transit_access_green(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_transit_coverage double precision;
  v_green_access double precision;
BEGIN
  v_transit_coverage := metrics.compute_transit_coverage_500m(p_city, p_geom);
  v_green_access := metrics.compute_cmp_green_accessibility(p_city, p_geom);

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_transit_coverage, v_green_access],
    ARRAY[0.55, 0.45]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.compute_cmp_compactness(
  p_city text,
  p_geom geometry
)
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_bcr double precision;
  v_intersection_density double precision;
  v_mix_index double precision;
  v_circuity double precision;
  v_intersection_norm double precision;
  v_mix_norm double precision;
  v_circuity_score double precision;
BEGIN
  v_bcr := metrics.compute_bldg_bcr(p_city, p_geom);
  v_intersection_density := metrics.compute_road_intersection_density(p_city, p_geom);
  v_mix_index := metrics.compute_lulc_mix_index(p_city, p_geom);
  v_circuity := metrics.compute_road_circuity(p_city, p_geom);
  v_intersection_norm := CASE
    WHEN v_intersection_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_intersection_density / 120.0) * 100.0)
  END;
  v_mix_norm := CASE
    WHEN v_mix_index IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_mix_index / LN(11.0)) * 100.0)
  END;
  v_circuity_score := CASE
    WHEN v_circuity IS NULL THEN NULL
    WHEN v_circuity <= 1.0 THEN 100.0
    ELSE 100.0 / (1.0 + ((v_circuity - 1.0) * 5.0))
  END;

  RETURN metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_bcr, v_intersection_norm, v_mix_norm, v_circuity_score],
    ARRAY[0.30, 0.25, 0.25, 0.20]
  ));
END;
$$;

CREATE OR REPLACE FUNCTION metrics.analyse_composites_from_metrics(
  p_metrics jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_metrics jsonb := COALESCE(p_metrics, '{}'::jsonb);
  v_road_intersection_density double precision;
  v_road_cnr double precision;
  v_road_culdesac_ratio double precision;
  v_road_ped_ratio double precision;
  v_transit_coverage double precision;
  v_transit_distance_m double precision;
  v_bldg_density double precision;
  v_green_cover double precision;
  v_vacant_pct double precision;
  v_impervious double precision;
  v_flat_area double precision;
  v_road_edge_density double precision;
  v_topo_natural_constraint double precision;
  v_topo_steep_pct double precision;
  v_distance_to_park_m double precision;
  v_park_density double precision;
  v_bcr double precision;
  v_lulc_mix_index double precision;
  v_road_circuity double precision;
  v_intersection_norm double precision;
  v_transit_distance_score double precision;
  v_walkability double precision;
  v_density_norm double precision;
  v_informality double precision;
  v_greenness_inv double precision;
  v_heat_island double precision;
  v_edge_norm double precision;
  v_low_density_score double precision;
  v_development_pressure double precision;
  v_topographic_constraint double precision;
  v_distance_score double precision;
  v_park_density_norm double precision;
  v_green_accessibility double precision;
  v_transit_access_green double precision;
  v_mix_norm double precision;
  v_circuity_score double precision;
  v_compactness double precision;
BEGIN
  v_road_intersection_density := metrics._json_metric_number(v_metrics, 'road.intersection_density');
  v_road_cnr := metrics._json_metric_number(v_metrics, 'road.cnr');
  v_road_culdesac_ratio := metrics._json_metric_number(v_metrics, 'road.culdesac_ratio');
  v_road_ped_ratio := metrics._json_metric_number(v_metrics, 'road.pedestrian_infra_ratio');
  v_transit_coverage := metrics._json_metric_number(v_metrics, 'transit.coverage_500m');
  v_transit_distance_m := metrics._json_metric_number(v_metrics, 'transit.distance_to_metro_or_rail');
  v_bldg_density := metrics._json_metric_number(v_metrics, 'bldg.density_per_ha');
  v_green_cover := metrics._json_metric_number(v_metrics, 'lulc.green_cover_pct');
  v_vacant_pct := metrics._json_metric_number(v_metrics, 'open.vacant_land_pct');
  v_impervious := metrics._json_metric_number(v_metrics, 'lulc.impervious_ratio');
  v_flat_area := metrics._json_metric_number(v_metrics, 'topo.flat_area_pct');
  v_road_edge_density := metrics._json_metric_number(v_metrics, 'road.edge_density');
  v_topo_natural_constraint := metrics._json_metric_number(v_metrics, 'topo.natural_constraint_index');
  v_topo_steep_pct := metrics._json_metric_number(v_metrics, 'topo.steep_area_pct');
  v_distance_to_park_m := metrics._json_metric_number(v_metrics, 'open.distance_to_nearest_park');
  v_park_density := metrics._json_metric_number(v_metrics, 'open.park_green_space_density');
  v_bcr := metrics._json_metric_number(v_metrics, 'bldg.bcr');
  v_lulc_mix_index := metrics._json_metric_number(v_metrics, 'lulc.mix_index');
  v_road_circuity := metrics._json_metric_number(v_metrics, 'road.circuity');

  v_intersection_norm := CASE
    WHEN v_road_intersection_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_road_intersection_density / 120.0) * 100.0)
  END;
  v_transit_distance_score := CASE
    WHEN v_transit_distance_m IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 / (1.0 + (GREATEST(0.0, v_transit_distance_m) / 500.0)))
  END;
  v_walkability := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_intersection_norm, v_road_cnr, v_road_ped_ratio, v_transit_coverage, v_transit_distance_score],
    ARRAY[0.25, 0.25, 0.20, 0.20, 0.10]
  ));

  v_density_norm := CASE
    WHEN v_bldg_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_bldg_density / 250.0) * 100.0)
  END;
  v_greenness_inv := CASE
    WHEN v_green_cover IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 - v_green_cover)
  END;
  v_informality := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_density_norm, v_road_culdesac_ratio, v_greenness_inv, v_vacant_pct],
    ARRAY[0.35, 0.25, 0.20, 0.20]
  ));

  v_heat_island := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_impervious, v_greenness_inv, v_flat_area],
    ARRAY[0.50, 0.35, 0.15]
  ));

  v_edge_norm := CASE
    WHEN v_road_edge_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_road_edge_density / 30.0) * 100.0)
  END;
  v_low_density_score := CASE
    WHEN v_density_norm IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 - v_density_norm)
  END;
  v_development_pressure := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_vacant_pct, v_low_density_score, v_edge_norm],
    ARRAY[0.50, 0.20, 0.30]
  ));

  v_topographic_constraint := CASE
    WHEN v_topo_natural_constraint IS NOT NULL THEN metrics._clamp_0_100(v_topo_natural_constraint)
    ELSE metrics._clamp_0_100(v_topo_steep_pct)
  END;

  v_distance_score := CASE
    WHEN v_distance_to_park_m IS NULL THEN NULL
    ELSE metrics._clamp_0_100(100.0 / (1.0 + (GREATEST(0.0, v_distance_to_park_m) / 300.0)))
  END;
  v_park_density_norm := CASE
    WHEN v_park_density IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_park_density / 80.0) * 100.0)
  END;
  v_green_accessibility := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_distance_score, v_park_density_norm, v_green_cover],
    ARRAY[0.40, 0.30, 0.30]
  ));

  v_transit_access_green := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_transit_coverage, v_green_accessibility],
    ARRAY[0.55, 0.45]
  ));

  v_mix_norm := CASE
    WHEN v_lulc_mix_index IS NULL THEN NULL
    ELSE metrics._clamp_0_100((v_lulc_mix_index / LN(11.0)) * 100.0)
  END;
  v_circuity_score := CASE
    WHEN v_road_circuity IS NULL THEN NULL
    WHEN v_road_circuity <= 1.0 THEN 100.0
    ELSE 100.0 / (1.0 + ((v_road_circuity - 1.0) * 5.0))
  END;
  v_compactness := metrics._clamp_0_100(metrics._weighted_score(
    ARRAY[v_bcr, v_intersection_norm, v_mix_norm, v_circuity_score],
    ARRAY[0.30, 0.25, 0.25, 0.20]
  ));

  RETURN jsonb_build_object(
    'cmp.walkability_index', v_walkability,
    'cmp.informality_index', v_informality,
    'cmp.heat_island_proxy', v_heat_island,
    'cmp.development_pressure', v_development_pressure,
    'cmp.topographic_constraint_expansion', v_topographic_constraint,
    'cmp.green_accessibility', v_green_accessibility,
    'cmp.transit_access_green', v_transit_access_green,
    'cmp.compactness', v_compactness
  );
END;
$$;

CREATE OR REPLACE FUNCTION metrics.analyse_composites(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_all jsonb;
BEGIN
  v_all := COALESCE(metrics.analyse_roads(p_city, p_geom), '{}'::jsonb)
    || COALESCE(metrics.analyse_buildings(p_city, p_geom), '{}'::jsonb)
    || COALESCE(metrics.analyse_landuse(p_city, p_geom), '{}'::jsonb)
    || COALESCE(metrics.analyse_topography(p_city, p_geom), '{}'::jsonb);

  RETURN metrics.analyse_composites_from_metrics(v_all);
END;
$$;
