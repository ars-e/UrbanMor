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

  IF v_intersection_density IS NULL
     AND v_cnr IS NULL
     AND v_ped_ratio IS NULL
     AND v_transit_coverage IS NULL
     AND v_transit_distance_m IS NULL THEN
    RETURN NULL;
  END IF;

  v_intersection_norm := metrics._clamp_0_100((COALESCE(v_intersection_density, 0.0) / 120.0) * 100.0);
  v_transit_distance_score := CASE
    WHEN v_transit_distance_m IS NULL THEN 0.0
    ELSE 100.0 / (1.0 + (v_transit_distance_m / 500.0))
  END;

  RETURN metrics._clamp_0_100(
    (0.25 * v_intersection_norm) +
    (0.25 * COALESCE(v_cnr, 0.0)) +
    (0.20 * COALESCE(v_ped_ratio, 0.0)) +
    (0.20 * COALESCE(v_transit_coverage, 0.0)) +
    (0.10 * v_transit_distance_score)
  );
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
  v_cnr double precision;
  v_green_cover double precision;
  v_vacant_pct double precision;
  v_density_norm double precision;
BEGIN
  v_bldg_density := metrics.compute_bldg_density_per_ha(p_city, p_geom);
  v_cnr := metrics.compute_road_cnr(p_city, p_geom);
  v_green_cover := metrics.compute_lulc_green_cover_pct(p_city, p_geom);
  v_vacant_pct := metrics.compute_open_vacant_land_pct(p_city, p_geom);

  IF v_bldg_density IS NULL
     AND v_cnr IS NULL
     AND v_green_cover IS NULL
     AND v_vacant_pct IS NULL THEN
    RETURN NULL;
  END IF;

  v_density_norm := metrics._clamp_0_100((COALESCE(v_bldg_density, 0.0) / 250.0) * 100.0);

  RETURN metrics._clamp_0_100(
    (0.35 * v_density_norm) +
    (0.25 * (100.0 - COALESCE(v_cnr, 0.0))) +
    (0.20 * (100.0 - COALESCE(v_green_cover, 0.0))) +
    (0.20 * COALESCE(v_vacant_pct, 0.0))
  );
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
  v_bcr double precision;
  v_green_cover double precision;
  v_flat_area double precision;
BEGIN
  v_impervious := metrics.compute_lulc_impervious_ratio(p_city, p_geom);
  v_bcr := metrics.compute_bldg_bcr(p_city, p_geom);
  v_green_cover := metrics.compute_lulc_green_cover_pct(p_city, p_geom);
  v_flat_area := metrics.compute_topo_flat_area_pct(p_city, p_geom);

  IF v_impervious IS NULL
     AND v_bcr IS NULL
     AND v_green_cover IS NULL
     AND v_flat_area IS NULL THEN
    RETURN NULL;
  END IF;

  RETURN metrics._clamp_0_100(
    (0.35 * COALESCE(v_impervious, 0.0)) +
    (0.25 * COALESCE(v_bcr, 0.0)) +
    (0.25 * (100.0 - COALESCE(v_green_cover, 0.0))) +
    (0.15 * COALESCE(v_flat_area, 0.0))
  );
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
  v_edge_norm double precision;
BEGIN
  v_vacant_pct := metrics.compute_open_vacant_land_pct(p_city, p_geom);
  v_bldg_density := metrics.compute_bldg_density_per_ha(p_city, p_geom);
  v_edge_density := metrics.compute_road_edge_density(p_city, p_geom);

  IF v_vacant_pct IS NULL
     AND v_bldg_density IS NULL
     AND v_edge_density IS NULL THEN
    RETURN NULL;
  END IF;

  v_density_norm := metrics._clamp_0_100((COALESCE(v_bldg_density, 0.0) / 250.0) * 100.0);
  v_edge_norm := metrics._clamp_0_100((COALESCE(v_edge_density, 0.0) / 30.0) * 100.0);

  RETURN metrics._clamp_0_100(
    (0.40 * COALESCE(v_vacant_pct, 0.0)) +
    (0.35 * v_density_norm) +
    (0.25 * v_edge_norm)
  );
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
  v_flood_risk double precision;
BEGIN
  v_natural_constraint := metrics.compute_topo_natural_constraint_index(p_city, p_geom);
  v_steep_pct := metrics.compute_topo_steep_area_pct(p_city, p_geom);
  v_flood_risk := metrics.compute_topo_flood_risk_proxy(p_city, p_geom);

  IF v_natural_constraint IS NULL
     AND v_steep_pct IS NULL
     AND v_flood_risk IS NULL THEN
    RETURN NULL;
  END IF;

  RETURN metrics._clamp_0_100(
    (0.50 * COALESCE(v_natural_constraint, 0.0)) +
    (0.30 * COALESCE(v_steep_pct, 0.0)) +
    (0.20 * COALESCE(v_flood_risk, 0.0))
  );
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

  IF v_distance_to_park_m IS NULL
     AND v_park_density IS NULL
     AND v_green_cover IS NULL THEN
    RETURN NULL;
  END IF;

  v_distance_score := CASE
    WHEN v_distance_to_park_m IS NULL THEN 0.0
    ELSE 100.0 / (1.0 + (v_distance_to_park_m / 300.0))
  END;
  v_park_density_norm := metrics._clamp_0_100((COALESCE(v_park_density, 0.0) / 80.0) * 100.0);

  RETURN metrics._clamp_0_100(
    (0.40 * v_distance_score) +
    (0.30 * v_park_density_norm) +
    (0.30 * COALESCE(v_green_cover, 0.0))
  );
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

  IF v_transit_coverage IS NULL AND v_green_access IS NULL THEN
    RETURN NULL;
  END IF;

  RETURN metrics._clamp_0_100(
    (0.55 * COALESCE(v_transit_coverage, 0.0)) +
    (0.45 * COALESCE(v_green_access, 0.0))
  );
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

  IF v_bcr IS NULL
     AND v_intersection_density IS NULL
     AND v_mix_index IS NULL
     AND v_circuity IS NULL THEN
    RETURN NULL;
  END IF;

  v_intersection_norm := metrics._clamp_0_100((COALESCE(v_intersection_density, 0.0) / 120.0) * 100.0);
  v_mix_norm := metrics._clamp_0_100((COALESCE(v_mix_index, 0.0) / 3.0) * 100.0);
  v_circuity_score := CASE
    WHEN v_circuity IS NULL THEN 0.0
    WHEN v_circuity <= 1.0 THEN 100.0
    ELSE 100.0 / (1.0 + ((v_circuity - 1.0) * 5.0))
  END;

  RETURN metrics._clamp_0_100(
    (0.30 * COALESCE(v_bcr, 0.0)) +
    (0.25 * v_intersection_norm) +
    (0.25 * v_mix_norm) +
    (0.20 * v_circuity_score)
  );
END;
$$;

CREATE OR REPLACE FUNCTION metrics.analyse_composites(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'cmp.walkability_index', metrics.compute_cmp_walkability_index(p_city, p_geom),
    'cmp.informality_index', metrics.compute_cmp_informality_index(p_city, p_geom),
    'cmp.heat_island_proxy', metrics.compute_cmp_heat_island_proxy(p_city, p_geom),
    'cmp.development_pressure', metrics.compute_cmp_development_pressure(p_city, p_geom),
    'cmp.topographic_constraint_expansion', metrics.compute_cmp_topographic_constraint_expansion(p_city, p_geom),
    'cmp.green_accessibility', metrics.compute_cmp_green_accessibility(p_city, p_geom),
    'cmp.transit_access_green', metrics.compute_cmp_transit_access_green(p_city, p_geom),
    'cmp.compactness', metrics.compute_cmp_compactness(p_city, p_geom)
  );
$$;
