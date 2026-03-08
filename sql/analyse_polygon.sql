-- sql/analyse_polygon.sql
-- Master polygon analysis aggregator across metric families.

SET search_path = public, metrics;

CREATE SCHEMA IF NOT EXISTS metrics;

CREATE OR REPLACE FUNCTION metrics._call_family_two_args(
  p_func_qualified text,
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_exists regprocedure;
  v_sql text;
  v_out jsonb;
BEGIN
  v_exists := to_regprocedure(p_func_qualified || '(text,geometry)');
  IF v_exists IS NULL THEN
    RETURN '{}'::jsonb;
  END IF;

  v_sql := format('SELECT %s($1, $2)', p_func_qualified);
  EXECUTE v_sql INTO v_out USING p_city, p_geom;
  RETURN COALESCE(v_out, '{}'::jsonb);
END;
$$;

CREATE OR REPLACE FUNCTION metrics.analyse_polygon(
  p_city text,
  p_geom geometry
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_city text;
  v_geom geometry(MultiPolygon, 4326);
  v_area_sqkm double precision;
  v_roads jsonb;
  v_buildings jsonb;
  v_landuse jsonb;
  v_topography jsonb;
  v_composites jsonb := '{}'::jsonb;
  v_all jsonb;
BEGIN
  v_city := metrics._normalize_city(p_city);
  v_geom := metrics._normalize_polygon_geom(p_geom);

  IF v_geom IS NULL THEN
    RETURN jsonb_build_object(
      'city', v_city,
      'error', 'invalid_polygon_geometry',
      'families', jsonb_build_object(
        'roads', '{}'::jsonb,
        'buildings', '{}'::jsonb,
        'landuse', '{}'::jsonb,
        'topography', '{}'::jsonb,
        'composites', '{}'::jsonb
      ),
      'all_metrics', '{}'::jsonb,
      'metric_count', 0
    );
  END IF;

  v_area_sqkm := metrics._area_sqkm(v_geom);

  v_roads := metrics._call_family_two_args('metrics.analyse_roads', v_city, v_geom);
  v_buildings := metrics._call_family_two_args('metrics.analyse_buildings', v_city, v_geom);
  v_landuse := metrics._call_family_two_args('metrics.analyse_landuse', v_city, v_geom);
  v_topography := metrics._call_family_two_args('metrics.analyse_topography', v_city, v_geom);

  -- Optional composites (if later implemented as a SQL function in DB).
  IF to_regprocedure('metrics.analyse_composites(text,geometry)') IS NOT NULL THEN
    EXECUTE 'SELECT metrics.analyse_composites($1, $2)' INTO v_composites USING v_city, v_geom;
    v_composites := COALESCE(v_composites, '{}'::jsonb);
  END IF;

  v_all := COALESCE(v_roads, '{}'::jsonb)
       || COALESCE(v_buildings, '{}'::jsonb)
       || COALESCE(v_landuse, '{}'::jsonb)
       || COALESCE(v_topography, '{}'::jsonb)
       || COALESCE(v_composites, '{}'::jsonb);

  RETURN jsonb_build_object(
    'city', v_city,
    'input', jsonb_build_object(
      'area_sqkm', v_area_sqkm,
      'geom_srid', 4326,
      'geom_type', GeometryType(v_geom)
    ),
    'families', jsonb_build_object(
      'roads', COALESCE(v_roads, '{}'::jsonb),
      'buildings', COALESCE(v_buildings, '{}'::jsonb),
      'landuse', COALESCE(v_landuse, '{}'::jsonb),
      'topography', COALESCE(v_topography, '{}'::jsonb),
      'composites', COALESCE(v_composites, '{}'::jsonb)
    ),
    'all_metrics', v_all,
    'metric_count', (SELECT COUNT(*) FROM jsonb_each(v_all))
  );
END;
$$;
