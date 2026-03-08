\set ON_ERROR_STOP on

-- Usage:
--   psql -d urbanmor -f qa_prerequisite_layers.sql
-- Optional:
--   psql -v db_name=urbanmor -d postgres -f qa_prerequisite_layers.sql

\if :{?db_name}
\connect :db_name
\endif

DROP TABLE IF EXISTS pg_temp.qa_prereq_summary;
CREATE TEMP TABLE qa_prereq_summary (
  layer_family TEXT,
  city TEXT,
  table_name TEXT,
  row_count BIGINT,
  issue_count BIGINT,
  status TEXT,
  notes TEXT
);

DO $$
DECLARE
  c TEXT;
  rc BIGINT;
  issues BIGINT;
BEGIN
  FOREACH c IN ARRAY ARRAY['ahmedabad','bengaluru','chandigarh','chennai','delhi','kolkata','mumbai']
  LOOP
    BEGIN
      EXECUTE format('SELECT count(*) FROM transport.%I_network_blocks', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM transport.%I_network_blocks WHERE geom IS NULL OR ST_IsEmpty(geom) OR NOT ST_IsValid(geom) OR ST_SRID(geom) <> 4326',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('network_blocks', c, c || '_network_blocks', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'grid-clipped block proxy');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('network_blocks', c, c || '_network_blocks', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM transport.%I_routing_graph_edges', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM transport.%I_routing_graph_edges WHERE source_vertex_id IS NULL OR target_vertex_id IS NULL OR length_m <= 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('routing_graph_edges', c, c || '_routing_graph_edges', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'edge-level connectivity');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('routing_graph_edges', c, c || '_routing_graph_edges', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM transport.%I_roads_pedestrian_enriched', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM transport.%I_roads_pedestrian_enriched WHERE is_pedestrian_link IS TRUE',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('footway_tags', c, c || '_roads_pedestrian_enriched', rc, 0, CASE WHEN rc > 0 THEN 'ok' ELSE 'issue' END, 'pedestrian_link_rows=' || issues::text);
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('footway_tags', c, c || '_roads_pedestrian_enriched', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM buildings.%I_building_levels_enriched', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM buildings.%I_building_levels_enriched WHERE levels_estimated IS NULL OR floor_area_proxy_m2 < 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('building_levels_tags', c, c || '_building_levels_enriched', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'heuristic levels proxy');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('building_levels_tags', c, c || '_building_levels_enriched', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM buildings.%I_building_centroids', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM buildings.%I_building_centroids WHERE geom IS NULL OR ST_IsEmpty(geom) OR ST_SRID(geom) <> 4326 OR population_proxy < 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('building_centroids', c, c || '_building_centroids', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'centroids + pop proxy');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('building_centroids', c, c || '_building_centroids', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM green.%I_water_bodies_canonical', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM green.%I_water_bodies_canonical WHERE geom IS NULL OR ST_IsEmpty(geom) OR NOT ST_IsValid(geom)',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('water_bodies', c, c || '_water_bodies_canonical', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'osm + lulc water merge');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('water_bodies', c, c || '_water_bodies_canonical', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM green.%I_open_surfaces', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM green.%I_open_surfaces WHERE geom IS NULL OR ST_IsEmpty(geom) OR NOT ST_IsValid(geom)',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('open_surfaces', c, c || '_open_surfaces', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'osm + lulc open classes');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('open_surfaces', c, c || '_open_surfaces', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM green.%I_riparian_buffers', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM green.%I_riparian_buffers WHERE geom IS NULL OR ST_IsEmpty(geom) OR NOT ST_IsValid(geom)',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('buffers', c, c || '_riparian_buffers', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, '30m buffer from canonical water');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('buffers', c, c || '_riparian_buffers', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM lulc.%I_built_up_layer', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM lulc.%I_built_up_layer WHERE ST_IsEmpty(rast) OR ST_SRID(rast) = 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('built_up', c, c || '_built_up_layer', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'binary class-7 raster');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('built_up', c, c || '_built_up_layer', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM green.%I_vacant_land', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM green.%I_vacant_land WHERE ST_IsEmpty(rast) OR ST_SRID(rast) = 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('vacant_land', c, c || '_vacant_land', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'binary class-8/11 raster proxy');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('vacant_land', c, c || '_vacant_land', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM dem.%I_flood_risk_proxy', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM dem.%I_flood_risk_proxy WHERE ST_IsEmpty(rast) OR ST_SRID(rast) = 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('flood_risk', c, c || '_flood_risk_proxy', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'binary low-elevation + low-slope raster proxy');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('flood_risk', c, c || '_flood_risk_proxy', 0, 0, 'missing', 'table not found');
    END;

    BEGIN
      EXECUTE format('SELECT count(*) FROM metrics.%I_population_proxy', c) INTO rc;
      EXECUTE format(
        'SELECT count(*) FROM metrics.%I_population_proxy WHERE population_proxy < 0 OR population_density_proxy < 0',
        c
      ) INTO issues;
      INSERT INTO qa_prereq_summary VALUES ('population_proxy', c, c || '_population_proxy', rc, issues, CASE WHEN rc > 0 AND issues = 0 THEN 'ok' ELSE 'issue' END, 'ward-level population proxy');
    EXCEPTION WHEN undefined_table THEN
      INSERT INTO qa_prereq_summary VALUES ('population_proxy', c, c || '_population_proxy', 0, 0, 'missing', 'table not found');
    END;
  END LOOP;
END
$$;

-- LULC class map QA.
DO $$
BEGIN
  INSERT INTO qa_prereq_summary
  SELECT
    'lulc_class_map' AS layer_family,
    'all' AS city,
    'lulc_class_map' AS table_name,
    count(*)::bigint AS row_count,
    count(*) FILTER (WHERE class_value IS NULL OR class_label IS NULL OR canonical_class IS NULL)::bigint AS issue_count,
    CASE
      WHEN count(*) > 0
        AND count(*) FILTER (WHERE class_value IS NULL OR class_label IS NULL OR canonical_class IS NULL) = 0
        THEN 'ok'
      ELSE 'issue'
    END AS status,
    'class crosswalk coverage'
  FROM lulc.lulc_class_map;
EXCEPTION WHEN undefined_table THEN
  INSERT INTO qa_prereq_summary VALUES ('lulc_class_map', 'all', 'lulc_class_map', 0, 0, 'missing', 'table not found');
END;
$$;

-- Detailed report.
SELECT *
FROM qa_prereq_summary
ORDER BY layer_family, city;

-- Rollup.
SELECT
  layer_family,
  sum(row_count) AS total_rows,
  sum(issue_count) AS total_issues,
  CASE WHEN sum(issue_count) = 0 THEN 'ok' ELSE 'issue' END AS status
FROM qa_prereq_summary
GROUP BY layer_family
ORDER BY layer_family;
