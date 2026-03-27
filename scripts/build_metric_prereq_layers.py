#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

ROOT = Path("/Users/ars-e/projects/Morph")
DB_NAME = os.getenv("DB_NAME", "urbanmor")
CITIES = [
    "ahmedabad",
    "bengaluru",
    "chandigarh",
    "chennai",
    "delhi",
    "kolkata",
    "mumbai",
]


def run_sql(sql: str) -> None:
    subprocess.run(
        ["psql", "-d", DB_NAME, "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
    )


def query_scalar(sql: str) -> str:
    out = subprocess.run(
        ["psql", "-d", DB_NAME, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out


def ident(name: str) -> str:
    if not re.match(r"^[a-z0-9_]+$", name):
        raise ValueError(f"Unsafe identifier: {name}")
    return name


def bootstrap_sources_and_class_map() -> None:
    run_sql(
        """
        CREATE SCHEMA IF NOT EXISTS metrics;

        INSERT INTO meta.source_registry (
          source_code, source_name, source_type, provider, source_url, acquisition_method, metadata
        )
        VALUES
          ('network_blocks_derivation', 'Network Blocks (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('routing_graph_derivation', 'Routing Graph (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('transit_points_derivation', 'Transit Points (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('pedestrian_enrichment_derivation', 'Pedestrian Enriched Roads (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('building_levels_derivation', 'Building Levels Proxy (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('building_centroids_derivation', 'Building Centroids (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('lulc_class_map_derivation', 'LULC Class Map (Derived)', 'table', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('water_bodies_derivation', 'Water Bodies Canonical (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('open_surfaces_derivation', 'Open Surfaces Canonical (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('riparian_buffers_derivation', 'Riparian Buffers (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('built_up_derivation', 'Built-up Layer (Derived)', 'raster', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('vacant_land_derivation', 'Vacant Land Layer (Derived)', 'raster', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('flood_risk_derivation', 'Flood Risk Proxy (Derived)', 'raster', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb),
          ('population_proxy_derivation', 'Population Proxy (Derived)', 'vector', 'UrbanMor derived pipeline', NULL, 'sql_derivation', '{}'::jsonb)
        ON CONFLICT (source_code) DO UPDATE SET
          source_name = EXCLUDED.source_name,
          source_type = EXCLUDED.source_type,
          provider = EXCLUDED.provider,
          acquisition_method = EXCLUDED.acquisition_method;

        CREATE TABLE IF NOT EXISTS lulc.lulc_class_map (
          class_value INTEGER PRIMARY KEY,
          class_label TEXT NOT NULL,
          canonical_class TEXT NOT NULL,
          is_water BOOLEAN NOT NULL DEFAULT FALSE,
          is_green BOOLEAN NOT NULL DEFAULT FALSE,
          is_open_surface BOOLEAN NOT NULL DEFAULT FALSE,
          is_built_up BOOLEAN NOT NULL DEFAULT FALSE,
          is_residential_proxy BOOLEAN NOT NULL DEFAULT FALSE,
          is_vacant_candidate BOOLEAN NOT NULL DEFAULT FALSE,
          notes TEXT
        );

        INSERT INTO lulc.lulc_class_map (
          class_value, class_label, canonical_class, is_water, is_green, is_open_surface,
          is_built_up, is_residential_proxy, is_vacant_candidate, notes
        )
        VALUES
          (1,  'Water',               'water',            TRUE,  FALSE, FALSE, FALSE, FALSE, FALSE, 'ESRI 10m class'),
          (2,  'Trees',               'green_vegetation', FALSE, TRUE,  FALSE, FALSE, FALSE, FALSE, 'ESRI 10m class'),
          (3,  'Grass',               'grass',            FALSE, TRUE,  TRUE,  FALSE, FALSE, TRUE,  'ESRI 10m class'),
          (4,  'Flooded Vegetation',  'wet_vegetation',   FALSE, TRUE,  TRUE,  FALSE, FALSE, FALSE, 'ESRI 10m class'),
          (5,  'Crops',               'agriculture',      FALSE, TRUE,  FALSE, FALSE, FALSE, FALSE, 'ESRI 10m class'),
          (6,  'Scrub/Shrub',         'scrub_shrub',      FALSE, TRUE,  TRUE,  FALSE, FALSE, TRUE,  'ESRI 10m class'),
          (7,  'Built Area',          'built_up',         FALSE, FALSE, FALSE, TRUE,  TRUE,  FALSE, 'Residential is proxy via built class'),
          (8,  'Bare Ground',         'bare_ground',      FALSE, FALSE, TRUE,  FALSE, FALSE, TRUE,  'Candidate vacant'),
          (9,  'Snow/Ice',            'other',            FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, 'ESRI 10m class'),
          (10, 'Clouds',              'other',            FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, 'ESRI 10m class'),
          (11, 'Rangeland',           'open_rangeland',   FALSE, TRUE,  TRUE,  FALSE, FALSE, TRUE,  'Candidate vacant')
        ON CONFLICT (class_value) DO UPDATE SET
          class_label = EXCLUDED.class_label,
          canonical_class = EXCLUDED.canonical_class,
          is_water = EXCLUDED.is_water,
          is_green = EXCLUDED.is_green,
          is_open_surface = EXCLUDED.is_open_surface,
          is_built_up = EXCLUDED.is_built_up,
          is_residential_proxy = EXCLUDED.is_residential_proxy,
          is_vacant_candidate = EXCLUDED.is_vacant_candidate,
          notes = EXCLUDED.notes;
        """
    )


def build_network_blocks(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS transport.{c}_network_blocks CASCADE;
        CREATE TABLE transport.{c}_network_blocks AS
        WITH ward_boundaries AS (
          SELECT
            w.ward_uid,
            w.ward_id,
            ST_Transform(w.geom, 3857)::geometry(MultiPolygon, 3857) AS geom
          FROM boundaries.{c}_wards_normalized w
        ),
        roads AS (
          SELECT
            lower(COALESCE(r.highway, '')) AS highway,
            (ST_Dump(
              ST_CollectionExtract(
                ST_MakeValid(ST_Transform(r.geom, 3857)),
                2
              )
            )).geom::geometry(LineString, 3857) AS geom
          FROM transport.{c}_roads_normalized r
          WHERE r.geom IS NOT NULL
            AND NOT ST_IsEmpty(r.geom)
            AND lower(COALESCE(r.highway, '')) IN (
              'motorway', 'motorway_link',
              'trunk', 'trunk_link',
              'primary', 'primary_link',
              'secondary', 'secondary_link',
              'tertiary', 'tertiary_link',
              'unclassified', 'residential', 'living_street', 'service', 'road', 'busway'
            )
        ),
        ward_road_segments AS (
          SELECT
            w.ward_uid,
            w.ward_id,
            (ST_Dump(
              ST_CollectionExtract(
                ST_MakeValid(ST_Intersection(r.geom, w.geom)),
                2
              )
            )).geom::geometry(LineString, 3857) AS geom
          FROM ward_boundaries w
          JOIN roads r
            ON ST_Intersects(r.geom, w.geom)
        ),
        ward_boundary_edges AS (
          SELECT
            w.ward_uid,
            w.ward_id,
            (ST_Dump(
              ST_CollectionExtract(
                ST_Boundary(w.geom),
                2
              )
            )).geom::geometry(LineString, 3857) AS geom
          FROM ward_boundaries w
        ),
        ward_linework AS (
          SELECT
            ward_uid,
            ward_id,
            geom
          FROM ward_road_segments
          WHERE geom IS NOT NULL
            AND NOT ST_IsEmpty(geom)
            AND ST_Length(geom) > 1.0

          UNION ALL

          SELECT
            ward_uid,
            ward_id,
            geom
          FROM ward_boundary_edges
          WHERE geom IS NOT NULL
            AND NOT ST_IsEmpty(geom)
        ),
        ward_noded AS (
          SELECT
            ward_uid,
            ward_id,
            ST_Node(ST_UnaryUnion(ST_Collect(ST_SnapToGrid(geom, 0.5)))) AS geom
          FROM ward_linework
          GROUP BY ward_uid, ward_id
        ),
        ward_polygons AS (
          SELECT
            n.ward_uid,
            n.ward_id,
            (ST_Dump(
              ST_CollectionExtract(
                ST_MakeValid(ST_Polygonize(ARRAY[n.geom])),
                3
              )
            )).geom::geometry(Polygon, 3857) AS geom
          FROM ward_noded n
          WHERE n.geom IS NOT NULL
            AND NOT ST_IsEmpty(n.geom)
        ),
        cleaned AS (
          SELECT
            p.ward_uid,
            p.ward_id,
            p.geom
          FROM ward_polygons p
          WHERE p.geom IS NOT NULL
            AND NOT ST_IsEmpty(p.geom)
            AND ST_Area(p.geom) >= 25.0
        )
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS block_id,
          ward_uid,
          ward_id,
          'road_polygonize'::text AS derivation_method,
          ST_Transform(ST_Multi(geom), 4326)::geometry(MultiPolygon, 4326) AS geom,
          ST_Area(ST_Transform(geom, 4326)::geography)::double precision AS area_m2
        FROM cleaned
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom);

        ALTER TABLE transport.{c}_network_blocks ADD PRIMARY KEY (block_id);
        CREATE INDEX {c}_network_blocks_geom_idx ON transport.{c}_network_blocks USING GIST (geom);
        CREATE INDEX {c}_network_blocks_area_idx ON transport.{c}_network_blocks (area_m2);
        ANALYZE transport.{c}_network_blocks;
        """
    )


def build_routing_graph(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS transport.{c}_routing_graph_edges CASCADE;
        CREATE TABLE transport.{c}_routing_graph_edges AS
        WITH lines AS (
          SELECT
            '{c}'::text AS city,
            r.source_layer,
            ST_SetSRID(
              (ST_Dump(ST_CollectionExtract(ST_MakeValid(r.geom), 2))).geom,
              4326
            )::geometry(LineString, 4326) AS geom
          FROM transport.{c}_roads_normalized r
          WHERE r.geom IS NOT NULL
            AND NOT ST_IsEmpty(r.geom)
        ),
        filtered AS (
          SELECT city, source_layer, geom
          FROM lines
          WHERE ST_Length(geom::geography) > 1
        )
        SELECT
          row_number() OVER ()::bigint AS edge_id,
          city,
          source_layer,
          md5(ST_AsText(ST_SnapToGrid(ST_StartPoint(geom), 0.00001))) AS source_key,
          md5(ST_AsText(ST_SnapToGrid(ST_EndPoint(geom), 0.00001))) AS target_key,
          ST_Length(geom::geography)::double precision AS length_m,
          geom
        FROM filtered;

        ALTER TABLE transport.{c}_routing_graph_edges ADD PRIMARY KEY (edge_id);
        CREATE INDEX {c}_routing_edges_geom_idx ON transport.{c}_routing_graph_edges USING GIST (geom);
        CREATE INDEX {c}_routing_edges_source_key_idx ON transport.{c}_routing_graph_edges (source_key);
        CREATE INDEX {c}_routing_edges_target_key_idx ON transport.{c}_routing_graph_edges (target_key);

        DROP TABLE IF EXISTS transport.{c}_routing_graph_vertices CASCADE;
        CREATE TABLE transport.{c}_routing_graph_vertices AS
        WITH pts AS (
          SELECT source_key AS vertex_key, ST_SnapToGrid(ST_StartPoint(geom), 0.00001)::geometry(Point, 4326) AS geom
          FROM transport.{c}_routing_graph_edges
          UNION ALL
          SELECT target_key AS vertex_key, ST_SnapToGrid(ST_EndPoint(geom), 0.00001)::geometry(Point, 4326) AS geom
          FROM transport.{c}_routing_graph_edges
        ),
        uniq AS (
          SELECT vertex_key, ST_Centroid(ST_Collect(geom))::geometry(Point, 4326) AS geom
          FROM pts
          GROUP BY vertex_key
        )
        SELECT
          '{c}'::text AS city,
          row_number() OVER (ORDER BY vertex_key)::bigint AS vertex_id,
          vertex_key,
          geom
        FROM uniq;

        ALTER TABLE transport.{c}_routing_graph_vertices ADD PRIMARY KEY (vertex_id);
        CREATE UNIQUE INDEX {c}_routing_vertices_key_idx ON transport.{c}_routing_graph_vertices (vertex_key);
        CREATE INDEX {c}_routing_vertices_geom_idx ON transport.{c}_routing_graph_vertices USING GIST (geom);

        ALTER TABLE transport.{c}_routing_graph_edges ADD COLUMN source_vertex_id BIGINT;
        ALTER TABLE transport.{c}_routing_graph_edges ADD COLUMN target_vertex_id BIGINT;

        UPDATE transport.{c}_routing_graph_edges e
        SET source_vertex_id = v.vertex_id
        FROM transport.{c}_routing_graph_vertices v
        WHERE e.source_key = v.vertex_key;

        UPDATE transport.{c}_routing_graph_edges e
        SET target_vertex_id = v.vertex_id
        FROM transport.{c}_routing_graph_vertices v
        WHERE e.target_key = v.vertex_key;

        ALTER TABLE transport.{c}_routing_graph_edges ALTER COLUMN source_vertex_id SET NOT NULL;
        ALTER TABLE transport.{c}_routing_graph_edges ALTER COLUMN target_vertex_id SET NOT NULL;

        CREATE INDEX {c}_routing_edges_source_vid_idx ON transport.{c}_routing_graph_edges (source_vertex_id);
        CREATE INDEX {c}_routing_edges_target_vid_idx ON transport.{c}_routing_graph_edges (target_vertex_id);
        ANALYZE transport.{c}_routing_graph_edges;
        ANALYZE transport.{c}_routing_graph_vertices;
        """
    )


def build_transit_points(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS transport.{c}_transit_points CASCADE;
        CREATE TABLE transport.{c}_transit_points AS
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS transit_point_id,
          t.source_layer,
          ST_PointOnSurface(
            CASE
              WHEN ST_SRID(t.geom) = 4326 THEN ST_MakeValid(t.geom)
              WHEN ST_SRID(t.geom) = 0 THEN ST_MakeValid(ST_SetSRID(t.geom, 4326))
              ELSE ST_MakeValid(ST_Transform(t.geom, 4326))
            END
          )::geometry(Point, 4326) AS geom
        FROM transport.{c}_transit_normalized t
        WHERE t.geom IS NOT NULL
          AND NOT ST_IsEmpty(t.geom);

        ALTER TABLE transport.{c}_transit_points ADD PRIMARY KEY (transit_point_id);
        CREATE INDEX {c}_transit_points_geom_idx ON transport.{c}_transit_points USING GIST (geom);
        CREATE INDEX {c}_transit_points_src_idx ON transport.{c}_transit_points (source_layer);
        ANALYZE transport.{c}_transit_points;
        """
    )


def build_pedestrian_enriched_roads(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS transport.{c}_roads_pedestrian_enriched CASCADE;
        CREATE TABLE transport.{c}_roads_pedestrian_enriched AS
        SELECT
          row_number() OVER ()::bigint AS ped_id,
          r.*,
          CASE
            WHEN r.source_layer = 'walkability_access' THEN TRUE
            WHEN lower(COALESCE(r.highway, '')) IN (
              'footway', 'pedestrian', 'path', 'steps', 'living_street', 'cycleway', 'track'
            ) THEN TRUE
            WHEN lower(COALESCE(r.foot, '')) IN ('yes', 'designated', 'permissive') THEN TRUE
            WHEN lower(COALESCE(r.bicycle, '')) IN ('yes', 'designated') THEN TRUE
            ELSE FALSE
          END AS is_pedestrian_link,
          CASE
            WHEN r.source_layer = 'walkability_access' THEN 'high'
            WHEN lower(COALESCE(r.highway, '')) IN ('footway', 'pedestrian', 'steps') THEN 'high'
            WHEN lower(COALESCE(r.foot, '')) IN ('yes', 'designated') THEN 'medium'
            ELSE 'low'
          END AS pedestrian_confidence,
          CASE
            WHEN r.source_layer = 'walkability_access' THEN 1.0
            WHEN lower(COALESCE(r.highway, '')) IN ('footway', 'pedestrian', 'steps') THEN 0.9
            WHEN lower(COALESCE(r.foot, '')) IN ('yes', 'designated', 'permissive') THEN 0.7
            WHEN lower(COALESCE(r.bicycle, '')) IN ('yes', 'designated') THEN 0.5
            ELSE 0.1
          END::double precision AS pedestrian_score
        FROM transport.{c}_roads_normalized r;

        ALTER TABLE transport.{c}_roads_pedestrian_enriched ADD PRIMARY KEY (ped_id);
        CREATE INDEX {c}_ped_roads_geom_idx ON transport.{c}_roads_pedestrian_enriched USING GIST (geom);
        CREATE INDEX {c}_ped_roads_flag_idx ON transport.{c}_roads_pedestrian_enriched (is_pedestrian_link);
        CREATE INDEX {c}_ped_roads_layer_idx ON transport.{c}_roads_pedestrian_enriched (source_layer);
        ANALYZE transport.{c}_roads_pedestrian_enriched;
        """
    )


def build_building_levels_and_centroids(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS buildings.{c}_building_levels_enriched CASCADE;
        CREATE TABLE buildings.{c}_building_levels_enriched AS
        WITH thresholds AS (
          SELECT
            percentile_cont(0.25) WITHIN GROUP (ORDER BY footprint_area_m2) AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY footprint_area_m2) AS p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY footprint_area_m2) AS p75
          FROM buildings.{c}_buildings_normalized
          WHERE footprint_area_m2 IS NOT NULL
            AND footprint_area_m2 > 0
        )
        SELECT
          row_number() OVER ()::bigint AS bldg_id,
          b.*,
          NULL::double precision AS levels_observed,
          CASE
            WHEN b.footprint_area_m2 IS NULL THEN 2
            WHEN b.footprint_area_m2 <= COALESCE(t.p25, 80.0) THEN 1
            WHEN b.footprint_area_m2 <= COALESCE(t.p50, 180.0) THEN 2
            WHEN b.footprint_area_m2 <= COALESCE(t.p75, 360.0) THEN 3
            ELSE 4
          END::double precision AS levels_estimated,
          'heuristic_area_quantile'::text AS levels_source,
          CASE
            WHEN b.footprint_area_m2 IS NULL THEN 'low'
            ELSE 'low'
          END::text AS levels_confidence,
          (
            COALESCE(b.footprint_area_m2, 0) *
            CASE
              WHEN b.footprint_area_m2 IS NULL THEN 2
              WHEN b.footprint_area_m2 <= COALESCE(t.p25, 80.0) THEN 1
              WHEN b.footprint_area_m2 <= COALESCE(t.p50, 180.0) THEN 2
              WHEN b.footprint_area_m2 <= COALESCE(t.p75, 360.0) THEN 3
              ELSE 4
            END
          )::double precision AS floor_area_proxy_m2,
          ST_SetSRID(b.geom, 4326)::geometry(Geometry, 4326) AS geom_fixed
        FROM buildings.{c}_buildings_normalized b
        CROSS JOIN thresholds t;

        ALTER TABLE buildings.{c}_building_levels_enriched DROP COLUMN geom;
        ALTER TABLE buildings.{c}_building_levels_enriched RENAME COLUMN geom_fixed TO geom;

        ALTER TABLE buildings.{c}_building_levels_enriched ADD PRIMARY KEY (bldg_id);
        CREATE INDEX {c}_bldg_levels_geom_idx ON buildings.{c}_building_levels_enriched USING GIST (geom);
        CREATE INDEX {c}_bldg_levels_ward_idx ON buildings.{c}_building_levels_enriched (ward_ref);
        CREATE INDEX {c}_bldg_levels_source_idx ON buildings.{c}_building_levels_enriched (source_feature_id);
        ANALYZE buildings.{c}_building_levels_enriched;

        DROP TABLE IF EXISTS buildings.{c}_building_centroids CASCADE;
        CREATE TABLE buildings.{c}_building_centroids AS
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS centroid_id,
          source_feature_id,
          ward_ref,
          floor_area_proxy_m2,
          (floor_area_proxy_m2 / 35.0)::double precision AS population_proxy,
          ST_PointOnSurface(geom)::geometry(Point, 4326) AS geom
        FROM buildings.{c}_building_levels_enriched
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom);

        ALTER TABLE buildings.{c}_building_centroids ADD PRIMARY KEY (centroid_id);
        CREATE INDEX {c}_bldg_centroids_geom_idx ON buildings.{c}_building_centroids USING GIST (geom);
        CREATE INDEX {c}_bldg_centroids_ward_idx ON buildings.{c}_building_centroids (ward_ref);
        ANALYZE buildings.{c}_building_centroids;
        """
    )


def build_builtup_vacant_flood_rasters(city: str) -> None:
    c = ident(city)
    threshold = query_scalar(
        f"""
        WITH s AS (
          SELECT ST_SummaryStatsAgg(rast, 1, TRUE) AS st
          FROM dem.{c}_dem_normalized
        )
        SELECT ((st).min + (((st).max - (st).min) * 0.25))::double precision
        FROM s;
        """
    )
    if not threshold:
        threshold = "0"

    run_sql(
        f"""
        DROP TABLE IF EXISTS lulc.{c}_built_up_layer CASCADE;
        CREATE TABLE lulc.{c}_built_up_layer AS
        SELECT
          1::bigint AS rid,
          ST_MapAlgebra(
            rast,
            1,
            '8BUI',
            'CASE WHEN [rast] = 7 THEN 1 ELSE 0 END',
            0
          ) AS rast
        FROM lulc.{c}_lulc_normalized;
        ALTER TABLE lulc.{c}_built_up_layer ADD PRIMARY KEY (rid);
        CREATE INDEX {c}_built_up_rast_idx ON lulc.{c}_built_up_layer USING GIST (ST_ConvexHull(rast));
        ANALYZE lulc.{c}_built_up_layer;

        DROP TABLE IF EXISTS green.{c}_vacant_land CASCADE;
        CREATE TABLE green.{c}_vacant_land AS
        SELECT
          1::bigint AS rid,
          ST_MapAlgebra(
            rast,
            1,
            '8BUI',
            'CASE WHEN ([rast] = 3 OR [rast] = 6 OR [rast] = 8 OR [rast] = 11) THEN 1 ELSE 0 END',
            0
          ) AS rast
        FROM lulc.{c}_lulc_normalized;
        ALTER TABLE green.{c}_vacant_land ADD PRIMARY KEY (rid);
        CREATE INDEX {c}_vacant_land_rast_idx ON green.{c}_vacant_land USING GIST (ST_ConvexHull(rast));
        ANALYZE green.{c}_vacant_land;

        DROP TABLE IF EXISTS dem.{c}_flood_risk_proxy CASCADE;
        CREATE TABLE dem.{c}_flood_risk_proxy AS
        WITH paired AS (
          SELECT
            row_number() OVER ()::bigint AS rid,
            ST_MapAlgebra(
              d.rast,
              1,
              CASE
                WHEN ST_SameAlignment(d.rast, s.rast) THEN s.rast
                ELSE ST_Resample(s.rast, d.rast)
              END,
              1,
              'CASE WHEN ([rast1] <= {threshold} AND [rast2] <= 5) THEN 1 ELSE 0 END',
              '8BUI',
              'INTERSECTION'
            ) AS rast
          FROM dem.{c}_dem_normalized d
          JOIN dem.{c}_slope_deg_normalized s
            ON ST_Intersects(d.rast, s.rast)
        )
        SELECT rid, rast
        FROM paired
        WHERE rast IS NOT NULL;
        ALTER TABLE dem.{c}_flood_risk_proxy ADD PRIMARY KEY (rid);
        CREATE INDEX {c}_flood_risk_rast_idx ON dem.{c}_flood_risk_proxy USING GIST (ST_ConvexHull(rast));
        ANALYZE dem.{c}_flood_risk_proxy;
        """
    )


def build_water_open_riparian(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS green.{c}_water_bodies_canonical CASCADE;
        CREATE TABLE green.{c}_water_bodies_canonical AS
        WITH osm_water AS (
          SELECT
            CASE
              WHEN GeometryType(geom) IN ('LINESTRING', 'MULTILINESTRING')
                THEN ST_Buffer(ST_Transform(ST_SetSRID(geom, 4326), 3857), 12)
              WHEN GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON')
                THEN ST_Transform(ST_SetSRID(geom, 4326), 3857)
              ELSE NULL
            END AS geom_3857
          FROM green.{c}_open_spaces_normalized
          WHERE source_layer IN ('water_wetlands', 'waterways_linear')
        ),
        lulc_water AS (
          SELECT ST_Transform(dp.geom, 3857) AS geom_3857
          FROM lulc.{c}_lulc_normalized l,
               LATERAL ST_DumpAsPolygons(l.rast, 1, TRUE) AS dp
          WHERE dp.val::int = 1
        ),
        all_water AS (
          SELECT geom_3857 FROM osm_water WHERE geom_3857 IS NOT NULL
          UNION ALL
          SELECT geom_3857 FROM lulc_water WHERE geom_3857 IS NOT NULL
        ),
        merged AS (
          SELECT ST_UnaryUnion(ST_Collect(ST_MakeValid(geom_3857))) AS geom
          FROM all_water
        ),
        parts AS (
          SELECT (ST_Dump(ST_CollectionExtract(ST_MakeValid(geom), 3))).geom AS geom
          FROM merged
        )
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS water_id,
          ST_Transform(ST_Multi(geom), 4326)::geometry(MultiPolygon, 4326) AS geom,
          ST_Area(geom)::double precision AS area_m2
        FROM parts
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
          AND ST_Area(geom) >= 25;

        UPDATE green.{c}_water_bodies_canonical
        SET geom = ST_Multi(
          ST_CollectionExtract(ST_MakeValid(geom), 3)
        )::geometry(MultiPolygon, 4326);

        DELETE FROM green.{c}_water_bodies_canonical
        WHERE geom IS NULL
          OR ST_IsEmpty(geom)
          OR NOT ST_IsValid(geom);

        ALTER TABLE green.{c}_water_bodies_canonical ADD PRIMARY KEY (water_id);
        CREATE INDEX {c}_water_canonical_geom_idx ON green.{c}_water_bodies_canonical USING GIST (geom);
        ANALYZE green.{c}_water_bodies_canonical;

        DROP TABLE IF EXISTS green.{c}_open_surfaces CASCADE;
        CREATE TABLE green.{c}_open_surfaces AS
        WITH osm_open AS (
          SELECT
            source_layer,
            ST_Transform(
              ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_SetSRID(geom, 4326)), 3)),
              3857
            ) AS geom_3857
          FROM green.{c}_open_spaces_normalized
          WHERE source_layer IN (
            'open_barren_sands_rocky',
            'other_open_landuse',
            'open_space_master',
            'plazas_open_paved',
            'sports_play_open',
            'coastal_marine_edges'
          )
            AND GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON')
        ),
        lulc_open AS (
          SELECT
            'lulc_open_surface'::text AS source_layer,
            ST_Transform(dp.geom, 3857) AS geom_3857
          FROM lulc.{c}_lulc_normalized l,
               LATERAL ST_DumpAsPolygons(l.rast, 1, TRUE) AS dp
          WHERE dp.val::int IN (3, 6, 8, 11)
        ),
        all_open AS (
          SELECT source_layer, geom_3857 FROM osm_open
          UNION ALL
          SELECT source_layer, geom_3857 FROM lulc_open
        ),
        cleaned AS (
          SELECT source_layer, (ST_Dump(ST_CollectionExtract(ST_MakeValid(geom_3857), 3))).geom AS geom
          FROM all_open
          WHERE geom_3857 IS NOT NULL
        )
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS open_id,
          source_layer,
          ST_Transform(ST_Multi(geom), 4326)::geometry(MultiPolygon, 4326) AS geom,
          ST_Area(geom)::double precision AS area_m2
        FROM cleaned
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
          AND ST_Area(geom) >= 25;

        ALTER TABLE green.{c}_open_surfaces ADD PRIMARY KEY (open_id);
        CREATE INDEX {c}_open_surfaces_geom_idx ON green.{c}_open_surfaces USING GIST (geom);
        CREATE INDEX {c}_open_surfaces_src_idx ON green.{c}_open_surfaces (source_layer);
        ANALYZE green.{c}_open_surfaces;

        DROP TABLE IF EXISTS green.{c}_riparian_buffers CASCADE;
        CREATE TABLE green.{c}_riparian_buffers AS
        WITH water AS (
          SELECT ST_Transform(geom, 3857) AS geom
          FROM green.{c}_water_bodies_canonical
        ),
        buf AS (
          SELECT ST_Buffer(geom, 30) AS geom
          FROM water
        ),
        merged AS (
          SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
          FROM buf
        ),
        parts AS (
          SELECT (ST_Dump(ST_CollectionExtract(ST_MakeValid(geom), 3))).geom AS geom
          FROM merged
        )
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS buffer_id,
          ST_Transform(ST_Multi(geom), 4326)::geometry(MultiPolygon, 4326) AS geom,
          ST_Area(geom)::double precision AS area_m2
        FROM parts
        WHERE geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
          AND ST_Area(geom) >= 100;

        ALTER TABLE green.{c}_riparian_buffers ADD PRIMARY KEY (buffer_id);
        CREATE INDEX {c}_riparian_buffers_geom_idx ON green.{c}_riparian_buffers USING GIST (geom);
        ANALYZE green.{c}_riparian_buffers;
        """
    )


def build_population_proxy(city: str) -> None:
    c = ident(city)
    run_sql(
        f"""
        DROP TABLE IF EXISTS metrics.{c}_population_proxy CASCADE;
        CREATE TABLE metrics.{c}_population_proxy AS
        SELECT
          '{c}'::text AS city,
          row_number() OVER ()::bigint AS pop_id,
          w.ward_id,
          w.ward_name,
          COALESCE(sum(cn.population_proxy), 0)::double precision AS population_proxy,
          (ST_Area(w.geom::geography) / 1000000.0)::double precision AS area_sqkm,
          CASE
            WHEN ST_Area(w.geom::geography) > 0 THEN
              COALESCE(sum(cn.population_proxy), 0) / (ST_Area(w.geom::geography) / 1000000.0)
            ELSE NULL
          END::double precision AS population_density_proxy,
          w.geom::geometry(MultiPolygon, 4326) AS geom
        FROM boundaries.{c}_wards_normalized w
        LEFT JOIN buildings.{c}_building_centroids cn
          ON ST_Intersects(cn.geom, w.geom)
        GROUP BY w.ward_id, w.ward_name, w.geom;

        ALTER TABLE metrics.{c}_population_proxy ADD PRIMARY KEY (pop_id);
        CREATE INDEX {c}_population_proxy_geom_idx ON metrics.{c}_population_proxy USING GIST (geom);
        CREATE INDEX {c}_population_proxy_ward_idx ON metrics.{c}_population_proxy (ward_id);
        ANALYZE metrics.{c}_population_proxy;
        """
    )


def upsert_layer_registry() -> None:
    run_sql(
        """
        WITH candidates AS (
          -- LULC class map (non-spatial table)
          SELECT
            'lulc'::text AS schema_name,
            'lulc_class_map'::text AS table_name,
            'lulc_class_map'::text AS layer_family,
            'table'::text AS data_kind,
            'lulc_class_map_derivation'::text AS source_code,
            NULL::text AS city,
            'table'::text AS geometry_type,
            NULL::text AS declared_crs
          UNION ALL
          SELECT
            n.nspname, c.relname,
            CASE
              WHEN c.relname LIKE '%_network_blocks' THEN 'network_blocks'
              WHEN c.relname LIKE '%_routing_graph_edges' THEN 'routing_graph_edges'
              WHEN c.relname LIKE '%_routing_graph_vertices' THEN 'routing_graph_vertices'
              WHEN c.relname LIKE '%_transit_points' THEN 'transit_points'
              WHEN c.relname LIKE '%_roads_pedestrian_enriched' THEN 'footway_tags'
              WHEN c.relname LIKE '%_building_levels_enriched' THEN 'building_levels_tags'
              WHEN c.relname LIKE '%_building_centroids' THEN 'building_centroids'
              WHEN c.relname LIKE '%_water_bodies_canonical' THEN 'water_bodies'
              WHEN c.relname LIKE '%_open_surfaces' THEN 'open_surfaces'
              WHEN c.relname LIKE '%_riparian_buffers' THEN 'buffers'
              WHEN c.relname LIKE '%_built_up_layer' THEN 'built_up'
              WHEN c.relname LIKE '%_vacant_land' THEN 'vacant_land'
              WHEN c.relname LIKE '%_flood_risk_proxy' THEN 'flood_risk'
              WHEN c.relname LIKE '%_population_proxy' THEN 'population_proxy'
              ELSE 'derived_layer'
            END AS layer_family,
            CASE
              WHEN c.relname LIKE '%_built_up_layer' OR c.relname LIKE '%_vacant_land' OR c.relname LIKE '%_flood_risk_proxy'
                THEN 'raster'
              ELSE 'vector'
            END AS data_kind,
            CASE
              WHEN c.relname LIKE '%_network_blocks' THEN 'network_blocks_derivation'
              WHEN c.relname LIKE '%_routing_graph_edges' OR c.relname LIKE '%_routing_graph_vertices' THEN 'routing_graph_derivation'
              WHEN c.relname LIKE '%_transit_points' THEN 'transit_points_derivation'
              WHEN c.relname LIKE '%_roads_pedestrian_enriched' THEN 'pedestrian_enrichment_derivation'
              WHEN c.relname LIKE '%_building_levels_enriched' THEN 'building_levels_derivation'
              WHEN c.relname LIKE '%_building_centroids' THEN 'building_centroids_derivation'
              WHEN c.relname LIKE '%_water_bodies_canonical' THEN 'water_bodies_derivation'
              WHEN c.relname LIKE '%_open_surfaces' THEN 'open_surfaces_derivation'
              WHEN c.relname LIKE '%_riparian_buffers' THEN 'riparian_buffers_derivation'
              WHEN c.relname LIKE '%_built_up_layer' THEN 'built_up_derivation'
              WHEN c.relname LIKE '%_vacant_land' THEN 'vacant_land_derivation'
              WHEN c.relname LIKE '%_flood_risk_proxy' THEN 'flood_risk_derivation'
              WHEN c.relname LIKE '%_population_proxy' THEN 'population_proxy_derivation'
              ELSE 'network_blocks_derivation'
            END AS source_code,
            split_part(c.relname, '_', 1) AS city,
            COALESCE(gc.type, rc.pixel_types[1], 'derived')::text AS geometry_type,
            CASE
              WHEN gc.srid > 0 THEN 'EPSG:' || gc.srid::text
              WHEN rc.srid > 0 THEN 'EPSG:' || rc.srid::text
              ELSE NULL
            END AS declared_crs
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN geometry_columns gc
            ON gc.f_table_schema = n.nspname
           AND gc.f_table_name = c.relname
           AND gc.f_geometry_column = 'geom'
          LEFT JOIN raster_columns rc
            ON rc.r_table_schema = n.nspname
           AND rc.r_table_name = c.relname
           AND rc.r_raster_column = 'rast'
          WHERE c.relkind = 'r'
            AND (
              (n.nspname = 'transport' AND (c.relname LIKE '%_network_blocks' OR c.relname LIKE '%_routing_graph_edges' OR c.relname LIKE '%_routing_graph_vertices' OR c.relname LIKE '%_transit_points' OR c.relname LIKE '%_roads_pedestrian_enriched'))
              OR (n.nspname = 'buildings' AND (c.relname LIKE '%_building_levels_enriched' OR c.relname LIKE '%_building_centroids'))
              OR (n.nspname = 'green' AND (c.relname LIKE '%_water_bodies_canonical' OR c.relname LIKE '%_open_surfaces' OR c.relname LIKE '%_riparian_buffers' OR c.relname LIKE '%_vacant_land'))
              OR (n.nspname = 'dem' AND c.relname LIKE '%_flood_risk_proxy')
              OR (n.nspname = 'lulc' AND c.relname LIKE '%_built_up_layer')
              OR (n.nspname = 'metrics' AND c.relname LIKE '%_population_proxy')
            )
        )
        INSERT INTO meta.layer_registry (
          layer_key, layer_name, layer_family, data_kind, source_id, source_layer_name, source_path,
          canonical_schema, canonical_table, file_format, geometry_type, declared_crs, city,
          readiness_status, is_canonical, row_count, last_refresh_at, validation_state, provenance, notes
        )
        SELECT
          c.schema_name || '.' || c.table_name AS layer_key,
          c.table_name AS layer_name,
          c.layer_family,
          c.data_kind,
          (SELECT source_id FROM meta.source_registry s WHERE s.source_code = c.source_code),
          NULL::text AS source_layer_name,
          'db://' || c.schema_name || '.' || c.table_name AS source_path,
          c.schema_name AS canonical_schema,
          c.table_name AS canonical_table,
          'postgis' AS file_format,
          c.geometry_type,
          c.declared_crs,
          c.city,
          'ready' AS readiness_status,
          TRUE AS is_canonical,
          COALESCE(st.n_live_tup::bigint, 0) AS row_count,
          now() AS last_refresh_at,
          '{"load_status":"loaded"}'::jsonb AS validation_state,
          jsonb_build_object('pipeline', 'build_metric_prereq_layers') AS provenance,
          NULL::text AS notes
        FROM candidates c
        LEFT JOIN pg_stat_all_tables st
          ON st.schemaname = c.schema_name
         AND st.relname = c.table_name
        ON CONFLICT (layer_key) DO UPDATE SET
          layer_name = EXCLUDED.layer_name,
          layer_family = EXCLUDED.layer_family,
          data_kind = EXCLUDED.data_kind,
          source_id = EXCLUDED.source_id,
          source_path = EXCLUDED.source_path,
          canonical_schema = EXCLUDED.canonical_schema,
          canonical_table = EXCLUDED.canonical_table,
          file_format = EXCLUDED.file_format,
          geometry_type = EXCLUDED.geometry_type,
          declared_crs = EXCLUDED.declared_crs,
          city = EXCLUDED.city,
          readiness_status = EXCLUDED.readiness_status,
          is_canonical = EXCLUDED.is_canonical,
          row_count = EXCLUDED.row_count,
          last_refresh_at = EXCLUDED.last_refresh_at,
          validation_state = EXCLUDED.validation_state,
          provenance = EXCLUDED.provenance;
        """
    )

    run_sql(
        """
        INSERT INTO meta.pipeline_runs (
          pipeline_name, run_type, run_status, started_at, finished_at, initiated_by,
          city_scope, input_layers, output_layers, run_params, metrics_summary, artifacts,
          warning_count, error_count, error_log
        )
        VALUES (
          'build_metric_prereq_layers',
          'manual',
          'success',
          now(),
          now(),
          current_user,
          ARRAY['ahmedabad','bengaluru','chandigarh','chennai','delhi','kolkata','mumbai']::text[],
          ARRAY[]::text[],
          ARRAY[]::text[],
          '{}'::jsonb,
          '{}'::jsonb,
          '[{"type":"sql_pipeline","name":"build_metric_prereq_layers"}]'::jsonb,
          0,
          0,
          NULL
        );
        """
    )


def main() -> int:
    print("Bootstrapping source registry + LULC class map...")
    bootstrap_sources_and_class_map()

    for city in CITIES:
        print(f"[{city}] network blocks...")
        build_network_blocks(city)
        print(f"[{city}] routing graph...")
        build_routing_graph(city)
        print(f"[{city}] transit points...")
        build_transit_points(city)
        print(f"[{city}] pedestrian roads...")
        build_pedestrian_enriched_roads(city)
        print(f"[{city}] building levels + centroids...")
        build_building_levels_and_centroids(city)
        print(f"[{city}] built-up/vacant/flood rasters...")
        build_builtup_vacant_flood_rasters(city)
        print(f"[{city}] water/open/riparian...")
        build_water_open_riparian(city)
        print(f"[{city}] population proxy...")
        build_population_proxy(city)

    print("Upserting meta.layer_registry entries...")
    upsert_layer_registry()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
