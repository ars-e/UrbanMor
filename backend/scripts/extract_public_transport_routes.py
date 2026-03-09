#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import requests
from shapely.geometry import LineString, MultiLineString
from shapely.ops import unary_union

ROOT = Path('/Users/ars-e/projects/UrbanMor')
OUT = ROOT / 'output'

WARD_CANON_DIR = OUT / 'ward_bound' / 'canonical'
TRANSPORT_PARQUET_DIR = OUT / 'transport_network_osm' / 'parquet'
TRANSPORT_GPKG_DIR = OUT / 'transport_network_osm' / 'gpkg'
TRANSPORT_MANIFEST = OUT / 'transport_network_osm' / 'transport_manifest.csv'

CITIES = [
    'ahmedabad',
    'bengaluru',
    'chandigarh',
    'chennai',
    'delhi',
    'kolkata',
    'mumbai',
]

ROUTE_MODES = ['bus', 'trolleybus', 'tram', 'subway', 'train', 'light_rail', 'monorail', 'ferry']

OVERPASS_ENDPOINTS = [
    'https://overpass-api.de/api/interpreter',
    'https://overpass.kumi.systems/api/interpreter',
]

BASE_COLUMNS = [
    'city',
    'source',
    'osm_element',
    'osm_id',
    'name',
    'ref',
    'operator',
    'highway',
    'railway',
    'station',
    'subway',
    'light_rail',
    'train',
    'public_transport',
    'route',
    'network',
    'amenity',
    'shelter',
    'barrier',
    'access',
    'motor_vehicle',
    'bicycle',
    'foot',
    'segregated',
    'sidewalk',
    'footway',
    'crossing',
    'kerb',
    'tactile_paving',
    'surface',
    'smoothness',
    'lit',
    'incline',
    'width',
    'traffic_calming',
    'junction',
    'maxspeed',
    'lanes',
    'oneway',
    'bridge',
    'tunnel',
    'layer',
    'construction',
    'disused',
    'service',
    'man_made',
    'entrance',
    'geometry',
]


@dataclass
class CityResult:
    city: str
    relations_found: int
    features_written: int
    endpoint: str
    status: str
    message: str


def _query_overpass_city(minx: float, miny: float, maxx: float, maxy: float) -> tuple[dict, str]:
    bbox = f"{miny},{minx},{maxy},{maxx}"  # south,west,north,east
    query = f'''[out:json][timeout:300];
(
  relation["type"="route"]["route"~"^({'|'.join(ROUTE_MODES)})$"]({bbox});
);
(._;>;);
out body geom;'''

    last_err = None
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(3):
            try:
                resp = requests.post(endpoint, data={'data': query}, timeout=360)
                resp.raise_for_status()
                return resp.json(), endpoint
            except Exception as e:
                last_err = f"{endpoint} attempt={attempt+1} error={type(e).__name__}:{e}"
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(last_err or 'overpass query failed')


def _extract_lineal(geom):
    if geom is None or geom.is_empty:
        return None
    gt = geom.geom_type
    if gt == 'LineString':
        return MultiLineString([geom])
    if gt == 'MultiLineString':
        return geom
    if gt == 'GeometryCollection':
        lines = []
        for g in geom.geoms:
            if g.geom_type == 'LineString':
                lines.append(g)
            elif g.geom_type == 'MultiLineString':
                lines.extend(list(g.geoms))
        if not lines:
            return None
        return MultiLineString(lines)
    return None


def _relation_to_geometry(rel: dict, ways: dict[int, dict], city_poly):
    lines = []
    for m in rel.get('members', []):
        if m.get('type') != 'way':
            continue
        way = ways.get(m.get('ref'))
        if not way:
            continue
        coords = way.get('geometry')
        if not coords or len(coords) < 2:
            continue
        line = LineString([(pt['lon'], pt['lat']) for pt in coords])
        if line.is_empty or line.length == 0:
            continue
        lines.append(line)

    if not lines:
        return None

    merged = unary_union(lines)
    clipped = merged.intersection(city_poly)
    return _extract_lineal(clipped)


def _drop_gpkg_layer_if_exists(gpkg_path: Path, layer_name: str) -> None:
    if not gpkg_path.exists():
        return

    con = sqlite3.connect(gpkg_path)
    cur = con.cursor()

    row = cur.execute(
        "SELECT table_name FROM gpkg_contents WHERE table_name=?",
        (layer_name,),
    ).fetchone()

    if row:
        cur.execute("DELETE FROM gpkg_contents WHERE table_name=?", (layer_name,))
        cur.execute("DELETE FROM gpkg_geometry_columns WHERE table_name=?", (layer_name,))
        cur.execute("DELETE FROM gpkg_ogr_contents WHERE table_name=?", (layer_name,))

        cur.execute(f'DROP TABLE IF EXISTS "{layer_name}"')
        cur.execute(f'DROP TABLE IF EXISTS "rtree_{layer_name}_geometry"')
        cur.execute(f'DROP TABLE IF EXISTS "rtree_{layer_name}_geometry_rowid"')
        cur.execute(f'DROP TABLE IF EXISTS "rtree_{layer_name}_geometry_node"')
        cur.execute(f'DROP TABLE IF EXISTS "rtree_{layer_name}_geometry_parent"')

    con.commit()
    con.close()


def _write_city_outputs(city: str, gdf: gpd.GeoDataFrame) -> None:
    city_parquet = TRANSPORT_PARQUET_DIR / city / 'public_transport_routes.parquet'
    city_parquet.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(city_parquet, index=False)

    city_gpkg = TRANSPORT_GPKG_DIR / f'{city}_transport_network.gpkg'
    _drop_gpkg_layer_if_exists(city_gpkg, 'public_transport_routes')
    gdf.to_file(city_gpkg, layer='public_transport_routes', driver='GPKG', mode='a')


def _write_all_cities_output(gdf: gpd.GeoDataFrame) -> None:
    all_parquet = TRANSPORT_PARQUET_DIR / 'all_cities' / 'public_transport_routes.parquet'
    all_parquet.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(all_parquet, index=False)

    all_gpkg = TRANSPORT_GPKG_DIR / 'all_cities_transport_network.gpkg'
    _drop_gpkg_layer_if_exists(all_gpkg, 'public_transport_routes')
    gdf.to_file(all_gpkg, layer='public_transport_routes', driver='GPKG', mode='a')


def _update_manifest(city_counts: dict[str, int], all_count: int) -> None:
    with TRANSPORT_MANIFEST.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = reader.fieldnames

    for row in rows:
        if row.get('layer') != 'public_transport_routes':
            continue
        city = row.get('city')
        if city in city_counts:
            row['feature_count'] = str(city_counts[city])
        elif city == 'all_cities':
            row['feature_count'] = str(all_count)

    with TRANSPORT_MANIFEST.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def extract_city(city: str) -> tuple[gpd.GeoDataFrame, CityResult]:
    ward_path = WARD_CANON_DIR / f'{city}_wards_canonical.geojson'
    wards = gpd.read_file(ward_path)
    city_poly = wards.geometry.union_all()

    minx, miny, maxx, maxy = city_poly.bounds
    data, endpoint = _query_overpass_city(minx, miny, maxx, maxy)

    elements = data.get('elements', [])
    ways = {e['id']: e for e in elements if e.get('type') == 'way'}
    relations = [e for e in elements if e.get('type') == 'relation']

    rows = []
    for rel in relations:
        tags = rel.get('tags', {})
        route_mode = tags.get('route')
        if route_mode not in ROUTE_MODES:
            continue

        geom = _relation_to_geometry(rel, ways, city_poly)
        if geom is None or geom.is_empty:
            continue

        row = {k: None for k in BASE_COLUMNS}
        row['city'] = city
        row['source'] = 'osm'
        row['osm_element'] = 'relation'
        row['osm_id'] = int(rel.get('id'))
        row['geometry'] = geom

        for col in BASE_COLUMNS:
            if col in ('city', 'source', 'osm_element', 'osm_id', 'geometry'):
                continue
            if col in tags:
                row[col] = tags.get(col)

        rows.append(row)

    if rows:
        gdf = gpd.GeoDataFrame(rows, geometry='geometry', crs='EPSG:4326')
        gdf = gdf.drop_duplicates(subset=['osm_id']).reset_index(drop=True)
    else:
        gdf = gpd.GeoDataFrame(columns=BASE_COLUMNS, geometry='geometry', crs='EPSG:4326')

    # enforce column order
    gdf = gdf.reindex(columns=BASE_COLUMNS)

    result = CityResult(
        city=city,
        relations_found=len(relations),
        features_written=len(gdf),
        endpoint=endpoint,
        status='ok',
        message='',
    )
    return gdf, result


def main() -> int:
    city_results = []
    city_gdfs = {}

    for city in CITIES:
        print(f'processing city={city}', flush=True)
        try:
            gdf, result = extract_city(city)
            _write_city_outputs(city, gdf)
            city_gdfs[city] = gdf
            city_results.append(result)
            print(
                f"done city={city} relations_found={result.relations_found} features_written={result.features_written} endpoint={result.endpoint}",
                flush=True,
            )
        except Exception as e:
            msg = f'{type(e).__name__}: {e}'
            city_results.append(
                CityResult(
                    city=city,
                    relations_found=0,
                    features_written=0,
                    endpoint='none',
                    status='failed',
                    message=msg,
                )
            )
            print(f'failed city={city} error={msg}', flush=True)

    ok_gdfs = [city_gdfs[c] for c in CITIES if c in city_gdfs]
    if ok_gdfs:
        all_gdf = gpd.GeoDataFrame(pd.concat(ok_gdfs, ignore_index=True), geometry='geometry', crs='EPSG:4326')
    else:
        all_gdf = gpd.GeoDataFrame(columns=BASE_COLUMNS, geometry='geometry', crs='EPSG:4326')

    all_gdf = all_gdf.reindex(columns=BASE_COLUMNS)
    _write_all_cities_output(all_gdf)

    city_counts = {r.city: r.features_written for r in city_results if r.status == 'ok'}
    _update_manifest(city_counts, len(all_gdf))

    report_path = ROOT / 'public_transport_routes_extraction_report.csv'
    with report_path.open('w', encoding='utf-8', newline='') as f:
        fields = ['city', 'status', 'relations_found', 'features_written', 'endpoint', 'message']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in city_results:
            w.writerow({
                'city': r.city,
                'status': r.status,
                'relations_found': r.relations_found,
                'features_written': r.features_written,
                'endpoint': r.endpoint,
                'message': r.message,
            })

    failures = [r for r in city_results if r.status != 'ok']
    print(f'completed total_features={len(all_gdf)} failures={len(failures)} report={report_path}', flush=True)

    return 0 if not failures else 2


if __name__ == '__main__':
    raise SystemExit(main())
