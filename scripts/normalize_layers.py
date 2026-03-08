#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import subprocess
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import rasterio
from rasterio.warp import Resampling, calculate_default_transform, reproject
from shapely.geometry import MultiPolygon, box
from shapely import make_valid

ROOT = Path('/Users/ars-e/projects/UrbanMor')
OUT = ROOT / 'output'
NORM = OUT / 'normalized'

CITIES = [
    'ahmedabad',
    'bengaluru',
    'chandigarh',
    'chennai',
    'delhi',
    'kolkata',
    'mumbai',
]

# --- Paths ---
WARD_CANON_DIR = OUT / 'ward_bound' / 'canonical'
WARD_CANON_MANIFEST = WARD_CANON_DIR / 'ward_canonical_manifest.csv'

BUILDINGS_DIR = OUT / 'city_buildings_gpkg'
TRANSPORT_PARQUET_DIR = OUT / 'transport_network_osm' / 'parquet'
OPEN_PARQUET_DIR = OUT / 'open_spaces_osm' / 'parquet'

DEM_CLIP_DIR = OUT / 'dem_terrain_ward_stats' / 'clipped_rasters'
SLOPE_DIR = OUT / 'dem_terrain_ward_stats' / 'terrain_products'
LULC_CLIP_DIR = OUT / 'lulc_ward_stats_fullrun' / 'clipped_rasters'

# --- Output dirs ---
WARDS_OUT = NORM / 'wards'
VECTORS_OUT = NORM / 'vectors'
RASTERS_OUT = NORM / 'rasters'

for p in [
    WARDS_OUT,
    WARDS_OUT / 'source_normalized',
    WARDS_OUT / 'canonical_normalized',
    VECTORS_OUT / 'buildings',
    VECTORS_OUT / 'roads',
    VECTORS_OUT / 'transit',
    VECTORS_OUT / 'open_spaces',
    RASTERS_OUT / 'dem',
    RASTERS_OUT / 'slope',
    RASTERS_OUT / 'lulc',
]:
    p.mkdir(parents=True, exist_ok=True)


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def ensure_epsg4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf = gdf.set_crs('EPSG:4326', allow_override=True)
    elif str(gdf.crs).upper() != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')
    return gdf


def fix_invalid(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, int]:
    invalid_mask = ~gdf.geometry.is_valid
    invalid_count = int(invalid_mask.sum())
    if invalid_count > 0:
        gdf.loc[invalid_mask, 'geometry'] = gdf.loc[invalid_mask, 'geometry'].apply(make_valid)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    return gdf, invalid_count


def polygonal_only(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, int]:
    def _coerce_polygonal(geom):
        if geom is None or geom.is_empty:
            return None
        gt = geom.geom_type
        if gt in ('Polygon', 'MultiPolygon'):
            return geom
        if gt == 'GeometryCollection':
            polys = []
            for part in geom.geoms:
                if part.is_empty:
                    continue
                if part.geom_type == 'Polygon':
                    polys.append(part)
                elif part.geom_type == 'MultiPolygon':
                    polys.extend(list(part.geoms))
            if not polys:
                return None
            if len(polys) == 1:
                return polys[0]
            return MultiPolygon(polys)
        return None

    before = len(gdf)
    out = gdf.copy()
    out['geometry'] = out.geometry.apply(_coerce_polygonal)
    out = out[out.geometry.notna() & ~out.geometry.is_empty].copy()
    dropped = before - len(out)
    return out, int(dropped)


def load_ward_manifest() -> list[dict[str, str]]:
    with WARD_CANON_MANIFEST.open('r', encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f))


def normalize_wards() -> None:
    rows = []
    manifest_rows = load_ward_manifest()

    for rec in manifest_rows:
        city = rec['city']
        src_rel = rec['source_file']
        src_path = ROOT / src_rel
        if not src_path.exists():
            rows.append({
                'city': city,
                'input_type': 'source',
                'input_file': src_rel,
                'output_geojson': '',
                'output_gpkg': '',
                'input_features': 0,
                'output_features': 0,
                'invalid_fixed': 0,
                'status': 'missing_source',
                'note': 'source file not found',
            })
            continue

        # Normalize source wards
        src_gdf = gpd.read_file(src_path)
        src_gdf = ensure_epsg4326(src_gdf)
        src_gdf, invalid_fixed = fix_invalid(src_gdf)
        src_gdf, dropped_non_polygon = polygonal_only(src_gdf)

        # Best-effort ward id/name extraction from raw source
        cols_lower = {c.lower(): c for c in src_gdf.columns}
        id_col = None
        name_col = None
        for cand in ['ward_id', 'wardid', 'ward_no', 'wardno', 'ward_num', 'id', 'zone_id']:
            if cand in cols_lower:
                id_col = cols_lower[cand]
                break
        for cand in ['ward_name', 'wardname', 'name', 'ward', 'zone_name']:
            if cand in cols_lower:
                name_col = cols_lower[cand]
                break

        src_norm = gpd.GeoDataFrame(
            {
                'city': city,
                'ward_id': src_gdf[id_col].astype(str) if id_col else pd.Series([None] * len(src_gdf)),
                'ward_name': src_gdf[name_col].astype(str) if name_col else pd.Series([None] * len(src_gdf)),
                'source_file': src_rel,
                'source_format': src_path.suffix.lower().lstrip('.'),
            },
            geometry=src_gdf.geometry,
            crs='EPSG:4326',
        )

        out_src_geojson = WARDS_OUT / 'source_normalized' / f'{city}_wards_source_normalized.geojson'
        out_src_gpkg = WARDS_OUT / 'source_normalized' / f'{city}_wards_source_normalized.gpkg'
        src_norm.to_file(out_src_geojson, driver='GeoJSON')
        src_norm.to_file(out_src_gpkg, layer=f'{city}_wards_source_normalized', driver='GPKG')

        rows.append({
            'city': city,
            'input_type': 'source',
            'input_file': src_rel,
            'output_geojson': rel(out_src_geojson),
            'output_gpkg': rel(out_src_gpkg),
            'input_features': int(rec['source_rows']),
            'output_features': len(src_norm),
            'invalid_fixed': invalid_fixed,
            'status': 'ok',
            'note': f'dropped_non_polygon={dropped_non_polygon}' if dropped_non_polygon else '',
        })

        # Normalize canonical wards
        canon_gpkg = ROOT / rec['canonical_gpkg']
        canon_gdf = gpd.read_file(canon_gpkg)
        canon_gdf = ensure_epsg4326(canon_gdf)
        canon_gdf, canon_invalid_fixed = fix_invalid(canon_gdf)
        canon_gdf, canon_dropped_non_polygon = polygonal_only(canon_gdf)

        canon_cols = {
            'ward_uid': 'ward_uid',
            'city': 'city',
            'ward_id_std': 'ward_id',
            'ward_name_std': 'ward_name',
            'source_name': 'source_name',
            'source_file': 'source_file',
            'source_format': 'source_format',
            'source_feature_count': 'source_feature_count',
        }
        keep = [c for c in canon_cols.keys() if c in canon_gdf.columns]
        canon_out = canon_gdf[keep + ['geometry']].rename(columns=canon_cols)
        if 'city' not in canon_out.columns:
            canon_out['city'] = city

        out_canon_geojson = WARDS_OUT / 'canonical_normalized' / f'{city}_wards_normalized.geojson'
        out_canon_gpkg = WARDS_OUT / 'canonical_normalized' / f'{city}_wards_normalized.gpkg'
        canon_out.to_file(out_canon_geojson, driver='GeoJSON')
        canon_out.to_file(out_canon_gpkg, layer=f'{city}_wards_normalized', driver='GPKG')

        rows.append({
            'city': city,
            'input_type': 'canonical',
            'input_file': rec['canonical_gpkg'],
            'output_geojson': rel(out_canon_geojson),
            'output_gpkg': rel(out_canon_gpkg),
            'input_features': int(rec['canonical_rows']),
            'output_features': len(canon_out),
            'invalid_fixed': canon_invalid_fixed,
            'status': 'ok',
            'note': f'dropped_non_polygon={canon_dropped_non_polygon}' if canon_dropped_non_polygon else '',
        })

    report = WARDS_OUT / 'ward_normalization_report.csv'
    with report.open('w', encoding='utf-8', newline='') as f:
        fields = [
            'city', 'input_type', 'input_file', 'output_geojson', 'output_gpkg',
            'input_features', 'output_features', 'invalid_fixed', 'status', 'note'
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def normalize_buildings_vector() -> list[dict]:
    rows = []
    for city in CITIES:
        in_gpkg = BUILDINGS_DIR / f'{city}_buildings_wards.gpkg'
        if not in_gpkg.exists():
            rows.append({
                'city': city,
                'family': 'buildings',
                'input_path': rel(in_gpkg),
                'output_path': '',
                'input_features': 0,
                'output_features': 0,
                'status': 'missing_input',
                'note': '',
            })
            continue

        layer = pyogrio.list_layers(in_gpkg)[0][0]
        info = pyogrio.read_info(in_gpkg, layer=layer)
        input_features = int(info.get('features') or 0)

        out_parquet = VECTORS_OUT / 'buildings' / f'{city}_buildings_normalized.parquet'

        # Use GDAL SQL projection for speed and deterministic schema.
        sql = (
            f"SELECT '{city}' AS city, "
            f"'buildings' AS layer_family, "
            f"'building_extraction_pipeline' AS source_dataset, "
            f"'{layer}' AS source_layer, "
            f"'{rel(in_gpkg)}' AS source_file, "
            f"CAST(boundary_id AS TEXT) AS source_feature_id, "
            f"CAST(ward AS TEXT) AS ward_ref, "
            f"CAST(area_in_meters AS REAL) AS footprint_area_m2, "
            f"CAST(confidence AS REAL) AS source_confidence, "
            f"CAST(bf_source AS TEXT) AS source_method, "
            f"CAST(s2_id AS TEXT) AS s2_id, "
            f"CAST(country_iso AS TEXT) AS country_iso, "
            f"CAST(geohash AS TEXT) AS geohash, "
            f"geom AS geometry "
            f"FROM {layer}"
        )

        cmd = [
            'ogr2ogr',
            '-f', 'Parquet',
            str(out_parquet),
            str(in_gpkg),
            '-dialect', 'SQLITE',
            '-sql', sql,
            '-lco', 'COMPRESSION=ZSTD',
        ]
        subprocess.run(cmd, check=True)

        # Normalize CRS metadata to explicit EPSG:4326.
        bdf = gpd.read_parquet(out_parquet)
        if bdf.crs is None:
            bdf = bdf.set_crs('EPSG:4326', allow_override=True)
        elif str(bdf.crs).upper() != 'EPSG:4326':
            bdf = bdf.to_crs('EPSG:4326')
        bdf.to_parquet(out_parquet, index=False)

        out_rows = pq_row_count(out_parquet)

        rows.append({
            'city': city,
            'family': 'buildings',
            'input_path': rel(in_gpkg),
            'output_path': rel(out_parquet),
            'input_features': input_features,
            'output_features': out_rows,
            'status': 'ok',
            'note': '',
        })
    return rows


def pq_row_count(path: Path) -> int:
    import pyarrow.parquet as pq
    return int(pq.ParquetFile(path).metadata.num_rows)


def normalize_city_parquet_family(city: str, family: str, input_base: Path, layers: list[str], output_path: Path) -> tuple[int, int, str, str]:
    frames = []
    for layer in layers:
        p = input_base / city / f'{layer}.parquet'
        if not p.exists():
            continue
        gdf = gpd.read_parquet(p)
        if gdf.crs is None:
            gdf = gdf.set_crs('EPSG:4326', allow_override=True)

        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
        gdf['city'] = city
        gdf['layer_family'] = family
        gdf['source_dataset'] = 'osm'
        gdf['source_layer'] = layer
        gdf['source_file'] = rel(p)

        rename_map = {
            'osm_element': 'source_element_type',
            'osm_id': 'source_feature_id',
            'name': 'feature_name',
            'ref': 'feature_ref',
            'operator': 'feature_operator',
            'route': 'route_type',
            'network': 'network_name',
            'layer': 'osm_layer',
            'source': 'source_provider',
        }
        for k, v in rename_map.items():
            if k in gdf.columns and v not in gdf.columns:
                gdf = gdf.rename(columns={k: v})

        # Preserve provenance + normalized common fields + useful OSM tags.
        preferred = [
            'city', 'layer_family', 'source_dataset', 'source_layer', 'source_file',
            'source_provider', 'source_element_type', 'source_feature_id',
            'feature_name', 'feature_ref', 'feature_operator',
            'route_type', 'network_name', 'public_transport', 'highway', 'railway', 'station',
            'amenity', 'barrier', 'access', 'motor_vehicle', 'bicycle', 'foot',
            'surface', 'smoothness', 'maxspeed', 'lanes', 'oneway', 'bridge', 'tunnel',
            'osm_layer', 'geometry',
        ]
        keep = [c for c in preferred if c in gdf.columns]

        # Ensure geometry present and last
        if 'geometry' not in keep:
            keep.append('geometry')

        gdf = gdf[keep]
        frames.append(gdf)

    if not frames:
        return (0, 0, 'missing_layers', 'no source parquet layers found')

    out = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry='geometry', crs='EPSG:4326')
    out.to_parquet(output_path, index=False)
    return (sum(len(f) for f in frames), len(out), 'ok', '')


def normalize_vectors() -> None:
    report_rows = []

    # Buildings
    report_rows.extend(normalize_buildings_vector())

    roads_layers = ['roads_major', 'lanes_minor', 'walkability_access']
    transit_layers = [
        'public_transport_routes', 'public_transport_stops', 'public_transport_platforms',
        'public_transport_stations', 'public_transport_shelters',
        'metro_lines', 'metro_stations', 'metro_entrances',
        'rail_lines', 'rail_stations',
    ]
    open_layers = [
        'open_space_master', 'green_parks_vegetation', 'sports_play_open',
        'plazas_open_paved', 'water_wetlands', 'waterways_linear',
        'open_barren_sands_rocky', 'other_open_landuse', 'coastal_marine_edges', 'trees',
    ]

    for city in CITIES:
        out_roads = VECTORS_OUT / 'roads' / f'{city}_roads_normalized.parquet'
        in_count, out_count, status, note = normalize_city_parquet_family(
            city, 'roads', TRANSPORT_PARQUET_DIR, roads_layers, out_roads
        )
        report_rows.append({
            'city': city, 'family': 'roads',
            'input_path': f"{rel(TRANSPORT_PARQUET_DIR)}/{city}/[{','.join(roads_layers)}].parquet",
            'output_path': rel(out_roads),
            'input_features': in_count,
            'output_features': out_count,
            'status': status,
            'note': note,
        })

        out_transit = VECTORS_OUT / 'transit' / f'{city}_transit_normalized.parquet'
        in_count, out_count, status, note = normalize_city_parquet_family(
            city, 'transit', TRANSPORT_PARQUET_DIR, transit_layers, out_transit
        )
        report_rows.append({
            'city': city, 'family': 'transit',
            'input_path': f"{rel(TRANSPORT_PARQUET_DIR)}/{city}/[{','.join(transit_layers)}].parquet",
            'output_path': rel(out_transit),
            'input_features': in_count,
            'output_features': out_count,
            'status': status,
            'note': note,
        })

        out_open = VECTORS_OUT / 'open_spaces' / f'{city}_open_spaces_normalized.parquet'
        in_count, out_count, status, note = normalize_city_parquet_family(
            city, 'open_spaces', OPEN_PARQUET_DIR, open_layers, out_open
        )
        report_rows.append({
            'city': city, 'family': 'open_spaces',
            'input_path': f"{rel(OPEN_PARQUET_DIR)}/{city}/[{','.join(open_layers)}].parquet",
            'output_path': rel(out_open),
            'input_features': in_count,
            'output_features': out_count,
            'status': status,
            'note': note,
        })

    report = VECTORS_OUT / 'vector_normalization_report.csv'
    with report.open('w', encoding='utf-8', newline='') as f:
        fields = [
            'city', 'family', 'input_path', 'output_path',
            'input_features', 'output_features', 'status', 'note'
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(report_rows)


def reproject_raster_if_needed(src: Path, dst: Path, target_crs: str) -> tuple[str, bool]:
    with rasterio.open(src) as ds:
        src_crs = str(ds.crs) if ds.crs else 'none'
        if src_crs.upper() == target_crs.upper():
            return src_crs, False

        transform, width, height = calculate_default_transform(
            ds.crs, target_crs, ds.width, ds.height, *ds.bounds
        )
        profile = ds.profile.copy()
        profile.update({
            'crs': target_crs,
            'transform': transform,
            'width': width,
            'height': height,
            'compress': 'LZW',
            'tiled': True,
        })

        with rasterio.open(dst, 'w', **profile) as out:
            for i in range(1, ds.count + 1):
                reproject(
                    source=rasterio.band(ds, i),
                    destination=rasterio.band(out, i),
                    src_transform=ds.transform,
                    src_crs=ds.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest,
                )
        return src_crs, True


def derive_slope_from_dem(dem_path: Path, slope_out: Path) -> None:
    with rasterio.open(dem_path) as ds:
        arr = ds.read(1).astype('float32')
        nodata = ds.nodata
        mask = np.isnan(arr) if nodata is None else (arr == nodata)

        # Approximate slope in degrees from pixel gradients.
        xres = abs(ds.transform.a)
        yres = abs(ds.transform.e)
        gy, gx = np.gradient(arr, yres, xres)
        slope = np.degrees(np.arctan(np.sqrt(gx * gx + gy * gy))).astype('float32')
        slope[mask] = -9999.0

        profile = ds.profile.copy()
        profile.update(dtype='float32', count=1, nodata=-9999.0, compress='LZW', tiled=True)

        with rasterio.open(slope_out, 'w', **profile) as out:
            out.write(slope, 1)


def ward_union_by_city() -> dict[str, object]:
    unions = {}
    for city in CITIES:
        g = gpd.read_file(WARD_CANON_DIR / f'{city}_wards_canonical.geojson')
        g = ensure_epsg4326(g)
        unions[city] = g.geometry.union_all()
    return unions


def overlap_ratio(raster_path: Path, ward_geom) -> float:
    with rasterio.open(raster_path) as ds:
        rbox = box(*ds.bounds)
        ward_g = gpd.GeoSeries([ward_geom], crs='EPSG:4326')
        if ds.crs and str(ds.crs).upper() != 'EPSG:4326':
            ward_g = ward_g.to_crs(ds.crs)
        inter = ward_g.iloc[0].intersection(rbox)
        area = ward_g.iloc[0].area
        if area <= 0:
            return 0.0
        return float(inter.area / area)


def normalize_single_raster(src: Path, dst: Path, expected_crs: str | None, nodata_default: float | int, resampling: str = 'nearest') -> tuple[str, str, bool]:
    reprojected = False
    if expected_crs is not None:
        # Reproject to expected CRS if needed via temp file.
        with rasterio.open(src) as ds:
            src_crs = str(ds.crs) if ds.crs else 'none'
        if src_crs.upper() != expected_crs.upper() and src_crs != 'none':
            tmp = dst.with_suffix('.tmp.tif')
            if tmp.exists():
                tmp.unlink()
            reproject_raster_if_needed(src, tmp, expected_crs)
            src_use = tmp
            reprojected = True
        else:
            src_use = src
    else:
        src_use = src

    with rasterio.open(src_use) as ds:
        profile = ds.profile.copy()
        nodata = ds.nodata if ds.nodata is not None else nodata_default
        profile.update(nodata=nodata, compress='LZW', tiled=True)
        with rasterio.open(dst, 'w', **profile) as out:
            for i in range(1, ds.count + 1):
                out.write(ds.read(i), i)

        out_crs = str(ds.crs) if ds.crs else 'none'
        out_nodata = str(nodata)

    tmp = dst.with_suffix('.tmp.tif')
    if tmp.exists():
        tmp.unlink()

    return out_crs, out_nodata, reprojected


def normalize_rasters() -> None:
    report_rows = []
    ward_unions = ward_union_by_city()

    for city in CITIES:
        dem_in = DEM_CLIP_DIR / f'{city}_dem_clipped.tif'
        slope_in = SLOPE_DIR / f'{city}_slope_deg.tif'
        lulc_in = LULC_CLIP_DIR / f'{city}_20240101-20241231_lulc_clipped.tif'

        dem_out = RASTERS_OUT / 'dem' / f'{city}_dem_normalized.tif'
        slope_out = RASTERS_OUT / 'slope' / f'{city}_slope_deg_normalized.tif'
        lulc_out = RASTERS_OUT / 'lulc' / f'{city}_lulc_normalized.tif'

        # DEM normalize
        if dem_in.exists():
            crs, nodata, reprojected = normalize_single_raster(dem_in, dem_out, 'EPSG:4326', -9999.0)
            with rasterio.open(dem_out) as ds:
                overlap = overlap_ratio(dem_out, ward_unions[city])
                extent = f"{ds.bounds.left:.6f},{ds.bounds.bottom:.6f},{ds.bounds.right:.6f},{ds.bounds.top:.6f}"
            report_rows.append({
                'city': city, 'raster_family': 'dem', 'input_path': rel(dem_in), 'output_path': rel(dem_out),
                'crs': crs, 'nodata': nodata, 'extent': extent, 'ward_overlap_ratio': round(overlap, 6),
                'clip_strategy': 'city_union_of_canonical_wards', 'derived': False, 'reprojected': reprojected,
                'status': 'ok', 'note': '',
            })
        else:
            report_rows.append({
                'city': city, 'raster_family': 'dem', 'input_path': rel(dem_in), 'output_path': '',
                'crs': '', 'nodata': '', 'extent': '', 'ward_overlap_ratio': 0,
                'clip_strategy': 'city_union_of_canonical_wards', 'derived': False, 'reprojected': False,
                'status': 'missing_input', 'note': '',
            })

        # Slope normalize/derive
        if slope_in.exists():
            crs, nodata, reprojected = normalize_single_raster(slope_in, slope_out, 'EPSG:4326', -9999.0)
            derived = False
        elif dem_out.exists():
            derive_slope_from_dem(dem_out, slope_out)
            with rasterio.open(slope_out) as ds:
                crs = str(ds.crs) if ds.crs else 'none'
                nodata = str(ds.nodata)
            reprojected = False
            derived = True
        else:
            crs = ''
            nodata = ''
            reprojected = False
            derived = False

        if slope_out.exists():
            with rasterio.open(slope_out) as ds:
                overlap = overlap_ratio(slope_out, ward_unions[city])
                extent = f"{ds.bounds.left:.6f},{ds.bounds.bottom:.6f},{ds.bounds.right:.6f},{ds.bounds.top:.6f}"
            report_rows.append({
                'city': city, 'raster_family': 'slope', 'input_path': rel(slope_in) if slope_in.exists() else rel(dem_out),
                'output_path': rel(slope_out), 'crs': crs, 'nodata': nodata,
                'extent': extent, 'ward_overlap_ratio': round(overlap, 6),
                'clip_strategy': 'city_union_of_canonical_wards', 'derived': derived, 'reprojected': reprojected,
                'status': 'ok', 'note': '',
            })
        else:
            report_rows.append({
                'city': city, 'raster_family': 'slope', 'input_path': rel(slope_in), 'output_path': '',
                'crs': '', 'nodata': '', 'extent': '', 'ward_overlap_ratio': 0,
                'clip_strategy': 'city_union_of_canonical_wards', 'derived': False, 'reprojected': False,
                'status': 'missing_input', 'note': 'slope missing and dem unavailable',
            })

        # LULC normalize (keep native CRS if already explicit)
        if lulc_in.exists():
            # keep source CRS as canonical for lulc, only enforce nodata and compression
            crs, nodata, reprojected = normalize_single_raster(lulc_in, lulc_out, None, 255)
            with rasterio.open(lulc_out) as ds:
                overlap = overlap_ratio(lulc_out, ward_unions[city])
                extent = f"{ds.bounds.left:.6f},{ds.bounds.bottom:.6f},{ds.bounds.right:.6f},{ds.bounds.top:.6f}"
            report_rows.append({
                'city': city, 'raster_family': 'lulc', 'input_path': rel(lulc_in), 'output_path': rel(lulc_out),
                'crs': crs, 'nodata': nodata, 'extent': extent, 'ward_overlap_ratio': round(overlap, 6),
                'clip_strategy': 'city_union_of_canonical_wards', 'derived': False, 'reprojected': reprojected,
                'status': 'ok', 'note': '',
            })
        else:
            report_rows.append({
                'city': city, 'raster_family': 'lulc', 'input_path': rel(lulc_in), 'output_path': '',
                'crs': '', 'nodata': '', 'extent': '', 'ward_overlap_ratio': 0,
                'clip_strategy': 'city_union_of_canonical_wards', 'derived': False, 'reprojected': False,
                'status': 'missing_input', 'note': '',
            })

    report = RASTERS_OUT / 'raster_normalization_report.csv'
    with report.open('w', encoding='utf-8', newline='') as f:
        fields = [
            'city', 'raster_family', 'input_path', 'output_path', 'crs', 'nodata', 'extent',
            'ward_overlap_ratio', 'clip_strategy', 'derived', 'reprojected', 'status', 'note'
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(report_rows)


def write_summary() -> None:
    summary = {
        'wards_report': rel(WARDS_OUT / 'ward_normalization_report.csv'),
        'vector_report': rel(VECTORS_OUT / 'vector_normalization_report.csv'),
        'raster_report': rel(RASTERS_OUT / 'raster_normalization_report.csv'),
        'normalized_root': rel(NORM),
    }
    (NORM / 'normalization_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')


def main() -> int:
    print('normalizing wards...')
    normalize_wards()

    print('normalizing vectors...')
    normalize_vectors()

    print('normalizing rasters...')
    normalize_rasters()

    write_summary()
    print('done')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
