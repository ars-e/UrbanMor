#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pyogrio
import pyarrow.parquet as pq
import rasterio
from pyproj import CRS

ROOT = Path("/Users/ars-e/projects/UrbanMor")
OUT = ROOT / "output"

DB_NAME = os.getenv("DB_NAME", "urbanmor")
PG_OGR = f"PG:dbname={DB_NAME}"


@dataclass
class LayerSpec:
    layer_key: str
    layer_name: str
    layer_family: str
    data_kind: str
    schema_name: str
    table_name: str
    source_code: str
    source_layer_name: str | None
    source_path: str
    file_format: str
    geometry_type: str | None
    declared_crs: str | None
    city: str | None
    is_canonical: bool
    source_feature_count: int | None
    loaded_row_count: int | None = None
    status: str = "pending"
    note: str = ""


SOURCE_CATALOG = {
    "wards_canonical": {
        "source_name": "Canonical Ward Boundaries",
        "source_type": "boundary",
        "provider": "UrbanMor canonicalization",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "wards_source_normalized": {
        "source_name": "Source Ward Boundaries (Normalized)",
        "source_type": "boundary",
        "provider": "UrbanMor normalization",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "district_canonical": {
        "source_name": "Canonical District Boundaries",
        "source_type": "boundary",
        "provider": "UrbanMor canonicalization",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "subdistrict_canonical": {
        "source_name": "Canonical Subdistrict Boundaries",
        "source_type": "boundary",
        "provider": "UrbanMor canonicalization",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "buildings_normalized": {
        "source_name": "Buildings (Normalized)",
        "source_type": "vector",
        "provider": "Building extraction pipeline",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "roads_normalized": {
        "source_name": "Road Network (Normalized)",
        "source_type": "vector",
        "provider": "OpenStreetMap",
        "source_url": "https://www.openstreetmap.org",
        "acquisition_method": "osm_extract",
        "license": "ODbL-1.0",
    },
    "transit_normalized": {
        "source_name": "Transit Network (Normalized)",
        "source_type": "vector",
        "provider": "OpenStreetMap",
        "source_url": "https://www.openstreetmap.org",
        "acquisition_method": "osm_extract",
        "license": "ODbL-1.0",
    },
    "open_spaces_normalized": {
        "source_name": "Open Spaces (Normalized)",
        "source_type": "vector",
        "provider": "OpenStreetMap",
        "source_url": "https://www.openstreetmap.org",
        "acquisition_method": "osm_extract",
        "license": "ODbL-1.0",
    },
    "dem_normalized": {
        "source_name": "DEM (Normalized)",
        "source_type": "raster",
        "provider": "DEM pipeline",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "slope_normalized": {
        "source_name": "Slope Raster (Normalized)",
        "source_type": "raster",
        "provider": "DEM terrain products pipeline",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
    "lulc_normalized": {
        "source_name": "LULC Raster (Normalized)",
        "source_type": "raster",
        "provider": "LULC pipeline",
        "source_url": None,
        "acquisition_method": "pipeline",
        "license": None,
    },
}


def rel(p: Path) -> str:
    return str(p.relative_to(ROOT))


def run(cmd: list[str], *, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, text=True, capture_output=capture)


def run_shell(cmd: str) -> None:
    subprocess.run(cmd, check=True, shell=True)


def sanitize_name(raw: str) -> str:
    out = re.sub(r"[^a-zA-Z0-9_]+", "_", raw.strip().lower())
    out = re.sub(r"_+", "_", out).strip("_")
    if not out:
        out = "layer"
    if out[0].isdigit():
        out = f"l_{out}"
    return out


def city_from_stem(stem: str) -> str | None:
    token = stem.split("_", 1)[0].strip().lower()
    return token or None


def crs_to_epsg_label(value: object) -> str | None:
    if value is None:
        return None
    try:
        crs = CRS.from_user_input(value)
        epsg = crs.to_epsg()
        if epsg is not None:
            return f"EPSG:{epsg}"
        return crs.to_string()
    except Exception:
        txt = str(value)
        return txt if txt else None


def vector_meta(path: Path) -> tuple[str | None, int | None, str | None]:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        pf = pq.ParquetFile(path)
        count = int(pf.metadata.num_rows) if pf.metadata else None
        geom_type = "Geometry"
        declared_crs = None
        md = pf.metadata.metadata if pf.metadata is not None else None
        if md and b"geo" in md:
            try:
                geo = json.loads(md[b"geo"])
                primary = geo.get("primary_column")
                cols = geo.get("columns", {})
                if primary in cols:
                    declared_crs = crs_to_epsg_label(cols[primary].get("crs"))
            except Exception:
                pass
        return geom_type, count, declared_crs

    layer_name = None
    if suffix == ".gpkg":
        layers = pyogrio.list_layers(path)
        if len(layers) > 0:
            layer_name = layers[0][0]
    info = pyogrio.read_info(path, layer=layer_name)
    geom_type = info.get("geometry_type")
    count = int(info.get("features")) if info.get("features") is not None else None
    declared_crs = crs_to_epsg_label(info.get("crs"))
    return geom_type, count, declared_crs


def raster_meta(path: Path) -> tuple[str | None, str | None]:
    with rasterio.open(path) as ds:
        crs = crs_to_epsg_label(ds.crs)
        geom_type = "Raster"
    return geom_type, crs


def discover_layers() -> list[LayerSpec]:
    specs: list[LayerSpec] = []

    vector_groups = [
        {
            "glob": "output/normalized/wards/canonical_normalized/*.gpkg",
            "schema_name": "boundaries",
            "layer_family": "ward_boundaries",
            "source_code": "wards_canonical",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/wards/source_normalized/*.gpkg",
            "schema_name": "boundaries",
            "layer_family": "ward_boundaries_source",
            "source_code": "wards_source_normalized",
            "is_canonical": False,
        },
        {
            "glob": "output/dist_bound/canonical_epsg4326/*.gpkg",
            "schema_name": "boundaries",
            "layer_family": "district_boundaries",
            "source_code": "district_canonical",
            "is_canonical": True,
        },
        {
            "glob": "output/sub_dist_bound/canonical_epsg4326/*.gpkg",
            "schema_name": "boundaries",
            "layer_family": "subdistrict_boundaries",
            "source_code": "subdistrict_canonical",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/vectors/buildings/*.parquet",
            "schema_name": "buildings",
            "layer_family": "buildings",
            "source_code": "buildings_normalized",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/vectors/roads/*.parquet",
            "schema_name": "transport",
            "layer_family": "roads",
            "source_code": "roads_normalized",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/vectors/transit/*.parquet",
            "schema_name": "transport",
            "layer_family": "transit",
            "source_code": "transit_normalized",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/vectors/open_spaces/*.parquet",
            "schema_name": "green",
            "layer_family": "open_spaces",
            "source_code": "open_spaces_normalized",
            "is_canonical": True,
        },
    ]

    raster_groups = [
        {
            "glob": "output/normalized/rasters/dem/*.tif",
            "schema_name": "dem",
            "layer_family": "dem",
            "source_code": "dem_normalized",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/rasters/slope/*.tif",
            "schema_name": "dem",
            "layer_family": "slope",
            "source_code": "slope_normalized",
            "is_canonical": True,
        },
        {
            "glob": "output/normalized/rasters/lulc/*.tif",
            "schema_name": "lulc",
            "layer_family": "lulc",
            "source_code": "lulc_normalized",
            "is_canonical": True,
        },
    ]

    for group in vector_groups:
        for path in sorted(ROOT.glob(group["glob"])):
            table_name = sanitize_name(path.stem)
            layer_name = path.stem
            source_layer_name = None
            if path.suffix.lower() == ".gpkg":
                layers = pyogrio.list_layers(path)
                if len(layers) > 0:
                    source_layer_name = layers[0][0]
            geom_type, feature_count, crs = vector_meta(path)
            city = city_from_stem(path.stem)
            schema_name = group["schema_name"]
            specs.append(
                LayerSpec(
                    layer_key=f"{schema_name}.{table_name}",
                    layer_name=layer_name,
                    layer_family=group["layer_family"],
                    data_kind="vector",
                    schema_name=schema_name,
                    table_name=table_name,
                    source_code=group["source_code"],
                    source_layer_name=source_layer_name,
                    source_path=rel(path),
                    file_format=path.suffix.lower().lstrip("."),
                    geometry_type=geom_type,
                    declared_crs=crs,
                    city=city,
                    is_canonical=group["is_canonical"],
                    source_feature_count=feature_count,
                )
            )

    for group in raster_groups:
        for path in sorted(ROOT.glob(group["glob"])):
            table_name = sanitize_name(path.stem)
            layer_name = path.stem
            geom_type, crs = raster_meta(path)
            city = city_from_stem(path.stem)
            schema_name = group["schema_name"]
            specs.append(
                LayerSpec(
                    layer_key=f"{schema_name}.{table_name}",
                    layer_name=layer_name,
                    layer_family=group["layer_family"],
                    data_kind="raster",
                    schema_name=schema_name,
                    table_name=table_name,
                    source_code=group["source_code"],
                    source_layer_name=None,
                    source_path=rel(path),
                    file_format=path.suffix.lower().lstrip("."),
                    geometry_type=geom_type,
                    declared_crs=crs,
                    city=city,
                    is_canonical=group["is_canonical"],
                    source_feature_count=None,
                )
            )

    # Deterministic load order.
    specs.sort(key=lambda s: (s.data_kind, s.schema_name, s.table_name))
    return specs


def find_spec_path(spec: LayerSpec) -> Path:
    return ROOT / spec.source_path


def load_vector_layer(spec: LayerSpec) -> None:
    src = find_spec_path(spec)
    cmd = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        PG_OGR,
        "-overwrite",
        "-nln",
        f"{spec.schema_name}.{spec.table_name}",
        "-nlt",
        "GEOMETRY",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=id",
        "-lco",
        "SPATIAL_INDEX=GIST",
        "-gt",
        "65536",
        str(src),
    ]
    if spec.source_layer_name:
        cmd.append(spec.source_layer_name)
    run(cmd)


def raster_srid(path: Path) -> int:
    with rasterio.open(path) as ds:
        epsg = ds.crs.to_epsg() if ds.crs is not None else None
        if epsg is None:
            return 0
        return int(epsg)


def load_raster_layer(spec: LayerSpec) -> None:
    src = find_spec_path(spec)
    srid = raster_srid(src)
    cmd = (
        f"raster2pgsql -s {srid} -I -C -M -F -d "
        f"{sh_quote(str(src))} {spec.schema_name}.{spec.table_name} | "
        f"psql -d {sh_quote(DB_NAME)}"
    )
    run_shell(cmd)


def sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def query_count(schema_name: str, table_name: str) -> int:
    sql = f"SELECT count(*) FROM {schema_name}.{table_name};"
    out = run(["psql", "-d", DB_NAME, "-At", "-c", sql], capture=True).stdout.strip()
    return int(out) if out else 0


def sql_q(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    txt = str(value).replace("'", "''")
    return f"'{txt}'"


def sql_json(value: object) -> str:
    txt = json.dumps(value, ensure_ascii=True).replace("'", "''")
    return f"'{txt}'::jsonb"


def write_inventory(specs: Iterable[LayerSpec], path: Path) -> None:
    rows = [asdict(s) for s in specs]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "layer_key",
                "layer_name",
                "layer_family",
                "data_kind",
                "schema_name",
                "table_name",
                "source_code",
                "source_layer_name",
                "source_path",
                "file_format",
                "geometry_type",
                "declared_crs",
                "city",
                "is_canonical",
                "source_feature_count",
                "loaded_row_count",
                "status",
                "note",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def upsert_meta_sources() -> None:
    stmts = []
    for source_code, d in SOURCE_CATALOG.items():
        stmt = f"""
INSERT INTO meta.source_registry (
  source_code, source_name, source_type, provider, license, source_url, acquisition_method, metadata
)
VALUES (
  {sql_q(source_code)},
  {sql_q(d['source_name'])},
  {sql_q(d['source_type'])},
  {sql_q(d['provider'])},
  {sql_q(d['license'])},
  {sql_q(d['source_url'])},
  {sql_q(d['acquisition_method'])},
  '{{}}'::jsonb
)
ON CONFLICT (source_code) DO UPDATE SET
  source_name = EXCLUDED.source_name,
  source_type = EXCLUDED.source_type,
  provider = EXCLUDED.provider,
  license = EXCLUDED.license,
  source_url = EXCLUDED.source_url,
  acquisition_method = EXCLUDED.acquisition_method;
"""
        stmts.append(stmt)
    sql = "\n".join(stmts)
    run(["psql", "-d", DB_NAME, "-v", "ON_ERROR_STOP=1", "-c", sql])


def upsert_meta_layers(specs: Iterable[LayerSpec]) -> None:
    stmts = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for s in specs:
        if s.status != "loaded":
            continue
        provenance = {
            "source_path": s.source_path,
            "file_format": s.file_format,
            "source_layer_name": s.source_layer_name,
        }
        validation_state = {
            "load_status": "loaded",
            "loaded_at": now_iso,
        }
        stmt = f"""
INSERT INTO meta.layer_registry (
  layer_key, layer_name, layer_family, data_kind, source_id, source_layer_name, source_path,
  canonical_schema, canonical_table, file_format, geometry_type, declared_crs, city,
  readiness_status, is_canonical, row_count, last_refresh_at, validation_state, provenance, notes
)
VALUES (
  {sql_q(s.layer_key)},
  {sql_q(s.layer_name)},
  {sql_q(s.layer_family)},
  {sql_q(s.data_kind)},
  (SELECT source_id FROM meta.source_registry WHERE source_code = {sql_q(s.source_code)}),
  {sql_q(s.source_layer_name)},
  {sql_q(s.source_path)},
  {sql_q(s.schema_name)},
  {sql_q(s.table_name)},
  {sql_q(s.file_format)},
  {sql_q(s.geometry_type)},
  {sql_q(s.declared_crs)},
  {sql_q(s.city)},
  'ready',
  {sql_q(s.is_canonical)},
  {sql_q(s.loaded_row_count)},
  now(),
  {sql_json(validation_state)},
  {sql_json(provenance)},
  {sql_q(s.note if s.note else None)}
)
ON CONFLICT (layer_key) DO UPDATE SET
  layer_name = EXCLUDED.layer_name,
  layer_family = EXCLUDED.layer_family,
  data_kind = EXCLUDED.data_kind,
  source_id = EXCLUDED.source_id,
  source_layer_name = EXCLUDED.source_layer_name,
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
  provenance = EXCLUDED.provenance,
  notes = EXCLUDED.notes;
"""
        stmts.append(stmt)
    if not stmts:
        return
    sql = "\n".join(stmts)
    run(["psql", "-d", DB_NAME, "-v", "ON_ERROR_STOP=1", "-c", sql])


def insert_pipeline_run(specs: Iterable[LayerSpec], run_status: str, error_log: str | None) -> None:
    loaded = [s.layer_key for s in specs if s.status == "loaded"]
    failed = [s.layer_key for s in specs if s.status == "failed"]
    cities = sorted({s.city for s in specs if s.city})
    artifacts = [
        {"path": "output/layer_inventory_master.csv", "type": "inventory"},
        {"path": "output/layer_load_report.csv", "type": "load_report"},
    ]

    def array_lit(values: list[str]) -> str:
        if not values:
            return "ARRAY[]::text[]"
        vals = ",".join(sql_q(v) for v in values)
        return f"ARRAY[{vals}]::text[]"

    sql = f"""
INSERT INTO meta.pipeline_runs (
  pipeline_name, run_type, run_status, started_at, finished_at, initiated_by, city_scope,
  input_layers, output_layers, run_params, metrics_summary, artifacts, warning_count, error_count, error_log
)
VALUES (
  'load_all_available_layers',
  'manual',
  {sql_q(run_status)},
  now(),
  now(),
  current_user,
  {array_lit(cities)},
  {array_lit([s.source_path for s in specs])},
  {array_lit(loaded)},
  '{{}}'::jsonb,
  {sql_json({'loaded_layers': len(loaded), 'failed_layers': len(failed)})},
  {sql_json(artifacts)},
  0,
  {len(failed)},
  {sql_q(error_log)}
);
"""
    run(["psql", "-d", DB_NAME, "-v", "ON_ERROR_STOP=1", "-c", sql])


def load_all(specs: list[LayerSpec]) -> tuple[int, int]:
    loaded = 0
    failed = 0
    for spec in specs:
        try:
            if spec.data_kind == "vector":
                load_vector_layer(spec)
            else:
                load_raster_layer(spec)
            spec.loaded_row_count = query_count(spec.schema_name, spec.table_name)
            spec.status = "loaded"
            spec.note = ""
            loaded += 1
        except subprocess.CalledProcessError as exc:
            spec.status = "failed"
            spec.note = f"load_failed: {exc}"
            failed += 1
    return loaded, failed


def main() -> int:
    specs = discover_layers()
    if not specs:
        raise SystemExit("No layers discovered for loading.")

    write_inventory(specs, OUT / "layer_inventory_master.csv")

    loaded, failed = load_all(specs)
    write_inventory(specs, OUT / "layer_load_report.csv")

    upsert_meta_sources()
    upsert_meta_layers(specs)
    run_status = "success" if failed == 0 else "partial_success"
    insert_pipeline_run(specs, run_status=run_status, error_log=None if failed == 0 else "See output/layer_load_report.csv")

    summary = {
        "db_name": DB_NAME,
        "discovered_layers": len(specs),
        "loaded_layers": loaded,
        "failed_layers": failed,
        "inventory_csv": "output/layer_inventory_master.csv",
        "load_report_csv": "output/layer_load_report.csv",
    }
    (OUT / "layer_load_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
