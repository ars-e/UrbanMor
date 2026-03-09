#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq
import pyogrio
import rasterio

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"

BUILDINGS_DIR = OUT / "city_buildings_gpkg"

MANIFESTS = [
    OUT / "city_buildings_gpkg" / "extraction_summary.csv",
    OUT / "dem_terrain_ward_stats" / "dem_terrain_manifest.csv",
    OUT / "open_spaces_osm" / "open_spaces_manifest.csv",
    OUT / "transport_network_osm" / "transport_manifest.csv",
    OUT / "ward_bound" / "canonical" / "ward_canonical_manifest.csv",
]

ABS_PREFIXES = [
    "/Users/ars-e/projects/UrbanMorph/",
    "/Users/ars-e/projects/UrbanMor/",
]


@dataclass
class BuildingFixResult:
    file_name: str
    source_layer: str
    source_crs: str
    source_features: int
    output_crs: str
    output_geometry: str
    status: str
    message: str


def _norm_crs(crs: object) -> str:
    if crs is None:
        return "none"
    s = str(crs)
    if "Undefined geographic SRS" in s:
        return "Undefined geographic SRS"
    if "EPSG:" in s:
        idx = s.find("EPSG:")
        return s[idx:].split(",")[0].split("]")[0].strip()
    if "AUTHORITY[\"EPSG\",\"" in s:
        code = s.split("AUTHORITY[\"EPSG\",\"")[-1].split("\"")[0]
        return f"EPSG:{code}"
    return s


def _list_building_gpks() -> list[Path]:
    return sorted(p for p in BUILDINGS_DIR.glob("*_buildings_wards.gpkg") if p.is_file())


def _count_non_polygon_with_ogrinfo(path: Path, layer: str, geom_col: str) -> tuple[bool, str]:
    sql = (
        f"SELECT COUNT(*) AS non_poly FROM {layer} "
        f"WHERE ST_GeometryType({geom_col}) NOT IN ('POLYGON','MULTIPOLYGON')"
    )
    cmd = ["ogrinfo", str(path), "-dialect", "SQLite", "-sql", sql]
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        return False, f"ogrinfo_error:{(e.output or '').strip()[:240]}"

    marker = "non_poly (Integer) ="
    for line in out.splitlines():
        if marker in line:
            val = int(line.split(marker, 1)[1].strip())
            return (val == 0), f"non_poly={val}"
    return False, "non_poly_parse_failed"


def _normalize_one_building_gpkg(path: Path) -> BuildingFixResult:
    try:
        layers = pyogrio.list_layers(path)
        if len(layers) == 0:
            return BuildingFixResult(path.name, "", "none", 0, "none", "none", "failed", "no layers")

        layer = str(layers[0][0])
        info = pyogrio.read_info(path, layer=layer)
        source_crs = _norm_crs(info.get("crs"))
        source_features = int(info.get("features") or 0)

        con = sqlite3.connect(path)
        cur = con.cursor()

        row = cur.execute(
            "SELECT table_name,column_name FROM gpkg_geometry_columns WHERE table_name=?",
            (layer,),
        ).fetchone()
        if not row:
            con.close()
            return BuildingFixResult(path.name, layer, source_crs, source_features, "none", "none", "failed", "gpkg_geometry_columns row missing")

        table_name, geom_col = row

        cur.execute(
            "UPDATE gpkg_geometry_columns SET geometry_type_name=?, srs_id=?, z=?, m=? WHERE table_name=?",
            ("MULTIPOLYGON", 4326, 0, 0, table_name),
        )
        cur.execute("UPDATE gpkg_contents SET srs_id=? WHERE table_name=?", (4326, table_name))
        con.commit()
        con.close()

        out_info = pyogrio.read_info(path, layer=layer)
        out_crs = _norm_crs(out_info.get("crs"))
        out_geom = str(out_info.get("geometry_type") or "Unknown")

        if not out_crs.startswith("EPSG:4326"):
            return BuildingFixResult(path.name, layer, source_crs, source_features, out_crs, out_geom, "failed", "output CRS is not EPSG:4326")

        if "Polygon" not in out_geom:
            return BuildingFixResult(path.name, layer, source_crs, source_features, out_crs, out_geom, "failed", "output geometry type not polygonal")

        ok_poly, msg_poly = _count_non_polygon_with_ogrinfo(path, layer, geom_col)
        if not ok_poly:
            return BuildingFixResult(path.name, layer, source_crs, source_features, out_crs, out_geom, "failed", msg_poly)

        return BuildingFixResult(path.name, layer, source_crs, source_features, out_crs, out_geom, "ok", msg_poly)

    except Exception as e:
        return BuildingFixResult(path.name, "", "none", 0, "none", "none", "failed", f"exception:{type(e).__name__}:{e}")


def canonicalize_buildings() -> list[BuildingFixResult]:
    results: list[BuildingFixResult] = []
    for gpkg in _list_building_gpks():
        print(f"normalizing={gpkg.name}", flush=True)
        res = _normalize_one_building_gpkg(gpkg)
        results.append(res)
        print(f"normalized={gpkg.name} status={res.status} note={res.message}", flush=True)
    return results


def _relativize_cell(value: str) -> str:
    v = value.strip()
    if not v:
        return v

    for pref in ABS_PREFIXES:
        if v.startswith(pref):
            v = v[len(pref) :]

    if "/output/" in v and not v.startswith("output/"):
        v = "output/" + v.split("/output/", 1)[1]

    p = Path(v)
    if p.is_absolute():
        try:
            v = str(p.relative_to(ROOT))
        except Exception:
            pass

    return v


def rewrite_manifests_relative() -> list[tuple[Path, int]]:
    changed: list[tuple[Path, int]] = []
    for manifest in MANIFESTS:
        if not manifest.exists():
            continue

        with manifest.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []

        edits = 0
        for row in rows:
            for k in fieldnames:
                val = row.get(k, "")
                if not isinstance(val, str):
                    continue
                new_val = _relativize_cell(val)
                if new_val != val:
                    row[k] = new_val
                    edits += 1

        if edits > 0:
            with manifest.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

        changed.append((manifest, edits))

    return changed


def _vector_has_defined_crs(path: Path) -> tuple[bool, str, str]:
    try:
        layers = pyogrio.list_layers(path)
        if len(layers) == 0:
            return False, "none", "no_layers"
        info = pyogrio.read_info(path, layer=str(layers[0][0]))
        crs = _norm_crs(info.get("crs"))
        geom = str(info.get("geometry_type") or layers[0][1] or "Unknown")
        if crs in {"none", "Undefined geographic SRS"}:
            return False, crs, geom
        return True, crs, geom
    except Exception as e:
        return False, "none", f"error:{type(e).__name__}"


def _parquet_has_defined_crs(path: Path) -> tuple[bool, str, str]:
    try:
        pf = pq.ParquetFile(path)
        md = pf.metadata.metadata or {}
        blob = md.get(b"geo")
        if not blob:
            return False, "none", "no_geo_metadata"
        geo = json.loads(blob.decode("utf-8"))
        cols = geo.get("columns", {})
        geom_types = set()
        crs_labels = set()
        for spec in cols.values():
            for gt in spec.get("geometry_types") or []:
                geom_types.add(str(gt))
            crs = spec.get("crs")
            if isinstance(crs, dict):
                cid = crs.get("id") if isinstance(crs.get("id"), dict) else None
                if cid and cid.get("authority") and cid.get("code"):
                    crs_labels.add(f"{cid['authority']}:{cid['code']}")
            elif isinstance(crs, str):
                crs_labels.add(_norm_crs(crs))

        if not crs_labels:
            return False, "none", "|".join(sorted(geom_types)) if geom_types else "none"
        return True, "|".join(sorted(crs_labels)), "|".join(sorted(geom_types)) if geom_types else "Unknown"
    except Exception as e:
        return False, "none", f"error:{type(e).__name__}"


def _raster_has_defined_crs(path: Path) -> tuple[bool, str, str]:
    try:
        with rasterio.open(path) as ds:
            crs = _norm_crs(ds.crs)
            if crs in {"none", "Undefined geographic SRS"}:
                return False, crs, "raster"
            return True, crs, "raster"
    except Exception as e:
        return False, "none", f"error:{type(e).__name__}"


def run_validation_gate() -> tuple[bool, list[dict[str, str]]]:
    errors: list[dict[str, str]] = []

    targets: list[Path] = []
    targets += sorted(BUILDINGS_DIR.glob("*_buildings_wards.gpkg"))
    targets += sorted((OUT / "ward_bound" / "canonical").glob("*.gpkg"))
    targets += sorted((OUT / "transport_network_osm" / "gpkg").glob("*.gpkg"))
    targets += sorted((OUT / "open_spaces_osm" / "gpkg").glob("*.gpkg"))
    targets += sorted((OUT / "transport_network_osm" / "parquet").glob("*/*.parquet"))
    targets += sorted((OUT / "open_spaces_osm" / "parquet").glob("*/*.parquet"))
    targets += sorted((OUT / "dem_terrain_ward_stats" / "clipped_rasters").glob("*.tif"))
    targets += sorted((OUT / "lulc_ward_stats_fullrun" / "clipped_rasters").glob("*.tif"))

    seen = set()
    dedup = []
    for p in targets:
        if p.exists() and p not in seen:
            dedup.append(p)
            seen.add(p)

    for p in dedup:
        ext = p.suffix.lower()
        if ext in {".gpkg", ".shp", ".geojson", ".kml"}:
            ok, crs, geom = _vector_has_defined_crs(p)
        elif ext == ".parquet":
            ok, crs, geom = _parquet_has_defined_crs(p)
        elif ext == ".tif":
            ok, crs, geom = _raster_has_defined_crs(p)
        else:
            continue

        if not ok:
            errors.append(
                {
                    "file_path": str(p.relative_to(ROOT)),
                    "format": ext[1:].upper(),
                    "crs": crs,
                    "geometry_type": geom,
                    "reason": "undefined_or_missing_crs",
                }
            )

    report_path = ROOT / "validation_gate_report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Validation Gate Report\n\n")
        f.write("Gate rule: canonical outputs must have explicit CRS.\n\n")
        if errors:
            f.write(f"Status: FAIL ({len(errors)} issues)\n\n")
            f.write("| File | Format | CRS | Geometry | Reason |\n")
            f.write("|---|---|---|---|---|\n")
            for e in errors:
                f.write(f"| {e['file_path']} | {e['format']} | {e['crs']} | {e['geometry_type']} | {e['reason']} |\n")
        else:
            f.write("Status: PASS (no undefined CRS in gated outputs)\n")

    return len(errors) == 0, errors


def write_building_fix_report(results: Iterable[BuildingFixResult]) -> None:
    out_csv = ROOT / "building_canonicalization_report.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "file_name",
            "source_layer",
            "source_crs",
            "source_features",
            "output_crs",
            "output_geometry",
            "status",
            "message",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "file_name": r.file_name,
                    "source_layer": r.source_layer,
                    "source_crs": r.source_crs,
                    "source_features": r.source_features,
                    "output_crs": r.output_crs,
                    "output_geometry": r.output_geometry,
                    "status": r.status,
                    "message": r.message,
                }
            )


def main() -> int:
    results = canonicalize_buildings()
    write_building_fix_report(results)

    changes = rewrite_manifests_relative()

    ok, errors = run_validation_gate()

    failed_buildings = [r for r in results if r.status != "ok"]

    print(f"building_files_processed={len(results)}")
    print(f"building_files_failed={len(failed_buildings)}")
    for m, edits in changes:
        print(f"manifest_edit_cells={edits} path={m.relative_to(ROOT)}")
    print(f"validation_gate_errors={len(errors)}")

    if failed_buildings:
        for r in failed_buildings:
            print(f"BUILDING_FAIL {r.file_name}: {r.message}")
        return 2

    if not ok:
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
