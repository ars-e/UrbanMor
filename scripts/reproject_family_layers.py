#!/usr/bin/env python3
from __future__ import annotations

import csv
import subprocess
from pathlib import Path

import pyogrio

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"


def norm_city(s: str) -> str:
    t = s.lower()
    if "ahmd" in t or "ahd" in t:
        return "ahmedabad"
    if "beng" in t:
        return "bengaluru"
    if "chd" in t:
        return "chandigarh"
    if "chen" in t:
        return "chennai"
    if "del" in t:
        return "delhi"
    if "kol" in t:
        return "kolkata"
    if "mum" in t:
        return "mumbai"
    return t


def reproject_to_gpkg(src: Path, dst: Path, layer_name: str) -> tuple[bool, str]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    cmd = [
        "ogr2ogr",
        "-f",
        "GPKG",
        str(dst),
        str(src),
        "-nln",
        layer_name,
        "-t_srs",
        "EPSG:4326",
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return True, "ok"
    except subprocess.CalledProcessError as e:
        msg = (e.stderr or e.stdout or str(e)).strip().replace("\n", " ")[:260]
        return False, msg


def main() -> int:
    report_rows = []

    jobs = []
    for shp in sorted((OUT / "dist_bound").glob("*/*.shp")):
        city = norm_city(shp.parent.name)
        dst = OUT / "dist_bound" / "canonical_epsg4326" / f"{city}_dist_bound.gpkg"
        jobs.append(("admin_district", city, shp, dst, f"{city}_dist_bound"))

    for shp in sorted((OUT / "sub_dist_bound").glob("*/*.shp")):
        city = norm_city(shp.parent.name)
        dst = OUT / "sub_dist_bound" / "canonical_epsg4326" / f"{city}_subdist_bound.gpkg"
        jobs.append(("admin_subdistrict", city, shp, dst, f"{city}_subdist_bound"))

    for family, city, src, dst, layer in jobs:
        src_layer = pyogrio.list_layers(src)[0][0]
        src_info = pyogrio.read_info(src, layer=src_layer)
        src_crs = str(src_info.get("crs"))

        ok, msg = reproject_to_gpkg(src, dst, layer)

        out_crs = "none"
        out_geom = "none"
        out_features = 0
        if ok:
            out_layer = pyogrio.list_layers(dst)[0][0]
            out_info = pyogrio.read_info(dst, layer=out_layer)
            out_crs = str(out_info.get("crs"))
            out_geom = str(out_info.get("geometry_type"))
            out_features = int(out_info.get("features") or 0)

        report_rows.append(
            {
                "family": family,
                "city": city,
                "source_file": str(src.relative_to(ROOT)),
                "source_crs": src_crs,
                "target_file": str(dst.relative_to(ROOT)),
                "target_crs": out_crs,
                "target_geometry_type": out_geom,
                "target_features": out_features,
                "status": "ok" if ok else "failed",
                "message": msg,
            }
        )

    out_csv = ROOT / "reprojection_stage_report.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "family",
            "city",
            "source_file",
            "source_crs",
            "target_file",
            "target_crs",
            "target_geometry_type",
            "target_features",
            "status",
            "message",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(report_rows)

    failed = sum(1 for r in report_rows if r["status"] != "ok")
    print(f"jobs={len(report_rows)} failed={failed} report={out_csv}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
