#!/usr/bin/env python3
"""
Download building footprint data for Manhattan from the Google-Microsoft-OSM Open Buildings dataset.
Source: https://source.coop/vida/google-microsoft-osm-open-buildings
"""

import requests
from pathlib import Path

# Manhattan bounding box (approximate)
# Southwest: 40.6795, -74.0479
# Northeast: 40.8820, -73.9067
MANHATTAN_BOUNDS = {
    'min_lat': 40.6795,
    'max_lat': 40.8820,
    'min_lon': -74.0479,
    'max_lon': -73.9067
}

# Base URL for the dataset
BASE_URL = "https://source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA"

# Known S2 cells that cover Manhattan (level 4)
# These S2 cell IDs cover Manhattan and immediate surrounding areas of NYC
# S2 cells at level 4 are large (~1000 km² each), so Manhattan spans 4 cells
MANHATTAN_S2_CELLS = [
    "9714264396238159872",  # Southwest Manhattan + Lower Manhattan
    "9714266595261415424",  # Midtown + Upper West Side
    "9714268794284670976",  # Upper Manhattan + Harlem
    "9714270993307926528",  # Northern Manhattan + surrounding areas
]

def download_file(url: str, output_path: Path):
    """Download a file from URL to output path."""
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = output_path.stat().st_size / (1024 * 1024)  # MB
        print(f"✓ Downloaded: {output_path.name} ({file_size:.2f} MB)")
        return True
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to download: {e}")
        return False


def list_available_s2_cells():
    """
    Try to discover available S2 cell IDs for USA.
    This is a workaround since we don't have direct S2 library access.
    """
    print("Known S2 cells for Manhattan area:")
    for cell_id in MANHATTAN_S2_CELLS:
        print(f"  - {cell_id}")
    print()
    print("Note: Additional cells may be needed for complete Manhattan coverage.")
    print("To find all cells, you can:")
    print("1. Use the S2 geometry library in Python: pip install s2sphere")
    print("2. Browse the dataset at: https://source.coop/vida/google-microsoft-osm-open-buildings")
    print()


def download_manhattan_buildings(output_dir: str = "./manhattan_buildings"):
    """Download building footprints for Manhattan."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Manhattan Building Footprints Downloader")
    print("=" * 80)
    print(f"\nDataset: Google-Microsoft-OSM Open Buildings")
    print(f"Source: https://source.coop/vida/google-microsoft-osm-open-buildings")
    print(f"Output directory: {output_path.absolute()}\n")

    list_available_s2_cells()

    print("Downloading parquet files...\n")

    downloaded = []
    failed = []

    for cell_id in MANHATTAN_S2_CELLS:
        url = f"{BASE_URL}/{cell_id}.parquet"
        output_file = output_path / f"{cell_id}.parquet"

        if output_file.exists():
            print(f"⊙ Already exists: {output_file.name}")
            downloaded.append(cell_id)
        else:
            if download_file(url, output_file):
                downloaded.append(cell_id)
            else:
                failed.append(cell_id)

    # Summary
    print("\n" + "=" * 80)
    print("DOWNLOAD SUMMARY")
    print("=" * 80)
    print(f"Successfully downloaded: {len(downloaded)} files")
    if failed:
        print(f"Failed downloads: {len(failed)} files")
        for cell_id in failed:
            print(f"  - {cell_id}")

    print(f"\nFiles saved to: {output_path.absolute()}")

    # Instructions for next steps
    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("To load and filter the data for Manhattan specifically:")
    print()
    print("import geopandas as gpd")
    print("import pandas as pd")
    print()
    print("# Load parquet file")
    print(f"gdf = gpd.read_parquet('{output_path / MANHATTAN_S2_CELLS[0]}.parquet')")
    print()
    print("# Filter to Manhattan bounding box")
    print(f"manhattan = gdf.cx[{MANHATTAN_BOUNDS['min_lon']}:{MANHATTAN_BOUNDS['max_lon']}, "
          f"{MANHATTAN_BOUNDS['min_lat']}:{MANHATTAN_BOUNDS['max_lat']}]")
    print()
    print("# View statistics")
    print("print(f'Total buildings in Manhattan: {len(manhattan)}')")
    print("print(manhattan.head())")


def find_more_s2_cells():
    """
    Helper function to discover additional S2 cells covering Manhattan.
    Requires s2sphere library: pip install s2sphere
    """
    try:
        from s2sphere import LatLng, CellId, RegionCoverer

        print("Finding S2 cells covering Manhattan...")

        # Define Manhattan bounding box corners
        sw = LatLng.from_degrees(MANHATTAN_BOUNDS['min_lat'], MANHATTAN_BOUNDS['min_lon'])
        ne = LatLng.from_degrees(MANHATTAN_BOUNDS['max_lat'], MANHATTAN_BOUNDS['max_lon'])

        # Create region coverer
        coverer = RegionCoverer()
        coverer.min_level = 4
        coverer.max_level = 4  # Level 4 S2 cells (match dataset partitioning)

        # Get covering cells (simplified - using just the corners)
        cells = [
            CellId.from_lat_lng(sw).parent(4),
            CellId.from_lat_lng(ne).parent(4),
            CellId.from_lat_lng(LatLng.from_degrees(MANHATTAN_BOUNDS['min_lat'], MANHATTAN_BOUNDS['max_lon'])).parent(4),
            CellId.from_lat_lng(LatLng.from_degrees(MANHATTAN_BOUNDS['max_lat'], MANHATTAN_BOUNDS['min_lon'])).parent(4),
        ]

        # Get unique cell IDs
        cell_ids = sorted(set(str(cell.id()) for cell in cells))

        print(f"\nFound {len(cell_ids)} S2 cells (level 4) covering Manhattan:")
        for cell_id in cell_ids:
            print(f"  - {cell_id}")

        return cell_ids

    except ImportError:
        print("s2sphere library not installed.")
        print("Install with: pip install s2sphere")
        print("\nUsing known cells only.")
        return MANHATTAN_S2_CELLS


if __name__ == "__main__":
    import sys

    # Try to find additional S2 cells if library is available
    print("Attempting to discover S2 cells for Manhattan...\n")
    discovered_cells = find_more_s2_cells()

    if discovered_cells and discovered_cells != MANHATTAN_S2_CELLS:
        print("\nUpdate MANHATTAN_S2_CELLS in the script with these cell IDs.")
        MANHATTAN_S2_CELLS.extend([c for c in discovered_cells if c not in MANHATTAN_S2_CELLS])

    print("\n")

    # Download files
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "./manhattan_buildings"
    download_manhattan_buildings(output_dir)
