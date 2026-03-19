#!/usr/bin/env python3
"""
Explore and filter Manhattan building footprints from downloaded parquet files.
"""
import geopandas as gpd
import pandas as pd
from pathlib import Path

# Manhattan bounding box (approximate)
MANHATTAN_BBOX = {
    'min_lon': -74.0479,
    'max_lon': -73.9067,
    'min_lat': 40.6795,
    'max_lat': 40.8820
}

def explore_buildings():
    """Load and explore Manhattan building data."""
    data_dir = Path("manhattan_buildings")
    parquet_files = sorted(data_dir.glob("*.parquet"))

    if not parquet_files:
        print("No parquet files found in manhattan_buildings/")
        print("Please run download_manhattan_buildings.py first.")
        return

    print("=" * 80)
    print("MANHATTAN BUILDING FOOTPRINTS EXPLORER")
    print("=" * 80)
    print(f"\nFound {len(parquet_files)} parquet files\n")

    # Load all files
    all_gdfs = []
    for file in parquet_files:
        print(f"Loading {file.name}...")
        try:
            gdf = gpd.read_parquet(file)
            print(f"  - Rows: {len(gdf):,}")
            print(f"  - CRS: {gdf.crs}")
            if len(gdf) > 0:
                print(f"  - Columns: {', '.join(gdf.columns)}")
            all_gdfs.append(gdf)
        except Exception as e:
            print(f"  ✗ Error loading {file.name}: {e}")

    if not all_gdfs:
        print("\n✗ No data could be loaded")
        return

    # Combine all data
    print(f"\nCombining {len(all_gdfs)} datasets...")
    combined = pd.concat(all_gdfs, ignore_index=True)
    print(f"✓ Total buildings loaded: {len(combined):,}")

    # Filter to Manhattan bounding box
    print(f"\nFiltering to Manhattan bounding box:")
    print(f"  Longitude: {MANHATTAN_BBOX['min_lon']} to {MANHATTAN_BBOX['max_lon']}")
    print(f"  Latitude: {MANHATTAN_BBOX['min_lat']} to {MANHATTAN_BBOX['max_lat']}")

    manhattan = combined.cx[
        MANHATTAN_BBOX['min_lon']:MANHATTAN_BBOX['max_lon'],
        MANHATTAN_BBOX['min_lat']:MANHATTAN_BBOX['max_lat']
    ]

    print(f"✓ Buildings in Manhattan bbox: {len(manhattan):,}")

    if len(manhattan) == 0:
        print("\n⚠️  No buildings found in Manhattan bounding box!")
        print("This might mean the S2 cells don't cover Manhattan properly.")
        print("\nShowing sample of raw data:")
        print(combined.head())
        return

    # Statistics
    print("\n" + "=" * 80)
    print("MANHATTAN BUILDING STATISTICS")
    print("=" * 80)

    # Data sources
    if 'bf_source' in manhattan.columns:
        print("\nBuilding Sources:")
        print(manhattan['bf_source'].value_counts())

    # Confidence scores
    if 'confidence' in manhattan.columns:
        print(f"\nConfidence Scores:")
        print(f"  Mean: {manhattan['confidence'].mean():.3f}")
        print(f"  Median: {manhattan['confidence'].median():.3f}")
        print(f"  Min: {manhattan['confidence'].min():.3f}")
        print(f"  Max: {manhattan['confidence'].max():.3f}")

    # Area statistics
    if 'area_in_meters' in manhattan.columns:
        print(f"\nBuilding Area (m²):")
        area_stats = manhattan['area_in_meters'].describe()
        print(f"  Mean: {area_stats['mean']:.1f}")
        print(f"  Median: {area_stats['50%']:.1f}")
        print(f"  Min: {area_stats['min']:.1f}")
        print(f"  Max: {area_stats['max']:.1f}")
        print(f"  Total: {manhattan['area_in_meters'].sum():,.0f} m²")

    # Sample data
    print("\n" + "=" * 80)
    print("SAMPLE DATA (first 5 buildings)")
    print("=" * 80)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(manhattan.head())

    # Save filtered data
    print("\n" + "=" * 80)
    print("SAVING FILTERED DATA")
    print("=" * 80)

    output_dir = Path("manhattan_buildings_filtered")
    output_dir.mkdir(exist_ok=True)

    # Save as GeoJSON
    geojson_path = output_dir / "manhattan_buildings.geojson"
    print(f"Saving GeoJSON: {geojson_path}")
    manhattan.to_file(geojson_path, driver="GeoJSON")

    # Save as Parquet
    parquet_path = output_dir / "manhattan_buildings.parquet"
    print(f"Saving Parquet: {parquet_path}")
    manhattan.to_parquet(parquet_path)

    # Save as CSV (without geometry)
    csv_path = output_dir / "manhattan_buildings.csv"
    print(f"Saving CSV (no geometry): {csv_path}")
    manhattan_no_geom = manhattan.drop(columns=['geometry'])
    manhattan_no_geom.to_csv(csv_path, index=False)

    print(f"\n✓ All files saved to: {output_dir.absolute()}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total buildings in Manhattan: {len(manhattan):,}")
    print(f"Total building area: {manhattan['area_in_meters'].sum() / 1_000_000:.2f} km²")
    print(f"Average building size: {manhattan['area_in_meters'].mean():.1f} m²")
    print(f"\nFiles ready for import into UrbanMor!")


if __name__ == "__main__":
    try:
        import geopandas
    except ImportError:
        print("✗ GeoPandas is not installed")
        print("Install with: pip install geopandas")
        exit(1)

    explore_buildings()
