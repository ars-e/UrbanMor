# Manhattan Building Footprints Download Guide

## Dataset Information

**Source**: [Google-Microsoft-OSM Open Buildings](https://source.coop/vida/google-microsoft-osm-open-buildings) by VIDA
**Coverage**: 2.7+ billion building footprints worldwide
**Format**: GeoParquet, FlatGeobuf, PMTiles
**Organization**: Partitioned by country (ISO code) and S2 grid cells (level 4)

---

## Manhattan S2 Cells (Level 4)

Manhattan is covered by the following S2 cell IDs at level 4:

```
9714264396238159872
9714266595261415424
9714268794284670976
9714270993307926528
```

**Note**: These cells cover Manhattan and some surrounding areas in NYC.

---

## Download Methods

### Method 1: Direct Download (Recommended for Single Files)

Download individual parquet files using curl or wget:

```bash
# Create output directory
mkdir -p manhattan_buildings

# Download S2 cell parquet files
cd manhattan_buildings

# Cell 1
curl -O "https://source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA/9714264396238159872.parquet"

# Cell 2
curl -O "https://source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA/9714266595261415424.parquet"

# Cell 3
curl -O "https://source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA/9714268794284670976.parquet"

# Cell 4
curl -O "https://source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA/9714270993307926528.parquet"
```

---

### Method 2: Python Script (Automated Batch Download)

Use the provided Python script:

```bash
python download_manhattan_buildings.py manhattan_buildings/
```

---

### Method 3: DuckDB Query (Cloud-Native, No Download)

Query the data directly without downloading:

```sql
INSTALL httpfs;
LOAD httpfs;
SET s3_region='us-west-2';

-- Query buildings in Manhattan bounding box
SELECT *
FROM read_parquet('https://source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA/*.parquet')
WHERE ST_Within(
    geometry,
    ST_MakeEnvelope(-74.0479, 40.6795, -73.9067, 40.8820, 4326)
);
```

---

## Processing the Data

### Load Parquet Files with GeoPandas

```python
import geopandas as gpd
import pandas as pd
from pathlib import Path

# Manhattan bounding box
MANHATTAN_BBOX = (-74.0479, 40.6795, -73.9067, 40.8820)  # (min_lon, min_lat, max_lon, max_lat)

# Load all downloaded parquet files
data_dir = Path("manhattan_buildings")
parquet_files = list(data_dir.glob("*.parquet"))

print(f"Found {len(parquet_files)} parquet files")

# Read and combine
gdfs = []
for file in parquet_files:
    print(f"Loading {file.name}...")
    gdf = gpd.read_parquet(file)
    gdfs.append(gdf)

# Combine all
all_buildings = pd.concat(gdfs, ignore_index=True)
print(f"Total buildings loaded: {len(all_buildings)}")

# Filter to Manhattan bounding box
manhattan_buildings = all_buildings.cx[MANHATTAN_BBOX[0]:MANHATTAN_BBOX[2],
                                       MANHATTAN_BBOX[1]:MANHATTAN_BBOX[3]]

print(f"Buildings in Manhattan bbox: {len(manhattan_buildings)}")

# Save filtered data
manhattan_buildings.to_file("manhattan_buildings_only.geojson", driver="GeoJSON")
manhattan_buildings.to_parquet("manhattan_buildings_only.parquet")

print("✓ Manhattan buildings saved!")
```

---

### Explore the Data

```python
# View statistics
print(manhattan_buildings.info())
print(manhattan_buildings.head())

# Check data sources
print("\nBuilding Sources:")
print(manhattan_buildings['bf_source'].value_counts())

# Average confidence score
print(f"\nAverage Confidence: {manhattan_buildings['confidence'].mean():.2f}")

# Area statistics
print(f"\nArea Statistics (m²):")
print(manhattan_buildings['area_in_meters'].describe())

# Visualize
import matplotlib.pyplot as plt

manhattan_buildings.plot(figsize=(10, 15), alpha=0.5)
plt.title("Manhattan Building Footprints")
plt.savefig("manhattan_buildings_map.png", dpi=300, bbox_inches='tight')
plt.show()
```

---

## Data Schema

| Field | Type | Description |
|-------|------|-------------|
| `geometry` | Polygon | Building footprint polygon (EPSG:4326) |
| `confidence` | float | Model confidence score (0-1) |
| `bf_source` | string | Source: "google", "microsoft", or "osm" |
| `area_in_meters` | float | Building area in square meters |
| `full_plus_code` | string | Plus code location identifier |

---

## Manhattan Geographic Extent

```
Bounding Box:
  Southwest: 40.6795° N, 74.0479° W
  Northeast: 40.8820° N, 73.9067° W

Approximate Area: ~59 km²
Estimated Building Count: ~50,000 - 100,000 buildings
```

---

## Integration with UrbanMor

### Convert to PostGIS

```python
from sqlalchemy import create_engine

# Connect to UrbanMor database
engine = create_engine('postgresql://user:password@localhost:5432/urbanmor')

# Load to PostGIS
manhattan_buildings.to_postgis(
    name='manhattan_buildings_source',
    con=engine,
    schema='buildings',
    if_exists='replace',
    index=True
)

print("✓ Loaded to PostGIS table: buildings.manhattan_buildings_source")
```

### Normalize for UrbanMor Schema

```sql
-- Create normalized buildings table
CREATE TABLE buildings.manhattan_buildings_normalized AS
SELECT
    row_number() OVER () AS id,
    ST_Transform(geom, 3857) AS geom,  -- Convert to Web Mercator
    confidence,
    bf_source AS source_layer,
    area_in_meters AS footprint_area_m2,
    full_plus_code
FROM buildings.manhattan_buildings_source
WHERE ST_IsValid(geom) AND ST_Area(geom::geography) > 1.0;

-- Create spatial index
CREATE INDEX idx_manhattan_buildings_geom
ON buildings.manhattan_buildings_normalized USING GIST(geom);

-- Add to city boundaries (if not already in UrbanMor)
-- This would require creating a Manhattan/NYC ward/boundary entry
```

---

## Finding More S2 Cells

To discover S2 cells for other areas:

### Online Tool
Use the [S2 Geometry Calculator](https://gojekfarm.github.io/s2-calc/)
1. Enter latitude and longitude
2. Set level to 4
3. Copy the S2 cell ID

### Python (with s2sphere)
```python
from s2sphere import LatLng, CellId

# Convert coordinates to S2 cell ID at level 4
lat, lon = 40.7831, -73.9712
cell = CellId.from_lat_lng(LatLng.from_degrees(lat, lon)).parent(4)
print(f"S2 Cell ID: {cell.id()}")
```

---

## References

- **Dataset**: [Google-Microsoft-OSM Open Buildings](https://source.coop/vida/google-microsoft-osm-open-buildings)
- **Blog**: [Cloud-Native Building Footprints Dataset](https://medium.com/vida-engineering/updating-the-ultimate-cloud-native-building-footprints-dataset-6d4384cb93c4)
- **S2 Geometry**: [S2 Geometry Library](https://s2geometry.io/)
- **Source Cooperative**: [Source.coop Platform](https://source.coop/)

---

## File Size Estimates

Each S2 cell (level 4) parquet file for urban areas like NYC:
- **File Size**: ~50-200 MB per cell (depending on building density)
- **Manhattan Total**: ~200-500 MB for all 4 cells
- **Uncompressed**: ~2-3x larger when loaded into memory

Plan for adequate disk space and bandwidth.
