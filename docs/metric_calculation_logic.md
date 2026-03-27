# Metric Calculation Logic

## Purpose
This document describes how UrbanMor computes each metric, which source layers it depends on, what CRS/measurement rules are used, and where a metric is a proxy rather than a direct measurement.

## Global Rules

### Geometry normalization
- Input polygons are normalized to valid `MULTIPOLYGON` geometries in EPSG:4326.
- Empty or invalid geometries return `NULL`.
- City ids are normalized to lowercase alphanumeric identifiers.

### Area, length, and distance policy
- Polygon areas are computed geodesically with `ST_Area(geom::geography)`.
- Distance-to-target metrics use geodesic distance with `ST_Distance(...::geography, ...::geography)`.
- Road-length density and pedestrian-share metrics use geodesic line lengths after clipping.
- Some shape/topology metrics still use EPSG:3857 internally where a projected plane is required for noding, oriented envelopes, or segment-chord calculations.

### Raster policy
- Raster metrics clip rasters to the input polygon in the raster SRID.
- Elevation/slope statistics use raster summary/value-count functions over valid pixels only.
- Binary raster percentages compute true-pixel area divided by polygon area.

### Sampling policy
- `open.distance_to_nearest_park` and `transit.distance_to_metro_or_rail` sample the polygon on a 300 m square grid, capped at 800 samples, with centroid fallback when no grid cell is produced.
- `bldg.clustering_coeff` uses up to 400 building centroids.
- `bldg.avg_interbuilding_distance` uses up to 600 building centroids.
- `bldg.elongation` and `bldg.orientation` use up to 2000 buildings.
- `bldg.edge_coverage` samples clipped road centerlines at roughly 20 m spacing.

### Null behavior
- Metrics return `NULL` when required source tables are missing, the input geometry is invalid/empty, or no valid observations exist.
- Composite metrics return `NULL` only when all component metrics are `NULL`; otherwise missing components are excluded and remaining weights are renormalized.

## Road and Transit Metrics

### Shared road graph derivation
The road-family metrics use a shared graph derivation:
- Keep only motorized road classes (`motorway` through `service`/`road`/`busway`).
- Clip roads to the polygon in EPSG:3857.
- Node the unioned network with `ST_Node(ST_UnaryUnion(...))`.
- Split the result into noded edges.
- Build node degrees from snapped start/endpoints (0.5 m snap grid).
- Compute orientation entropy from 10-degree bearing bins weighted by edge length.
- Compute block statistics by polygonizing the noded road network inside the AOI.

| Metric | Formula / Logic | Unit | Notes |
| --- | --- | --- | --- |
| `road.intersection_density` | `intersection_count / area_sqkm` | intersections/sq km | Intersections are nodes with degree `>= 3`. |
| `road.cnr` | `connected_node_count / node_count * 100` | percent | Connected nodes are graph nodes with degree `>= 2`. |
| `road.node_density` | `node_count / area_sqkm` | nodes/sq km | Node count comes from snapped graph endpoints. |
| `road.edge_density` | `edge_length_km / area_sqkm` | km/sq km | Edge length comes from noded graph edges. |
| `road.avg_block_size` | Mean polygonized motorized-road block area inside AOI | sq m | Derived directly from the noded graph. |
| `road.block_size_variance` | Population variance of polygonized motorized-road block area | sq m^2 | Derived directly from the noded graph. |
| `road.street_connectivity_index` | `clamp((edge_count / node_count) / 2 * 100)` | index | Link-node ratio normalized so planar 4-way mesh behavior maps near 100. |
| `road.culdesac_ratio` | `culdesac_count / node_count * 100` | percent | Cul-de-sacs are nodes with degree `= 1`. |
| `road.circuity` | Length-weighted mean of `segment_length / endpoint_chord_length` | ratio | Proxy only. This is segment sinuosity, not OD routing circuity. |
| `road.orientation_entropy` | Shannon entropy of 10-degree road-bearing bins | bits | Length-weighted. Higher means less directional order. |
| `road.network_density_by_type` | Geodesic road length per sq km by `highway` class | JSON | Returns `total_km_per_sq_km` and `by_highway`. |
| `road.pedestrian_infra_ratio` | `pedestrian_capable_length / total_clipped_length * 100` | percent | Uses `is_pedestrian_link` when enriched table exists, else OSM/tag heuristics. |
| `transit.stop_density` | `stop_count / area_sqkm` | stops/sq km | Count of transit features intersecting the polygon. |
| `transit.distance_to_metro_or_rail` | Mean geodesic nearest distance from 300 m interior samples to nearest metro/rail target | meters | Uses KNN shortlist then exact geography distance. |
| `transit.distance_to_bus_stop` | Mean geodesic nearest distance from 300 m interior samples to nearest bus-stop/platform/station target | meters | Uses KNN shortlist then exact geography distance. |
| `transit.coverage_500m` | `area(intersection(polygon, union(buffer_500m(transit_targets)))) / area(polygon) * 100` | percent | Uses 500 m station/stop buffers. |

## Built Form Metrics

| Metric | Formula / Logic | Unit | Notes |
| --- | --- | --- | --- |
| `bldg.bcr` | `sum(clipped_building_area_m2) / polygon_area_m2 * 100` | percent | Uses clipped footprint area, not full-building area. |
| `bldg.density_per_ha` | `building_count / area_ha` | buildings/ha | Counts buildings whose interior representative point is contained by the polygon. |
| `bldg.avg_footprint_size` | Mean clipped footprint area | sq m | Buildings under 1 sq m are ignored. |
| `bldg.size_distribution` | Population variance, P50, and P90 of clipped footprint area | summary stats | Returns JSON with `variance_m2`, `p50_m2`, `p90_m2`. |
| `bldg.clustering_coeff` | Mean local clustering coefficient of a 60 m centroid-neighbor graph, scaled by `* 100` | index | Uses up to 400 centroids. This is now an actual graph coefficient, not an inverse-distance proxy. |
| `bldg.avg_interbuilding_distance` | Mean geodesic nearest-neighbor centroid distance | meters | Uses up to 600 centroids and a KNN shortlist for speed. |
| `bldg.elongation` | Mean `major_axis / minor_axis` from oriented-envelope rectangles | ratio | Uses up to 2000 buildings. |
| `bldg.orientation` | Circular mean axial orientation of major oriented-envelope axis | degrees | Axial mean is computed on doubled angles and folded back to `0..180`. |
| `bldg.footprint_regularity` | Mean isoperimetric quotient `(4πA / P^2) * 100` | index | Computed on clipped footprint polygons. |
| `bldg.edge_coverage` | Share of roughly 20 m road samples within 15 m of nearby building footprints | percent | Proxy only. This approximates active/fronted road edge coverage without parcel frontage lines. |
| `bldg.far_proxy` | `sum(area_weighted_floor_area_proxy_m2) / polygon_area_m2` | ratio proxy | If building levels exist, floor-area proxy is scaled by clipped footprint share; otherwise clipped footprint area is used as a one-storey fallback. Derived levels are a low-confidence area-quantile heuristic. |

## Land Use and Open Space Metrics

### LULC helper policy
- LULC percentages use per-class area extracted from the clipped LULC raster and the `lulc_class_map` metadata.
- Canonical-class metrics use `lulc_class_map.canonical_class`.
- Flag-based metrics use boolean flags such as `is_green`, `is_built_up`, `is_water`, and `is_vacant_candidate`.

| Metric | Formula / Logic | Unit | Notes |
| --- | --- | --- | --- |
| `lulc.green_cover_pct` | Share of raster area flagged `is_green` | percent | Direct raster-area share. |
| `lulc.mix_index` | Shannon entropy of LULC class area proportions | nats | Higher means more mixed land-use composition. |
| `lulc.residential_cover_pct` | Share of raster area flagged `is_residential_proxy` | percent | Returns null until the class map defines a residential proxy distinct from generic built-up. |
| `lulc.agriculture_pct` | Share of raster area mapped to canonical class `agriculture` | percent | Uses canonical class mapping. |
| `lulc.water_coverage_pct` | Prefer clipped canonical water polygons; otherwise raster water share | percent | Vector water bodies override raster-derived water when available. |
| `lulc.impervious_ratio` | `max(lulc_built_area, area(union(clipped_buildings, clipped_road_buffers))) / polygon_area * 100`, capped to polygon area | percent | Road area uses a 4 m buffer around clipped road centerlines and is re-clipped to AOI before union. |
| `open.bare_ground_pct` | Share of raster area mapped to canonical class `bare_ground` | percent | Raster-derived. |
| `open.park_green_space_density` | `park_area_ha / area_sqkm` | hectares/sq km | Uses open-space polygons from `green_parks_vegetation` and `sports_play_open`. |
| `open.distance_to_nearest_park` | Mean geodesic nearest distance from 300 m interior samples to citywide park polygons | meters | Fixed to search the citywide park layer rather than only parks clipped inside the polygon. |
| `open.vacant_land_pct` | Prefer binary vacant-land raster share; else LULC `is_vacant_candidate` share | percent | Raster path returns true-pixel area share. |
| `open.riparian_buffer_integrity` | `open_or_natural_area_inside_riparian_buffers / riparian_buffer_area * 100` | percent | Uses `open_surfaces` when available; otherwise falls back to selected open-space polygons. |

## Topography Metrics

| Metric | Formula / Logic | Unit | Notes |
| --- | --- | --- | --- |
| `topo.mean_elevation` | Mean DEM value over clipped valid pixels | meters | Raster summary stat. |
| `topo.elevation_range` | `max_elevation - min_elevation` | meters | Raster summary stat. |
| `topo.mean_slope` | Mean slope-raster value over clipped valid pixels | degrees | Raster summary stat. |
| `topo.steep_area_pct` | Share of slope pixels with value `> 15` | percent | Threshold is fixed at 15 degrees. |
| `topo.flat_area_pct` | Share of slope pixels with value `< 3` | percent | Threshold is fixed at 3 degrees. |
| `topo.natural_constraint_index` | `clamp(0.7 * steep_area_pct + 0.3 * water_body_pct)` | index | Proxy only. Weights are heuristic and factors overlap. |

## Composite Metrics

### Composite conventions
- Every composite is clamped to `0..100`.
- If every component is `NULL`, the composite is `NULL`.
- Otherwise missing components are excluded and weights are renormalized over available components.

| Metric | Formula / Logic | Unit | Notes |
| --- | --- | --- | --- |
| `cmp.walkability_index` | `0.25*intersection_norm + 0.25*cnr + 0.20*ped_ratio + 0.20*transit_coverage + 0.10*transit_distance_score` | index | `intersection_norm = clamp(intersection_density / 120 * 100)`; `transit_distance_score = 100 / (1 + distance_m / 500)`. |
| `cmp.informality_index` | `0.35*density_norm + 0.25*culdesac_ratio + 0.20*(100-green_cover) + 0.20*vacant_pct` | index | `density_norm = clamp(building_density_per_ha / 250 * 100)`. |
| `cmp.heat_island_proxy` | `0.50*impervious + 0.35*(100-green_cover) + 0.15*flat_area` | index proxy | Proxy only; no thermal imagery/station observations are used. |
| `cmp.development_pressure` | `0.50*vacant_pct + 0.20*(100-density_norm) + 0.30*edge_norm` | index | `density_norm = clamp(building_density_per_ha / 250 * 100)`; `edge_norm = clamp(edge_density / 30 * 100)`. |
| `cmp.topographic_constraint_expansion` | `natural_constraint` (fallback: `steep_pct`) | index | Avoids double-counting steep slope where natural-constraint already includes it. |
| `cmp.green_accessibility` | `0.40*park_distance_score + 0.30*park_density_norm + 0.30*green_cover` | index | `park_distance_score = 100 / (1 + distance_m / 300)`; `park_density_norm = clamp(park_density_ha_sqkm / 80 * 100)`. |
| `cmp.transit_access_green` | `0.55*transit_coverage + 0.45*green_accessibility` | index | Combines proximity to transit and access to green space. |
| `cmp.compactness` | `0.30*bcr + 0.25*intersection_norm + 0.25*mix_norm + 0.20*circuity_score` | index | `mix_norm = clamp(mix_index / ln(11) * 100)`; `circuity_score = 100` when `circuity <= 1`, else `100 / (1 + ((circuity - 1) * 5))`. |

## Known Proxy / Blocked Metrics

| Metric | Status | Reason |
| --- | --- | --- |
| `road.circuity` | Proxy only | Uses segment sinuosity because OD routing circuity is not available without a routing solver in-database. |
| `bldg.edge_coverage` | Proxy only | Uses road samples and building proximity because parcel/frontage lines are not available. |
| `bldg.far_proxy` | Proxy only | Depends on inferred or tagged building levels and falls back to footprint area when levels are missing. |
| `cmp.heat_island_proxy` | Proxy only | Composite proxy; no thermal imagery or station observations are used. |

## File Map
- Core helpers: `sql/000_metric_helpers.sql`
- Road and transit metrics: `sql/roads_metrics.sql`
- Building metrics: `sql/building_metrics.sql`
- Land use and open space metrics: `sql/landuse_metrics.sql`
- Topography metrics: `sql/topography_metrics.sql`
- Composite metrics: `sql/composite_metrics.sql`
- Registry / metadata: `metrics_registry.yaml`, `metric_coverage_matrix.csv`, `metric_quality_flags.csv`
