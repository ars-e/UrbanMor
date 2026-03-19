# Urban Design Metric Formula Review

**Reviewer:** Claude (Urban Design & Planning Perspective)
**Date:** 2026-03-09
**Scope:** All SQL metric calculation functions in `/sql/` directory

---

## Executive Summary

### Critical Issues Found
1. ✅ **FIXED**: Connected Node Ratio (CNR) in `roads_metrics.sql`
   - **Was**: `intersection_count / (intersection_count + culdesac_count)`
   - **Now**: `connected_node_count / node_count` ✓

2. ⚠️ **ISSUE**: Informality Index (line 69-104, `composite_metrics.sql`)
   - Inverts CNR as `(100 - CNR)` which double-penalizes disconnected networks
   - Should use a direct informality proxy like `culdesac_ratio` instead

3. ⚠️ **QUESTIONABLE**: Impervious Ratio weighting (line 478, `landuse_metrics.sql`)
   - Uses `LEAST(area, GREATEST(lulc_built, bldg + road))` logic
   - May underestimate when LULC data is incomplete

---

## 1. ROADS_METRICS.SQL

### ✅ Connected Node Ratio (CNR) - CORRECTED
**Lines 320-338**
```sql
RETURN (v_stats.connected_node_count / v_stats.node_count) * 100.0
```
**Status**: ✅ **NOW CORRECT**
- **Formula**: Percentage of nodes with degree ≥ 2
- **Urban Design Rationale**: Measures network connectivity; higher = better connected, less dead-ends
- **Reference**: Standard graph theory metric adapted for street networks

---

### ✅ Intersection Density
**Lines 298-318**
```sql
RETURN intersection_count / v_area_sqkm
```
**Status**: ✅ **CORRECT**
- **Formula**: Intersections (degree ≥ 3) per km²
- **Urban Design Rationale**: Higher density = better walkability, more route choices
- **Typical Range**: 60-150 per km² (walkable neighborhoods)

---

### ✅ Street Connectivity Index
**Lines 417-438**
```sql
v_raw := edge_count / node_count
RETURN LEAST(100.0, GREATEST(0.0, (v_raw / 3.0) * 100.0))
```
**Status**: ✅ **CORRECT**
- **Formula**: Normalized link-node ratio (edges/nodes), scaled where 3.0 = 100%
- **Urban Design Rationale**: Grid ~3.0, tree ~1.0; measures network completeness
- **Reference**: Allan Jacobs, "Great Streets" (1993)

---

### ✅ Cul-de-sac Ratio
**Lines 440-458**
```sql
RETURN (culdesac_count / node_count) * 100.0
```
**Status**: ✅ **CORRECT**
- **Formula**: Dead-end nodes as % of all nodes
- **Urban Design Rationale**: Lower = better connectivity; inverse of CNR for degree-1 nodes
- **Note**: Complementary to CNR but not identical (CNR includes degree-2 nodes)

---

### ✅ Circuity
**Lines 461-533**
```sql
weighted_avg_ratio = SUM(ratio * path_m) / SUM(path_m)
ratio = path_m / chord_m
```
**Status**: ✅ **CORRECT**
- **Formula**: Length-weighted average of segment path/chord ratio
- **Urban Design Rationale**: 1.0 = perfectly straight; >1.2 may indicate inefficient routing
- **Note**: Measures sinuosity per-segment, not OD route circuity (as documented)

---

### ✅ Orientation Entropy
**Lines 535-549, computed in _road_graph_stats**
```sql
h_bits = -SUM(p * (LN(p) / LN(2.0)))
-- where p = bin_length_m / total_length_m
-- bins are 10° intervals (0-180°)
```
**Status**: ✅ **CORRECT**
- **Formula**: Shannon entropy of road orientations, binned by 10° intervals
- **Urban Design Rationale**:
  - Grid = low entropy (~2-3 bits)
  - Organic = high entropy (~4+ bits)
- **Reference**: Geoff Boeing, "OSMnx" (2017)

---

### ⚠️ Pedestrian Infrastructure Ratio
**Lines 621-723**
```sql
RETURN (ped_m / total_m) * 100.0
```
**Status**: ⚠️ **CONCEPTUALLY VALID, DATA-DEPENDENT**
- **Formula**: Pedestrian-designated road length / total road length
- **Issue**: Classification relies on OSM tags (footway, pedestrian, path, steps, living_street, cycleway, track, foot=yes, bicycle=yes)
- **Urban Design Concern**:
  - Many streets have sidewalks but aren't tagged as pedestrian
  - Better named "dedicated_pedestrian_path_ratio"
  - Doesn't measure sidewalk presence on regular streets
- **Recommendation**: Clarify metric name or enhance with sidewalk data

---

### ✅ Transit Stop Density
**Lines 729-783**
```sql
RETURN stop_count / v_area_sqkm
```
**Status**: ✅ **CORRECT**
- **Formula**: Transit stops per km²
- **Urban Design Rationale**: 4-8 per km² = good coverage (400-800m spacing)

---

### ⚠️ Transit Distance to Metro/Rail
**Lines 785-872**
```sql
SELECT AVG(nearest_m) FROM grid_samples
```
**Status**: ⚠️ **CORRECT CALCULATION, SEMANTIC AMBIGUITY**
- **Formula**: Average distance from grid sample points to nearest metro/rail station
- **Issue**: Mixes metro_stations, rail_stations, public_transport_stations, public_transport_stops
- **Urban Design Concern**: "Metro/Rail" name implies rapid transit only, but includes all public transport stops
- **Recommendation**:
  - Rename to `transit_distance_to_rapid_transit` OR
  - Filter to only metro_stations + rail_stations

---

### ✅ Transit Coverage 500m
**Lines 874-948**
```sql
coverage_pct = (buffered_area ∩ polygon_area) / polygon_area * 100
```
**Status**: ✅ **CORRECT**
- **Formula**: % of area within 500m walking distance of transit stops
- **Urban Design Rationale**: 500m = ~6 min walk, standard TOD radius
- **Reference**: Peter Calthorpe, "The Next American Metropolis" (1993)

---

## 2. BUILDING_METRICS.SQL

### ✅ Building Coverage Ratio (BCR)
**Lines 110-153**
```sql
RETURN (bldg_area_m2 / area_m2) * 100.0
```
**Status**: ✅ **CORRECT**
- **Formula**: Building footprint area / total area
- **Urban Design Rationale**: 15-40% typical for urban areas; >60% = very dense
- **Reference**: Standard urban form metric

---

### ✅ Building Density
**Lines 155-193**
```sql
RETURN building_count / area_ha
```
**Status**: ✅ **CORRECT**
- **Formula**: Buildings per hectare
- **Urban Design Rationale**: 20-60 per ha typical residential; >100 = high density

---

### ✅ Average Footprint Size
**Lines 195-235**
```sql
RETURN AVG(clipped_area_m2)
```
**Status**: ✅ **CORRECT**
- **Formula**: Mean building footprint area in m²
- **Urban Design Rationale**: 50-150m² = small-scale; >500m² = large-scale

---

### ✅ Building Size Distribution
**Lines 237-282**
```sql
SELECT jsonb_build_object(
  'variance_m2', VAR_POP(a),
  'p50_m2', percentile_cont(0.5),
  'p90_m2', percentile_cont(0.9)
)
```
**Status**: ✅ **CORRECT**
- **Formula**: Variance + percentiles of building sizes
- **Urban Design Rationale**: High variance = mixed-scale urbanism; low = monotonous

---

### ⚠️ Building Clustering Coefficient
**Lines 284-432**
```sql
local_coeff = closed_pairs / total_pairs
global_coeff = AVG(local_coeff) * 100.0
-- link_threshold = 60m
```
**Status**: ⚠️ **VALID, BUT ARBITRARY THRESHOLD**
- **Formula**: Graph clustering coefficient with 60m adjacency threshold
- **Issue**: 60m threshold is hardcoded and arbitrary
- **Urban Design Concern**:
  - 60m may be too large for dense urban cores (double-counts across streets)
  - Too small for suburban areas with large setbacks
- **Recommendation**: Make threshold city-dependent or document rationale for 60m

---

### ✅ Average Interbuilding Distance
**Lines 434-519**
```sql
SELECT AVG(nearest_neighbor_distance)
```
**Status**: ✅ **CORRECT**
- **Formula**: Mean nearest-neighbor distance between building centroids
- **Urban Design Rationale**: <20m = very dense; >100m = low density

---

### ✅ Building Elongation
**Lines 521-589**
```sql
ratio = GREATEST(l1, l2) / LEAST(l1, l2)
-- from oriented bounding box
```
**Status**: ✅ **CORRECT**
- **Formula**: Aspect ratio from oriented minimum bounding rectangle
- **Urban Design Rationale**: 1.0 = square; >3.0 = elongated/linear
- **Reference**: Standard morphometric descriptor

---

### ✅ Building Orientation
**Lines 591-673**
```sql
mean_orientation = MOD(ATAN2(SUM(SIN(2θ)), SUM(COS(2θ))) / 2, 180)
```
**Status**: ✅ **CORRECT**
- **Formula**: Circular mean of major axis orientations
- **Urban Design Rationale**: Reveals alignment patterns (grid vs organic)
- **Note**: Uses circular statistics (correct for angular data)

---

### ✅ Footprint Regularity
**Lines 675-730**
```sql
isoperimetric_quotient = (4π * area) / perimeter²
-- scaled to 0-100
```
**Status**: ✅ **CORRECT**
- **Formula**: Isoperimetric quotient (1.0 = circle, lower = more irregular)
- **Urban Design Rationale**: Measures shape complexity
- **Reference**: Standard geometric shape descriptor

---

### ⚠️ Building Edge Coverage
**Lines 732-833**
```sql
frontage_samples = road points within 15m of buildings
coverage = (frontage_samples / total_samples) * 100
```
**Status**: ⚠️ **CONCEPTUALLY VALID, IMPLEMENTATION CONCERNS**
- **Formula**: % of road sample points within 15m of buildings
- **Issues**:
  1. 15m threshold is arbitrary (works for narrow streets, fails for wide boulevards)
  2. Sample spacing = 20m may miss small gaps
  3. Doesn't distinguish setback vs street width
- **Urban Design Concern**: Conflates "street wall continuity" with "building proximity to roads"
- **Recommendation**: Consider separate metrics for:
  - Setback uniformity
  - Street wall continuity (gap analysis)
  - Building-to-street ratio by road type

---

### ⚠️ FAR Proxy
**Lines 835-913**
```sql
-- If enriched data exists:
FAR = SUM(floor_area_proxy_m2 * clip_fraction) / polygon_area
-- Else:
FAR = SUM(footprint_area) / polygon_area  -- equivalent to BCR
```
**Status**: ⚠️ **CORRECT FOR WHAT IT IS, MISLEADING NAME**
- **Formula**: Floor area / site area (when floor data available)
- **Issue**: Falls back to BCR when no floor data → not really "FAR"
- **Urban Design Concern**: FAR is meaningless without floor data
- **Recommendation**: Return NULL when no floor data, or rename to `far_proxy_or_bcr`

---

## 3. LANDUSE_METRICS.SQL

### ✅ Green Cover Percentage
**Lines 289-300**
```sql
RETURN lulc_flag_pct(is_green)
```
**Status**: ✅ **CORRECT**
- **Formula**: % of LULC pixels flagged as green
- **Urban Design Rationale**: >30% = healthy; <15% = insufficient

---

### ✅ Land Use Mix Index
**Lines 302-323**
```sql
H = -SUM(p_i * LN(p_i))
-- where p_i = area_i / total_area
```
**Status**: ✅ **CORRECT**
- **Formula**: Shannon entropy of LULC class areas
- **Urban Design Rationale**:
  - 0 = monoculture
  - 2-3 = mixed-use (typical for diverse neighborhoods)
- **Reference**: Jane Jacobs, "Death and Life of Great American Cities" (1961)

---

### ⚠️ Impervious Ratio
**Lines 400-480**
```sql
RETURN (LEAST(area_m2, GREATEST(lulc_built_m2, bldg_m2 + road_m2)) / area_m2) * 100
```
**Status**: ⚠️ **COMPLEX LOGIC, DEBATABLE APPROACH**
- **Formula**: Min of area vs max of (LULC_built, bldg+road_buffered)
- **Issue**: Logic prioritizes LULC over vector when LULC > vector, but this may hide gaps
- **Urban Design Concern**:
  - Road buffer = 4m is arbitrary (excludes parking, sidewalks)
  - LEAST caps result at 100%, but inner GREATEST may underestimate
- **Recommendation**:Consider whether the intent is "conservative estimate" or "best available"
  - If conservative → use GREATEST (current)
  - If best available → use LULC primarily, vector as fallback

---

### ✅ Water Coverage
**Lines 351-398**
```sql
-- Prefers vector water_bodies over LULC raster
RETURN poly_pct IF available ELSE lulc_pct
```
**Status**: ✅ **CORRECT PRIORITY**
- **Formula**: Vector polygons preferred over raster (more accurate)
- **Urban Design Rationale**: Water bodies are discrete features, vector is more precise

---

### ✅ Park Green Space Density
**Lines 499-546**
```sql
RETURN (park_area_m2 / 10000) / area_sqkm
-- i.e., hectares of parks per km²
```
**Status**: ✅ **CORRECT**
- **Formula**: Park hectares per km²
- **Urban Design Rationale**: 2-4 ha/km² = good provision
- **Reference**: Urban green space standards (WHO recommends 9m² per person)

---

### ✅ Distance to Nearest Park
**Lines 548-632**
```sql
SELECT AVG(nearest_distance) FROM grid_samples
```
**Status**: ✅ **CORRECT**
- **Formula**: Average grid-sampled distance to nearest park
- **Urban Design Rationale**: <300m = excellent; >800m = poor
- **Reference**: 10-minute neighborhood concept

---

### ⚠️ Riparian Buffer Integrity
**Lines 659-763**
```sql
integrity = (riparian_buffer ∩ open_surface) / riparian_buffer * 100
```
**Status**: ⚠️ **CONCEPTUALLY SOUND, DATA-DEPENDENT**
- **Formula**: % of riparian buffer that remains open/undeveloped
- **Issue**: Relies on existence of pre-defined riparian_buffers layer
- **Urban Design Rationale**: Ecologically important for water quality and biodiversity
- **Recommendation**: Document expected buffer width (typically 30-100m from water)

---

## 4. TOPOGRAPHY_METRICS.SQL

### ✅ Mean Elevation, Elevation Range, Mean Slope
**Lines 322-383**
```sql
-- All use ST_SummaryStatsAgg on DEM rasters
```
**Status**: ✅ **CORRECT**
- **Formula**: Raster statistics (mean, min, max, range)
- **Urban Design Rationale**: Baseline topographic descriptors

---

### ✅ Steep Area Percentage
**Lines 385-399**
```sql
RETURN % pixels where slope > 15°
```
**Status**: ✅ **CORRECT**
- **Formula**: % of area with slope >15°
- **Urban Design Rationale**: 15° (~27% grade) is threshold for development difficulty
- **Reference**: Standard engineering cutoff for construction

---

### ✅ Flat Area Percentage
**Lines 401-415**
```sql
RETURN % pixels where slope < 3°
```
**Status**: ✅ **CORRECT**
- **Formula**: % of area with slope <3°
- **Urban Design Rationale**: 3° (~5% grade) is ideal for accessibility, easy construction

---

### ⚠️ Natural Constraint Index
**Lines 433-482**
```sql
constraint = (0.7 * steep_pct) + (0.3 * water_pct)
```
**Status**: ⚠️ **VALID, WEIGHTS ARE ADMITTEDLY ARBITRARY**
- **Formula**: Weighted sum of steep slopes and water bodies
- **Issue**: 70/30 weights are not calibrated (as noted in comments)
- **Urban Design Concern**:
  - Overlapping factors (e.g., steep riverbanks double-counted)
  - Water is less of a "constraint" in modern engineering (landfill, bridges)
- **Recommendation**:
  - Use as ordinal ranking only (as documented)
  - Consider separate metrics rather than composite
  - If composite needed, justify weights empirically

---

## 5. COMPOSITE_METRICS.SQL

### ✅ Walkability Index
**Lines 22-67**
```sql
walkability =
  0.25 * intersection_density_norm +
  0.25 * CNR +
  0.20 * ped_ratio +
  0.20 * transit_coverage +
  0.10 * transit_distance_score
```
**Status**: ✅ **CONCEPTUALLY SOUND**
- **Formula**: Weighted composite of 5 walkability factors
- **Weights**: Reasonable prioritization of street connectivity and transit
- **Urban Design Rationale**: Aligns with Walk Score methodology
- **Reference**: Jeff Speck, "Walkable City" (2012)

---

### ⚠️ Informality Index
**Lines 69-104**
```sql
informality =
  0.35 * (bldg_density_norm) +
  0.25 * (100 - CNR) +  ← ISSUE HERE
  0.20 * (100 - green_cover) +
  0.20 * vacant_pct
```
**Status**: ⚠️ **CONCEPTUAL ERROR**
- **Issue**: `(100 - CNR)` double-penalizes disconnected networks
  - CNR already measures lack of connection
  - Inverting it creates `(100 - connected_pct)` = disconnected_pct
  - But culdesac_ratio already directly measures degree-1 dead-ends
- **Urban Design Concern**: Informal settlements have dead-end paths, but also organic connectivity
- **Recommendation**: Replace `(100 - CNR)` with `culdesac_ratio` for direct measurement
  - OR use `(100 - street_connectivity_index)` to measure tree-like patterns

---

### ✅ Heat Island Proxy
**Lines 107-140**
```sql
heat_proxy =
  0.35 * impervious_ratio +
  0.25 * BCR +
  0.25 * (100 - green_cover) +
  0.15 * flat_area_pct
```
**Status**: ✅ **REASONABLE PROXY**
- **Formula**: Weighted sum of heat-related factors
- **Urban Design Rationale**: Impervious surfaces + lack of green = higher heat
- **Flat area inclusion**: Debatable (flat areas may have more air circulation issues)
- **Reference**: Aligned with UHI research (Oke, 1982)

---

### ✅ Development Pressure
**Lines 142-176**
```sql
pressure =
  0.40 * vacant_pct +
  0.35 * bldg_density_norm +
  0.25 * edge_density_norm
```
**Status**: ✅ **REASONABLE INTERPRETATION**
- **Formula**: Vacant land + existing development intensity + road infrastructure
- **Urban Design Rationale**: High vacancy + existing infrastructure = likely development
- **Note**: "Pressure" could also mean "constraint" - ensure consistent interpretation

---

### ⚠️ Topographic Constraint (Expansion)
**Lines 178-204**
```sql
constraint =
  0.625 * natural_constraint_index +
  0.375 * steep_pct
```
**Status**: ⚠️ **PARTIALLY REDUNDANT**
- **Issue**: natural_constraint_index ALREADY includes steep_pct (weighted at 0.7)
- **Result**: Steep slopes are double-weighted (0.625 * 0.7 + 0.375 = ~0.81 total)
- **Recommendation**: Use natural_constraint_index alone OR redefine to avoid overlap

---

### ✅ Green Accessibility
**Lines 206-243**
```sql
green_access =
  0.40 * distance_score +
  0.30 * park_density_norm +
  0.30 * green_cover
```
**Status**: ✅ **WELL-BALANCED**
- **Formula**: Proximity + quantity + ambient green
- **Urban Design Rationale**: Covers both access and presence of green space

---

### ✅ Transit-Green Access
**Lines 245-269**
```sql
transit_green =
  0.55 * transit_coverage +
  0.45 * green_accessibility
```
**Status**: ✅ **VALID COMPOSITE**
- **Formula**: Joint measure of TOD + green space access
- **Urban Design Rationale**: Captures sustainable neighborhood accessibility

---

### ⚠️ Compactness
**Lines 271-315**
```sql
compactness =
  0.30 * BCR +
  0.25 * intersection_density_norm +
  0.25 * mix_index_norm +
  0.20 * circuity_score
```
**Status**: ⚠️ **VALID BUT AMBIGUOUS NAMING**
- **Formula**: Combines density, connectivity, and mixed-use
- **Issue**: "Compactness" in urban design often refers to geometric compactness (shape)
  - This metric is more like "compact urbanism" or "urban intensity"
- **Recommendation**: Rename to `urban_intensity_index` or `compact_development_index`

---

## SUMMARY OF RECOMMENDATIONS

### CRITICAL FIXES NEEDED:
1. ✅ **DONE**: CNR formula corrected
2. ⚠️ **TODO**: Informality Index - replace `(100 - CNR)` with `culdesac_ratio`
3. ⚠️ **TODO**: Topographic Constraint - remove double-weighting of steep slopes

### NAMING CLARIFICATIONS:
- `pedestrian_infra_ratio` → `dedicated_ped_path_ratio` (or enhance with sidewalk data)
- `transit_distance_to_metro_or_rail` → `transit_distance_to_rapid_transit` (or filter source layers)
- `far_proxy` → `far_when_available` or return NULL when no floor data
- `compactness` → `urban_intensity_index`

### PARAMETER REVIEW:
- Building clustering: 60m threshold - justify or make adaptive
- Edge coverage: 15m threshold and 20m sampling - document rationale
- Impervious ratio: 4m road buffer - consider expanding

### DATA QUALITY DEPENDENCIES:
- Riparian buffer integrity requires pre-defined buffer layer
- FAR proxy requires floor-level enrichment
- Pedestrian ratio depends on OSM tagging completeness

---

## OVERALL ASSESSMENT

**Strengths:**
- Core metrics (BCR, density, CNR, etc.) are correctly implemented
- Good use of established urban theory (entropy, graph metrics)
- Proper geometric handling (spheroidal area, circular statistics)
- Comprehensive coverage of morphological, functional, and environmental dimensions

**Weaknesses:**
- Some composite indices have arbitrary weights (acknowledged in comments)
- A few metrics have overlapping/redundant components
- Some names don't match their actual measurement scope

**Urban Design Grade:** **B+**
- Mathematically sound and conceptually aligned with urban design literature
- Minor issues with composite weighting and naming conventions
- Excellent foundation for comparative urban analysis
