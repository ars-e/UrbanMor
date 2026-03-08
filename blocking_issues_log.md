# Blocking Issues Log

Prioritization rule: issues are ranked by whether they prevent metric computation.

## Blocking

- No current blocking issues detected.

## Non-Blocking

### NBL-001 - DEM manifest references source paths not present in workspace
- Severity: non-blocking
- Evidence: `missing_dem_input_paths=7`
- Impact: Does not block current clipped-raster metrics, but reduces reproducibility of full upstream reruns.
- Proposed fix: Add `_dem/*.tif` source rasters to workspace or point manifest to correct source location.

### NBL-002 - Route layers are empty in transport manifests
- Severity: non-blocking
- Evidence: `cities_with_zero_public_transport_routes=ahmedabad,bengaluru,chandigarh,chennai,delhi,kolkata,mumbai`
- Impact: Stops/stations metrics remain usable; route-dependent metrics are weak.
- Proposed fix: Keep route metrics optional/deferred or enrich with GTFS/agency route feeds.

### NBL-003 - Legacy non-canonical admin shapefiles remain in LCC CRS
- Severity: non-blocking
- Evidence: `canonical EPSG:4326 copies created under dist_bound/sub_dist_bound canonical_epsg4326`
- Impact: No blocker if canonical copies are used, but accidental use of legacy files can cause CRS mismatch.
- Proposed fix: Point downstream loaders only to canonical_epsg4326 layers.

### NBL-004 - Manifest absolute path staleness check
- Severity: non-blocking
- Evidence: `manifest_files_with_stale_paths=0`
- Impact: Path portability issue only.
- Proposed fix: Keep manifests relative-path only (already updated).

## Cosmetic

### COS-001 - MacOS .DS_Store files present in output tree
- Severity: cosmetic
- Evidence: `count=12`
- Impact: No metric impact.
- Proposed fix: Add cleanup and .gitignore rule.

## Exit Criteria Check

- Every critical blocker has a proposed fix: not applicable (no blockers)
