# City Readiness Report

Scoring dimensions: wards, roads, buildings, LULC, DEM, open spaces, transit.
Scale: 0-100 per dimension, overall score is the simple average.

## Readiness Table

| City | Wards | Roads | Buildings | LULC | DEM | Open Spaces | Transit | Overall | Band |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ahmedabad | 100 | 90 | 90 | 85 | 85 | 80 | 85 | 87.9 | ready_with_caveats |
| bengaluru | 95 | 90 | 90 | 85 | 85 | 80 | 85 | 87.1 | ready_with_caveats |
| chandigarh | 100 | 90 | 90 | 85 | 85 | 80 | 70 | 85.7 | ready_with_caveats |
| chennai | 95 | 90 | 90 | 85 | 85 | 80 | 85 | 87.1 | ready_with_caveats |
| delhi | 95 | 90 | 90 | 85 | 85 | 80 | 85 | 87.1 | ready_with_caveats |
| kolkata | 95 | 90 | 90 | 85 | 85 | 80 | 85 | 87.1 | ready_with_caveats |
| mumbai | 100 | 90 | 90 | 85 | 85 | 80 | 85 | 87.9 | ready_with_caveats |

## Evidence Snapshot

| City | Canonical Wards | Roads Major Features | Building Rows | LULC Ward Rows | DEM Ward Rows | Open Space Master Features | Transit Features |
|---|---:|---:|---:|---:|---:|---:|---:|
| ahmedabad | 48 | 43129 | 945662 | 48 | 48 | 3051 | 1092 |
| bengaluru | 112 | 130405 | 1636046 | 112 | 112 | 22826 | 11023 |
| chandigarh | 27 | 10350 | 165216 | 27 | 27 | 1197 | 141 |
| chennai | 200 | 57308 | 1102189 | 200 | 200 | 4504 | 3714 |
| delhi | 251 | 146648 | 2397674 | 251 | 251 | 15737 | 12684 |
| kolkata | 141 | 33692 | 714708 | 141 | 141 | 2777 | 1902 |
| mumbai | 24 | 40146 | 674141 | 24 | 24 | 10425 | 6856 |

## Weak-Data Caveats

- Buildings are now normalized to EPSG:4326 MultiPolygon and pass the validation gate.
- DEM clipped and terrain rasters exist per city; however dem_input manifest entries reference `_dem/*.tif` files not currently found in this workspace.
- Transit stop/station layers are present, but `public_transport_routes` counts are zero for all cities.
- OSM semantic completeness still varies by city for open-space and transport layer tags.
