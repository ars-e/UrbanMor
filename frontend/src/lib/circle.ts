import circle from '@turf/circle'
import booleanIntersects from '@turf/boolean-intersects'
import type { FeatureCollection, Polygon } from 'geojson'

/**
 * Color palette for locked circles (amber shades)
 */
const CIRCLE_COLORS = ['#D97706', '#F59E0B', '#FBBF24', '#FCD34D']

/**
 * Labels for locked circles
 */
const CIRCLE_LABELS = ['A', 'B', 'C', 'D']

/**
 * Interface for preview metrics (quick estimates)
 */
export interface PreviewMetrics {
  area_km2: number
  cnr_estimate: number | null
  intersection_density: number | null
  intersection_count: number | null
  open_ratio: number | null
}

/**
 * Convert a circle (center + radius) to a 64-vertex polygon using Turf.js
 *
 * @param center - [longitude, latitude]
 * @param radiusMeters - Radius in meters
 * @returns Polygon geometry
 */
export function createCirclePolygon(
  center: [number, number],
  radiusMeters: number
): Polygon {
  // Convert meters to kilometers for Turf
  const radiusKm = radiusMeters / 1000

  // Create circle with 64 steps for smooth appearance
  const circleFeature = circle(center, radiusKm, {
    steps: 64,
    units: 'kilometers'
  })

  return circleFeature.geometry as Polygon
}

/**
 * Get color for a circle based on its index
 *
 * @param index - Circle index (0-3)
 * @returns Hex color string
 */
export function getCircleColor(index: number): string {
  return CIRCLE_COLORS[index % CIRCLE_COLORS.length]
}

/**
 * Get label for a circle based on its index
 *
 * @param index - Circle index (0-3)
 * @returns Label string (A, B, C, D)
 */
export function getCircleLabel(index: number): string {
  return CIRCLE_LABELS[index % CIRCLE_LABELS.length]
}

/**
 * Estimate preview metrics for a circle
 * Provides quick approximations before full analysis
 *
 * @param center - [longitude, latitude]
 * @param radius - Radius in meters
 * @param roadsData - Optional roads GeoJSON for better estimates
 * @returns Preview metrics with estimates
 */
export function estimatePreviewMetrics(
  center: [number, number],
  radius: number,
  roadsData?: FeatureCollection
): PreviewMetrics {
  // Calculate area
  const areaM2 = Math.PI * radius * radius
  const area_km2 = areaM2 / 1_000_000

  // If no roads data, return basic metrics
  if (!roadsData) {
    return {
      area_km2: parseFloat(area_km2.toFixed(3)),
      cnr_estimate: null,
      intersection_density: null,
      intersection_count: null,
      open_ratio: null
    }
  }

  // Quick estimation from roads
  const circlePolygon = circle(center, radius / 1000, {
    steps: 32, // Fewer steps for faster estimation
    units: 'kilometers'
  })

  let intersectingRoadsCount = 0

  // Count roads that intersect the circle
  for (const road of roadsData.features) {
    try {
      if (booleanIntersects(road, circlePolygon)) {
        intersectingRoadsCount++
      }
    } catch {
      // Skip features that cause errors
      continue
    }
  }

  // Estimate intersections (rough heuristic: ~15% of road segments meet)
  const estimatedIntersections = Math.round(intersectingRoadsCount * 0.15)
  const density = estimatedIntersections / area_km2

  // Estimate CNR based on density (rough heuristic)
  // High density (150+ intersections/km²) ≈ 0.8 CNR
  const cnr_estimate = Math.min(density / 150, 0.8)

  return {
    area_km2: parseFloat(area_km2.toFixed(3)),
    cnr_estimate: cnr_estimate > 0 ? parseFloat(cnr_estimate.toFixed(2)) : null,
    intersection_density: Math.round(density),
    intersection_count: estimatedIntersections,
    open_ratio: null // Cannot estimate without building data
  }
}

/**
 * Create an empty GeoJSON FeatureCollection
 */
export function emptyFeatureCollection(): FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: []
  }
}
