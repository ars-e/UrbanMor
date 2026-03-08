import area from '@turf/area'
import booleanValid from '@turf/boolean-valid'
import { multiPolygon, polygon } from '@turf/helpers'
import kinks from '@turf/kinks'
import type { MultiPolygon, Polygon } from 'geojson'

export interface GeometryValidationResult {
  normalizedGeometry: Polygon | MultiPolygon | null
  errors: string[]
  warnings: string[]
  areaSqM: number | null
  vertexCount: number
  isValid: boolean
}

const MIN_AREA_SQM = 400
const MAX_AREA_SQM_WARNING = 300_000_000
const MAX_VERTEX_COUNT = 2000

function normalizeRingCoordinates(ring: number[][]): number[][] {
  const cleaned: number[][] = []
  for (const coord of ring) {
    if (!Array.isArray(coord) || coord.length < 2) {
      continue
    }
    const lon = Number(coord[0])
    const lat = Number(coord[1])
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
      continue
    }
    const rounded: [number, number] = [
      Number(lon.toFixed(7)),
      Number(lat.toFixed(7)),
    ]
    const prev = cleaned[cleaned.length - 1]
    if (!prev || prev[0] !== rounded[0] || prev[1] !== rounded[1]) {
      cleaned.push(rounded)
    }
  }

  if (cleaned.length === 0) {
    return cleaned
  }

  const first = cleaned[0]
  const last = cleaned[cleaned.length - 1]
  if (first[0] !== last[0] || first[1] !== last[1]) {
    cleaned.push([first[0], first[1]])
  }

  return cleaned
}

function normalizePolygonCoordinates(input: number[][][]): number[][][] {
  return input
    .map((ring) => normalizeRingCoordinates(ring))
    .filter((ring) => ring.length >= 4)
}

function normalizeMultiPolygonCoordinates(input: number[][][][]): number[][][][] {
  return input
    .map((poly) => normalizePolygonCoordinates(poly))
    .filter((poly) => poly.length > 0)
}

function countVertices(geometry: Polygon | MultiPolygon): number {
  if (geometry.type === 'Polygon') {
    return geometry.coordinates.reduce((acc, ring) => acc + Math.max(0, ring.length - 1), 0)
  }
  return geometry.coordinates.reduce(
    (polyAcc, poly) => polyAcc + poly.reduce((ringAcc, ring) => ringAcc + Math.max(0, ring.length - 1), 0),
    0,
  )
}

function getMinRingVertices(geometry: Polygon | MultiPolygon): number {
  if (geometry.type === 'Polygon') {
    return Math.min(...geometry.coordinates.map((ring) => ring.length), Number.POSITIVE_INFINITY)
  }

  let min = Number.POSITIVE_INFINITY
  for (const poly of geometry.coordinates) {
    for (const ring of poly) {
      min = Math.min(min, ring.length)
    }
  }
  return min
}

function normalizeGeometry(geometry: Polygon | MultiPolygon): Polygon | MultiPolygon | null {
  if (geometry.type === 'Polygon') {
    const normalized = normalizePolygonCoordinates(geometry.coordinates)
    if (normalized.length === 0) {
      return null
    }
    return { type: 'Polygon', coordinates: normalized }
  }

  const normalized = normalizeMultiPolygonCoordinates(geometry.coordinates)
  if (normalized.length === 0) {
    return null
  }
  return { type: 'MultiPolygon', coordinates: normalized }
}

export function validateAndNormalizeGeometry(input: Polygon | MultiPolygon | null): GeometryValidationResult {
  if (!input) {
    return {
      normalizedGeometry: null,
      errors: [],
      warnings: [],
      areaSqM: null,
      vertexCount: 0,
      isValid: false,
    }
  }

  const normalizedGeometry = normalizeGeometry(input)
  if (!normalizedGeometry) {
    return {
      normalizedGeometry: null,
      errors: ['Geometry is empty or malformed after normalization. Draw again.'],
      warnings: [],
      areaSqM: null,
      vertexCount: 0,
      isValid: false,
    }
  }

  const errors: string[] = []
  const warnings: string[] = []
  const vertexCount = countVertices(normalizedGeometry)
  const minRingVertices = getMinRingVertices(normalizedGeometry)

  if (vertexCount > MAX_VERTEX_COUNT) {
    errors.push(`Polygon is too complex (${vertexCount} vertices). Simplify and try again.`)
  }

  if (minRingVertices < 4) {
    errors.push('Polygon rings need at least 3 distinct points.')
  }

  const turfGeom =
    normalizedGeometry.type === 'Polygon'
      ? polygon(normalizedGeometry.coordinates)
      : multiPolygon(normalizedGeometry.coordinates)

  if (!booleanValid(turfGeom)) {
    errors.push('Geometry is invalid. Fix overlaps or crossing edges.')
  }

  const hasSelfIntersection =
    normalizedGeometry.type === 'Polygon'
      ? kinks(polygon(normalizedGeometry.coordinates)).features.length > 0
      : normalizedGeometry.coordinates.some((poly) => kinks(polygon(poly)).features.length > 0)
  if (hasSelfIntersection) {
    errors.push('Self-intersection detected. Edit the polygon to remove crossing edges.')
  }

  const areaSqM = area(turfGeom)
  if (!Number.isFinite(areaSqM) || areaSqM <= 0) {
    errors.push('Geometry area is invalid. Draw a valid polygon.')
  }

  if (areaSqM < MIN_AREA_SQM) {
    errors.push(`Polygon is too small (${areaSqM.toFixed(1)} m2). Draw a larger area.`)
  }

  if (areaSqM > MAX_AREA_SQM_WARNING) {
    warnings.push('Polygon is very large. Analysis may take longer than usual.')
  }

  return {
    normalizedGeometry,
    errors,
    warnings,
    areaSqM,
    vertexCount,
    isValid: errors.length === 0,
  }
}
