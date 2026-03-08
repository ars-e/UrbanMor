import area from '@turf/area'
import type { MultiPolygon, Polygon } from 'geojson'
import type { StyleSpecification } from 'maplibre-gl'

import type { WardGeometryFeature } from '../types'

export interface MapViewport {
  west: number
  south: number
  east: number
  north: number
  zoom: number
}

export function createBasemapStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {
      street: {
        type: 'raster',
        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors',
      },
      satellite: {
        type: 'raster',
        tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
        tileSize: 256,
        attribution: 'Tiles © Esri',
      },
    },
    layers: [
      {
        id: 'basemap-street',
        type: 'raster',
        source: 'street',
        layout: { visibility: 'visible' },
      },
      {
        id: 'basemap-satellite',
        type: 'raster',
        source: 'satellite',
        layout: { visibility: 'none' },
      },
    ],
  }
}

export function buildSelectedWardAoiGeometry(features: WardGeometryFeature[]): Polygon | MultiPolygon | null {
  if (features.length === 0) {
    return null
  }

  if (features.length === 1) {
    return features[0].geometry
  }

  const polygons: number[][][][] = []
  for (const feature of features) {
    if (feature.geometry.type === 'Polygon') {
      polygons.push(feature.geometry.coordinates)
      continue
    }
    polygons.push(...feature.geometry.coordinates)
  }

  if (polygons.length === 0) {
    return null
  }

  return {
    type: 'MultiPolygon',
    coordinates: polygons,
  }
}

export function areaSqKm(geometry: Polygon | MultiPolygon | null): number | null {
  if (!geometry) {
    return null
  }
  return area(geometry) / 1_000_000
}

export function formatAreaSqKm(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return 'n/a'
  }
  if (value >= 100) {
    return `${value.toFixed(0)} sq km`
  }
  if (value >= 10) {
    return `${value.toFixed(1)} sq km`
  }
  return `${value.toFixed(2)} sq km`
}

export function serializeViewportBbox(viewport: MapViewport | null): string {
  if (!viewport) {
    return ''
  }
  return [viewport.west, viewport.south, viewport.east, viewport.north].map((value) => value.toFixed(5)).join(',')
}
