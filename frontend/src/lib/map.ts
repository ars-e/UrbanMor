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

export type BasemapPresetId =
  | 'soft_light'
  | 'voyager'
  | 'osm_classic'
  | 'dark_focus'
  | 'terrain_relief'
  | 'satellite_hybrid'

export interface BasemapPreset {
  id: BasemapPresetId
  label: string
  hint: string
}

export const BASEMAP_PRESETS: BasemapPreset[] = [
  { id: 'soft_light', label: 'Soft Light', hint: 'Best default' },
  { id: 'voyager', label: 'Voyager', hint: 'Balanced streets' },
  { id: 'osm_classic', label: 'OSM Classic', hint: 'Familiar look' },
  { id: 'dark_focus', label: 'Dark Focus', hint: 'Low glare' },
  { id: 'terrain_relief', label: 'Terrain', hint: 'Topography' },
  { id: 'satellite_hybrid', label: 'Satellite Hybrid', hint: 'Imagery + labels' },
]

export const ALL_BASEMAP_LAYER_IDS = [
  'basemap-carto-light',
  'basemap-carto-voyager',
  'basemap-osm-standard',
  'basemap-carto-dark',
  'basemap-opentopo',
  'basemap-satellite-imagery',
  'basemap-satellite-labels',
] as const

const BASEMAP_PRESET_LAYER_MAP: Record<BasemapPresetId, readonly string[]> = {
  soft_light: ['basemap-carto-light'],
  voyager: ['basemap-carto-voyager'],
  osm_classic: ['basemap-osm-standard'],
  dark_focus: ['basemap-carto-dark'],
  terrain_relief: ['basemap-opentopo'],
  satellite_hybrid: ['basemap-satellite-imagery', 'basemap-satellite-labels'],
}

export function basemapLayerIdsForPreset(preset: BasemapPresetId): readonly string[] {
  return BASEMAP_PRESET_LAYER_MAP[preset]
}

export function isDarkBasemapPreset(preset: BasemapPresetId): boolean {
  return preset === 'dark_focus'
}

export function isImageryBasemapPreset(preset: BasemapPresetId): boolean {
  return preset === 'satellite_hybrid'
}

export function createBasemapStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {
      carto_light: {
        type: 'raster',
        tiles: [
          'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
          'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
          'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
          'https://d.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
        ],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors © CARTO',
      },
      carto_voyager: {
        type: 'raster',
        tiles: [
          'https://a.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
          'https://b.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
          'https://c.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
          'https://d.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
        ],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors © CARTO',
      },
      osm_standard: {
        type: 'raster',
        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors',
      },
      carto_dark: {
        type: 'raster',
        tiles: [
          'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',
          'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',
          'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',
          'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',
        ],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors © CARTO',
      },
      opentopo: {
        type: 'raster',
        tiles: [
          'https://a.tile.opentopomap.org/{z}/{x}/{y}.png',
          'https://b.tile.opentopomap.org/{z}/{x}/{y}.png',
          'https://c.tile.opentopomap.org/{z}/{x}/{y}.png',
        ],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors, © OpenTopoMap (CC-BY-SA)',
      },
      esri_imagery: {
        type: 'raster',
        tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
        tileSize: 256,
        attribution: 'Tiles © Esri',
      },
      esri_reference: {
        type: 'raster',
        tiles: ['https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}'],
        tileSize: 256,
        attribution: 'Labels © Esri',
      },
    },
    layers: [
      {
        id: 'basemap-carto-light',
        type: 'raster',
        source: 'carto_light',
        layout: { visibility: 'visible' },
        paint: {
          'raster-contrast': 0.12,
          'raster-saturation': -0.05,
          'raster-brightness-min': 0.04,
          'raster-brightness-max': 0.94,
        },
      },
      {
        id: 'basemap-carto-voyager',
        type: 'raster',
        source: 'carto_voyager',
        layout: { visibility: 'none' },
        paint: {
          'raster-contrast': 0.1,
          'raster-saturation': -0.08,
          'raster-brightness-min': 0.04,
          'raster-brightness-max': 0.94,
        },
      },
      {
        id: 'basemap-osm-standard',
        type: 'raster',
        source: 'osm_standard',
        layout: { visibility: 'none' },
      },
      {
        id: 'basemap-carto-dark',
        type: 'raster',
        source: 'carto_dark',
        layout: { visibility: 'none' },
        paint: {
          'raster-contrast': 0.12,
          'raster-saturation': 0.04,
          'raster-brightness-min': 0.02,
          'raster-brightness-max': 0.8,
        },
      },
      {
        id: 'basemap-opentopo',
        type: 'raster',
        source: 'opentopo',
        layout: { visibility: 'none' },
        paint: {
          'raster-contrast': 0.08,
          'raster-saturation': -0.02,
          'raster-brightness-min': 0.02,
          'raster-brightness-max': 0.94,
        },
      },
      {
        id: 'basemap-satellite-imagery',
        type: 'raster',
        source: 'esri_imagery',
        layout: { visibility: 'none' },
        paint: {
          'raster-contrast': 0.12,
          'raster-saturation': -0.1,
          'raster-brightness-min': 0.04,
          'raster-brightness-max': 0.9,
        },
      },
      {
        id: 'basemap-satellite-labels',
        type: 'raster',
        source: 'esri_reference',
        layout: { visibility: 'none' },
        paint: {
          'raster-opacity': 0.96,
        },
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
