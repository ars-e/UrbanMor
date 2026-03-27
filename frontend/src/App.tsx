import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import MapboxDraw from '@mapbox/mapbox-gl-draw'
import { useMutation, useQuery } from '@tanstack/react-query'
import maplibregl, { MapMouseEvent } from 'maplibre-gl'
import type { FeatureCollection, Geometry, MultiPolygon, Polygon } from 'geojson'
import type { MapGeoJSONFeature } from 'maplibre-gl'

import {
  API_BASE,
  analyse,
  getAnalyseJob,
  getCities,
  getCityMetrics,
  getCityRoadsGeoJSON,
  getCityTransitGeoJSON,
  getCityWards,
  getCityWardsGeoJSON,
  getMetaMetrics,
  getWardMetrics,
} from './lib/api'
import { buildHighlightCards, MAP_METRIC_IDS } from './lib/highlights'
import { validateAndNormalizeGeometry } from './lib/geometry'
import {
  ALL_BASEMAP_LAYER_IDS,
  BASEMAP_PRESETS,
  areaSqKm,
  basemapLayerIdsForPreset,
  buildSelectedWardAoiGeometry,
  createBasemapStyle,
  formatAreaSqKm,
  isDarkBasemapPreset,
  isImageryBasemapPreset,
  serializeViewportBbox,
  type BasemapPresetId,
  type MapViewport,
} from './lib/map'
import { createCirclePolygon, getCircleColor, getCircleLabel } from './lib/circle'
import {
  buildMetricPanelRows,
  downloadTextFile,
  formatMetricNumber,
  groupMetricPanelRows,
  makeMetricsExportCsv,
  makeMetricsExportJson,
} from './lib/metrics'
import type {
  AnalyseJobResponse,
  AnalyseResponse,
  CircleMetricsData,
  CityMapLayerGeoJSONResponse,
  LockedCircle,
  MetricMetaItem,
  WardGeometryFeature,
  WardMetricResponse,
  WardsGeoJSON,
} from './types'

const DRAW_CLASSES = (MapboxDraw as unknown as {
  constants: {
    classes: {
      CANVAS: string
      CONTROL_BASE: string
      CONTROL_PREFIX: string
      CONTROL_GROUP: string
      ATTRIBUTION: string
    }
  }
}).constants.classes

DRAW_CLASSES.CANVAS = 'maplibregl-canvas'
DRAW_CLASSES.CONTROL_BASE = 'maplibregl-ctrl'
DRAW_CLASSES.CONTROL_PREFIX = 'maplibregl-ctrl-'
DRAW_CLASSES.CONTROL_GROUP = 'maplibregl-ctrl-group'
DRAW_CLASSES.ATTRIBUTION = 'maplibregl-ctrl-attrib'

const DEFAULT_METRIC_ID = 'road.intersection_density'
const DEFAULT_CITY_SLUG = 'delhi'
const RETIRED_METRIC_IDS = new Set(['bldg.growth_rate', 'topo.flood_risk_proxy'])
const CHOROPLETH_COLOR_STOPS = {
  low: '#fef3c7',
  mid: '#f97316',
  high: '#b91c1c',
  max: '#4c0519',
}
const CIRCLE_RADIUS_OPTIONS = [250, 500, 1000, 2000] as const
const MAX_COMPARE_CIRCLES = 4
const CIRCLE_POLL_INTERVAL_MS = 1000
const NON_METRIC_CIRCLE_KEYS = new Set(['cache_hit', 'city', 'computed_at', 'geom_hash', 'metrics_json', 'quality_summary', 'vintage_year'])

type PanelSourceType = 'city' | 'ward' | 'selected_wards' | 'drawn_polygon' | 'circle'
type AutoAnalysisSource = 'selected_wards' | 'drawn_polygon' | null
type BasemapMode = BasemapPresetId

interface PanelSnapshot {
  sourceType: PanelSourceType
  sourceId: string
  title: string
  subtitle: string
  metrics: Record<string, unknown>
  qualitySummary: Record<string, unknown>
}

function isJobResponse(value: AnalyseResponse | AnalyseJobResponse): value is AnalyseJobResponse {
  return 'job_id' in value
}

function toNumberOrNull(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value
  }
  if (typeof value === 'string') {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return parsed
    }
  }
  return null
}

function emptyFeatureCollection<T extends Geometry = Geometry>(): FeatureCollection<T, Record<string, unknown>> {
  return {
    type: 'FeatureCollection',
    features: [],
  }
}

function parseWardRows(response?: AnalyseResponse): WardMetricResponse[] {
  if (!response) {
    return []
  }
  const result = response.result as { wards?: unknown }
  if (!Array.isArray(result.wards)) {
    return []
  }
  return result.wards as WardMetricResponse[]
}

function formatElapsed(seconds: number): string {
  if (seconds <= 0) {
    return '0s'
  }
  if (seconds < 60) {
    return `${seconds}s`
  }
  const mins = Math.floor(seconds / 60)
  const rem = seconds % 60
  return `${mins}m ${rem}s`
}

function formatCircleRadius(radiusMeters: number): string {
  if (radiusMeters >= 1000) {
    return `${radiusMeters / 1000}km`
  }
  return `${radiusMeters}m`
}

function parseCircleResultPayload(payload: Record<string, unknown> | null): {
  metrics: Record<string, unknown>
  qualitySummary: Record<string, unknown>
  error?: string
} {
  if (!payload) {
    return {
      metrics: {},
      qualitySummary: {},
      error: 'No analysis result returned for this circle.',
    }
  }

  const qualitySummaryValue = payload.quality_summary
  const qualitySummary =
    typeof qualitySummaryValue === 'object' && qualitySummaryValue !== null && !Array.isArray(qualitySummaryValue)
      ? (qualitySummaryValue as Record<string, unknown>)
      : {}

  const metricsJsonValue = payload.metrics_json
  if (typeof metricsJsonValue === 'object' && metricsJsonValue !== null && !Array.isArray(metricsJsonValue)) {
    const allMetricsValue = (metricsJsonValue as { all_metrics?: unknown }).all_metrics
    if (typeof allMetricsValue === 'object' && allMetricsValue !== null && !Array.isArray(allMetricsValue)) {
      return {
        metrics: allMetricsValue as Record<string, unknown>,
        qualitySummary,
      }
    }
  }

  const fallbackEntries = Object.entries(payload).filter(([key]) => !NON_METRIC_CIRCLE_KEYS.has(key))
  if (fallbackEntries.length > 0) {
    return {
      metrics: Object.fromEntries(fallbackEntries),
      qualitySummary,
    }
  }

  return {
    metrics: {},
    qualitySummary,
    error: 'No computed metrics found in circle result.',
  }
}

function computeBounds(geojson: WardsGeoJSON): [[number, number], [number, number]] | null {
  if (geojson.features.length === 0) {
    return null
  }

  let minX = Number.POSITIVE_INFINITY
  let minY = Number.POSITIVE_INFINITY
  let maxX = Number.NEGATIVE_INFINITY
  let maxY = Number.NEGATIVE_INFINITY

  const ingest = (coord: number[]) => {
    if (coord.length < 2) {
      return
    }
    const [x, y] = coord
    minX = Math.min(minX, x)
    minY = Math.min(minY, y)
    maxX = Math.max(maxX, x)
    maxY = Math.max(maxY, y)
  }

  for (const feature of geojson.features) {
    if (feature.geometry.type === 'Polygon') {
      for (const ring of feature.geometry.coordinates) {
        for (const coord of ring) {
          ingest(coord)
        }
      }
    } else if (feature.geometry.type === 'MultiPolygon') {
      for (const polygon of feature.geometry.coordinates) {
        for (const ring of polygon) {
          for (const coord of ring) {
            ingest(coord)
          }
        }
      }
    }
  }

  if (!Number.isFinite(minX) || !Number.isFinite(maxX) || !Number.isFinite(minY) || !Number.isFinite(maxY)) {
    return null
  }

  return [
    [minX, minY],
    [maxX, maxY],
  ]
}

function buildFillExpression(min: number | null, max: number | null): unknown {
  if (min === null || max === null) {
    return '#d6d3d1'
  }

  if (Math.abs(max - min) < 1e-12) {
    return CHOROPLETH_COLOR_STOPS.high
  }

  const lowerMid = min + (max - min) / 3
  const upperMid = min + ((max - min) * 2) / 3
  return [
    'interpolate',
    ['linear'],
    ['coalesce', ['to-number', ['get', 'metric_value']], min],
    min,
    CHOROPLETH_COLOR_STOPS.low,
    lowerMid,
    CHOROPLETH_COLOR_STOPS.mid,
    upperMid,
    CHOROPLETH_COLOR_STOPS.high,
    max,
    CHOROPLETH_COLOR_STOPS.max,
  ]
}

function isDrawInteractionMode(mode: string): boolean {
  return mode === 'draw_polygon' || mode === 'direct_select'
}

function qualityBadgeClass(flag: string): string {
  if (flag === 'ok' || flag === 'zero') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  }
  if (flag === 'not_computed') {
    return 'border-amber-200 bg-amber-50 text-amber-800'
  }
  if (flag === 'composite_object') {
    return 'border-sky-200 bg-sky-50 text-sky-700'
  }
  return 'border-rose-200 bg-rose-50 text-rose-700'
}

function statusBadgeClass(status: string): string {
  if (status === 'implemented' || status === 'implemented_v1') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  }
  if (status === 'planned' || status.startsWith('planned_')) {
    return 'border-sky-200 bg-sky-50 text-sky-700'
  }
  if (status === 'blocked_data' || status === 'proxy_only') {
    return 'border-amber-200 bg-amber-50 text-amber-800'
  }
  if (status === 'deprecated_or_revised') {
    return 'border-rose-200 bg-rose-50 text-rose-700'
  }
  return 'border-slate-200 bg-slate-100 text-slate-600'
}

function describeAoi(
  source: PanelSourceType,
  city: string,
  selectedFeatures: WardGeometryFeature[],
  drawnArea: number | null,
  selectedCircle: LockedCircle | null,
): { title: string; subtitle: string; areaLabel: string } {
  if (source === 'drawn_polygon') {
    return {
      title: 'Drawn area of interest',
      subtitle: 'Custom geometry analysed automatically after draw or edit.',
      areaLabel: formatAreaSqKm(drawnArea),
    }
  }

  if (source === 'selected_wards') {
    const lead = selectedFeatures[0]?.properties.ward_name || selectedFeatures[0]?.properties.ward_id || 'Selected wards'
    return {
      title: `${selectedFeatures.length} wards combined`,
      subtitle: `${lead}${selectedFeatures.length > 1 ? ` + ${selectedFeatures.length - 1} more` : ''}`,
      areaLabel: formatAreaSqKm(areaSqKm(buildSelectedWardAoiGeometry(selectedFeatures))),
    }
  }

  if (source === 'ward') {
    const first = selectedFeatures[0]
    return {
      title: first?.properties.ward_name || first?.properties.ward_id || 'Selected ward',
      subtitle: 'Single ward AOI from cached ward metrics.',
      areaLabel: formatAreaSqKm(areaSqKm(buildSelectedWardAoiGeometry(selectedFeatures))),
    }
  }

  if (source === 'circle' && selectedCircle) {
    const circleAreaKm2 = (Math.PI * selectedCircle.radius * selectedCircle.radius) / 1_000_000
    return {
      title: `Circle ${selectedCircle.label} AOI`,
      subtitle: selectedCircle.location || `${selectedCircle.center[1].toFixed(4)}, ${selectedCircle.center[0].toFixed(4)}`,
      areaLabel: formatAreaSqKm(circleAreaKm2),
    }
  }

  return {
    title: `${city || 'City'} baseline`,
    subtitle: 'City-wide baseline aggregated from latest ward metrics.',
    areaLabel: 'n/a',
  }
}

function App() {
  const mapContainerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const drawRef = useRef<MapboxDraw | null>(null)
  const lastSubmittedAnalysisKeyRef = useRef<string>('')

  const [selectedCity, setSelectedCity] = useState<string>('')
  const [selectedWardIds, setSelectedWardIds] = useState<string[]>([])
  const [hoverWardId, setHoverWardId] = useState<string>('')
  const [activeMetricId, setActiveMetricId] = useState<string>('')
  const [drawnGeometry, setDrawnGeometry] = useState<Polygon | MultiPolygon | null>(null)
  const [drawnFeatureId, setDrawnFeatureId] = useState<string>('')
  const [drawMode, setDrawMode] = useState<string>('simple_select')
  const [activeJobId, setActiveJobId] = useState<string>('')
  const [activeAnalysisSource, setActiveAnalysisSource] = useState<AutoAnalysisSource>(null)
  const [activeAnalysisLabel, setActiveAnalysisLabel] = useState<string>('')
  const [activeAnalysisKey, setActiveAnalysisKey] = useState<string>('')
  const [exportMessage, setExportMessage] = useState<string>('')
  const [jobClock, setJobClock] = useState<number>(Date.now())
  const [isSidebarOpen, setIsSidebarOpen] = useState<boolean>(false)
  const [showMapGuide, setShowMapGuide] = useState<boolean>(false)
  const [showMapPreview, setShowMapPreview] = useState<boolean>(true)
  const [basemapMode, setBasemapMode] = useState<BasemapMode>('soft_light')
  const [showWardLayer, setShowWardLayer] = useState<boolean>(true)
  const [showRoadOverlay, setShowRoadOverlay] = useState<boolean>(true)
  const [showTransitOverlay, setShowTransitOverlay] = useState<boolean>(true)
  const [mapViewport, setMapViewport] = useState<MapViewport | null>(null)

  // Circle tool state
  const [circleToolActive, setCircleToolActive] = useState<boolean>(false)
  const [circleRadius, setCircleRadius] = useState<number>(250) // Start with smaller 250m radius
  const [lockedCircles, setLockedCircles] = useState<LockedCircle[]>([])
  const [selectedAoiCircleId, setSelectedAoiCircleId] = useState<string>('')
  const [circleMetrics, setCircleMetrics] = useState<Map<string, CircleMetricsData>>(new Map())
  const [hoverCircleCenter, setHoverCircleCenter] = useState<[number, number] | null>(null)
  const [cursorPosition, setCursorPosition] = useState<{ x: number; y: number } | null>(null)
  const [circleNotice, setCircleNotice] = useState<string>('')
  const circleNoticeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const magnifyMapRef = useRef<maplibregl.Map | null>(null)
  const circleMetricsRef = useRef<Map<string, CircleMetricsData>>(new Map())
  const circlePollInFlightRef = useRef(false)

  const syncDrawnState = useCallback(() => {
    const draw = drawRef.current
    if (!draw) {
      return
    }

    const polygonFeatures = draw
      .getAll()
      .features.filter((feature) => feature.geometry.type === 'Polygon' || feature.geometry.type === 'MultiPolygon')

    if (polygonFeatures.length === 0) {
      setDrawnGeometry(null)
      setDrawnFeatureId('')
      return
    }

    const latest = polygonFeatures[polygonFeatures.length - 1]
    if (latest.geometry.type !== 'Polygon' && latest.geometry.type !== 'MultiPolygon') {
      setDrawnGeometry(null)
      setDrawnFeatureId('')
      return
    }

    setDrawnGeometry(latest.geometry)
    setDrawnFeatureId(latest.id === undefined ? '' : String(latest.id))
  }, [])

  const clearAnalysisSource = useCallback((source: AutoAnalysisSource) => {
    if (activeAnalysisSource !== source) {
      return
    }
    setActiveJobId('')
    setActiveAnalysisSource(null)
    setActiveAnalysisLabel('')
    setActiveAnalysisKey('')
  }, [activeAnalysisSource])

  const clearDrawState = useCallback(() => {
    const draw = drawRef.current
    if (draw) {
      draw.deleteAll()
      draw.changeMode('simple_select')
    }
    setDrawnGeometry(null)
    setDrawnFeatureId('')
    setDrawMode('simple_select')
    clearAnalysisSource('drawn_polygon')
  }, [clearAnalysisSource])

  const clearWardSelection = useCallback(() => {
    setSelectedWardIds([])
    setHoverWardId('')
    clearAnalysisSource('selected_wards')
  }, [clearAnalysisSource])

  const clearAllAoi = useCallback(() => {
    clearDrawState()
    clearWardSelection()
    setSelectedAoiCircleId('')
    setExportMessage('')
    lastSubmittedAnalysisKeyRef.current = ''
  }, [clearDrawState, clearWardSelection])

  const activateDrawMode = useCallback(() => {
    const draw = drawRef.current
    if (!draw) {
      return
    }

    draw.deleteAll()
    setSelectedWardIds([])
    setHoverWardId('')
    clearAnalysisSource('selected_wards')
    setDrawnGeometry(null)
    setDrawnFeatureId('')
    setExportMessage('')
    lastSubmittedAnalysisKeyRef.current = ''
    setSelectedAoiCircleId('')
    draw.changeMode('draw_polygon')
    setDrawMode('draw_polygon')

    // Deactivate circle tool if active
    setCircleToolActive(false)
  }, [clearAnalysisSource])

  const activateEditMode = useCallback(() => {
    const draw = drawRef.current
    if (!draw) {
      return
    }

    let targetFeatureId = drawnFeatureId
    if (!targetFeatureId) {
      const fallback = draw
        .getAll()
        .features.find((feature) => feature.geometry.type === 'Polygon' || feature.geometry.type === 'MultiPolygon')
      targetFeatureId = fallback?.id === undefined ? '' : String(fallback.id)
    }
    if (!targetFeatureId) {
      return
    }

    draw.changeMode('direct_select', { featureId: targetFeatureId })
    setDrawMode('direct_select')
  }, [drawnFeatureId])

  // Circle tool helper functions
  const updateLockedCirclesLayer = useCallback(() => {
    const map = mapRef.current
    if (!map) return

    const features = lockedCircles.map(circle => ({
      type: 'Feature' as const,
      properties: {
        id: circle.id,
        color: circle.color,
        label: circle.label,
      },
      geometry: createCirclePolygon(circle.center, circle.radius),
    }))

    const source = map.getSource('locked-circles-source') as maplibregl.GeoJSONSource
    if (source) {
      source.setData({
        type: 'FeatureCollection',
        features,
      })
    }
  }, [lockedCircles])

  const removeCircle = useCallback((circleId: string) => {
    setLockedCircles(prev => prev.filter(c => c.id !== circleId))
    setSelectedAoiCircleId((current) => (current === circleId ? '' : current))
    setCircleMetrics(prev => {
      const updated = new Map(prev)
      updated.delete(circleId)
      return updated
    })
  }, [])

  const clearAllCircles = useCallback(() => {
    setLockedCircles([])
    setSelectedAoiCircleId('')
    setCircleMetrics(new Map())

    const map = mapRef.current
    if (!map) return

    const hoverSource = map.getSource('hover-circle-source') as maplibregl.GeoJSONSource
    if (hoverSource) {
      hoverSource.setData(emptyFeatureCollection())
    }

    const lockedSource = map.getSource('locked-circles-source') as maplibregl.GeoJSONSource
    if (lockedSource) {
      lockedSource.setData(emptyFeatureCollection())
    }
  }, [])

  const showCircleNotice = useCallback((message: string) => {
    setCircleNotice(message)
    if (circleNoticeTimerRef.current) {
      clearTimeout(circleNoticeTimerRef.current)
    }
    circleNoticeTimerRef.current = setTimeout(() => {
      setCircleNotice('')
      circleNoticeTimerRef.current = null
    }, 2400)
  }, [])

  const handleToggleCircleTool = useCallback(() => {
    const nextActive = !circleToolActive
    setCircleToolActive(nextActive)

    if (nextActive && drawMode !== 'simple_select') {
      const draw = drawRef.current
      if (draw) {
        draw.changeMode('simple_select')
        setDrawMode('simple_select')
      }
    }

    showCircleNotice(nextActive ? 'Lens mode enabled. Move on map, click to pin, and use a circle as AOI.' : 'Lens mode disabled.')
  }, [circleToolActive, drawMode, showCircleNotice])

  const citiesQuery = useQuery({
    queryKey: ['cities'],
    queryFn: getCities,
  })
  const effectiveCity = selectedCity || citiesQuery.data?.cities?.[0]?.city || ''

  useEffect(() => {
    if (selectedCity || !citiesQuery.data?.cities?.length) {
      return
    }
    const delhiMatch = citiesQuery.data.cities.find((city) => city.city.trim().toLowerCase() === DEFAULT_CITY_SLUG)
    if (delhiMatch) {
      setSelectedCity(delhiMatch.city)
    }
  }, [citiesQuery.data?.cities, selectedCity])

  const analyzeCircle = useCallback(async (circle: LockedCircle) => {
    // Convert circle to polygon
    const polygon = createCirclePolygon(circle.center, circle.radius)

    // Set loading state
    setCircleMetrics(prev => new Map(prev).set(circle.id, {
      circleId: circle.id,
      status: 'queued',
      progressPct: 0,
      progressMessage: 'Submitting comparison job…',
      isLoading: true,
      metrics: {},
      qualitySummary: {},
      location: circle.location,
    }))

    try {
      // Call existing analyse API
      const response = await analyse({
        mode: 'custom_polygon',
        city: effectiveCity,
        geometry: polygon,
        run_async: true,
      })

      if (isJobResponse(response)) {
        // Store job ID for polling
        setCircleMetrics(prev => {
          const updated = new Map(prev)
          const existing = updated.get(circle.id)
          if (existing) {
            updated.set(circle.id, {
              ...existing,
              jobId: response.job_id,
              status: 'queued',
              progressPct: 0,
              progressMessage: 'Queued. Smaller circles (250m/500m) usually finish faster.',
            })
          }
          return updated
        })
      } else {
        // Immediate result
        const parsedResult = parseCircleResultPayload(response.result as Record<string, unknown>)
        setCircleMetrics(prev => new Map(prev).set(circle.id, {
          circleId: circle.id,
          status: 'succeeded',
          progressPct: 100,
          progressMessage: parsedResult.error || 'Circle analysis complete.',
          isLoading: false,
          metrics: parsedResult.error ? { error: parsedResult.error } : parsedResult.metrics,
          qualitySummary: parsedResult.qualitySummary,
          location: circle.location,
        }))
      }
    } catch (error) {
      console.error('Circle analysis failed:', error)
      setCircleMetrics(prev => new Map(prev).set(circle.id, {
        circleId: circle.id,
        status: 'failed',
        progressPct: 0,
        progressMessage: 'Circle analysis failed to start.',
        isLoading: false,
        metrics: { error: 'Analysis failed' },
        qualitySummary: {},
        location: circle.location,
      }))
    }
  }, [effectiveCity])

  const pinCircleAt = useCallback((center: [number, number]) => {
    if (lockedCircles.length >= MAX_COMPARE_CIRCLES) {
      showCircleNotice(`Comparison is limited to ${MAX_COMPARE_CIRCLES} circles for clarity.`)
      return
    }

    clearDrawState()
    clearWardSelection()

    const circleId = `circle-${Date.now()}`
    const newCircle: LockedCircle = {
      id: circleId,
      center,
      radius: circleRadius,
      color: getCircleColor(lockedCircles.length),
      label: getCircleLabel(lockedCircles.length),
      location: null,
      createdAt: Date.now(),
    }

    setLockedCircles((prev) => [...prev, newCircle])
    setSelectedAoiCircleId(newCircle.id)

    const map = mapRef.current
    if (map) {
      const hoverSource = map.getSource('hover-circle-source') as maplibregl.GeoJSONSource | undefined
      if (hoverSource) {
        hoverSource.setData(emptyFeatureCollection())
      }
    }
    setHoverCircleCenter(null)
    setCursorPosition(null)

    analyzeCircle(newCircle)
    showCircleNotice(`Pinned ${newCircle.label}. It is now the active AOI and comparison target.`)
  }, [analyzeCircle, circleRadius, clearDrawState, clearWardSelection, lockedCircles.length, showCircleNotice])

  const wardsQuery = useQuery({
    queryKey: ['city-wards', effectiveCity],
    queryFn: () => getCityWards(effectiveCity),
    enabled: Boolean(effectiveCity),
  })

  const wardsGeoQuery = useQuery({
    queryKey: ['city-wards-geojson', effectiveCity],
    queryFn: () => getCityWardsGeoJSON(effectiveCity),
    enabled: Boolean(effectiveCity),
  })

  const cityWardsMetricsQuery = useQuery({
    queryKey: ['city-ward-metrics', effectiveCity],
    queryFn: async () => {
      const response = await analyse({
        mode: 'wards',
        city: effectiveCity,
        limit: 1000,
        run_async: false,
      })
      if (isJobResponse(response)) {
        throw new Error('Expected synchronous ward collection response')
      }
      return response
    },
    enabled: Boolean(effectiveCity),
  })

  const effectiveSelectedWardId = selectedWardIds.length === 1 ? selectedWardIds[0] : ''

  const wardDetailsQuery = useQuery({
    queryKey: ['ward-details', effectiveCity, effectiveSelectedWardId],
    queryFn: () => getWardMetrics(effectiveCity, effectiveSelectedWardId),
    enabled: Boolean(effectiveCity && effectiveSelectedWardId),
  })

  const cityMetricsQuery = useQuery({
    queryKey: ['city-metrics', effectiveCity],
    queryFn: () => getCityMetrics(effectiveCity),
    enabled: Boolean(effectiveCity),
  })

  const cityFullMetricsQuery = useQuery({
    queryKey: ['city-full-metrics', effectiveCity],
    queryFn: async () => {
      const response = await analyse({
        mode: 'city',
        city: effectiveCity,
        run_async: false,
      })
      if (isJobResponse(response)) {
        throw new Error('Expected synchronous city analysis response')
      }
      return response
    },
    enabled: Boolean(effectiveCity),
  })

  const metaMetricsQuery = useQuery({
    queryKey: ['meta-metrics'],
    queryFn: getMetaMetrics,
  })

  const mapMetricOptions = useMemo(() => {
    const allMetrics = (metaMetricsQuery.data?.metrics ?? []).filter((metric) => !RETIRED_METRIC_IDS.has(metric.metric_id))
    const preferred = allMetrics.filter((metric) => MAP_METRIC_IDS.includes(metric.metric_id))
    return preferred.length > 0 ? preferred : allMetrics
  }, [metaMetricsQuery.data?.metrics])

  useEffect(() => {
    if (!mapMetricOptions.length) {
      return
    }
    const stillValid = mapMetricOptions.some((metric) => metric.metric_id === activeMetricId)
    if (!activeMetricId || !stillValid) {
      setActiveMetricId(mapMetricOptions[0].metric_id)
    }
  }, [activeMetricId, mapMetricOptions])

  const effectiveMetricId = activeMetricId || mapMetricOptions[0]?.metric_id || DEFAULT_METRIC_ID

  const wardRows = useMemo(() => parseWardRows(cityWardsMetricsQuery.data), [cityWardsMetricsQuery.data])

  const metricByWard = useMemo(() => {
    const map = new Map<string, number | null>()
    for (const ward of wardRows) {
      const value = ward.metrics_json?.all_metrics?.[effectiveMetricId]
      map.set(ward.ward_id, toNumberOrNull(value))
    }
    return map
  }, [wardRows, effectiveMetricId])

  const geojsonData = useMemo<WardsGeoJSON>(() => {
    const source = wardsGeoQuery.data
    if (!source) {
      return {
        type: 'FeatureCollection',
        features: [],
      }
    }

    return {
      type: 'FeatureCollection',
      features: source.features.map((feature) => {
        const wardId = feature.properties.ward_id
        const metricValue = metricByWard.get(wardId) ?? null
        return {
          ...feature,
          id: wardId,
          properties: {
            ...feature.properties,
            metric_value: metricValue,
          },
        }
      }),
    }
  }, [metricByWard, wardsGeoQuery.data])

  const metricRange = useMemo(() => {
    const values = geojsonData.features
      .map((feature) => toNumberOrNull(feature.properties.metric_value))
      .filter((value): value is number => value !== null)

    if (!values.length) {
      return { min: null, max: null }
    }

    return {
      min: Math.min(...values),
      max: Math.max(...values),
    }
  }, [geojsonData])

  const selectedWardFeatures = useMemo(
    () => geojsonData.features.filter((feature) => selectedWardIds.includes(feature.properties.ward_id)),
    [geojsonData.features, selectedWardIds],
  )

  const selectedWardGeometry = useMemo(
    () => buildSelectedWardAoiGeometry(selectedWardFeatures as WardGeometryFeature[]),
    [selectedWardFeatures],
  )

  const geometryValidation = useMemo(() => validateAndNormalizeGeometry(drawnGeometry), [drawnGeometry])
  const selectedAoiCircle = useMemo(
    () => lockedCircles.find((circle) => circle.id === selectedAoiCircleId) ?? null,
    [lockedCircles, selectedAoiCircleId],
  )

  const currentAoiSource: PanelSourceType = drawnGeometry
    ? 'drawn_polygon'
    : selectedAoiCircle
      ? 'circle'
    : selectedWardIds.length > 1
      ? 'selected_wards'
      : effectiveSelectedWardId
        ? 'ward'
        : 'city'

  const currentAoiDescription = useMemo(
    () =>
      describeAoi(
        currentAoiSource,
        effectiveCity,
        selectedWardFeatures as WardGeometryFeature[],
        geometryValidation.areaSqM ? geometryValidation.areaSqM / 1_000_000 : null,
        selectedAoiCircle,
      ),
    [currentAoiSource, effectiveCity, geometryValidation.areaSqM, selectedAoiCircle, selectedWardFeatures],
  )

  const submitAreaAnalysisMutation = useMutation({
    mutationFn: async (variables: {
      geometry: Polygon | MultiPolygon
      source: Exclude<AutoAnalysisSource, null>
      key: string
      label: string
    }) => {
      const response = await analyse({
        mode: 'custom_polygon',
        city: effectiveCity,
        geometry: variables.geometry as Geometry,
        run_async: true,
      })
      if (!isJobResponse(response)) {
        throw new Error('Expected async response for AOI analysis')
      }
      return { job: response, variables }
    },
    onSuccess: ({ job, variables }) => {
      setActiveJobId(job.job_id)
      setActiveAnalysisSource(variables.source)
      setActiveAnalysisLabel(variables.label)
      setActiveAnalysisKey(variables.key)
      setExportMessage('')
    },
  })

  const analyseJobQuery = useQuery({
    queryKey: ['analyse-job', activeJobId],
    queryFn: () => getAnalyseJob(activeJobId),
    enabled: Boolean(activeJobId),
    refetchInterval: (query) => {
      const current = query.state.data as AnalyseJobResponse | undefined
      if (!current) {
        return 1500
      }
      if (current.status === 'queued') {
        const createdMs = current.created_at ? new Date(current.created_at).getTime() : Number.NaN
        if (Number.isFinite(createdMs) && Date.now() - createdMs > 12 * 60_000) {
          return false
        }
        return 1500
      }
      if (current.status === 'running') {
        const startedMs = current.started_at ? new Date(current.started_at).getTime() : Number.NaN
        if (Number.isFinite(startedMs)) {
          const elapsedMs = Date.now() - startedMs
          if (elapsedMs > 12 * 60_000) {
            return false
          }
          if (elapsedMs < 20_000) {
            return 1000
          }
          if (elapsedMs < 90_000) {
            return 2000
          }
        }
        return 3000
      }
      return false
    },
  })

  const activeJob = analyseJobQuery.data
  const activeJobInProgress = activeJob?.status === 'queued' || activeJob?.status === 'running'

  const drawnAnalysisKey = useMemo(() => {
    if (!effectiveCity || !geometryValidation.normalizedGeometry || !geometryValidation.isValid) {
      return ''
    }
    return `draw:${effectiveCity}:${JSON.stringify(geometryValidation.normalizedGeometry)}`
  }, [effectiveCity, geometryValidation.isValid, geometryValidation.normalizedGeometry])

  const selectedWardsAnalysisKey = useMemo(() => {
    if (!effectiveCity || selectedWardIds.length <= 1 || !selectedWardGeometry) {
      return ''
    }
    return `wards:${effectiveCity}:${selectedWardIds.slice().sort().join('|')}`
  }, [effectiveCity, selectedWardGeometry, selectedWardIds])

  useEffect(() => {
    const normalizedGeometry = geometryValidation.normalizedGeometry
    if (!drawnAnalysisKey || !normalizedGeometry) {
      return
    }
    if (lastSubmittedAnalysisKeyRef.current === drawnAnalysisKey) {
      return
    }
    if (submitAreaAnalysisMutation.isPending) {
      return
    }
    if (activeJobInProgress && activeAnalysisSource === 'drawn_polygon') {
      return
    }

    const timer = window.setTimeout(() => {
      if (lastSubmittedAnalysisKeyRef.current === drawnAnalysisKey) {
        return
      }
      lastSubmittedAnalysisKeyRef.current = drawnAnalysisKey
      submitAreaAnalysisMutation.mutate({
        geometry: normalizedGeometry,
        source: 'drawn_polygon',
        key: drawnAnalysisKey,
        label: 'Drawn area of interest',
      })
    }, 600)

    return () => {
      window.clearTimeout(timer)
    }
  }, [
    activeAnalysisSource,
    activeJobInProgress,
    drawnAnalysisKey,
    geometryValidation.normalizedGeometry,
    submitAreaAnalysisMutation,
    submitAreaAnalysisMutation.isPending,
  ])

  useEffect(() => {
    if (!selectedWardsAnalysisKey || !selectedWardGeometry || selectedWardIds.length <= 1) {
      return
    }
    if (lastSubmittedAnalysisKeyRef.current === selectedWardsAnalysisKey) {
      return
    }
    if (submitAreaAnalysisMutation.isPending) {
      return
    }
    if (activeJobInProgress && activeAnalysisSource === 'selected_wards') {
      return
    }

    const timer = window.setTimeout(() => {
      if (lastSubmittedAnalysisKeyRef.current === selectedWardsAnalysisKey) {
        return
      }
      lastSubmittedAnalysisKeyRef.current = selectedWardsAnalysisKey
      submitAreaAnalysisMutation.mutate({
        geometry: selectedWardGeometry,
        source: 'selected_wards',
        key: selectedWardsAnalysisKey,
        label: `${selectedWardIds.length} wards combined`,
      })
    }, 600)

    return () => {
      window.clearTimeout(timer)
    }
  }, [
    activeAnalysisSource,
    activeJobInProgress,
    selectedWardGeometry,
    selectedWardIds.length,
    selectedWardsAnalysisKey,
    submitAreaAnalysisMutation,
    submitAreaAnalysisMutation.isPending,
  ])

  useEffect(() => {
    if (currentAoiSource !== 'selected_wards') {
      clearAnalysisSource('selected_wards')
    }
    if (currentAoiSource !== 'drawn_polygon') {
      clearAnalysisSource('drawn_polygon')
    }
  }, [clearAnalysisSource, currentAoiSource])

  useEffect(() => {
    if (activeJob?.status !== 'queued' && activeJob?.status !== 'running') {
      return
    }
    const timer = window.setInterval(() => {
      setJobClock(Date.now())
    }, 1000)
    return () => {
      window.clearInterval(timer)
    }
  }, [activeJob?.status])

  useEffect(() => {
    circleMetricsRef.current = circleMetrics
  }, [circleMetrics])

  useEffect(() => {
    const pollCircleJobs = async () => {
      if (circlePollInFlightRef.current) {
        return
      }

      const pending = Array.from(circleMetricsRef.current.values()).filter((entry) => entry.isLoading && entry.jobId)
      if (pending.length === 0) {
        return
      }

      circlePollInFlightRef.current = true
      try {
        const results = await Promise.all(
          pending.map(async (entry) => {
            try {
              const response = await getAnalyseJob(entry.jobId as string)
              return { circleId: entry.circleId, response }
            } catch (error) {
              return {
                circleId: entry.circleId,
                error: error instanceof Error ? error.message : 'Unable to refresh circle job status.',
              }
            }
          }),
        )

        setCircleMetrics((prev) => {
          const updated = new Map(prev)
          for (const result of results) {
            const existing = updated.get(result.circleId)
            if (!existing) {
              continue
            }

            if ('error' in result) {
              updated.set(result.circleId, {
                ...existing,
                progressMessage: result.error,
              })
              continue
            }

            const job = result.response
            if (job.status === 'succeeded' && job.result) {
              const parsedResult = parseCircleResultPayload(job.result as Record<string, unknown>)
              updated.set(result.circleId, {
                ...existing,
                isLoading: false,
                status: 'succeeded',
                progressPct: 100,
                progressMessage: parsedResult.error || 'Circle analysis complete.',
                metrics: parsedResult.error ? { error: parsedResult.error } : parsedResult.metrics,
                qualitySummary: parsedResult.qualitySummary,
              })
              continue
            }

            if (job.status === 'failed') {
              updated.set(result.circleId, {
                ...existing,
                isLoading: false,
                status: 'failed',
                progressPct: job.progress_pct ?? 0,
                progressMessage: job.error || 'Circle analysis failed.',
                metrics: { error: job.error || 'Analysis failed' },
              })
              continue
            }

            updated.set(result.circleId, {
              ...existing,
              status: job.status,
              progressPct: job.progress_pct ?? 0,
              progressMessage: job.progress_message || (job.status === 'queued' ? 'Queued for comparison analysis.' : 'Computing circle metrics...'),
            })
          }
          return updated
        })
      } finally {
        circlePollInFlightRef.current = false
      }
    }

    const intervalId = window.setInterval(() => {
      void pollCircleJobs()
    }, CIRCLE_POLL_INTERVAL_MS)

    void pollCircleJobs()

    return () => {
      window.clearInterval(intervalId)
    }
  }, [])

  // Update locked circles layer when circles change
  useEffect(() => {
    updateLockedCirclesLayer()
  }, [updateLockedCirclesLayer])

  // Clear circles when city changes
  useEffect(() => {
    if (effectiveCity) {
      clearAllCircles()
    }
  }, [effectiveCity, clearAllCircles])

  const activeJobElapsedSeconds = useMemo(() => {
    if (!activeJob) {
      return 0
    }
    const baseIso = activeJob.started_at ?? activeJob.created_at
    if (!baseIso) {
      return 0
    }
    const startMillis = new Date(baseIso).getTime()
    if (!Number.isFinite(startMillis)) {
      return 0
    }
    return Math.max(0, Math.floor((jobClock - startMillis) / 1000))
  }, [activeJob, jobClock])

  const activeJobProgressHint = useMemo(() => {
    if (!activeJob) {
      return ''
    }
    if (activeJob.status === 'queued') {
      return 'Queued for AOI analysis.'
    }
    if (activeJob.status === 'running') {
      if (activeJobElapsedSeconds > 12 * 60) {
        return 'Analysis timed out. Try a smaller area or reset the AOI.'
      }
      if (activeJobElapsedSeconds < 20) {
        return 'Normalizing geometry and loading metrics.'
      }
      if (activeJobElapsedSeconds < 90) {
        return 'Computing AOI metrics. Medium and large areas take longer.'
      }
      return 'Still running. This AOI is relatively heavy.'
    }
    if (activeJob.status === 'failed') {
      return 'AOI analysis failed.'
    }
    return 'AOI analysis complete.'
  }, [activeJob, activeJobElapsedSeconds])

  const roadLayerQuery = useQuery({
    queryKey: ['city-roads-layer', effectiveCity, serializeViewportBbox(mapViewport), mapViewport?.zoom, showRoadOverlay],
    queryFn: () =>
      getCityRoadsGeoJSON(
        effectiveCity,
        serializeViewportBbox(mapViewport),
        mapViewport?.zoom ?? 10,
        (mapViewport?.zoom ?? 10) >= 12 ? 'full' : 'major',
      ),
    enabled: Boolean(effectiveCity && mapViewport && showRoadOverlay),
    staleTime: 30_000,
  })

  const transitLayerQuery = useQuery({
    queryKey: ['city-transit-layer', effectiveCity, serializeViewportBbox(mapViewport), showTransitOverlay],
    queryFn: () => getCityTransitGeoJSON(effectiveCity, serializeViewportBbox(mapViewport)),
    enabled: Boolean(effectiveCity && mapViewport && showTransitOverlay),
    staleTime: 30_000,
  })

  const activeCustomPanelData = useMemo<PanelSnapshot | null>(() => {
    if (!activeJob || activeJob.status !== 'succeeded' || !activeJob.result) {
      return null
    }

    const metricsJson = activeJob.result.metrics_json as { all_metrics?: Record<string, unknown> } | undefined
    if (!metricsJson?.all_metrics) {
      return null
    }

    const sourceType = activeAnalysisSource === 'selected_wards' ? 'selected_wards' : 'drawn_polygon'
    return {
      sourceType,
      sourceId: activeAnalysisKey || activeJob.job_id,
      title: activeAnalysisLabel || 'Area of interest',
      subtitle: sourceType === 'selected_wards' ? 'Multi-ward AOI analysed through the polygon engine.' : 'Custom drawn AOI analysed through the polygon engine.',
      metrics: metricsJson.all_metrics,
      qualitySummary: (activeJob.result.quality_summary as Record<string, unknown> | undefined) ?? {},
    }
  }, [activeAnalysisKey, activeAnalysisLabel, activeAnalysisSource, activeJob])

  const selectedCircleMetrics = useMemo(
    () => (selectedAoiCircle ? circleMetrics.get(selectedAoiCircle.id) ?? null : null),
    [circleMetrics, selectedAoiCircle],
  )

  const circlePanelData = useMemo<PanelSnapshot | null>(() => {
    if (!selectedAoiCircle || !selectedCircleMetrics || selectedCircleMetrics.isLoading) {
      return null
    }
    if ('error' in selectedCircleMetrics.metrics) {
      return null
    }
    if (!Object.keys(selectedCircleMetrics.metrics).length) {
      return null
    }

    return {
      sourceType: 'circle',
      sourceId: selectedAoiCircle.id,
      title: `Circle ${selectedAoiCircle.label} AOI`,
      subtitle: selectedAoiCircle.location || `${selectedAoiCircle.center[1].toFixed(4)}, ${selectedAoiCircle.center[0].toFixed(4)}`,
      metrics: selectedCircleMetrics.metrics,
      qualitySummary: selectedCircleMetrics.qualitySummary,
    }
  }, [selectedAoiCircle, selectedCircleMetrics])

  const wardPanelData = useMemo<PanelSnapshot | null>(() => {
    if (!wardDetailsQuery.data?.metrics_json?.all_metrics || !effectiveSelectedWardId) {
      return null
    }

    return {
      sourceType: 'ward',
      sourceId: effectiveSelectedWardId,
      title: currentAoiDescription.title,
      subtitle: currentAoiDescription.subtitle,
      metrics: wardDetailsQuery.data.metrics_json.all_metrics as Record<string, unknown>,
      qualitySummary: wardDetailsQuery.data.quality_summary,
    }
  }, [currentAoiDescription.subtitle, currentAoiDescription.title, effectiveSelectedWardId, wardDetailsQuery.data])

  const cityPanelData = useMemo<PanelSnapshot | null>(() => {
    const result = cityFullMetricsQuery.data?.result as {
      metrics_json?: { all_metrics?: Record<string, unknown> }
      quality_summary?: Record<string, unknown>
    } | undefined

    if (!result?.metrics_json?.all_metrics) {
      return null
    }

    return {
      sourceType: 'city',
      sourceId: effectiveCity || 'city',
      title: `${effectiveCity || 'City'} full extent`,
      subtitle: 'City-wide aggregate of the latest ward metrics.',
      metrics: result.metrics_json.all_metrics,
      qualitySummary: result.quality_summary ?? {},
    }
  }, [cityFullMetricsQuery.data?.result, effectiveCity])

  const activePanelData = useMemo<PanelSnapshot | null>(() => {
    if (currentAoiSource === 'city') {
      return cityPanelData
    }
    if (currentAoiSource === 'circle') {
      return circlePanelData
    }
    if (currentAoiSource === 'ward') {
      return wardPanelData
    }
    if (currentAoiSource === 'selected_wards' && activeAnalysisSource === 'selected_wards') {
      return activeCustomPanelData
    }
    if (currentAoiSource === 'drawn_polygon' && activeAnalysisSource === 'drawn_polygon') {
      return activeCustomPanelData
    }
    return null
  }, [activeAnalysisSource, activeCustomPanelData, circlePanelData, cityPanelData, currentAoiSource, wardPanelData])

  const cityAverageMetricMap = useMemo(() => {
    const map = new Map<string, number>()
    for (const row of cityMetricsQuery.data?.metrics ?? []) {
      map.set(row.metric_id, row.avg_value)
    }
    return map
  }, [cityMetricsQuery.data])

  const cityAveragePayload = useMemo<Record<string, unknown>>(
    () => Object.fromEntries((cityMetricsQuery.data?.metrics ?? []).map((row) => [row.metric_id, row.avg_value])),
    [cityMetricsQuery.data],
  )

  const narrativeContext = useMemo(() => {
    if (currentAoiSource === 'city') {
      return `${effectiveCity || 'the city'} overall`
    }
    if (currentAoiSource === 'circle' && selectedAoiCircle) {
      return `circle ${selectedAoiCircle.label} AOI`
    }
    return 'this area of interest'
  }, [currentAoiSource, effectiveCity, selectedAoiCircle])

  const highlightCards = useMemo(
    () => buildHighlightCards(activePanelData?.metrics ?? cityAveragePayload, cityAverageMetricMap, metaMetricsQuery.data?.metrics ?? [], narrativeContext),
    [activePanelData?.metrics, cityAverageMetricMap, cityAveragePayload, metaMetricsQuery.data?.metrics, narrativeContext],
  )

  const metricPanelRows = useMemo(
    () => buildMetricPanelRows(activePanelData?.metrics ?? {}, metaMetricsQuery.data?.metrics ?? []),
    [activePanelData?.metrics, metaMetricsQuery.data?.metrics],
  )
  const metricPanelGroups = useMemo(() => groupMetricPanelRows(metricPanelRows), [metricPanelRows])

  const drawGuidance = useMemo(() => {
    if (geometryValidation.errors.length > 0) {
      return geometryValidation.errors[0]
    }
    if (drawMode === 'draw_polygon') {
      return 'Click to add vertices. Double-click or finish the ring to submit immediately.'
    }
    if (drawMode === 'direct_select' && drawnGeometry) {
      return 'Drag a vertex and release. The AOI refreshes automatically after the edit.'
    }
    if (drawnGeometry) {
      return 'AOI is ready. Edit it or clear it. Results refresh automatically.'
    }
    if (selectedAoiCircle) {
      return `Circle ${selectedAoiCircle.label} is the active AOI. Pin another circle or switch to wards/polygon.`
    }
    if (selectedWardIds.length > 1) {
      return 'Multiple wards are combined and analysed as one AOI automatically.'
    }
    if (selectedWardIds.length === 1) {
      return 'Single ward selected. Click more wards to grow the AOI or draw a custom polygon.'
    }
    return 'Click wards to build an AOI or draw a custom polygon.'
  }, [drawMode, drawnGeometry, geometryValidation.errors, selectedAoiCircle, selectedWardIds.length])

  const handleExportJson = useCallback(() => {
    if (!activePanelData || metricPanelRows.length === 0) {
      setExportMessage('No AOI metric payload is ready yet.')
      return
    }

    const exportSourceType =
      activePanelData.sourceType === 'selected_wards' || activePanelData.sourceType === 'drawn_polygon' || activePanelData.sourceType === 'circle'
        ? 'custom_polygon'
        : activePanelData.sourceType
    const fileName = `urbanmor_${effectiveCity}_${activePanelData.sourceType}_${activePanelData.sourceId}.json`
    const payload = makeMetricsExportJson(
      {
        city: effectiveCity,
        sourceType: exportSourceType,
        sourceId: activePanelData.sourceId,
        qualitySummary: activePanelData.qualitySummary,
      },
      metricPanelRows,
    )
    downloadTextFile(fileName, payload, 'application/json')
    setExportMessage(`Exported ${fileName}`)
  }, [activePanelData, effectiveCity, metricPanelRows])

  const handleExportCsv = useCallback(() => {
    if (!activePanelData || metricPanelRows.length === 0) {
      setExportMessage('No AOI metric payload is ready yet.')
      return
    }

    const exportSourceType =
      activePanelData.sourceType === 'selected_wards' || activePanelData.sourceType === 'drawn_polygon' || activePanelData.sourceType === 'circle'
        ? 'custom_polygon'
        : activePanelData.sourceType
    const fileName = `urbanmor_${effectiveCity}_${activePanelData.sourceType}_${activePanelData.sourceId}.csv`
    const payload = makeMetricsExportCsv(
      {
        city: effectiveCity,
        sourceType: exportSourceType,
        sourceId: activePanelData.sourceId,
        qualitySummary: activePanelData.qualitySummary,
      },
      metricPanelRows,
    )
    downloadTextFile(fileName, payload, 'text/csv;charset=utf-8')
    setExportMessage(`Exported ${fileName}`)
  }, [activePanelData, effectiveCity, metricPanelRows])

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return
    }

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: createBasemapStyle(),
      center: [78.9629, 20.5937],
      zoom: 4,
      attributionControl: {},
    })

    map.addControl(new maplibregl.NavigationControl({ showCompass: true }), 'top-left')

    const draw = new MapboxDraw({
      displayControlsDefault: false,
      controls: {},
      defaultMode: 'simple_select',
      styles: [
        // Fix line-dasharray compatibility with MapLibre GL
        {
          id: 'gl-draw-polygon-fill-inactive',
          type: 'fill',
          filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Polygon'], ['!=', 'mode', 'static']],
          paint: {
            'fill-color': '#3bb2d0',
            'fill-outline-color': '#3bb2d0',
            'fill-opacity': 0.1
          }
        },
        {
          id: 'gl-draw-polygon-fill-active',
          type: 'fill',
          filter: ['all', ['==', 'active', 'true'], ['==', '$type', 'Polygon']],
          paint: {
            'fill-color': '#fbb03b',
            'fill-outline-color': '#fbb03b',
            'fill-opacity': 0.1
          }
        },
        {
          id: 'gl-draw-polygon-midpoint',
          type: 'circle',
          filter: ['all', ['==', '$type', 'Point'], ['==', 'meta', 'midpoint']],
          paint: {
            'circle-radius': 3,
            'circle-color': '#fbb03b'
          }
        },
        {
          id: 'gl-draw-polygon-stroke-inactive',
          type: 'line',
          filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Polygon'], ['!=', 'mode', 'static']],
          layout: {
            'line-cap': 'round',
            'line-join': 'round'
          },
          paint: {
            'line-color': '#3bb2d0',
            'line-width': 2
          }
        },
        {
          id: 'gl-draw-polygon-stroke-active',
          type: 'line',
          filter: ['all', ['==', 'active', 'true'], ['==', '$type', 'Polygon']],
          layout: {
            'line-cap': 'round',
            'line-join': 'round'
          },
          paint: {
            'line-color': '#fbb03b',
            'line-width': 2
          }
        },
        {
          id: 'gl-draw-line-inactive',
          type: 'line',
          filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'LineString'], ['!=', 'mode', 'static']],
          layout: {
            'line-cap': 'round',
            'line-join': 'round'
          },
          paint: {
            'line-color': '#3bb2d0',
            'line-width': 2
          }
        },
        {
          id: 'gl-draw-line-active',
          type: 'line',
          filter: ['all', ['==', '$type', 'LineString'], ['==', 'active', 'true']],
          layout: {
            'line-cap': 'round',
            'line-join': 'round'
          },
          paint: {
            'line-color': '#fbb03b',
            'line-width': 2
          }
        },
        {
          id: 'gl-draw-polygon-and-line-vertex-stroke-inactive',
          type: 'circle',
          filter: ['all', ['==', 'meta', 'vertex'], ['==', '$type', 'Point'], ['!=', 'mode', 'static']],
          paint: {
            'circle-radius': 5,
            'circle-color': '#fff'
          }
        },
        {
          id: 'gl-draw-polygon-and-line-vertex-inactive',
          type: 'circle',
          filter: ['all', ['==', 'meta', 'vertex'], ['==', '$type', 'Point'], ['!=', 'mode', 'static']],
          paint: {
            'circle-radius': 3,
            'circle-color': '#fbb03b'
          }
        },
        {
          id: 'gl-draw-point-point-stroke-inactive',
          type: 'circle',
          filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Point'], ['==', 'meta', 'feature'], ['!=', 'mode', 'static']],
          paint: {
            'circle-radius': 5,
            'circle-opacity': 1,
            'circle-color': '#fff'
          }
        },
        {
          id: 'gl-draw-point-inactive',
          type: 'circle',
          filter: ['all', ['==', 'active', 'false'], ['==', '$type', 'Point'], ['==', 'meta', 'feature'], ['!=', 'mode', 'static']],
          paint: {
            'circle-radius': 3,
            'circle-color': '#3bb2d0'
          }
        },
        {
          id: 'gl-draw-point-stroke-active',
          type: 'circle',
          filter: ['all', ['==', '$type', 'Point'], ['==', 'active', 'true'], ['!=', 'meta', 'midpoint']],
          paint: {
            'circle-radius': 7,
            'circle-color': '#fff'
          }
        },
        {
          id: 'gl-draw-point-active',
          type: 'circle',
          filter: ['all', ['==', '$type', 'Point'], ['!=', 'meta', 'midpoint'], ['==', 'active', 'true']],
          paint: {
            'circle-radius': 5,
            'circle-color': '#fbb03b'
          }
        },
        {
          id: 'gl-draw-polygon-fill-static',
          type: 'fill',
          filter: ['all', ['==', 'mode', 'static'], ['==', '$type', 'Polygon']],
          paint: {
            'fill-color': '#404040',
            'fill-outline-color': '#404040',
            'fill-opacity': 0.1
          }
        },
        {
          id: 'gl-draw-polygon-stroke-static',
          type: 'line',
          filter: ['all', ['==', 'mode', 'static'], ['==', '$type', 'Polygon']],
          layout: {
            'line-cap': 'round',
            'line-join': 'round'
          },
          paint: {
            'line-color': '#404040',
            'line-width': 2
          }
        },
        {
          id: 'gl-draw-line-static',
          type: 'line',
          filter: ['all', ['==', 'mode', 'static'], ['==', '$type', 'LineString']],
          layout: {
            'line-cap': 'round',
            'line-join': 'round'
          },
          paint: {
            'line-color': '#404040',
            'line-width': 2
          }
        },
        {
          id: 'gl-draw-point-static',
          type: 'circle',
          filter: ['all', ['==', 'mode', 'static'], ['==', '$type', 'Point']],
          paint: {
            'circle-radius': 5,
            'circle-color': '#404040'
          }
        }
      ]
    })

    map.addControl(draw as unknown as maplibregl.IControl, 'top-left')

    const ensureLayers = () => {
      if (!map.getSource('roads-overlay-source')) {
        map.addSource('roads-overlay-source', {
          type: 'geojson',
          data: emptyFeatureCollection(),
        })
      }

      if (!map.getLayer('roads-overlay-line')) {
        map.addLayer({
          id: 'roads-overlay-line',
          type: 'line',
          source: 'roads-overlay-source',
          paint: {
            'line-color': '#ea580c',
            'line-opacity': 0.58,
            'line-width': [
              'interpolate',
              ['linear'],
              ['coalesce', ['to-number', ['get', 'style_rank']], 1],
              1,
              0.7,
              2,
              1.2,
              3,
              1.8,
              4,
              2.6,
            ],
          },
        })
      }

      if (!map.getSource('wards-source')) {
        map.addSource('wards-source', {
          type: 'geojson',
          data: emptyFeatureCollection(),
          promoteId: 'ward_id',
        })
      }

      if (!map.getLayer('wards-fill')) {
        map.addLayer({
          id: 'wards-fill',
          type: 'fill',
          source: 'wards-source',
          paint: {
            'fill-color': '#d6d3d1',
            'fill-opacity': 0.34,
          },
        })
      }

      if (!map.getLayer('wards-outline')) {
        map.addLayer({
          id: 'wards-outline',
          type: 'line',
          source: 'wards-source',
          paint: {
            'line-color': '#0f172a',
            'line-width': 1.15,
            'line-opacity': 0.74,
          },
        })
      }

      if (!map.getLayer('wards-hover')) {
        map.addLayer({
          id: 'wards-hover',
          type: 'line',
          source: 'wards-source',
          paint: {
            'line-color': '#0f172a',
            'line-width': 2,
          },
          filter: ['==', ['get', 'ward_id'], ''],
        })
      }

      if (!map.getLayer('wards-selected')) {
        map.addLayer({
          id: 'wards-selected',
          type: 'line',
          source: 'wards-source',
          paint: {
            'line-color': '#0f766e',
            'line-width': 2.8,
          },
          filter: ['in', ['get', 'ward_id'], ['literal', []]],
        })
      }

      if (!map.getSource('transit-overlay-source')) {
        map.addSource('transit-overlay-source', {
          type: 'geojson',
          data: emptyFeatureCollection(),
        })
      }

      if (!map.getLayer('transit-overlay-circle')) {
        map.addLayer({
          id: 'transit-overlay-circle',
          type: 'circle',
          source: 'transit-overlay-source',
          paint: {
            'circle-radius': [
              'match',
              ['get', 'stop_kind'],
              'metro',
              5.5,
              'rail',
              4.8,
              'station',
              4.2,
              3.2,
            ],
            'circle-color': [
              'match',
              ['get', 'stop_kind'],
              'metro',
              '#dc2626',
              'rail',
              '#7c3aed',
              'station',
              '#0f766e',
              '#1d4ed8',
            ],
            'circle-stroke-color': '#fffbeb',
            'circle-stroke-width': 1.2,
            'circle-opacity': 0.9,
          },
        })
      }

      // Circle tool sources and layers
      if (!map.getSource('hover-circle-source')) {
        map.addSource('hover-circle-source', {
          type: 'geojson',
          data: emptyFeatureCollection(),
        })
      }

      if (!map.getLayer('hover-circle-fill')) {
        map.addLayer({
          id: 'hover-circle-fill',
          type: 'fill',
          source: 'hover-circle-source',
          paint: {
            'fill-color': '#D97706',
            'fill-opacity': 0.15,
          },
        })
      }

      if (!map.getLayer('hover-circle-line')) {
        map.addLayer({
          id: 'hover-circle-line',
          type: 'line',
          source: 'hover-circle-source',
          paint: {
            'line-color': '#D97706',
            'line-width': 3,
            'line-opacity': 0.9,
          },
        })
      }

      if (!map.getSource('locked-circles-source')) {
        map.addSource('locked-circles-source', {
          type: 'geojson',
          data: emptyFeatureCollection(),
        })
      }

      if (!map.getLayer('locked-circles-fill')) {
        map.addLayer({
          id: 'locked-circles-fill',
          type: 'fill',
          source: 'locked-circles-source',
          paint: {
            'fill-color': ['get', 'color'],
            'fill-opacity': 0.15,
          },
        })
      }

      if (!map.getLayer('locked-circles-line')) {
        map.addLayer({
          id: 'locked-circles-line',
          type: 'line',
          source: 'locked-circles-source',
          paint: {
            'line-color': ['get', 'color'],
            'line-width': 2.5,
            'line-opacity': 0.9,
          },
        })
      }

      if (!map.getLayer('locked-circles-labels')) {
        map.addLayer({
          id: 'locked-circles-labels',
          type: 'symbol',
          source: 'locked-circles-source',
          layout: {
            'text-field': ['get', 'label'],
            'text-size': 14,
            'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0, -2],
          },
          paint: {
            'text-color': ['get', 'color'],
            'text-halo-color': '#ffffff',
            'text-halo-width': 2,
          },
        })
      }
    }

    const updateViewport = () => {
      const bounds = map.getBounds()
      setMapViewport({
        west: bounds.getWest(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        north: bounds.getNorth(),
        zoom: map.getZoom(),
      })
    }

    const onWardClick = (event: MapMouseEvent & { features?: MapGeoJSONFeature[] }) => {
      if (isDrawInteractionMode(draw.getMode())) {
        return
      }
      const feature = event.features?.[0]
      const wardId = feature?.properties?.ward_id
      if (typeof wardId !== 'string') {
        return
      }

      if (
        draw
          .getAll()
          .features.some((item) => item.geometry.type === 'Polygon' || item.geometry.type === 'MultiPolygon')
      ) {
        draw.deleteAll()
        setDrawnGeometry(null)
        setDrawnFeatureId('')
        setDrawMode('simple_select')
        setActiveJobId('')
        setActiveAnalysisSource(null)
        setActiveAnalysisLabel('')
        setActiveAnalysisKey('')
      }

      setSelectedWardIds((current) => (current.includes(wardId) ? current.filter((item) => item !== wardId) : [...current, wardId]))
      setSelectedAoiCircleId('')
      setExportMessage('')
    }

    const onWardHover = (event: MapMouseEvent & { features?: MapGeoJSONFeature[] }) => {
      if (isDrawInteractionMode(draw.getMode())) {
        setHoverWardId('')
        return
      }
      const feature = event.features?.[0]
      const wardId = feature?.properties?.ward_id
      if (typeof wardId === 'string') {
        setHoverWardId(wardId)
      } else {
        setHoverWardId('')
      }
    }

    const onWardLeave = () => {
      setHoverWardId('')
    }

    const onDrawCreate = () => {
      syncDrawnState()
      draw.changeMode('simple_select')
      setDrawMode('simple_select')
      setExportMessage('')
    }

    const onDrawUpdate = () => {
      syncDrawnState()
      setExportMessage('')
    }

    const onDrawDelete = () => {
      syncDrawnState()
      setExportMessage('')
    }

    map.on('load', () => {
      ensureLayers()
      map.on('click', 'wards-fill', onWardClick)
      map.on('mousemove', 'wards-fill', onWardHover)
      map.on('mouseleave', 'wards-fill', onWardLeave)
      updateViewport()
    })

    map.on('moveend', updateViewport)
    map.on('draw.create', onDrawCreate)
    map.on('draw.update', onDrawUpdate)
    map.on('draw.delete', onDrawDelete)
    map.on('draw.modechange', (event: { mode?: string }) => {
      if (typeof event.mode === 'string' && event.mode) {
        setDrawMode(event.mode)
      }
    })

    mapRef.current = map
    drawRef.current = draw

    return () => {
      map.remove()
      mapRef.current = null
      drawRef.current = null
    }
  }, [syncDrawnState])

  useEffect(() => {
    const map = mapRef.current
    if (!map) {
      return
    }

    const source = map.getSource('wards-source') as maplibregl.GeoJSONSource | undefined
    if (!source) {
      return
    }

    source.setData(geojsonData)
    if (map.getLayer('wards-fill')) {
      map.setPaintProperty('wards-fill', 'fill-color', buildFillExpression(metricRange.min, metricRange.max) as never)
    }
  }, [geojsonData, metricRange])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getLayer('wards-hover')) {
      return
    }
    map.setFilter('wards-hover', ['==', ['get', 'ward_id'], hoverWardId || ''])
  }, [hoverWardId])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getLayer('wards-selected')) {
      return
    }
    map.setFilter('wards-selected', ['in', ['get', 'ward_id'], ['literal', selectedWardIds]])
  }, [selectedWardIds])

  useEffect(() => {
    const map = mapRef.current
    if (!map) {
      return
    }

    const visibility = showWardLayer ? 'visible' : 'none'
    const wardLayers = ['wards-fill', 'wards-outline', 'wards-hover', 'wards-selected']
    for (const layerId of wardLayers) {
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, 'visibility', visibility)
      }
    }

    if (!showWardLayer) {
      setHoverWardId('')
    }
  }, [showWardLayer])

  useEffect(() => {
    const map = mapRef.current
    if (!map) {
      return
    }

    for (const layerId of ALL_BASEMAP_LAYER_IDS) {
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, 'visibility', 'none')
      }
    }
    for (const layerId of basemapLayerIdsForPreset(basemapMode)) {
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, 'visibility', 'visible')
      }
    }

    if (map.getLayer('roads-overlay-line')) {
      if (isImageryBasemapPreset(basemapMode)) {
        map.setPaintProperty('roads-overlay-line', 'line-color', '#fef3c7')
        map.setPaintProperty('roads-overlay-line', 'line-opacity', 0.82)
      } else if (isDarkBasemapPreset(basemapMode)) {
        map.setPaintProperty('roads-overlay-line', 'line-color', '#fdba74')
        map.setPaintProperty('roads-overlay-line', 'line-opacity', 0.72)
      } else {
        map.setPaintProperty('roads-overlay-line', 'line-color', '#ea580c')
        map.setPaintProperty('roads-overlay-line', 'line-opacity', 0.56)
      }
    }

    if (map.getLayer('wards-outline')) {
      map.setPaintProperty('wards-outline', 'line-color', isDarkBasemapPreset(basemapMode) ? '#e2e8f0' : '#0f172a')
      map.setPaintProperty('wards-outline', 'line-opacity', isDarkBasemapPreset(basemapMode) ? 0.62 : 0.74)
    }
    if (map.getLayer('wards-selected')) {
      map.setPaintProperty('wards-selected', 'line-color', isDarkBasemapPreset(basemapMode) ? '#5eead4' : '#0f766e')
    }
  }, [basemapMode])

  useEffect(() => {
    const magnifyMap = magnifyMapRef.current
    if (!magnifyMap) {
      return
    }

    for (const layerId of ALL_BASEMAP_LAYER_IDS) {
      if (magnifyMap.getLayer(layerId)) {
        magnifyMap.setLayoutProperty(layerId, 'visibility', 'none')
      }
    }
    for (const layerId of basemapLayerIdsForPreset(basemapMode)) {
      if (magnifyMap.getLayer(layerId)) {
        magnifyMap.setLayoutProperty(layerId, 'visibility', 'visible')
      }
    }
  }, [basemapMode])

  useEffect(() => {
    const map = mapRef.current
    if (!map) {
      return
    }
    const source = map.getSource('roads-overlay-source') as maplibregl.GeoJSONSource | undefined
    if (!source) {
      return
    }
    source.setData(showRoadOverlay ? (roadLayerQuery.data as CityMapLayerGeoJSONResponse | undefined) ?? emptyFeatureCollection() : emptyFeatureCollection())
  }, [roadLayerQuery.data, showRoadOverlay])

  useEffect(() => {
    const map = mapRef.current
    if (!map) {
      return
    }
    const source = map.getSource('transit-overlay-source') as maplibregl.GeoJSONSource | undefined
    if (!source) {
      return
    }
    source.setData(showTransitOverlay ? (transitLayerQuery.data as CityMapLayerGeoJSONResponse | undefined) ?? emptyFeatureCollection() : emptyFeatureCollection())
  }, [showTransitOverlay, transitLayerQuery.data])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getLayer('wards-fill')) {
      return
    }

    const isLocked = isDrawInteractionMode(drawMode)
    map.setPaintProperty('wards-fill', 'fill-opacity', isLocked ? 0.22 : 0.34)
    map.getCanvas().style.cursor = isLocked ? 'crosshair' : ''
  }, [drawMode])

  // Circle tool hover interaction
  useEffect(() => {
    const map = mapRef.current
    if (!map || !circleToolActive) {
      // Clear hover circle when tool is inactive
      const hoverSource = map?.getSource('hover-circle-source') as maplibregl.GeoJSONSource | undefined
      if (hoverSource) {
        hoverSource.setData(emptyFeatureCollection())
      }
      setHoverCircleCenter(null)
      setCursorPosition(null)
      return
    }

    let lastUpdateTime = 0
    const THROTTLE_MS = 50

    const handleMouseMove = (event: MapMouseEvent) => {
      const now = Date.now()
      if (now - lastUpdateTime < THROTTLE_MS) {
        return
      }
      lastUpdateTime = now

      const { lng, lat } = event.lngLat
      setHoverCircleCenter([lng, lat])
      setCursorPosition({ x: event.point.x, y: event.point.y })

      // Update hover circle source
      const polygon = createCirclePolygon([lng, lat], circleRadius)
      const source = map.getSource('hover-circle-source') as maplibregl.GeoJSONSource
      if (source) {
        source.setData({
          type: 'Feature',
          properties: {},
          geometry: polygon,
        })
      }

      if (magnifyMapRef.current) {
        magnifyMapRef.current.setCenter([lng, lat])
        magnifyMapRef.current.setZoom(Math.min(map.getZoom() + 3, 19))
      }
    }

    const handleMouseLeave = () => {
      const source = map.getSource('hover-circle-source') as maplibregl.GeoJSONSource | undefined
      if (source) {
        source.setData(emptyFeatureCollection())
      }
      setHoverCircleCenter(null)
      setCursorPosition(null)
    }

    map.on('mousemove', handleMouseMove)
    map.on('mouseleave', handleMouseLeave)
    map.getCanvas().style.cursor = 'crosshair'

    return () => {
      map.off('mousemove', handleMouseMove)
      map.off('mouseleave', handleMouseLeave)
      if (!isDrawInteractionMode(drawMode)) {
        map.getCanvas().style.cursor = ''
      }
    }
  }, [circleToolActive, circleRadius, drawMode])

  // Circle tool click to lock
  useEffect(() => {
    const map = mapRef.current
    if (!map || !circleToolActive) {
      return
    }

    const handleMapClick = (event: MapMouseEvent) => {
      const { lng, lat } = event.lngLat
      pinCircleAt([lng, lat])
    }

    map.on('click', handleMapClick)

    return () => {
      map.off('click', handleMapClick)
    }
  }, [circleToolActive, pinCircleAt])

  useEffect(() => {
    if (!circleToolActive && magnifyMapRef.current) {
      magnifyMapRef.current.remove()
      magnifyMapRef.current = null
    }
  }, [circleToolActive])

  useEffect(() => {
    return () => {
      if (circleNoticeTimerRef.current) {
        clearTimeout(circleNoticeTimerRef.current)
      }
      if (magnifyMapRef.current) {
        magnifyMapRef.current.remove()
        magnifyMapRef.current = null
      }
    }
  }, [])

  useEffect(() => {
    if (!effectiveCity) {
      return
    }
    const map = mapRef.current
    if (!map || !wardsGeoQuery.data) {
      return
    }
    const bounds = computeBounds({ type: 'FeatureCollection', features: wardsGeoQuery.data.features })
    if (!bounds) {
      return
    }
    map.fitBounds(bounds, {
      padding: 40,
      duration: 700,
    })
  }, [effectiveCity, wardsGeoQuery.data])

  useEffect(() => {
    if (isSidebarOpen) {
      setShowMapGuide(false)
      setShowMapPreview(true)
    }
  }, [isSidebarOpen])

  const cityError = citiesQuery.error instanceof Error ? citiesQuery.error.message : ''
  const wardError = wardsQuery.error instanceof Error ? wardsQuery.error.message : ''
  const metricError = cityWardsMetricsQuery.error instanceof Error ? cityWardsMetricsQuery.error.message : ''
  const cityAverageError = cityMetricsQuery.error instanceof Error ? cityMetricsQuery.error.message : ''
  const noCitiesReturned = !citiesQuery.isLoading && !cityError && (citiesQuery.data?.cities?.length ?? 0) === 0
  const fullMetricError = wardDetailsQuery.error instanceof Error
    ? wardDetailsQuery.error.message
    : cityFullMetricsQuery.error instanceof Error
      ? cityFullMetricsQuery.error.message
      : ''
  const roadLayerError = roadLayerQuery.error instanceof Error ? roadLayerQuery.error.message : ''
  const transitLayerError = transitLayerQuery.error instanceof Error ? transitLayerQuery.error.message : ''

  return (
    <div className="urbanmor-shell h-screen w-screen text-slate-900">
      <div className="grid h-full grid-cols-12 gap-2 p-2">
        <aside className={`urbanmor-sidebar overflow-y-auto rounded-[28px] p-3 ${isSidebarOpen ? 'col-span-12 md:col-span-4 lg:col-span-3' : 'hidden'}`}>
          <div className="mb-2 flex justify-end">
            <button
              type="button"
              className="rounded-full border border-stone-300 bg-white px-3 py-1 text-[11px] font-semibold text-slate-700 transition hover:bg-stone-50"
              onClick={() => setIsSidebarOpen(false)}
            >
              Close Sidebar
            </button>
          </div>
          <div className="urbanmor-masthead rounded-[24px] p-3 text-white">
            <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-amber-100">UrbanMorph</p>
            <h1 className="mt-2 text-[2.15rem] leading-[1.02]">Area-first urban diagnostics</h1>
            <p className="mt-3 max-w-xs text-[13px] leading-5 text-slate-100/90">
              Pick a city, click wards to build an area of interest, or draw a polygon. Results update automatically.
            </p>
          </div>

          <section className="mt-3 rounded-[24px] border border-stone-200 bg-white/95 p-3 shadow-sm">
            <label className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">City</label>
            <select
              className="mt-2 w-full rounded-2xl border border-stone-300 bg-white px-3 py-3 text-sm outline-none transition focus:border-cyan-600 focus:ring-2 focus:ring-cyan-100"
              value={effectiveCity}
              disabled={!citiesQuery.data?.cities?.length}
              onChange={(event) => {
                setSelectedCity(event.target.value)
                clearAllAoi()
              }}
            >
              {citiesQuery.data?.cities.map((city) => (
                <option key={city.city} value={city.city}>
                  {city.city} ({city.cached_wards}/{city.expected_wards})
                </option>
              ))}
            </select>
            <p className="mt-2 text-[11px] text-slate-500">
              Start with one ward, add more wards, or switch to a fully custom polygon.
            </p>
            {noCitiesReturned ? (
              <p className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-[11px] text-amber-800">
                No cities were returned by <span className="font-semibold">{API_BASE}/cities</span>. Check that the backend is running and the database has
                boundary ward tables loaded.
              </p>
            ) : null}
          </section>

          <section className="mt-3 rounded-[24px] border border-stone-200 bg-white/95 p-3 shadow-sm">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Area Of Interest</p>
                <h2 className="mt-2 text-2xl text-slate-900">{currentAoiDescription.title}</h2>
                <p className="mt-1 text-sm text-slate-600">{currentAoiDescription.subtitle}</p>
              </div>
              <span className="rounded-full border border-stone-200 bg-stone-50 px-3 py-1 text-[11px] font-medium text-slate-600">
                {currentAoiDescription.areaLabel}
              </span>
            </div>

            <div className="mt-4 grid grid-cols-2 gap-2 text-[11px] text-slate-600">
              <div className="rounded-2xl border border-stone-200 bg-stone-50 px-3 py-2">
                <p className="font-semibold uppercase tracking-[0.16em] text-slate-500">Wards In AOI</p>
                <p className="mt-1 text-lg font-semibold text-slate-900">{selectedWardIds.length}</p>
              </div>
              <div className="rounded-2xl border border-stone-200 bg-stone-50 px-3 py-2">
                <p className="font-semibold uppercase tracking-[0.16em] text-slate-500">Mode</p>
                <p className="mt-1 text-lg font-semibold text-slate-900">{currentAoiSource.replace('_', ' ')}</p>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              <button type="button" className="rounded-full bg-slate-950 px-4 py-2 text-xs font-semibold text-white transition hover:bg-slate-800" onClick={activateDrawMode}>
                Draw AOI
              </button>
              <button
                type="button"
                className="rounded-full border border-stone-300 bg-white px-4 py-2 text-xs font-semibold text-slate-700 transition hover:bg-stone-50 disabled:cursor-not-allowed disabled:opacity-50"
                disabled={!drawnGeometry}
                onClick={activateEditMode}
              >
                Edit Shape
              </button>
              <button type="button" className="rounded-full border border-stone-300 bg-white px-4 py-2 text-xs font-semibold text-slate-700 transition hover:bg-stone-50" onClick={clearAllAoi}>
                Reset AOI
              </button>
            </div>

            <div className="mt-3 rounded-[18px] border border-amber-200 bg-gradient-to-r from-amber-50 to-orange-50 p-2.5">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-800">Lens Mode</p>
                  <p className="mt-1 text-[11px] leading-5 text-amber-900/80">
                    {circleToolActive
                      ? `Preview follows cursor. Click to pin up to ${MAX_COMPARE_CIRCLES} circles. Any pinned circle can become the active AOI.`
                      : drawGuidance}
                  </p>
                </div>
                <button
                  type="button"
                  className={`rounded-full border px-3 py-1 text-[10px] font-semibold transition ${
                    circleToolActive
                      ? 'border-amber-700 bg-amber-700 text-white hover:bg-amber-800'
                      : 'border-amber-300 bg-white text-amber-800 hover:bg-amber-100'
                  }`}
                  onClick={handleToggleCircleTool}
                >
                  {circleToolActive ? 'Lens On' : 'Lens Off'}
                </button>
              </div>

              <div className="mt-3 grid grid-cols-4 gap-1">
                {CIRCLE_RADIUS_OPTIONS.map((radius) => (
                  <button
                    key={radius}
                    type="button"
                    disabled={!circleToolActive}
                    onClick={() => setCircleRadius(radius)}
                    className={`rounded-xl px-2 py-1.5 text-[11px] font-semibold transition ${
                      circleRadius === radius
                        ? 'bg-slate-900 text-white shadow-sm'
                        : 'bg-white/85 text-slate-700 hover:bg-white'
                    } disabled:cursor-not-allowed disabled:opacity-45`}
                  >
                    {formatCircleRadius(radius)}
                  </button>
                ))}
              </div>

              <button
                type="button"
                className="mt-2 w-full rounded-xl border border-cyan-300 bg-cyan-50 px-3 py-1.5 text-[11px] font-semibold text-cyan-900 transition hover:bg-cyan-100 disabled:cursor-not-allowed disabled:opacity-45"
                disabled={!circleToolActive || !hoverCircleCenter}
                onClick={() => {
                  if (!hoverCircleCenter) {
                    showCircleNotice('Move the cursor over the map, then tap Set AOI from Lens.')
                    return
                  }
                  pinCircleAt(hoverCircleCenter)
                }}
              >
                Set AOI from Lens
              </button>

              <div className="mt-3 flex items-center justify-between gap-2 text-[10px] text-slate-600">
                <span className="rounded-full border border-amber-200 bg-white/70 px-2 py-1">
                  Radius: <span className="font-semibold text-slate-900">{formatCircleRadius(circleRadius)}</span>
                </span>
                <span className="rounded-full border border-amber-200 bg-white/70 px-2 py-1">
                  Pinned: <span className="font-semibold text-slate-900">{lockedCircles.length}/{MAX_COMPARE_CIRCLES}</span>
                </span>
              </div>
              <p className="mt-2 text-[10px] text-slate-600">
                AOI Circle: <span className="font-semibold text-slate-900">{selectedAoiCircle?.label ?? 'None'}</span> · Smaller circles usually analyse faster.
              </p>
            </div>

            {circleNotice ? (
              <p className="mt-3 rounded-2xl border border-cyan-100 bg-cyan-50 px-3 py-2 text-xs font-medium text-cyan-900">
                {circleNotice}
              </p>
            ) : null}

            {geometryValidation.errors.length > 0 ? (
              <div className="mt-3 rounded-2xl border border-rose-200 bg-rose-50 p-3 text-[11px] text-rose-700">
                {geometryValidation.errors.map((message) => (
                  <p key={message}>{message}</p>
                ))}
              </div>
            ) : null}

            {geometryValidation.warnings.length > 0 ? (
              <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 p-3 text-[11px] text-amber-800">
                {geometryValidation.warnings.map((message) => (
                  <p key={message}>{message}</p>
                ))}
              </div>
            ) : null}

            {submitAreaAnalysisMutation.isError && submitAreaAnalysisMutation.error instanceof Error ? (
              <div className="mt-3 rounded-2xl border border-rose-200 bg-rose-50 p-3 text-[11px] text-rose-700">
                {submitAreaAnalysisMutation.error.message}
              </div>
            ) : null}

            {activeJobId && currentAoiSource !== 'circle' ? (
              <div className="mt-4 rounded-[22px] border border-stone-200 bg-stone-50 p-3 text-sm text-slate-700">
                <div className="flex items-center justify-between gap-2">
                  <p className="font-semibold text-slate-900">Analysing: {activeAnalysisLabel || 'AOI'}</p>
                  <span className="rounded-full border border-stone-200 bg-white px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-slate-500">
                    {activeJob?.status ?? 'queued'}
                  </span>
                </div>
                <div className="mt-3 h-2 overflow-hidden rounded-full bg-stone-200">
                  <div className="h-full rounded-full bg-cyan-600 transition-all duration-500" style={{ width: `${activeJob?.progress_pct ?? 10}%` }} />
                </div>
                <p className="mt-2 text-[11px] text-slate-500">
                  {formatElapsed(activeJobElapsedSeconds)} · {activeJobProgressHint}
                </p>
                {activeJob?.error ? <p className="mt-2 text-[11px] text-rose-700">{activeJob.error}</p> : null}
              </div>
            ) : null}

            {currentAoiSource === 'circle' && selectedAoiCircle && selectedCircleMetrics?.isLoading ? (
              <div className="mt-4 rounded-[22px] border border-amber-200 bg-amber-50 p-3 text-[11px] text-amber-900">
                <p className="font-semibold">Analysing Circle {selectedAoiCircle.label} AOI…</p>
                <p className="mt-1 text-amber-800/90">
                  {selectedCircleMetrics.progressMessage || 'Running circle analysis.'}
                </p>
              </div>
            ) : null}

            {currentAoiSource === 'circle' && selectedCircleMetrics?.metrics?.error ? (
              <div className="mt-4 rounded-[22px] border border-rose-200 bg-rose-50 p-3 text-[11px] text-rose-700">
                {typeof selectedCircleMetrics.metrics.error === 'string' ? selectedCircleMetrics.metrics.error : 'Circle AOI analysis failed.'}
              </div>
            ) : null}
          </section>

          {lockedCircles.length > 0 && (
            <section className="mt-3 rounded-[24px] border border-amber-200/80 bg-gradient-to-b from-amber-50/80 to-white p-3 shadow-sm">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-amber-700">Circle Analysis</p>
                  <h2 className="mt-1 text-lg text-slate-900">{lockedCircles.length}-point comparison</h2>
                </div>
                <button
                  type="button"
                  className="rounded-full border border-amber-300 bg-white px-3 py-1 text-[11px] font-semibold text-amber-700 transition hover:bg-amber-100"
                  onClick={clearAllCircles}
                >
                  Clear All
                </button>
              </div>

              <div className="mt-4 space-y-3">
                {lockedCircles.map((circle) => {
                  const metrics = circleMetrics.get(circle.id)

                  return (
                    <article
                      key={circle.id}
                      className="rounded-[18px] border bg-white/95 p-3 shadow-[0_8px_24px_rgba(15,23,42,0.08)] transition hover:shadow-[0_12px_28px_rgba(15,23,42,0.12)]"
                      style={{ borderColor: circle.color }}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2">
                            <span
                              className="flex h-6 w-6 items-center justify-center rounded-full text-xs font-bold text-white"
                              style={{ backgroundColor: circle.color }}
                            >
                              {circle.label}
                            </span>
                            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                              Location {circle.label}
                            </p>
                          </div>
                          <p className="mt-2 truncate text-[11px] text-slate-600" title={circle.location || `${circle.center[1].toFixed(4)}, ${circle.center[0].toFixed(4)}`}>
                            {circle.location || `${circle.center[1].toFixed(4)}, ${circle.center[0].toFixed(4)}`}
                          </p>
                          <p className="mt-1 text-[11px] text-slate-500">
                            Radius: {formatCircleRadius(circle.radius)} · Area: {((Math.PI * circle.radius * circle.radius) / 1_000_000).toFixed(3)} km²
                          </p>
                        </div>
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            className={`rounded-full border px-2.5 py-1 text-[10px] font-semibold transition ${
                              selectedAoiCircleId === circle.id
                                ? 'border-cyan-300 bg-cyan-100 text-cyan-900'
                                : 'border-stone-300 bg-white text-slate-700 hover:bg-stone-50'
                            }`}
                            onClick={() => setSelectedAoiCircleId(circle.id)}
                          >
                            {selectedAoiCircleId === circle.id ? 'AOI' : 'Use as AOI'}
                          </button>
                          <button
                            type="button"
                            className="rounded-full border border-stone-200 px-2 text-slate-400 transition hover:border-rose-300 hover:text-rose-600"
                            onClick={() => removeCircle(circle.id)}
                          >
                            <span className="text-xl">×</span>
                          </button>
                        </div>
                      </div>

                      {metrics?.isLoading ? (
                        <div className="mt-3 rounded-2xl border border-stone-200 bg-stone-50 p-3 text-xs text-slate-500">
                          <div className="flex items-center justify-between gap-2">
                            <span className="rounded-full border border-stone-300 bg-white px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-600">
                              {metrics.status === 'queued' ? 'Queued' : 'Running'}
                            </span>
                            <span className="font-semibold text-slate-700">{Math.max(0, Math.round(metrics.progressPct ?? 0))}%</span>
                          </div>
                          <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-stone-200">
                            <div
                              className="h-full rounded-full bg-amber-500 transition-all duration-500"
                              style={{ width: `${Math.min(100, Math.max(8, metrics.progressPct ?? 8))}%` }}
                            />
                          </div>
                          <p className="mt-2 text-[11px] text-slate-600">
                            {metrics.progressMessage || (metrics.status === 'queued' ? 'Queued for comparison analysis.' : 'Computing circle metrics...')}
                          </p>
                        </div>
                      ) : metrics?.metrics?.error ? (
                        <div className="mt-3 rounded-2xl border border-rose-200 bg-rose-50 p-3 text-xs text-rose-700">
                          {typeof metrics.metrics.error === 'string' ? metrics.metrics.error : 'Analysis failed. Please try again.'}
                        </div>
                      ) : metrics?.metrics && Object.keys(metrics.metrics).length > 0 ? (
                        <div className="mt-3 space-y-2">
                          {buildMetricPanelRows(metrics.metrics, metaMetricsQuery.data?.metrics ?? []).slice(0, 8).map((row) => (
                            <div key={row.metric_id} className="flex min-w-0 items-center justify-between gap-2 rounded-2xl border border-stone-200 bg-stone-50 px-3 py-2">
                              <span className="min-w-0 flex-1 truncate text-[11px] text-slate-600" title={row.label}>{row.label}</span>
                              <span className="max-w-[58%] truncate text-[11px] font-semibold text-slate-900 text-right" title={`${row.valueDisplay}${row.unit ? ` ${row.unit}` : ''}`}>
                                {row.valueDisplay}
                                {row.unit && <span className="ml-1 font-normal text-slate-500">{row.unit}</span>}
                              </span>
                            </div>
                          ))}
                        </div>
                      ) : metrics ? (
                        <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 p-3 text-xs text-amber-800">
                          Circle result returned without metric rows.
                        </div>
                      ) : null}
                    </article>
                  )
                })}
              </div>
            </section>
          )}

          <section className="mt-3 rounded-[24px] border border-stone-200 bg-white/95 p-3 shadow-sm">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Preview Metrics</p>
                <h2 className="mt-1 text-xl text-slate-900">CNR · Block Size · Building Footprint · Open Spaces</h2>
              </div>
              <span className="rounded-full border border-stone-200 bg-stone-50 px-3 py-1 text-[11px] text-slate-500">
                {currentAoiSource === 'city' ? 'City baseline' : 'AOI result'}
              </span>
            </div>
            <div className="mt-4 space-y-3">
              {highlightCards.map((card) => (
                <article key={card.metricId} className="rounded-[22px] border border-stone-200 bg-stone-50 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{card.theme}</p>
                      <h3 className="mt-1 text-base font-semibold text-slate-900">{card.label}</h3>
                    </div>
                    <span className="rounded-full border border-stone-200 bg-white px-3 py-1 text-[11px] font-medium text-slate-600">
                      {card.comparison}
                    </span>
                  </div>
                  <p className="mt-3 text-3xl font-semibold text-slate-950">
                    {card.value === null ? 'N/A' : formatMetricNumber(card.value)}
                    <span className="ml-2 text-xs font-medium uppercase tracking-[0.14em] text-slate-500">{card.unit}</span>
                  </p>
                  <p className="mt-3 text-sm leading-6 text-slate-600">{card.narrative}</p>
                </article>
              ))}
            </div>
          </section>

          <details className="mt-3 rounded-[24px] border border-stone-200 bg-white/95 p-3 shadow-sm">
            <summary className="cursor-pointer list-none text-sm font-semibold text-slate-900">
              Drill Down: full metrics and downloads
            </summary>
            <p className="mt-2 text-[11px] text-slate-500">
              Keep the first screen lightweight. City-wide full metrics are available here by default; AOI selections replace them.
            </p>

            {!activePanelData ? (
              currentAoiSource === 'circle' && selectedAoiCircle ? (
                <p className="mt-4 rounded-2xl border border-stone-200 bg-stone-50 p-3 text-sm text-slate-600">
                  {selectedCircleMetrics?.isLoading
                    ? `Circle ${selectedAoiCircle.label} is being analysed. Full metric export will unlock when it completes.`
                    : selectedCircleMetrics?.metrics?.error
                      ? 'Circle AOI analysis failed. Pin another circle or retry.'
                      : `Select or pin a circle to load full AOI metrics for Circle ${selectedAoiCircle.label}.`}
                </p>
              ) : (
                <p className="mt-4 rounded-2xl border border-stone-200 bg-stone-50 p-3 text-sm text-slate-600">
                  Loading full city metrics. You can still select wards, use a lens circle, or draw a polygon for a custom AOI.
                </p>
              )
            ) : (
              <>
                <div className="mt-4 rounded-[22px] border border-stone-200 bg-stone-50 p-3 text-sm text-slate-700">
                  <p className="font-semibold text-slate-900">{activePanelData.title}</p>
                  <p className="mt-1 text-slate-600">{activePanelData.subtitle}</p>
                  <p className="mt-2 text-[11px] text-slate-500">{metricPanelRows.length} metrics available</p>
                </div>

                <div className="mt-4 flex flex-wrap gap-2">
                  <button type="button" className="rounded-full bg-slate-950 px-4 py-2 text-xs font-semibold text-white transition hover:bg-slate-800" onClick={handleExportJson}>
                    Download JSON
                  </button>
                  <button type="button" className="rounded-full border border-stone-300 bg-white px-4 py-2 text-xs font-semibold text-slate-700 transition hover:bg-stone-50" onClick={handleExportCsv}>
                    Download CSV
                  </button>
                </div>
                {exportMessage ? <p className="mt-2 text-[11px] text-slate-500">{exportMessage}</p> : null}

                <div className="mt-4 max-h-[28rem] space-y-3 overflow-y-auto rounded-[22px] border border-stone-200 bg-stone-50 p-3">
                  {metricPanelGroups.map((group) => (
                    <div key={group.key} className="space-y-2">
                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{group.label}</p>
                      {group.metrics.map((metric) => (
                        <div key={metric.metric_id} className="rounded-2xl border border-stone-200 bg-white p-3">
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <p className="font-semibold text-slate-900">{metric.label}</p>
                              <p className="text-[11px] text-slate-500">{metric.metric_id}</p>
                            </div>
                            <p className="text-right text-sm font-semibold text-slate-900">
                              {metric.valueDisplay}
                              <span className="ml-1 text-[11px] font-normal text-slate-500">{metric.unit}</span>
                            </p>
                          </div>
                          <p className="mt-2 text-[12px] leading-5 text-slate-600">{metric.explanation}</p>
                          <div className="mt-2 flex flex-wrap gap-1">
                            <span className={`rounded-full border px-2 py-1 text-[10px] ${statusBadgeClass(metric.status)}`}>{metric.status}</span>
                            <span className={`rounded-full border px-2 py-1 text-[10px] ${qualityBadgeClass(metric.qualityFlag)}`}>{metric.qualityFlag}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ))}
                </div>
              </>
            )}
          </details>

          {cityError || wardError || metricError || cityAverageError || fullMetricError || roadLayerError || transitLayerError ? (
            <section className="mt-3 rounded-2xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
              {cityError ? <p>{cityError}</p> : null}
              {wardError ? <p>{wardError}</p> : null}
              {metricError ? <p>{metricError}</p> : null}
              {cityAverageError ? <p>{cityAverageError}</p> : null}
              {fullMetricError ? <p>{fullMetricError}</p> : null}
              {roadLayerError ? <p>{roadLayerError}</p> : null}
              {transitLayerError ? <p>{transitLayerError}</p> : null}
            </section>
          ) : null}
        </aside>

        <main className={`urbanmor-mapframe overflow-hidden rounded-[30px] ${isSidebarOpen ? 'col-span-12 md:col-span-8 lg:col-span-9' : 'col-span-12'}`}>
          <div className="relative h-full w-full">
            <div ref={mapContainerRef} className="h-full w-full" />

            {circleToolActive ? (
              <div
                className="absolute pointer-events-none transition-opacity duration-100"
                style={{
                  left: `${(cursorPosition?.x ?? -9999) + 18}px`,
                  top: `${(cursorPosition?.y ?? -9999) - 18}px`,
                  transform: 'translate(0, -100%)',
                  opacity: cursorPosition && hoverCircleCenter ? 1 : 0,
                  zIndex: 1000,
                }}
              >
                <div className="um-lens-shell">
                  <div
                    ref={(el) => {
                      if (!el || magnifyMapRef.current) {
                        return
                      }

                      const initialCenter =
                        hoverCircleCenter ??
                        ((mapRef.current?.getCenter().toArray() as [number, number] | undefined) ?? [78.9629, 20.5937])

                      const magnifyMap = new maplibregl.Map({
                        container: el,
                        style: createBasemapStyle(),
                        center: initialCenter,
                        zoom: Math.min((mapRef.current?.getZoom() ?? 10) + 3, 19),
                        interactive: false,
                        attributionControl: false,
                      })

                      magnifyMap.on('load', () => {
                        for (const layerId of ALL_BASEMAP_LAYER_IDS) {
                          if (magnifyMap.getLayer(layerId)) {
                            magnifyMap.setLayoutProperty(layerId, 'visibility', 'none')
                          }
                        }
                        for (const layerId of basemapLayerIdsForPreset(basemapMode)) {
                          if (magnifyMap.getLayer(layerId)) {
                            magnifyMap.setLayoutProperty(layerId, 'visibility', 'visible')
                          }
                        }
                      })

                      magnifyMapRef.current = magnifyMap
                    }}
                    className="h-full w-full"
                  />
                  <div className="um-lens-gloss" />
                  <div className="um-lens-crosshair">
                    <div className="um-lens-crosshair-h" />
                    <div className="um-lens-crosshair-v" />
                  </div>
                </div>
              </div>
            ) : null}

            <div className="absolute left-4 top-4 z-20 flex items-center gap-2">
              <button
                type="button"
                className="rounded-full border border-white/70 bg-white/95 px-4 py-2 text-xs font-semibold text-slate-800 shadow-lg transition hover:bg-white"
                onClick={() => setIsSidebarOpen((current) => !current)}
              >
                {isSidebarOpen ? 'Hide sidebar' : 'Show sidebar'}
              </button>
              {!isSidebarOpen ? (
                <button
                  type="button"
                  className={`rounded-full border px-4 py-2 text-xs font-semibold shadow-lg transition ${
                    showMapGuide
                      ? 'border-cyan-300 bg-cyan-100 text-cyan-900 hover:bg-cyan-200'
                      : 'border-white/70 bg-white/95 text-slate-700 hover:bg-white'
                  }`}
                  onClick={() => setShowMapGuide((current) => !current)}
                >
                  {showMapGuide ? 'Hide guide' : 'Guide'}
                </button>
              ) : null}
            </div>

            {showMapGuide ? (
              <div className="absolute left-4 top-16 z-10 max-w-sm rounded-[22px] border border-white/70 bg-white/92 p-3 shadow-lg backdrop-blur pointer-events-none">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">How to use</p>
                <p className="mt-2 text-[13px] leading-5 text-slate-700">
                  Click wards to add or remove them from the AOI. Draw a polygon when the area does not align to administrative boundaries.
                  Analysis starts as soon as the shape is finished.
                </p>
              </div>
            ) : null}

            <div className="absolute right-4 top-4 z-10 w-[17rem] rounded-[24px] border border-white/75 bg-white/86 p-3 shadow-lg backdrop-blur-md">
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Map view</p>
              <p className="mt-2 text-[10px] text-slate-500">Curated styles optimized for AOI and ward overlays</p>
              <div className="mt-3 grid grid-cols-2 gap-2">
                {BASEMAP_PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className={`rounded-2xl border px-2.5 py-2 text-left transition ${
                      basemapMode === preset.id
                        ? 'border-cyan-300 bg-cyan-50 text-cyan-900'
                        : 'border-stone-300 bg-white text-slate-700 hover:bg-stone-50'
                    }`}
                    onClick={() => setBasemapMode(preset.id)}
                  >
                    <p className="text-[11px] font-semibold leading-4">{preset.label}</p>
                    <p className="mt-0.5 text-[10px] text-slate-500">{preset.hint}</p>
                  </button>
                ))}
              </div>

              <p className="mt-4 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Layer control</p>
              <div className="mt-2 grid grid-cols-3 gap-2">
                <button
                  type="button"
                  className={`rounded-2xl border px-3 py-2 text-xs font-semibold transition ${showWardLayer ? 'border-teal-300 bg-teal-50 text-teal-900' : 'border-stone-300 bg-white text-slate-600'}`}
                  onClick={() => setShowWardLayer((current) => !current)}
                >
                  {showWardLayer ? 'Wards on' : 'Wards off'}
                </button>
                <button
                  type="button"
                  className={`rounded-2xl border px-3 py-2 text-xs font-semibold transition ${showRoadOverlay ? 'border-amber-300 bg-amber-50 text-amber-900' : 'border-stone-300 bg-white text-slate-600'}`}
                  onClick={() => setShowRoadOverlay((current) => !current)}
                >
                  {showRoadOverlay ? 'Roads on' : 'Roads off'}
                </button>
                <button
                  type="button"
                  className={`rounded-2xl border px-3 py-2 text-xs font-semibold transition ${showTransitOverlay ? 'border-cyan-300 bg-cyan-50 text-cyan-900' : 'border-stone-300 bg-white text-slate-600'}`}
                  onClick={() => setShowTransitOverlay((current) => !current)}
                >
                  {showTransitOverlay ? 'Transit on' : 'Transit off'}
                </button>
              </div>
              <button
                type="button"
                className={`mt-2 w-full rounded-2xl border px-3 py-2 text-xs font-semibold transition ${
                  circleToolActive
                    ? 'border-amber-400 bg-amber-100 text-amber-900'
                    : 'border-stone-300 bg-white text-slate-700 hover:bg-stone-50'
                }`}
                onClick={handleToggleCircleTool}
              >
                {circleToolActive ? 'Lens cursor on' : 'Lens cursor off'}
              </button>

              <label className="mt-4 block text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Ward shading</label>
              <select
                className={`mt-2 w-full rounded-2xl border border-stone-300 px-3 py-3 text-sm outline-none transition focus:border-cyan-600 focus:ring-2 focus:ring-cyan-100 ${
                  showWardLayer ? 'bg-white' : 'cursor-not-allowed bg-stone-100 text-slate-500'
                }`}
                value={effectiveMetricId}
                disabled={!showWardLayer}
                onChange={(event) => setActiveMetricId(event.target.value)}
              >
                {mapMetricOptions.map((metric: MetricMetaItem) => (
                  <option key={metric.metric_id} value={metric.metric_id}>
                    {metric.label}
                  </option>
                ))}
              </select>
              <p className="mt-2 text-[11px] text-slate-500">
                {mapMetricOptions.find((metric) => metric.metric_id === effectiveMetricId)?.formula_summary ?? 'Selected ward metric for map shading.'}
              </p>
              <div
                className="mt-3 h-2 rounded-full"
                style={{
                  background: `linear-gradient(90deg, ${CHOROPLETH_COLOR_STOPS.low} 0%, ${CHOROPLETH_COLOR_STOPS.mid} 35%, ${CHOROPLETH_COLOR_STOPS.high} 70%, ${CHOROPLETH_COLOR_STOPS.max} 100%)`,
                }}
              />
              <div className="mt-2 flex items-center justify-between text-[10px] text-slate-500">
                <span>Lower</span>
                <span>
                  {metricRange.min === null ? 'n/a' : formatMetricNumber(metricRange.min)} to {metricRange.max === null ? 'n/a' : formatMetricNumber(metricRange.max)}
                </span>
                <span>Higher</span>
              </div>

              {!isSidebarOpen ? (
                <div className="mt-4 rounded-[18px] border border-stone-200 bg-white/90 p-2.5">
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Preview metrics</p>
                    <button
                      type="button"
                      className="rounded-full border border-stone-300 bg-white px-2 py-0.5 text-[10px] font-semibold text-slate-600 transition hover:bg-stone-50"
                      onClick={() => setShowMapPreview((current) => !current)}
                    >
                      {showMapPreview ? 'Hide' : 'Show'}
                    </button>
                  </div>
                  {showMapPreview ? (
                    <div className="mt-2 space-y-1.5">
                      {highlightCards.map((card) => (
                        <div key={`map-preview-${card.metricId}`} className="flex items-center justify-between gap-2 text-[11px]">
                          <span className="truncate text-slate-600" title={card.label}>{card.label}</span>
                          <span className="shrink-0 font-semibold text-slate-900">
                            {card.value === null ? 'N/A' : formatMetricNumber(card.value)}
                            <span className="ml-1 text-[10px] font-normal text-slate-500">{card.unit}</span>
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>

            <div className="pointer-events-none absolute bottom-4 left-4 z-10 rounded-[20px] border border-white/70 bg-white/90 px-4 py-2.5 text-[13px] text-slate-700 shadow-lg backdrop-blur">
              {isDrawInteractionMode(drawMode)
                ? 'Drawing/editing is active. Ward clicks are paused until the edit finishes.'
                : isSidebarOpen
                  ? 'Click wards to build the AOI. The sidebar stays focused on one area at a time.'
                  : 'Click wards to build the AOI. Preview metrics stay visible here; open the sidebar for full drill-down and exports.'}
            </div>
          </div>
        </main>
      </div>
    </div>
  )
}

export default App
