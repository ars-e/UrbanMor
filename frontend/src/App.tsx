import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import MapboxDraw from '@mapbox/mapbox-gl-draw'
import { useMutation, useQuery } from '@tanstack/react-query'
import maplibregl, { MapMouseEvent } from 'maplibre-gl'
import type { FeatureCollection, Geometry, MultiPolygon, Polygon } from 'geojson'
import type { MapGeoJSONFeature } from 'maplibre-gl'

import {
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
import { areaSqKm, buildSelectedWardAoiGeometry, createBasemapStyle, formatAreaSqKm, serializeViewportBbox, type MapViewport } from './lib/map'
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
  CityMapLayerGeoJSONResponse,
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
const RETIRED_METRIC_IDS = new Set(['bldg.growth_rate', 'topo.flood_risk_proxy'])
const CHOROPLETH_COLOR_STOPS = {
  low: '#f5f1e6',
  mid: '#f59e0b',
  high: '#c2410c',
  max: '#7c2d12',
}

type PanelSourceType = 'city' | 'ward' | 'selected_wards' | 'drawn_polygon'
type AutoAnalysisSource = 'selected_wards' | 'drawn_polygon' | null
type BasemapMode = 'street' | 'satellite'

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

function describeAoi(source: PanelSourceType, city: string, selectedFeatures: WardGeometryFeature[], drawnArea: number | null): { title: string; subtitle: string; areaLabel: string } {
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
  const [basemapMode, setBasemapMode] = useState<BasemapMode>('street')
  const [showRoadOverlay, setShowRoadOverlay] = useState<boolean>(true)
  const [showTransitOverlay, setShowTransitOverlay] = useState<boolean>(true)
  const [mapViewport, setMapViewport] = useState<MapViewport | null>(null)

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
    draw.changeMode('draw_polygon')
    setDrawMode('draw_polygon')
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

  const citiesQuery = useQuery({
    queryKey: ['cities'],
    queryFn: getCities,
  })
  const effectiveCity = selectedCity || citiesQuery.data?.cities?.[0]?.city || ''

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

  const currentAoiSource: PanelSourceType = drawnGeometry
    ? 'drawn_polygon'
    : selectedWardIds.length > 1
      ? 'selected_wards'
      : effectiveSelectedWardId
        ? 'ward'
        : 'city'

  const currentAoiDescription = useMemo(
    () => describeAoi(currentAoiSource, effectiveCity, selectedWardFeatures as WardGeometryFeature[], geometryValidation.areaSqM ? geometryValidation.areaSqM / 1_000_000 : null),
    [currentAoiSource, effectiveCity, geometryValidation.areaSqM, selectedWardFeatures],
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
        return 1500
      }
      if (current.status === 'running') {
        const startedIso = current.started_at ?? current.created_at
        const startedMs = startedIso ? new Date(startedIso).getTime() : Number.NaN
        if (Number.isFinite(startedMs)) {
          const elapsedMs = Date.now() - startedMs
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
  }, [activeAnalysisSource, activeCustomPanelData, cityPanelData, currentAoiSource, wardPanelData])

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
    return 'this area of interest'
  }, [currentAoiSource, effectiveCity])

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
    if (selectedWardIds.length > 1) {
      return 'Multiple wards are combined and analysed as one AOI automatically.'
    }
    if (selectedWardIds.length === 1) {
      return 'Single ward selected. Click more wards to grow the AOI or draw a custom polygon.'
    }
    return 'Click wards to build an AOI or draw a custom polygon.'
  }, [drawMode, drawnGeometry, geometryValidation.errors, selectedWardIds.length])

  const handleExportJson = useCallback(() => {
    if (!activePanelData || metricPanelRows.length === 0) {
      setExportMessage('No AOI metric payload is ready yet.')
      return
    }

    const exportSourceType =
      activePanelData.sourceType === 'selected_wards' || activePanelData.sourceType === 'drawn_polygon'
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
      activePanelData.sourceType === 'selected_wards' || activePanelData.sourceType === 'drawn_polygon'
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
            'line-opacity': 0.45,
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
            'fill-opacity': 0.22,
          },
        })
      }

      if (!map.getLayer('wards-outline')) {
        map.addLayer({
          id: 'wards-outline',
          type: 'line',
          source: 'wards-source',
          paint: {
            'line-color': '#334155',
            'line-width': 0.9,
            'line-opacity': 0.45,
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

    if (map.getLayer('basemap-street')) {
      map.setLayoutProperty('basemap-street', 'visibility', basemapMode === 'street' ? 'visible' : 'none')
    }
    if (map.getLayer('basemap-satellite')) {
      map.setLayoutProperty('basemap-satellite', 'visibility', basemapMode === 'satellite' ? 'visible' : 'none')
    }
    if (map.getLayer('roads-overlay-line')) {
      map.setPaintProperty('roads-overlay-line', 'line-color', basemapMode === 'satellite' ? '#fef3c7' : '#ea580c')
      map.setPaintProperty('roads-overlay-line', 'line-opacity', basemapMode === 'satellite' ? 0.72 : 0.42)
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
    map.setPaintProperty('wards-fill', 'fill-opacity', isLocked ? 0.12 : 0.22)
    map.getCanvas().style.cursor = isLocked ? 'crosshair' : ''
  }, [drawMode])

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

  const cityError = citiesQuery.error instanceof Error ? citiesQuery.error.message : ''
  const wardError = wardsQuery.error instanceof Error ? wardsQuery.error.message : ''
  const metricError = cityWardsMetricsQuery.error instanceof Error ? cityWardsMetricsQuery.error.message : ''
  const cityAverageError = cityMetricsQuery.error instanceof Error ? cityMetricsQuery.error.message : ''
  const fullMetricError = wardDetailsQuery.error instanceof Error
    ? wardDetailsQuery.error.message
    : cityFullMetricsQuery.error instanceof Error
      ? cityFullMetricsQuery.error.message
      : ''
  const roadLayerError = roadLayerQuery.error instanceof Error ? roadLayerQuery.error.message : ''
  const transitLayerError = transitLayerQuery.error instanceof Error ? transitLayerQuery.error.message : ''

  return (
    <div className="urbanmor-shell h-screen w-screen text-slate-900">
      <div className="grid h-full grid-cols-12 gap-3 p-3">
        <aside className="urbanmor-sidebar col-span-12 overflow-y-auto rounded-[28px] p-4 md:col-span-4 lg:col-span-3">
          <div className="urbanmor-masthead rounded-[24px] p-4 text-white">
            <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-amber-100">UrbanMorph</p>
            <h1 className="mt-2 text-4xl leading-none">Area-first urban diagnostics</h1>
            <p className="mt-3 max-w-xs text-sm text-slate-100/90">
              Pick a city, click wards to build an area of interest, or draw a polygon. Results update automatically.
            </p>
          </div>

          <section className="mt-4 rounded-[24px] border border-stone-200 bg-white/95 p-4 shadow-sm">
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
          </section>

          <section className="mt-4 rounded-[24px] border border-stone-200 bg-white/95 p-4 shadow-sm">
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

            <p className="mt-3 rounded-2xl border border-cyan-100 bg-cyan-50 px-3 py-2 text-sm text-cyan-900">{drawGuidance}</p>

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

            {activeJobId ? (
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
          </section>

          <section className="mt-4 rounded-[24px] border border-stone-200 bg-white/95 p-4 shadow-sm">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Key Insights</p>
                <h2 className="mt-1 text-xl text-slate-900">Four non-overlapping reads</h2>
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

          <details className="mt-4 rounded-[24px] border border-stone-200 bg-white/95 p-4 shadow-sm">
            <summary className="cursor-pointer list-none text-sm font-semibold text-slate-900">
              Drill Down: full metrics and downloads
            </summary>
            <p className="mt-2 text-[11px] text-slate-500">
              Keep the first screen lightweight. City-wide full metrics are available here by default; AOI selections replace them.
            </p>

            {!activePanelData ? (
              <p className="mt-4 rounded-2xl border border-stone-200 bg-stone-50 p-3 text-sm text-slate-600">
                Loading full city metrics. You can still select wards or draw a polygon for a custom AOI.
              </p>
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
            <section className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
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

        <main className="urbanmor-mapframe col-span-12 overflow-hidden rounded-[30px] md:col-span-8 lg:col-span-9">
          <div className="relative h-full w-full">
            <div ref={mapContainerRef} className="h-full w-full" />

            <div className="absolute left-4 top-4 z-10 max-w-sm rounded-[22px] border border-white/70 bg-white/92 p-4 shadow-lg backdrop-blur pointer-events-none">
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">How to use</p>
              <p className="mt-2 text-sm leading-6 text-slate-700">
                Click wards to add or remove them from the AOI. Draw a polygon when the area does not align to administrative boundaries.
                Analysis starts as soon as the shape is finished.
              </p>
            </div>

            <div className="absolute right-4 top-4 z-10 w-[18rem] rounded-[24px] border border-white/70 bg-white/92 p-4 shadow-lg backdrop-blur">
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Map view</p>
              <div className="mt-3 flex rounded-full bg-stone-100 p-1 text-xs font-semibold text-slate-600">
                <button
                  type="button"
                  className={`flex-1 rounded-full px-3 py-2 transition ${basemapMode === 'street' ? 'bg-white text-slate-950 shadow-sm' : ''}`}
                  onClick={() => setBasemapMode('street')}
                >
                  Street
                </button>
                <button
                  type="button"
                  className={`flex-1 rounded-full px-3 py-2 transition ${basemapMode === 'satellite' ? 'bg-white text-slate-950 shadow-sm' : ''}`}
                  onClick={() => setBasemapMode('satellite')}
                >
                  Satellite
                </button>
              </div>

              <div className="mt-4 grid grid-cols-2 gap-2">
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

              <label className="mt-4 block text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Ward shading</label>
              <select
                className="mt-2 w-full rounded-2xl border border-stone-300 bg-white px-3 py-3 text-sm outline-none transition focus:border-cyan-600 focus:ring-2 focus:ring-cyan-100"
                value={effectiveMetricId}
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
                  {metricRange.min === null ? 'n/a' : metricRange.min.toFixed(2)} to {metricRange.max === null ? 'n/a' : metricRange.max.toFixed(2)}
                </span>
                <span>Higher</span>
              </div>
            </div>

            <div className="pointer-events-none absolute bottom-4 left-4 z-10 rounded-[20px] border border-white/70 bg-white/90 px-4 py-3 text-sm text-slate-700 shadow-lg backdrop-blur">
              {isDrawInteractionMode(drawMode)
                ? 'Drawing/editing is active. Ward clicks are paused until the edit finishes.'
                : 'Click wards to build the AOI. The panel on the left stays focused on one area at a time.'}
            </div>
          </div>
        </main>
      </div>
    </div>
  )
}

export default App
