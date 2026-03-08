import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import MapboxDraw from '@mapbox/mapbox-gl-draw'
import { useMutation, useQuery } from '@tanstack/react-query'
import maplibregl, { MapMouseEvent } from 'maplibre-gl'
import type { Geometry, MultiPolygon, Polygon } from 'geojson'
import type { MapGeoJSONFeature } from 'maplibre-gl'

import { analyse, getAnalyseJob, getCities, getCityMetrics, getCityWards, getCityWardsGeoJSON, getMetaMetrics, getWardMetrics } from './lib/api'
import type { AnalyseJobResponse, AnalyseResponse, WardMetricResponse, WardsGeoJSON } from './types'
import { validateAndNormalizeGeometry } from './lib/geometry'
import {
  buildDeltaRows,
  buildMetricPanelRows,
  buildSideBySideRows,
  downloadTextFile,
  formatMetricNumber,
  groupMetricPanelRows,
  makeMetricsExportCsv,
  makeMetricsExportJson,
  numericMetricMapFromRows,
} from './lib/metrics'

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

const BASEMAP_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json'

function isJobResponse(value: AnalyseResponse | AnalyseJobResponse): value is AnalyseJobResponse {
  return 'job_id' in value
}

function toNumberOrNull(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value
  }
  return null
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
    return '#cbd5e1'
  }

  if (Math.abs(max - min) < 1e-12) {
    return '#2563eb'
  }

  const mid = min + (max - min) / 2
  return [
    'interpolate',
    ['linear'],
    ['coalesce', ['to-number', ['get', 'metric_value']], min],
    min,
    '#dbeafe',
    mid,
    '#3b82f6',
    max,
    '#1e3a8a',
  ]
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
  if (status === 'implemented_v1') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  }
  if (status.startsWith('planned_')) {
    return 'border-indigo-200 bg-indigo-50 text-indigo-700'
  }
  if (status === 'blocked_data' || status === 'proxy_only') {
    return 'border-amber-200 bg-amber-50 text-amber-800'
  }
  return 'border-slate-200 bg-slate-100 text-slate-600'
}

type PanelSourceType = 'ward' | 'custom_polygon'

interface PanelSnapshot {
  sourceType: PanelSourceType
  sourceId: string
  metrics: Record<string, unknown>
  qualitySummary: Record<string, unknown>
}

interface CompareSnapshot extends PanelSnapshot {
  title: string
}

function App() {
  const mapContainerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const drawRef = useRef<MapboxDraw | null>(null)

  const [selectedCity, setSelectedCity] = useState<string>('')
  const [selectedWardIds, setSelectedWardIds] = useState<string[]>([])
  const [selectedWardId, setSelectedWardId] = useState<string>('')
  const [hoverWardId, setHoverWardId] = useState<string>('')
  const [activeMetricId, setActiveMetricId] = useState<string>('')
  const [drawnGeometry, setDrawnGeometry] = useState<Polygon | MultiPolygon | null>(null)
  const [drawnFeatureId, setDrawnFeatureId] = useState<string>('')
  const [drawMode, setDrawMode] = useState<string>('simple_select')
  const [activeJobId, setActiveJobId] = useState<string>('')
  const [activeMetricPanelSource, setActiveMetricPanelSource] = useState<PanelSourceType>('ward')
  const [referenceMode, setReferenceMode] = useState<'city_average' | 'reference_ward'>('city_average')
  const [referenceWardId, setReferenceWardId] = useState<string>('')
  const [compareLeft, setCompareLeft] = useState<CompareSnapshot | null>(null)
  const [compareRight, setCompareRight] = useState<CompareSnapshot | null>(null)
  const [exportMessage, setExportMessage] = useState<string>('')

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

  const clearDrawnGeometry = useCallback(() => {
    const draw = drawRef.current
    if (!draw) {
      setDrawnGeometry(null)
      setDrawnFeatureId('')
      setDrawMode('simple_select')
      return
    }

    draw.deleteAll()
    draw.changeMode('simple_select')
    setDrawnGeometry(null)
    setDrawnFeatureId('')
    setDrawMode('simple_select')
    setActiveJobId('')
    setExportMessage('')
  }, [])

  const activateDrawMode = useCallback(() => {
    const draw = drawRef.current
    if (!draw) {
      return
    }

    draw.deleteAll()
    setDrawnGeometry(null)
    setDrawnFeatureId('')
    setActiveJobId('')
    draw.changeMode('draw_polygon')
    setDrawMode('draw_polygon')
  }, [])

  const completeDrawMode = useCallback(() => {
    const draw = drawRef.current
    if (!draw) {
      return
    }
    draw.changeMode('simple_select')
    setDrawMode('simple_select')
    syncDrawnState()
  }, [syncDrawnState])

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
  const effectiveReferenceWardId = referenceWardId || selectedWardId || wardsQuery.data?.wards?.[0]?.ward_id || ''

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

  const wardDetailsQuery = useQuery({
    queryKey: ['ward-details', effectiveCity, selectedWardId],
    queryFn: () => getWardMetrics(effectiveCity, selectedWardId),
    enabled: Boolean(effectiveCity && selectedWardId),
  })

  const referenceWardMetricsQuery = useQuery({
    queryKey: ['reference-ward-metrics', effectiveCity, effectiveReferenceWardId],
    queryFn: () => getWardMetrics(effectiveCity, effectiveReferenceWardId),
    enabled: Boolean(effectiveCity && effectiveReferenceWardId && referenceMode === 'reference_ward'),
  })

  const selectedCollectionQuery = useQuery({
    queryKey: ['ward-collection', effectiveCity, selectedWardIds],
    queryFn: async () => {
      const response = await analyse({
        mode: 'wards',
        city: effectiveCity,
        ward_ids: selectedWardIds,
        limit: selectedWardIds.length,
        run_async: false,
      })
      if (isJobResponse(response)) {
        throw new Error('Expected synchronous ward collection response')
      }
      return response
    },
    enabled: Boolean(effectiveCity && selectedWardIds.length > 1),
  })

  const cityMetricsQuery = useQuery({
    queryKey: ['city-metrics', effectiveCity],
    queryFn: () => getCityMetrics(effectiveCity),
    enabled: Boolean(effectiveCity),
  })

  const metaMetricsQuery = useQuery({
    queryKey: ['meta-metrics'],
    queryFn: getMetaMetrics,
  })
  const effectiveMetricId =
    activeMetricId || metaMetricsQuery.data?.metrics?.[0]?.metric_id || 'road.intersection_density'

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
  }, [wardsGeoQuery.data, metricByWard])

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

  const geometryValidation = useMemo(
    () => validateAndNormalizeGeometry(drawnGeometry),
    [drawnGeometry],
  )

  const enqueueCustomPolygonMutation = useMutation({
    mutationFn: async () => {
      if (!effectiveCity || !geometryValidation.normalizedGeometry || !geometryValidation.isValid) {
        throw new Error('Select a city and draw a polygon first')
      }

      const response = await analyse({
        mode: 'custom_polygon',
        city: effectiveCity,
        geometry: geometryValidation.normalizedGeometry as Geometry,
      })
      if (!isJobResponse(response)) {
        throw new Error('Expected async response for custom polygon analysis')
      }
      return response
    },
    onSuccess: (data) => {
      setActiveJobId(data.job_id)
      setActiveMetricPanelSource('custom_polygon')
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
        return 1000
      }
      return current.status === 'queued' || current.status === 'running' ? 1000 : false
    },
  })

  const collectionCount = useMemo(() => {
    const rows = parseWardRows(selectedCollectionQuery.data)
    return rows.length
  }, [selectedCollectionQuery.data])

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return
    }

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: BASEMAP_STYLE,
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

    const ensureWardLayers = () => {
      if (!map.getSource('wards-source')) {
        map.addSource('wards-source', {
          type: 'geojson',
          data: {
            type: 'FeatureCollection',
            features: [],
          },
          promoteId: 'ward_id',
        })
      }

      if (!map.getLayer('wards-fill')) {
        map.addLayer({
          id: 'wards-fill',
          type: 'fill',
          source: 'wards-source',
          paint: {
            'fill-color': '#cbd5e1',
            'fill-opacity': 0.75,
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
            'line-width': 0.7,
            'line-opacity': 0.5,
          },
        })
      }

      if (!map.getLayer('wards-hover')) {
        map.addLayer({
          id: 'wards-hover',
          type: 'line',
          source: 'wards-source',
          paint: {
            'line-color': '#111827',
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
            'line-color': '#ef4444',
            'line-width': 2.2,
          },
          filter: ['in', ['get', 'ward_id'], ['literal', []]],
        })
      }
    }

    const onWardClick = (event: MapMouseEvent & { features?: MapGeoJSONFeature[] }) => {
      const feature = event.features?.[0]
      const wardId = feature?.properties?.ward_id
      if (typeof wardId !== 'string') {
        return
      }

      setSelectedWardId(wardId)
      setActiveMetricPanelSource('ward')
      setExportMessage('')
      setSelectedWardIds((current) => {
        if (event.originalEvent.shiftKey || event.originalEvent.metaKey || event.originalEvent.ctrlKey) {
          if (current.includes(wardId)) {
            return current.filter((item) => item !== wardId)
          }
          return [...current, wardId]
        }
        return [wardId]
      })
    }

    const onWardHover = (event: MapMouseEvent & { features?: MapGeoJSONFeature[] }) => {
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

    map.on('load', () => {
      ensureWardLayers()
      map.on('click', 'wards-fill', onWardClick)
      map.on('mousemove', 'wards-fill', onWardHover)
      map.on('mouseleave', 'wards-fill', onWardLeave)
    })

    map.on('draw.create', syncDrawnState)
    map.on('draw.update', syncDrawnState)
    map.on('draw.delete', syncDrawnState)
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
    if (!map || !map.isStyleLoaded()) {
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
    if (!effectiveCity) {
      return
    }

    const map = mapRef.current
    if (!map) {
      return
    }

    const bounds = computeBounds(geojsonData)
    if (!bounds) {
      return
    }

    map.fitBounds(bounds, {
      padding: 40,
      duration: 700,
    })
  }, [effectiveCity, geojsonData])

  const cityError = citiesQuery.error instanceof Error ? citiesQuery.error.message : ''
  const wardError = wardsQuery.error instanceof Error ? wardsQuery.error.message : ''
  const metricError = cityWardsMetricsQuery.error instanceof Error ? cityWardsMetricsQuery.error.message : ''
  const cityAverageError = cityMetricsQuery.error instanceof Error ? cityMetricsQuery.error.message : ''
  const referenceWardError = referenceWardMetricsQuery.error instanceof Error ? referenceWardMetricsQuery.error.message : ''

  const activeJob = analyseJobQuery.data
  const customPolygonPanelData = useMemo<PanelSnapshot | null>(() => {
    if (activeJob?.status !== 'succeeded' || !activeJob.result) {
      return null
    }

    const metricsJson = activeJob.result.metrics_json as { all_metrics?: Record<string, unknown> } | undefined
    if (!metricsJson?.all_metrics) {
      return null
    }

    return {
      sourceType: 'custom_polygon' as const,
      sourceId: activeJob.job_id,
      metrics: metricsJson.all_metrics,
      qualitySummary: (activeJob.result.quality_summary as Record<string, unknown> | undefined) ?? {},
    }
  }, [activeJob])

  const wardPanelData = useMemo<PanelSnapshot | null>(() => {
    if (!wardDetailsQuery.data?.metrics_json?.all_metrics) {
      return null
    }
    return {
      sourceType: 'ward' as const,
      sourceId: wardDetailsQuery.data.ward_id,
      metrics: wardDetailsQuery.data.metrics_json.all_metrics as Record<string, unknown>,
      qualitySummary: wardDetailsQuery.data.quality_summary,
    }
  }, [wardDetailsQuery.data])

  const activePanelData = useMemo(() => {
    if (activeMetricPanelSource === 'custom_polygon' && customPolygonPanelData) {
      return customPolygonPanelData
    }
    if (activeMetricPanelSource === 'ward' && wardPanelData) {
      return wardPanelData
    }
    return customPolygonPanelData ?? wardPanelData
  }, [activeMetricPanelSource, customPolygonPanelData, wardPanelData])

  const metricPanelRows = useMemo(
    () => buildMetricPanelRows(activePanelData?.metrics ?? {}, metaMetricsQuery.data?.metrics ?? []),
    [activePanelData, metaMetricsQuery.data?.metrics],
  )
  const metricPanelGroups = useMemo(() => groupMetricPanelRows(metricPanelRows), [metricPanelRows])
  const cityAverageMetricMap = useMemo(() => {
    const map = new Map<string, number>()
    for (const row of cityMetricsQuery.data?.metrics ?? []) {
      map.set(row.metric_id, row.avg_value)
    }
    return map
  }, [cityMetricsQuery.data])
  const referenceWardRows = useMemo(
    () => buildMetricPanelRows(referenceWardMetricsQuery.data?.metrics_json?.all_metrics ?? {}, metaMetricsQuery.data?.metrics ?? []),
    [referenceWardMetricsQuery.data, metaMetricsQuery.data?.metrics],
  )
  const referenceWardMetricMap = useMemo(() => numericMetricMapFromRows(referenceWardRows), [referenceWardRows])
  const activeReferenceMap = useMemo(
    () => (referenceMode === 'city_average' ? cityAverageMetricMap : referenceWardMetricMap),
    [referenceMode, cityAverageMetricMap, referenceWardMetricMap],
  )
  const deltaRows = useMemo(
    () => buildDeltaRows(metricPanelRows, activeReferenceMap),
    [metricPanelRows, activeReferenceMap],
  )
  const referenceLabel =
    referenceMode === 'city_average'
      ? `City Average (${effectiveCity})`
      : `Ward ${effectiveReferenceWardId || 'N/A'}`

  const makeCompareSnapshot = useCallback((): CompareSnapshot | null => {
    if (!activePanelData || metricPanelRows.length === 0) {
      return null
    }

    const title =
      activePanelData.sourceType === 'ward'
        ? `Ward ${activePanelData.sourceId}`
        : `Polygon ${activePanelData.sourceId.slice(0, 8)}`
    return {
      sourceType: activePanelData.sourceType,
      sourceId: activePanelData.sourceId,
      metrics: activePanelData.metrics,
      qualitySummary: activePanelData.qualitySummary,
      title,
    }
  }, [activePanelData, metricPanelRows.length])

  const compareLeftRows = useMemo(
    () => buildMetricPanelRows(compareLeft?.metrics ?? {}, metaMetricsQuery.data?.metrics ?? []),
    [compareLeft?.metrics, metaMetricsQuery.data?.metrics],
  )
  const compareRightRows = useMemo(
    () => buildMetricPanelRows(compareRight?.metrics ?? {}, metaMetricsQuery.data?.metrics ?? []),
    [compareRight?.metrics, metaMetricsQuery.data?.metrics],
  )
  const sideBySideRows = useMemo(
    () => buildSideBySideRows(compareLeftRows, compareRightRows),
    [compareLeftRows, compareRightRows],
  )

  const drawGuidance = useMemo(() => {
    if (geometryValidation.errors.length > 0) {
      return geometryValidation.errors[0]
    }
    if (drawMode === 'draw_polygon') {
      return 'Click to add vertices. Double-click or click the first vertex to finish. Use Complete to exit draw mode.'
    }
    if (drawMode === 'direct_select' && drawnGeometry) {
      return 'Drag existing vertices to edit the polygon. Use Complete when edits are done.'
    }
    if (drawnGeometry) {
      return 'Polygon is ready. Use Edit to refine it or Clear to remove it.'
    }
    return 'Press Draw to start creating a polygon.'
  }, [drawMode, drawnGeometry, geometryValidation.errors])

  const handleExportJson = useCallback(() => {
    if (!activePanelData || metricPanelRows.length === 0) {
      setExportMessage('No metric payload available to export yet.')
      return
    }

    const fileName = `urbanmor_${effectiveCity}_${activePanelData.sourceType}_${activePanelData.sourceId}.json`
    const payload = makeMetricsExportJson(
      {
        city: effectiveCity,
        sourceType: activePanelData.sourceType,
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
      setExportMessage('No metric payload available to export yet.')
      return
    }

    const fileName = `urbanmor_${effectiveCity}_${activePanelData.sourceType}_${activePanelData.sourceId}.csv`
    const payload = makeMetricsExportCsv(
      {
        city: effectiveCity,
        sourceType: activePanelData.sourceType,
        sourceId: activePanelData.sourceId,
        qualitySummary: activePanelData.qualitySummary,
      },
      metricPanelRows,
    )
    downloadTextFile(fileName, payload, 'text/csv;charset=utf-8')
    setExportMessage(`Exported ${fileName}`)
  }, [activePanelData, effectiveCity, metricPanelRows])

  const handleSetCompareLeft = useCallback(() => {
    const snapshot = makeCompareSnapshot()
    if (!snapshot) {
      setExportMessage('Load a metric result first, then set compare slots.')
      return
    }
    setCompareLeft(snapshot)
    setExportMessage(`Set Left: ${snapshot.title}`)
  }, [makeCompareSnapshot])

  const handleSetCompareRight = useCallback(() => {
    const snapshot = makeCompareSnapshot()
    if (!snapshot) {
      setExportMessage('Load a metric result first, then set compare slots.')
      return
    }
    setCompareRight(snapshot)
    setExportMessage(`Set Right: ${snapshot.title}`)
  }, [makeCompareSnapshot])

  return (
    <div className="h-screen w-screen bg-slate-100 text-slate-900">
      <div className="grid h-full grid-cols-12">
        <aside className="col-span-12 border-b border-slate-300 bg-white p-4 shadow-sm md:col-span-4 md:border-b-0 md:border-r lg:col-span-3">
          <h1 className="text-lg font-semibold tracking-tight">UrbanMor Analytics</h1>
          <p className="mt-1 text-xs text-slate-500">City/ward metrics, choropleths, and custom polygon analysis</p>

          <div className="mt-4 space-y-3">
            <div>
              <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">City</label>
              <select
                className="w-full rounded border border-slate-300 bg-white px-2 py-2 text-sm"
                value={effectiveCity}
                disabled={!citiesQuery.data?.cities?.length}
                onChange={(event) => {
                  const nextCity = event.target.value
                  setSelectedCity(nextCity)
                  setSelectedWardId('')
                  setSelectedWardIds([])
                  setHoverWardId('')
                  setActiveJobId('')
                  setActiveMetricPanelSource('ward')
                  setReferenceWardId('')
                  setCompareLeft(null)
                  setCompareRight(null)
                  setExportMessage('')
                  clearDrawnGeometry()
                }}
              >
                {citiesQuery.data?.cities.map((city) => (
                  <option key={city.city} value={city.city}>
                    {city.city} ({city.cached_wards}/{city.expected_wards})
                  </option>
                ))}
              </select>
              {!citiesQuery.data?.cities?.length ? (
                <p className="mt-1 text-[11px] text-rose-700">No ready cities found. Check cache/data ingestion state.</p>
              ) : null}
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">Choropleth Metric</label>
              <select
                className="w-full rounded border border-slate-300 bg-white px-2 py-2 text-sm"
                value={effectiveMetricId}
                onChange={(event) => setActiveMetricId(event.target.value)}
              >
                {metaMetricsQuery.data?.metrics.map((metric) => (
                  <option key={metric.metric_id} value={metric.metric_id}>
                    {metric.metric_id}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-[11px] text-slate-500">
                Range: {metricRange.min === null ? 'n/a' : metricRange.min.toFixed(3)} to{' '}
                {metricRange.max === null ? 'n/a' : metricRange.max.toFixed(3)}
              </p>
            </div>

            <div className="rounded border border-slate-200 bg-slate-50 p-2 text-xs">
              <p>Wards loaded: {wardsQuery.data?.total_wards ?? 0}</p>
              <p>Selected wards: {selectedWardIds.length}</p>
              <p>Hovered ward: {hoverWardId || 'None'}</p>
              <p>Metric rows ready: {wardRows.length}</p>
            </div>

            <div className="rounded border border-slate-200 bg-slate-50 p-2 text-xs">
              <p className="font-semibold uppercase tracking-wide text-slate-600">Draw Toolbar</p>
              <div className="mt-2 grid grid-cols-2 gap-2">
                <button
                  type="button"
                  className="rounded border border-slate-400 bg-white px-2 py-2 text-xs"
                  onClick={activateDrawMode}
                >
                  Draw
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-400 bg-white px-2 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-50"
                  disabled={!drawnGeometry && drawMode !== 'draw_polygon'}
                  onClick={completeDrawMode}
                >
                  Complete
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-400 bg-white px-2 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-50"
                  disabled={!drawnGeometry}
                  onClick={activateEditMode}
                >
                  Edit
                </button>
                <button
                  type="button"
                  className="rounded border border-slate-400 bg-white px-2 py-2 text-xs disabled:cursor-not-allowed disabled:opacity-50"
                  disabled={!drawnGeometry}
                  onClick={clearDrawnGeometry}
                >
                  Clear
                </button>
              </div>
              <p className="mt-2 text-[11px] text-slate-600">{drawGuidance}</p>
              <p className="mt-1 text-[11px] text-slate-500">Current mode: {drawMode}</p>
              <p className="mt-1 text-[11px] text-slate-500">
                Geometry: {geometryValidation.vertexCount} vertices
                {geometryValidation.areaSqM ? `, ${geometryValidation.areaSqM.toFixed(1)} m2` : ''}
              </p>
              {geometryValidation.errors.length > 0 ? (
                <div className="mt-2 rounded border border-red-200 bg-red-50 p-2 text-[11px] text-red-700">
                  {geometryValidation.errors.map((message) => (
                    <p key={message}>{message}</p>
                  ))}
                </div>
              ) : null}
              {geometryValidation.warnings.length > 0 ? (
                <div className="mt-2 rounded border border-amber-200 bg-amber-50 p-2 text-[11px] text-amber-800">
                  {geometryValidation.warnings.map((message) => (
                    <p key={message}>{message}</p>
                  ))}
                </div>
              ) : null}
            </div>

            <div className="flex gap-2">
              <button
                type="button"
                className="rounded bg-slate-800 px-3 py-2 text-xs font-semibold text-white disabled:cursor-not-allowed disabled:bg-slate-400"
                disabled={!geometryValidation.isValid || enqueueCustomPolygonMutation.isPending || !effectiveCity}
                onClick={() => enqueueCustomPolygonMutation.mutate()}
              >
                Analyse Drawn Polygon
              </button>
              <button
                type="button"
                className="rounded border border-slate-400 px-3 py-2 text-xs"
                onClick={() => {
                  setSelectedWardId('')
                  setSelectedWardIds([])
                  setActiveMetricPanelSource('ward')
                  setExportMessage('')
                }}
              >
                Clear Selection
              </button>
            </div>
          </div>

          <section className="mt-4 space-y-2 text-xs">
            <h2 className="font-semibold uppercase tracking-wide text-slate-600">Custom Polygon Job</h2>
            {!activeJobId ? <p className="text-slate-500">Draw a polygon and start analysis.</p> : null}
            {enqueueCustomPolygonMutation.isError && enqueueCustomPolygonMutation.error instanceof Error ? (
              <p className="rounded border border-red-200 bg-red-50 p-2 text-red-700">{enqueueCustomPolygonMutation.error.message}</p>
            ) : null}
            {activeJob ? (
              <div className="rounded border border-slate-200 bg-white p-2">
                <p>Job: {activeJob.job_id.slice(0, 8)}...</p>
                <p>Status: {activeJob.status}</p>
                <p>
                  Progress: {activeJob.progress_pct}%{activeJob.progress_message ? ` (${activeJob.progress_message})` : ''}
                </p>
                {activeJob.error ? <p className="text-red-600">{activeJob.error}</p> : null}
              </div>
            ) : null}
          </section>

          <section className="mt-4 space-y-2 text-xs">
            <h2 className="font-semibold uppercase tracking-wide text-slate-600">Metric Panel</h2>
            {!activePanelData ? (
              <p className="text-slate-500">Click a ward or run custom polygon analysis to load metric details.</p>
            ) : (
              <>
                <div className="rounded border border-slate-200 bg-white p-2 text-[11px]">
                  <p>
                    Source: {activePanelData.sourceType} ({activePanelData.sourceId})
                  </p>
                  <p>Metrics available: {metricPanelRows.length}</p>
                  {typeof activePanelData.qualitySummary?.completeness_ratio === 'number' ? (
                    <p>Completeness: {(Number(activePanelData.qualitySummary.completeness_ratio) * 100).toFixed(1)}%</p>
                  ) : null}
                </div>

                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    className="rounded border border-slate-400 bg-white px-2 py-1 text-[11px]"
                    onClick={handleExportJson}
                    disabled={metricPanelRows.length === 0}
                  >
                    Export JSON
                  </button>
                  <button
                    type="button"
                    className="rounded border border-slate-400 bg-white px-2 py-1 text-[11px]"
                    onClick={handleExportCsv}
                    disabled={metricPanelRows.length === 0}
                  >
                    Export CSV
                  </button>
                  <button
                    type="button"
                    className="rounded border border-slate-400 bg-white px-2 py-1 text-[11px]"
                    onClick={handleSetCompareLeft}
                    disabled={metricPanelRows.length === 0}
                  >
                    Set As Left
                  </button>
                  <button
                    type="button"
                    className="rounded border border-slate-400 bg-white px-2 py-1 text-[11px]"
                    onClick={handleSetCompareRight}
                    disabled={metricPanelRows.length === 0}
                  >
                    Set As Right
                  </button>
                </div>
                {exportMessage ? <p className="text-[11px] text-slate-600">{exportMessage}</p> : null}

                <div className="rounded border border-slate-200 bg-white p-2 text-[11px]">
                  <p className="font-semibold uppercase tracking-wide text-slate-600">Delta View</p>
                  <div className="mt-2 flex flex-wrap items-center gap-2">
                    <select
                      className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
                      value={referenceMode}
                      onChange={(event) => setReferenceMode(event.target.value as 'city_average' | 'reference_ward')}
                    >
                      <option value="city_average">Compare To City Average</option>
                      <option value="reference_ward">Compare To Ward</option>
                    </select>
                    {referenceMode === 'reference_ward' ? (
                      <select
                        className="rounded border border-slate-300 bg-white px-2 py-1 text-[11px]"
                        value={effectiveReferenceWardId}
                        onChange={(event) => setReferenceWardId(event.target.value)}
                      >
                        {(wardsQuery.data?.wards ?? []).map((ward) => (
                          <option key={ward.ward_id} value={ward.ward_id}>
                            {ward.ward_id} {ward.ward_name ? `- ${ward.ward_name}` : ''}
                          </option>
                        ))}
                      </select>
                    ) : null}
                  </div>
                  <p className="mt-1 text-slate-500">Reference: {referenceLabel}</p>
                  <div className="mt-2 max-h-32 overflow-y-auto rounded border border-slate-200 bg-slate-50 p-2">
                    {deltaRows.slice(0, 12).map((row) => (
                      <div key={row.metric_id} className="mb-1 border-b border-slate-200 pb-1 last:mb-0 last:border-b-0 last:pb-0">
                        <p className="font-medium">{row.label}</p>
                        <p className="text-slate-500">{row.metric_id}</p>
                        <p>
                          Current: {formatMetricNumber(row.current)} {row.unit} | Ref: {formatMetricNumber(row.reference)} {row.unit}
                        </p>
                        <p className={row.deltaAbs >= 0 ? 'text-emerald-700' : 'text-rose-700'}>
                          Delta: {row.deltaAbs >= 0 ? '+' : ''}
                          {formatMetricNumber(row.deltaAbs)} {row.unit}
                          {row.deltaPct !== null ? ` (${row.deltaPct >= 0 ? '+' : ''}${row.deltaPct.toFixed(1)}%)` : ''}
                        </p>
                      </div>
                    ))}
                    {deltaRows.length === 0 ? (
                      <p className="text-slate-500">No numeric overlaps with the selected reference.</p>
                    ) : null}
                  </div>
                </div>

                <div className="max-h-72 space-y-2 overflow-y-auto rounded border border-slate-200 bg-white p-2">
                  {metricPanelGroups.map((group) => (
                    <div key={group.key} className="space-y-1">
                      <p className="font-semibold uppercase tracking-wide text-slate-600">{group.label}</p>
                      {group.metrics.map((metric) => (
                        <div key={metric.metric_id} className="rounded border border-slate-200 bg-slate-50 p-2">
                          <div className="flex items-start justify-between gap-2">
                            <div>
                              <p className="font-medium">{metric.label}</p>
                              <p className="text-[11px] text-slate-500">{metric.metric_id}</p>
                            </div>
                            <p className="text-right text-sm font-semibold">
                              {metric.valueDisplay}
                              <span className="ml-1 text-[11px] font-normal text-slate-500">{metric.unit}</span>
                            </p>
                          </div>
                          <p className="mt-1 text-[11px] text-slate-600">{metric.explanation}</p>
                          <div className="mt-1 flex gap-1">
                            <span className={`rounded border px-1.5 py-0.5 text-[10px] ${statusBadgeClass(metric.status)}`}>
                              {metric.status}
                            </span>
                            <span className={`rounded border px-1.5 py-0.5 text-[10px] ${qualityBadgeClass(metric.qualityFlag)}`}>
                              {metric.qualityFlag}
                            </span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ))}
                  {metricPanelGroups.length === 0 ? <p className="text-slate-500">No metric rows to display.</p> : null}
                </div>
              </>
            )}

            {selectedWardIds.length > 1 ? (
              <p className="rounded border border-slate-200 bg-white p-2">
                Multi-select rows returned: {collectionCount}
              </p>
            ) : null}
          </section>

          <section className="mt-4 space-y-2 text-xs">
            <h2 className="font-semibold uppercase tracking-wide text-slate-600">Compare Mode</h2>
            <div className="rounded border border-slate-200 bg-white p-2">
              <p>Left: {compareLeft ? compareLeft.title : 'Not set'}</p>
              <p>Right: {compareRight ? compareRight.title : 'Not set'}</p>
            </div>
            <button
              type="button"
              className="rounded border border-slate-400 bg-white px-2 py-1 text-[11px]"
              onClick={() => {
                setCompareLeft(null)
                setCompareRight(null)
              }}
            >
              Clear Compare Slots
            </button>
            {compareLeft && compareRight ? (
              <div className="max-h-44 overflow-y-auto rounded border border-slate-200 bg-white p-2">
                {sideBySideRows.slice(0, 14).map((row) => (
                  <div key={row.metric_id} className="mb-1 border-b border-slate-200 pb-1 last:mb-0 last:border-b-0 last:pb-0">
                    <p className="font-medium">{row.label}</p>
                    <p className="text-slate-500">{row.metric_id}</p>
                    <p>
                      L: {formatMetricNumber(row.leftValue)} {row.unit} | R: {formatMetricNumber(row.rightValue)} {row.unit}
                    </p>
                    <p className={row.deltaAbs >= 0 ? 'text-emerald-700' : 'text-rose-700'}>
                      Delta: {row.deltaAbs >= 0 ? '+' : ''}
                      {formatMetricNumber(row.deltaAbs)} {row.unit}
                      {row.deltaPct !== null ? ` (${row.deltaPct >= 0 ? '+' : ''}${row.deltaPct.toFixed(1)}%)` : ''}
                    </p>
                  </div>
                ))}
                {sideBySideRows.length === 0 ? <p className="text-slate-500">No numeric overlap between selected areas.</p> : null}
              </div>
            ) : (
              <p className="text-slate-500">Set Left and Right from the metric panel to enable side-by-side compare.</p>
            )}
          </section>

          {cityError || wardError || metricError || cityAverageError || referenceWardError ? (
            <section className="mt-4 rounded border border-red-200 bg-red-50 p-2 text-xs text-red-700">
              {cityError ? <p>{cityError}</p> : null}
              {wardError ? <p>{wardError}</p> : null}
              {metricError ? <p>{metricError}</p> : null}
              {cityAverageError ? <p>{cityAverageError}</p> : null}
              {referenceWardError ? <p>{referenceWardError}</p> : null}
            </section>
          ) : null}
        </aside>

        <main className="col-span-12 md:col-span-8 lg:col-span-9">
          <div ref={mapContainerRef} className="h-full w-full" />
        </main>
      </div>
    </div>
  )
}

export default App
