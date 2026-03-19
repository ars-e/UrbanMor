import type { Feature, FeatureCollection, Geometry, MultiPolygon, Polygon } from 'geojson'

export type AnalysisMode = 'ward' | 'wards' | 'city' | 'custom_polygon'

export interface CitySummary {
  city: string
  expected_wards: number
  cached_wards: number
  completeness_pct: number
}

export interface CitiesResponse {
  cities: CitySummary[]
}

export interface WardSummary {
  ward_id: string
  ward_name: string | null
  ward_uid: string | null
  has_cache: boolean
  computed_at: string | null
}

export interface CityWardsResponse {
  city: string
  total_wards: number
  cached_wards: number
  wards: WardSummary[]
}

export interface CityMetricAggregate {
  metric_id: string
  avg_value: number
  min_value: number
  max_value: number
  sample_count: number
}

export interface CityMetricsResponse {
  city: string
  ward_count: number
  metric_count: number
  metrics: CityMetricAggregate[]
}

export interface WardMetricResponse {
  city: string
  ward_id: string
  ward_uid: string | null
  ward_name: string | null
  vintage_year: number
  metrics_json: {
    all_metrics?: Record<string, number | null>
    [key: string]: unknown
  }
  quality_summary: Record<string, unknown>
  computed_at: string
}

export interface AnalyseResponse {
  mode: AnalysisMode
  city: string
  result: Record<string, unknown>
  timing_ms: number
}

export type AnalyseJobStatus = 'queued' | 'running' | 'succeeded' | 'failed'

export interface AnalyseJobResponse {
  job_id: string
  mode: AnalysisMode
  city: string
  status: AnalyseJobStatus
  progress_pct: number
  progress_message: string | null
  created_at: string | null
  started_at: string | null
  completed_at: string | null
  result: Record<string, unknown> | null
  error: string | null
}

export interface AnalysePayload {
  mode: AnalysisMode
  city: string
  ward_id?: string
  ward_ids?: string[]
  geometry?: Geometry
  vintage_year?: number
  limit?: number
  run_async?: boolean
}

export interface MetricMetaItem {
  metric_id: string
  label: string
  category?: string | null
  unit?: string | null
  frontend_group?: string | null
  status?: string | null
  release_target?: string | null
  formula_summary?: string | null
  source_layers?: string[] | null
  validation_rule?: string | null
}

export interface MetaMetricsResponse {
  source: string
  count: number
  metrics: MetricMetaItem[]
}

export type WardGeometryFeature = Feature<Polygon | MultiPolygon, {
  ward_id: string
  ward_name?: string | null
  ward_uid?: string | null
  metric_value?: number | null
}>

export type WardsGeoJSON = FeatureCollection<Polygon | MultiPolygon, {
  ward_id: string
  ward_name?: string | null
  ward_uid?: string | null
  metric_value?: number | null
}>

export interface CityWardsGeoJSONResponse {
  type: 'FeatureCollection'
  city: string
  features: WardGeometryFeature[]
}

export type MapRoadFeature = Feature<Geometry, {
  road_class?: string | null
  style_rank?: number | null
}>

export type MapTransitFeature = Feature<Geometry, {
  source_layer?: string | null
  stop_kind?: string | null
}>

export interface CityMapLayerGeoJSONResponse extends FeatureCollection<Geometry, Record<string, string | number | null>> {
  city: string
  layer: 'roads' | 'transit'
  feature_count: number
}

export interface LockedCircle {
  id: string
  center: [number, number]  // [lng, lat]
  radius: number            // meters
  color: string
  label: string             // A, B, C, D
  location: string | null   // From reverse geocoding
  createdAt: number
}

export interface CircleMetricsData {
  circleId: string
  jobId?: string           // For polling
  isLoading: boolean
  metrics: Record<string, unknown>
  qualitySummary: Record<string, unknown>
  location: string | null
}

export interface PreviewMetrics {
  area_km2: number
  cnr_estimate: number | null
  intersection_density: number | null
  intersection_count: number | null
  open_ratio: number | null
}
