import type {
  AnalyseJobResponse,
  AnalysePayload,
  AnalyseResponse,
  CitiesResponse,
  CityMetricsResponse,
  CityWardsGeoJSONResponse,
  CityWardsResponse,
  MetaMetricsResponse,
  WardMetricResponse,
} from '../types'

const API_BASE = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/+$/, '') ?? 'http://127.0.0.1:8000'

async function parseResponse<T>(response: Response): Promise<T> {
  if (response.ok) {
    return (await response.json()) as T
  }

  let detail = `HTTP ${response.status}`
  try {
    const errorBody = await response.json()
    const value = (errorBody as { detail?: string }).detail
    if (typeof value === 'string' && value) {
      detail = value
    }
  } catch {
    // keep default message
  }
  throw new Error(detail)
}

export async function getCities(): Promise<CitiesResponse> {
  const response = await fetch(`${API_BASE}/cities`)
  return parseResponse<CitiesResponse>(response)
}

export async function getCityWards(city: string): Promise<CityWardsResponse> {
  const response = await fetch(`${API_BASE}/cities/${city}/wards`)
  return parseResponse<CityWardsResponse>(response)
}

export async function getCityWardsGeoJSON(city: string): Promise<CityWardsGeoJSONResponse> {
  const response = await fetch(`${API_BASE}/cities/${city}/wards/geojson`)
  return parseResponse<CityWardsGeoJSONResponse>(response)
}

export async function getCityMetrics(city: string): Promise<CityMetricsResponse> {
  const response = await fetch(`${API_BASE}/cities/${city}/metrics`)
  return parseResponse<CityMetricsResponse>(response)
}

export async function getWardMetrics(city: string, wardId: string): Promise<WardMetricResponse> {
  const response = await fetch(`${API_BASE}/cities/${city}/wards/${wardId}`)
  return parseResponse<WardMetricResponse>(response)
}

export async function getMetaMetrics(): Promise<MetaMetricsResponse> {
  const response = await fetch(`${API_BASE}/meta/metrics`)
  return parseResponse<MetaMetricsResponse>(response)
}

export async function analyse(payload: AnalysePayload): Promise<AnalyseResponse | AnalyseJobResponse> {
  const response = await fetch(`${API_BASE}/analyse`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
  return parseResponse<AnalyseResponse | AnalyseJobResponse>(response)
}

export async function getAnalyseJob(jobId: string): Promise<AnalyseJobResponse> {
  const response = await fetch(`${API_BASE}/analyse/jobs/${jobId}`)
  return parseResponse<AnalyseJobResponse>(response)
}
