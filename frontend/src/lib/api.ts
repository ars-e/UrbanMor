import type {
  AnalyseJobResponse,
  AnalysePayload,
  AnalyseResponse,
  CitiesResponse,
  CityMapLayerGeoJSONResponse,
  CityMetricsResponse,
  CityWardsGeoJSONResponse,
  CityWardsResponse,
  MetaMetricsResponse,
  WardMetricResponse,
} from '../types'

const LOCAL_API_BASE = 'http://127.0.0.1:8000'
const PRODUCTION_API_BASE = 'https://urbanmor-api.onrender.com'
const DEPLOYED_HOSTS = new Set(['www.inkletlab.com', 'inkletlab.com'])
const NGROK_HOST_SUFFIXES = ['.ngrok-free.dev', '.ngrok.app', '.ngrok.dev', '.ngrok.io']

function normalizeApiBase(value: string): string {
  return value.trim().replace(/\/+$/, '')
}

function isNgrokApiBase(base: string): boolean {
  try {
    const hostname = new URL(base).hostname.toLowerCase()
    return NGROK_HOST_SUFFIXES.some((suffix) => hostname.endsWith(suffix))
  } catch {
    return false
  }
}

function resolveApiBase(): string {
  const envValue = import.meta.env.VITE_API_BASE_URL as string | undefined
  if (envValue && envValue.trim()) {
    return normalizeApiBase(envValue)
  }

  if (typeof window === 'undefined') {
    return LOCAL_API_BASE
  }

  const { protocol, hostname, port } = window.location
  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    return LOCAL_API_BASE
  }

  if (DEPLOYED_HOSTS.has(hostname)) {
    return PRODUCTION_API_BASE
  }

  return normalizeApiBase(`${protocol}//${hostname}${port ? `:${port}` : ''}`)
}

export const API_BASE = resolveApiBase()

async function apiFetch(path: string, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers ?? undefined)
  if (isNgrokApiBase(API_BASE)) {
    headers.set('ngrok-skip-browser-warning', '1')
  }

  try {
    return await fetch(`${API_BASE}${path}`, {
      ...init,
      headers,
    })
  } catch (error) {
    const detail = error instanceof Error && error.message ? error.message : 'network request failed'
    throw new Error(`Unable to reach UrbanMor API at ${API_BASE}: ${detail}`)
  }
}

async function parseResponse<T>(response: Response): Promise<T> {
  if (response.ok) {
    const contentType = response.headers.get('content-type') ?? ''
    if (contentType.includes('application/json')) {
      return (await response.json()) as T
    }

    const bodyText = await response.text()
    if (contentType.includes('text/html') && bodyText.includes('ERR_NGROK_')) {
      throw new Error(
        `ngrok blocked browser API access for ${API_BASE}. Request needs the "ngrok-skip-browser-warning" header.`,
      )
    }
    throw new Error(
      `UrbanMor API returned unexpected content type "${contentType || 'unknown'}" from ${API_BASE}`,
    )
  }

  let detail = `UrbanMor API returned HTTP ${response.status} from ${API_BASE}`
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
  const response = await apiFetch('/cities')
  return parseResponse<CitiesResponse>(response)
}

export async function getCityWards(city: string): Promise<CityWardsResponse> {
  const response = await apiFetch(`/cities/${city}/wards`)
  return parseResponse<CityWardsResponse>(response)
}

export async function getCityWardsGeoJSON(city: string): Promise<CityWardsGeoJSONResponse> {
  const response = await apiFetch(`/cities/${city}/wards/geojson`)
  return parseResponse<CityWardsGeoJSONResponse>(response)
}

export async function getCityRoadsGeoJSON(
  city: string,
  bbox: string,
  zoom: number,
  detail: 'major' | 'full',
): Promise<CityMapLayerGeoJSONResponse> {
  const response = await apiFetch(
    `/cities/${city}/roads/geojson?bbox=${encodeURIComponent(bbox)}&zoom=${encodeURIComponent(String(zoom))}&detail=${detail}`,
  )
  return parseResponse<CityMapLayerGeoJSONResponse>(response)
}

export async function getCityTransitGeoJSON(
  city: string,
  bbox: string,
): Promise<CityMapLayerGeoJSONResponse> {
  const response = await apiFetch(`/cities/${city}/transit/geojson?bbox=${encodeURIComponent(bbox)}`)
  return parseResponse<CityMapLayerGeoJSONResponse>(response)
}

export async function getCityMetrics(city: string): Promise<CityMetricsResponse> {
  const response = await apiFetch(`/cities/${city}/metrics`)
  return parseResponse<CityMetricsResponse>(response)
}

export async function getWardMetrics(city: string, wardId: string): Promise<WardMetricResponse> {
  const response = await apiFetch(`/cities/${city}/wards/${wardId}`)
  return parseResponse<WardMetricResponse>(response)
}

export async function getMetaMetrics(): Promise<MetaMetricsResponse> {
  const response = await apiFetch('/meta/metrics')
  return parseResponse<MetaMetricsResponse>(response)
}

export async function analyse(payload: AnalysePayload): Promise<AnalyseResponse | AnalyseJobResponse> {
  const response = await apiFetch('/analyse', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
  return parseResponse<AnalyseResponse | AnalyseJobResponse>(response)
}

export async function getAnalyseJob(jobId: string): Promise<AnalyseJobResponse> {
  const response = await apiFetch(`/analyse/jobs/${jobId}`)
  return parseResponse<AnalyseJobResponse>(response)
}
