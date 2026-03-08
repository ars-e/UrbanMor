import type { MetricMetaItem } from '../types'

export interface HighlightCard {
  metricId: string
  theme: string
  label: string
  unit: string
  value: number | null
  comparison: string
  narrative: string
}

interface HighlightDefinition {
  metricId: string
  theme: string
  fallbackLabel: string
  narrative: (value: number | null, cityAverage: number | null, context: string) => string
}

export const MAP_METRIC_IDS = [
  'road.intersection_density',
  'road.cnr',
  'transit.coverage_500m',
  'bldg.bcr',
  'lulc.green_cover_pct',
  'open.distance_to_nearest_park',
  'cmp.walkability_index',
  'topo.flood_risk_proxy',
]

const HIGHLIGHT_DEFINITIONS: HighlightDefinition[] = [
  {
    metricId: 'road.intersection_density',
    theme: 'Street fabric',
    fallbackLabel: 'Intersection Density',
    narrative: (_value, _avg, context) =>
      `This reads the street network grain for ${context}. Higher values usually mean shorter blocks and more route choice.`,
  },
  {
    metricId: 'transit.coverage_500m',
    theme: 'Transit reach',
    fallbackLabel: 'Transit Coverage (500m)',
    narrative: (_value, _avg, context) =>
      `This estimates how much of ${context} sits within a comfortable walk of transit stops or stations.`,
  },
  {
    metricId: 'bldg.bcr',
    theme: 'Built intensity',
    fallbackLabel: 'Building Coverage Ratio',
    narrative: (_value, _avg, context) =>
      `This shows how much ground in ${context} is occupied by building footprints, not how tall the area is.`,
  },
  {
    metricId: 'lulc.green_cover_pct',
    theme: 'Green relief',
    fallbackLabel: 'Green Cover',
    narrative: (_value, _avg, context) =>
      `This captures vegetation share inside ${context}, giving a direct read on environmental relief and openness.`,
  },
]

function classifyComparison(value: number | null, cityAverage: number | null): string {
  if (value === null || cityAverage === null) {
    return 'No city comparison'
  }

  if (cityAverage === 0) {
    return 'City average unavailable'
  }

  const ratio = value / cityAverage
  if (ratio >= 1.15) {
    return 'Above city average'
  }
  if (ratio <= 0.85) {
    return 'Below city average'
  }
  return 'Near city average'
}

export function buildHighlightCards(
  metricsPayload: Record<string, unknown>,
  cityAverageMap: Map<string, number>,
  metadata: MetricMetaItem[],
  contextLabel: string,
): HighlightCard[] {
  const metaById = new Map(metadata.map((item) => [item.metric_id, item]))

  return HIGHLIGHT_DEFINITIONS.map((definition) => {
    const meta = metaById.get(definition.metricId)
    const rawValue = metricsPayload[definition.metricId]
    const value = typeof rawValue === 'number' && Number.isFinite(rawValue) ? rawValue : null
    const cityAverage = cityAverageMap.get(definition.metricId) ?? null
    return {
      metricId: definition.metricId,
      theme: definition.theme,
      label: meta?.label ?? definition.fallbackLabel,
      unit: meta?.unit ?? '-',
      value,
      comparison: classifyComparison(value, cityAverage),
      narrative: definition.narrative(value, cityAverage, contextLabel),
    }
  })
}
