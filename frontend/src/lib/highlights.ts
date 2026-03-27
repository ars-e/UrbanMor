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
  'road.avg_block_size',
  'road.pedestrian_infra_ratio',
  'transit.coverage_500m',
  'bldg.avg_footprint_size',
  'open.park_green_space_density',
  'lulc.green_cover_pct',
  'open.distance_to_nearest_park',
  'cmp.walkability_index',
]

const HIGHLIGHT_DEFINITIONS: HighlightDefinition[] = [
  {
    metricId: 'road.cnr',
    theme: 'Connectivity',
    fallbackLabel: 'Connected Node Ratio (CNR)',
    narrative: (_value, _avg, context) =>
      `This indicates street-network connectivity in ${context}. Higher CNR usually means fewer dead ends and better route choice.`,
  },
  {
    metricId: 'road.avg_block_size',
    theme: 'Block grain',
    fallbackLabel: 'Average Block Size',
    narrative: (_value, _avg, context) =>
      `This reflects the typical block footprint in ${context}. Lower values usually mean finer-grained, more walkable blocks.`,
  },
  {
    metricId: 'bldg.avg_footprint_size',
    theme: 'Building footprint',
    fallbackLabel: 'Average Building Footprint Size',
    narrative: (_value, _avg, context) =>
      `This reports typical building footprint size in ${context}. Higher values usually indicate larger building parcels.`,
  },
  {
    metricId: 'open.park_green_space_density',
    theme: 'Open spaces',
    fallbackLabel: 'Park & Green Space Density',
    narrative: (_value, _avg, context) =>
      `This captures park and open-space availability across ${context}, reflecting access to breathable, non-built land.`,
  },
]

function toNumeric(value: unknown): number | null {
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
    const value = toNumeric(metricsPayload[definition.metricId])
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
