import type { MetricMetaItem } from '../types'

export interface MetricPanelRow {
  metric_id: string
  label: string
  family: string
  frontendGroup: string
  value: unknown
  valueDisplay: string
  unit: string
  explanation: string
  status: string
  qualityFlag: string
}

export interface MetricPanelGroup {
  key: string
  label: string
  metrics: MetricPanelRow[]
}

export interface MetricDeltaRow {
  metric_id: string
  label: string
  unit: string
  current: number | null
  reference: number | null
  deltaAbs: number | null
  deltaPct: number | null
  comparisonStatus: 'ok' | 'missing_current' | 'missing_reference' | 'missing_both'
}

export interface SideBySideRow {
  metric_id: string
  label: string
  unit: string
  leftValue: number | null
  rightValue: number | null
  deltaAbs: number | null
  deltaPct: number | null
  comparisonStatus: 'ok' | 'missing_left' | 'missing_right' | 'missing_both'
}

interface ExportContext {
  city: string
  sourceType: 'ward' | 'custom_polygon' | 'city'
  sourceId: string
  qualitySummary?: Record<string, unknown>
}

const RETIRED_METRIC_IDS = new Set(['bldg.growth_rate', 'topo.flood_risk_proxy'])

function toFamily(metricId: string, meta?: MetricMetaItem): string {
  if (meta?.frontend_group) {
    return meta.frontend_group
  }
  if (meta?.category) {
    return meta.category
  }
  const prefix = metricId.split('.')[0] ?? 'other'
  return prefix
}

function toGroupLabel(value: string): string {
  return value
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (s) => s.toUpperCase())
}

function formatNumeric(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1_000_000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 1 })
  }
  if (abs >= 1000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  if (abs >= 1) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 3 })
  }
  return value.toLocaleString(undefined, { maximumSignificantDigits: 4 })
}

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

function formatObjectValue(metricId: string, value: Record<string, unknown>): string {
  if (metricId === 'bldg.size_distribution') {
    const p50 = toNumeric(value.p50_m2)
    const p90 = toNumeric(value.p90_m2)
    const variance = toNumeric(value.variance_m2)
    const parts: string[] = []
    if (p50 !== null) {
      parts.push(`P50 ${formatNumeric(p50)} m²`)
    }
    if (p90 !== null) {
      parts.push(`P90 ${formatNumeric(p90)} m²`)
    }
    if (variance !== null) {
      parts.push(`Var ${formatNumeric(variance)} m²`)
    }
    if (parts.length > 0) {
      return parts.join(' · ')
    }
  }

  return JSON.stringify(value)
}

function formatValue(metricId: string, value: unknown): string {
  if (value === null || value === undefined) {
    return 'N/A'
  }
  const numeric = toNumeric(value)
  if (numeric !== null) {
    return formatNumeric(numeric)
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return 'Invalid'
    }
    return formatNumeric(value)
  }
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    return formatObjectValue(metricId, value as Record<string, unknown>)
  }
  return String(value)
}

function qualityFlagForValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'not_computed'
  }
  if (toNumeric(value) !== null) {
    return 'ok'
  }
  if (typeof value === 'number') {
    return 'invalid_numeric'
  }
  if (typeof value === 'object') {
    return 'composite_object'
  }
  return 'non_numeric'
}

export function buildMetricPanelRows(
  metricsPayload: Record<string, unknown>,
  metadata: MetricMetaItem[],
): MetricPanelRow[] {
  const metaById = new Map(metadata.map((meta) => [meta.metric_id, meta]))

  return Object.entries(metricsPayload)
    .filter(([metricId]) => !RETIRED_METRIC_IDS.has(metricId))
    .map(([metricId, value]) => {
      const meta = metaById.get(metricId)
      const family = toFamily(metricId, meta)
      return {
        metric_id: metricId,
        label: meta?.label || metricId,
        family,
        frontendGroup: toGroupLabel(family),
        value,
        valueDisplay: formatValue(metricId, value),
        unit: meta?.unit || '-',
        explanation: meta?.formula_summary || 'No formula summary yet.',
        status: meta?.status || 'unknown',
        qualityFlag: qualityFlagForValue(value),
      }
    })
    .sort((a, b) => a.metric_id.localeCompare(b.metric_id))
}

export function groupMetricPanelRows(rows: MetricPanelRow[]): MetricPanelGroup[] {
  const grouped = new Map<string, MetricPanelRow[]>()
  for (const row of rows) {
    const current = grouped.get(row.family) ?? []
    current.push(row)
    grouped.set(row.family, current)
  }

  return [...grouped.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, metrics]) => ({
      key,
      label: toGroupLabel(key),
      metrics,
    }))
}

export function numericMetricMapFromRows(rows: MetricPanelRow[]): Map<string, number> {
  const result = new Map<string, number>()
  for (const row of rows) {
    const numeric = toNumeric(row.value)
    if (numeric !== null) {
      result.set(row.metric_id, numeric)
    }
  }
  return result
}

export function buildDeltaRows(
  currentRows: MetricPanelRow[],
  referenceMap: Map<string, number>,
): MetricDeltaRow[] {
  const deltas: MetricDeltaRow[] = []

  for (const row of currentRows) {
    const current = toNumeric(row.value)
    const reference = referenceMap.get(row.metric_id) ?? null

    let comparisonStatus: MetricDeltaRow['comparisonStatus'] = 'ok'
    if (current === null && reference === null) {
      comparisonStatus = 'missing_both'
    } else if (current === null) {
      comparisonStatus = 'missing_current'
    } else if (reference === null) {
      comparisonStatus = 'missing_reference'
    }

    const deltaAbs = current !== null && reference !== null ? current - reference : null
    const deltaPct = deltaAbs === null || reference === null || reference === 0 ? null : (deltaAbs / Math.abs(reference)) * 100
    deltas.push({
      metric_id: row.metric_id,
      label: row.label,
      unit: row.unit,
      current,
      reference,
      deltaAbs,
      deltaPct,
      comparisonStatus,
    })
  }

  return deltas.sort((a, b) => {
    const pctA = a.deltaPct === null ? -1 : Math.abs(a.deltaPct)
    const pctB = b.deltaPct === null ? -1 : Math.abs(b.deltaPct)
    if (pctB !== pctA) {
      return pctB - pctA
    }
    const absA = a.deltaAbs === null ? -1 : Math.abs(a.deltaAbs)
    const absB = b.deltaAbs === null ? -1 : Math.abs(b.deltaAbs)
    return absB - absA
  })
}

export function buildSideBySideRows(
  leftRows: MetricPanelRow[],
  rightRows: MetricPanelRow[],
): SideBySideRow[] {
  const leftMap = new Map(leftRows.map((row) => [row.metric_id, row]))
  const rightMap = new Map(rightRows.map((row) => [row.metric_id, row]))
  const metricIds = new Set<string>([...leftMap.keys(), ...rightMap.keys()])
  const rows: SideBySideRow[] = []

  for (const metricId of metricIds) {
    const left = leftMap.get(metricId)
    const right = rightMap.get(metricId)
    const label = left?.label ?? right?.label ?? metricId
    const unit = left?.unit ?? right?.unit ?? '-'

    const leftValue = left ? toNumeric(left.value) : null
    const rightValue = right ? toNumeric(right.value) : null

    let comparisonStatus: SideBySideRow['comparisonStatus'] = 'ok'
    if (leftValue === null && rightValue === null) {
      comparisonStatus = 'missing_both'
    } else if (leftValue === null) {
      comparisonStatus = 'missing_left'
    } else if (rightValue === null) {
      comparisonStatus = 'missing_right'
    }

    const deltaAbs = leftValue !== null && rightValue !== null ? leftValue - rightValue : null
    const deltaPct = deltaAbs === null || rightValue === null || rightValue === 0 ? null : (deltaAbs / Math.abs(rightValue)) * 100
    rows.push({
      metric_id: metricId,
      label,
      unit,
      leftValue,
      rightValue,
      deltaAbs,
      deltaPct,
      comparisonStatus,
    })
  }

  return rows.sort((a, b) => {
    const pctA = a.deltaPct === null ? -1 : Math.abs(a.deltaPct)
    const pctB = b.deltaPct === null ? -1 : Math.abs(b.deltaPct)
    if (pctB !== pctA) {
      return pctB - pctA
    }
    const absA = a.deltaAbs === null ? -1 : Math.abs(a.deltaAbs)
    const absB = b.deltaAbs === null ? -1 : Math.abs(b.deltaAbs)
    return absB - absA
  })
}

export function formatMetricNumber(value: number): string {
  return formatNumeric(value)
}

function toCsvValue(raw: unknown): string {
  if (raw === null || raw === undefined) {
    return ''
  }
  if (typeof raw === 'object') {
    return JSON.stringify(raw)
  }
  return String(raw)
}

function escapeCsv(value: string): string {
  if (value.includes('"') || value.includes(',') || value.includes('\n')) {
    return `"${value.replaceAll('"', '""')}"`
  }
  return value
}

export function makeMetricsExportJson(
  context: ExportContext,
  rows: MetricPanelRow[],
): string {
  const payload = {
    generated_at: new Date().toISOString(),
    city: context.city,
    source_type: context.sourceType,
    source_id: context.sourceId,
    quality_summary: context.qualitySummary ?? {},
    metrics: rows,
  }

  return JSON.stringify(payload, null, 2)
}

export function makeMetricsExportCsv(
  context: ExportContext,
  rows: MetricPanelRow[],
): string {
  const headers = [
    'generated_at',
    'city',
    'source_type',
    'source_id',
    'metric_id',
    'label',
    'family',
    'unit',
    'value',
    'value_display',
    'status',
    'quality_flag',
  ]

  const generatedAt = new Date().toISOString()
  const contextFields: Record<string, string> = {
    generated_at: generatedAt,
    city: context.city,
    source_type: context.sourceType,
    source_id: context.sourceId,
  }

  const lines: string[] = [headers.map(escapeCsv).join(',')]

  for (const row of rows) {
    const record: Record<string, string> = {
      ...contextFields,
      metric_id: row.metric_id,
      label: row.label,
      family: row.frontendGroup,
      unit: row.unit,
      value: toCsvValue(row.value),
      value_display: row.valueDisplay,
      status: row.status,
      quality_flag: row.qualityFlag,
    }
    lines.push(headers.map((h) => escapeCsv(record[h] ?? '')).join(','))
  }

  return lines.join('\n') + '\n'
}

export function downloadTextFile(fileName: string, content: string, mimeType: string): void {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = fileName
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  URL.revokeObjectURL(url)
}
