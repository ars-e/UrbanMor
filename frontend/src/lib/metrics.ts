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
  current: number
  reference: number
  deltaAbs: number
  deltaPct: number | null
}

export interface SideBySideRow {
  metric_id: string
  label: string
  unit: string
  leftValue: number
  rightValue: number
  deltaAbs: number
  deltaPct: number | null
}

interface ExportContext {
  city: string
  sourceType: 'ward' | 'custom_polygon'
  sourceId: string
  qualitySummary?: Record<string, unknown>
}

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
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null
  }
  return value
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'N/A'
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return 'Invalid'
    }
    return formatNumeric(value)
  }
  if (typeof value === 'object') {
    return 'Composite value'
  }
  return String(value)
}

function qualityFlagForValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'not_computed'
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return 'invalid_numeric'
    }
    if (value === 0) {
      return 'zero'
    }
    return 'ok'
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
    .map(([metricId, value]) => {
      const meta = metaById.get(metricId)
      const family = toFamily(metricId, meta)
      return {
        metric_id: metricId,
        label: meta?.label || metricId,
        family,
        frontendGroup: toGroupLabel(family),
        value,
        valueDisplay: formatValue(value),
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
    const reference = referenceMap.get(row.metric_id)
    if (current === null || reference === undefined) {
      continue
    }

    const deltaAbs = current - reference
    const deltaPct = reference === 0 ? null : (deltaAbs / Math.abs(reference)) * 100
    deltas.push({
      metric_id: row.metric_id,
      label: row.label,
      unit: row.unit,
      current,
      reference,
      deltaAbs,
      deltaPct,
    })
  }

  return deltas.sort((a, b) => Math.abs(b.deltaAbs) - Math.abs(a.deltaAbs))
}

export function buildSideBySideRows(
  leftRows: MetricPanelRow[],
  rightRows: MetricPanelRow[],
): SideBySideRow[] {
  const rightMap = new Map(rightRows.map((row) => [row.metric_id, row]))
  const rows: SideBySideRow[] = []

  for (const left of leftRows) {
    const right = rightMap.get(left.metric_id)
    if (!right) {
      continue
    }

    const leftValue = toNumeric(left.value)
    const rightValue = toNumeric(right.value)
    if (leftValue === null || rightValue === null) {
      continue
    }

    const deltaAbs = leftValue - rightValue
    const deltaPct = rightValue === 0 ? null : (deltaAbs / Math.abs(rightValue)) * 100
    rows.push({
      metric_id: left.metric_id,
      label: left.label,
      unit: left.unit,
      leftValue,
      rightValue,
      deltaAbs,
      deltaPct,
    })
  }

  return rows.sort((a, b) => Math.abs(b.deltaAbs) - Math.abs(a.deltaAbs))
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

function toCsv(headers: string[], row: Record<string, string>): string {
  const headerLine = headers.map(escapeCsv).join(',')
  const valueLine = headers.map((header) => escapeCsv(row[header] ?? '')).join(',')
  return `${headerLine}\n${valueLine}\n`
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
  const base: Record<string, string> = {
    generated_at: new Date().toISOString(),
    city: context.city,
    source_type: context.sourceType,
    source_id: context.sourceId,
  }

  for (const [key, value] of Object.entries(context.qualitySummary ?? {})) {
    base[`quality_summary__${key}`] = toCsvValue(value)
  }

  for (const row of rows) {
    base[row.metric_id] = toCsvValue(row.value)
    base[`${row.metric_id}__label`] = row.label
    base[`${row.metric_id}__unit`] = row.unit
    base[`${row.metric_id}__status`] = row.status
    base[`${row.metric_id}__quality`] = row.qualityFlag
  }

  const headers = Object.keys(base)
  return toCsv(headers, base)
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
