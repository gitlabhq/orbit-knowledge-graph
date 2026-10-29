import { countBy, sumBy } from './lib.js'

export type ResponseCounts = {
  nodes: number
  edges: number
  byEntity: Record<string, number>
  aggregated: number
  kind: 'traversal' | 'aggregation' | 'other'
  grouped: boolean
}

export function parseResponse(text: string): ResponseCounts {
  const out: ResponseCounts = { nodes: 0, edges: 0, byEntity: {}, aggregated: 0, kind: 'other', grouped: false }
  const raw = text.trim().startsWith('{') ? tryJson(text) : null
  if (raw) {
    const r = (raw['result'] ?? raw) as { nodes?: unknown; edges?: unknown }
    const nodes = Array.isArray(r.nodes) ? (r.nodes as { type?: unknown }[]) : []
    out.byEntity = countBy(nodes, (node) => String(node.type))
    out.nodes = nodes.length
    out.edges = Array.isArray(r.edges) ? r.edges.length : 0
    if (Array.isArray(r.nodes)) out.kind = 'traversal'
    return out
  }
  if (/^query_type:\s*aggregation/m.test(text)) {
    out.kind = 'aggregation'
    out.grouped = /^group_columns\[[1-9]\d*\]/m.test(text)
    const columns = text.split(/^columns\[\d+\](?:\{[^}]*\})?:\s*$/m)[1]?.split(/^\S/m)[0] ?? ''
    const plainCount = (name = '', target = '', property = '') => !property || name === `count_${target}_${property}`
    const tabled = [...columns.matchAll(/^\s+(\w+),count,([^,\n]*)(?:,(.*))?$/gm)].map((m) => ({ name: m[1], plain: plainCount(m[1], m[2], m[3]?.trim()) }))
    const listed = [...columns.matchAll(/^\s+- name:\s*(\w+)\s*\n\s+function:\s*count\b\s*\n\s+target:\s*(\w+)(?:\s*\n\s+property:\s*(\w+))?/gm)].map((m) => ({ name: m[1], plain: plainCount(m[1], m[2], m[3]) }))
    const counts = [...tabled, ...listed].filter((column) => column.plain).map((column) => column.name)
    const header = text.match(/^rows\[\d+\]\{([^}]*)\}:\s*$/m)
    if (!header || header.index === undefined || counts.length === 0) return out
    const names = (header[1] ?? '').split(',')
    const rows = text.slice(header.index + header[0].length).split(/^\S/m)[0] ?? ''
    const lines = rows.split('\n').map((line) => line.trim().split(','))
    const cellValue = (cells: string[], i: number) => (/^\d+$/.test(cells[i] ?? '') ? Number(cells[i]) : 0)
    const totals = names.flatMap((name, i) => (counts.includes(name) ? [sumBy(lines, (cells) => cellValue(cells, i))] : []))
    out.aggregated = Math.max(0, ...totals)
    return out
  }
  if (/^query_type:\s*traversal/m.test(text)) out.kind = 'traversal'
  const nodeBlock = text.split(/^nodes:\s*$/m)[1]?.split(/^\S/m)[0] ?? ''
  const sets = [...nodeBlock.matchAll(/^\s+(\w+)\[(\d+)\]\{/gm)].map(([, type = '', count = '0']) => ({ type, count: Number(count) }))
  for (const { type, count } of sets) out.byEntity[type] = (out.byEntity[type] ?? 0) + count
  out.nodes = sumBy(sets, (set) => set.count)
  if (sets.length) out.kind = 'traversal'
  out.edges = Number(text.match(/^edges\[(\d+)\]/m)?.[1] ?? 0)
  return out
}

export function apiCallsFor(counts: ResponseCounts): number {
  const pages = (records: number) => Math.max(1, Math.ceil(records / 100))
  if (counts.kind === 'traversal') return pages(Math.max(0, ...Object.values(counts.byEntity)))
  if (counts.kind === 'aggregation') return counts.grouped ? pages(counts.aggregated) : 1
  return 0
}

function tryJson(text: string): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(text)
    return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : null
  } catch {
    return null
  }
}

export function graphStatus(json: string): { total: number; entities: string[] } | null {
  try {
    type Entity = { name?: unknown; count?: unknown }
    const domains = (JSON.parse(json) as { itemCounts?: { domains?: { entities?: Entity[] }[] } }).itemCounts?.domains
    if (!Array.isArray(domains)) return null
    const entities = domains.flatMap((domain) => domain.entities ?? [])
    return {
      total: sumBy(entities, (entity) => Number(entity.count) || 0),
      entities: entities.map((entity) => String(entity.name ?? '')).filter(Boolean),
    }
  } catch {
    return null
  }
}

export function grouped(n: number): string {
  return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ',')
}

export function compact(n: number): string {
  if (n < 1000) return String(n)
  if (n < 10000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  if (n < 1e6) return Math.round(n / 1000) + 'k'
  return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M'
}
