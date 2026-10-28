/** What one Orbit response returned. Schema and other non-query output reads as empty. */
export type ResponseCounts = {
  nodes: number
  edges: number
  byEntity: Record<string, number>
  /** The nodes that `count(...)` aggregates covered. */
  aggregated: number
  kind: 'traversal' | 'aggregation' | 'other'
  /** An aggregation with group keys, such as counts by author. */
  grouped: boolean
}

/** Counts one Orbit response, in the CLI's default llm format or the raw JSON format. */
export function parseResponse(text: string): ResponseCounts {
  const out: ResponseCounts = { nodes: 0, edges: 0, byEntity: {}, aggregated: 0, kind: 'other', grouped: false }
  const raw = text.trim().startsWith('{') ? tryJson(text) : null
  if (raw) {
    const r = (raw['result'] ?? raw) as { nodes?: unknown; edges?: unknown }
    for (const n of Array.isArray(r.nodes) ? (r.nodes as { type?: unknown }[]) : []) {
      const type = String(n.type)
      out.byEntity[type] = (out.byEntity[type] ?? 0) + 1
      out.nodes += 1
    }
    out.edges = Array.isArray(r.edges) ? r.edges.length : 0
    if (Array.isArray(r.nodes)) out.kind = 'traversal'
    return out
  }
  if (/^query_type:\s*aggregation/m.test(text)) {
    out.kind = 'aggregation'
    out.grouped = /^group_columns\[[1-9]\d*\]/m.test(text)
    // A row's count columns say how many nodes the aggregate covered; sum, avg, min, and max don't
    const columns = text.split(/^columns\[\d+\]\{[^}]*\}:\s*$/m)[1]?.split(/^\S/m)[0] ?? ''
    const counts = [...columns.matchAll(/^\s+(\w+),count,/gm)].map((m) => m[1])
    const header = text.match(/^rows\[\d+\]\{([^}]*)\}:\s*$/m)
    if (!header || header.index === undefined || counts.length === 0) return out
    const at = (header[1] ?? '').split(',').map((name) => counts.includes(name))
    const rows = text.slice(header.index + header[0].length).split(/^\S/m)[0] ?? ''
    for (const line of rows.split('\n').filter((l) => l.trim())) {
      line
        .trim()
        .split(',')
        .forEach((cell, i) => {
          if (at[i] && /^\d+$/.test(cell)) out.aggregated += Number(cell)
        })
    }
    return out
  }
  if (/^query_type:\s*traversal/m.test(text)) out.kind = 'traversal'
  const nodeBlock = text.split(/^nodes:\s*$/m)[1]?.split(/^\S/m)[0] ?? ''
  for (const m of nodeBlock.matchAll(/^\s+(\w+)\[(\d+)\]\{/gm)) {
    const [, type = '', count = '0'] = m
    out.byEntity[type] = (out.byEntity[type] ?? 0) + Number(count)
    out.nodes += Number(count)
    out.kind = 'traversal'
  }
  out.edges = Number(text.match(/^edges\[(\d+)\]/m)?.[1] ?? 0)
  return out
}

/**
 * The fewest GitLab API requests that read the same data as one Orbit query: GraphQL pages of
 * 100 with joins nested, so no per-record requests. A traversal reads its largest node set; a
 * grouped count has to read every record it counted; a plain count is one request. Schema and
 * other output replace none.
 */
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

/**
 * The project's whole graph as `glab orbit graph-status --response-format raw` reports it: the
 * node counts of every entity in every domain, summed. Null when the output has none.
 */
export function graphNodes(json: string): number | null {
  try {
    const domains = (JSON.parse(json) as { itemCounts?: { domains?: { entities?: { count?: unknown }[] }[] } })
      .itemCounts?.domains
    if (!Array.isArray(domains)) return null
    let total = 0
    for (const domain of domains) for (const entity of domain.entities ?? []) total += Number(entity.count) || 0
    return total
  } catch {
    return null
  }
}

/** A count in full with thousands separators, as 1,675,400. */
export function grouped(n: number): string {
  return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ',')
}

/** A count for the status line, as 950, 1.2k, 12k, or 1.2M. */
export function compact(n: number): string {
  if (n < 1000) return String(n)
  if (n < 10000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  if (n < 1e6) return Math.round(n / 1000) + 'k'
  return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M'
}
