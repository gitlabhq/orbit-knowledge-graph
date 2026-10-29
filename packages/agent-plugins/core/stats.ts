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
    const counts = plainCountNames(text)
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

type ColumnFields = Record<string, string | undefined>

function columnFields(text: string): ColumnFields[] {
  const section = text.match(/^columns\[\d+\](?:\{([^}]*)\})?:\s*$/m)
  if (!section || section.index === undefined) return []
  const body = text.slice(section.index + section[0].length).split(/^\S/m)[0] ?? ''
  if (section[1] !== undefined) {
    const fieldNames = section[1].split(',')
    return body
      .split('\n')
      .filter((line) => line.trim())
      .map((line) => Object.fromEntries(line.trim().split(',').map((cell, i) => [fieldNames[i] ?? '', cell.trim()])))
  }
  return body
    .split(/^\s+- /m)
    .slice(1)
    .map((item) => Object.fromEntries([...item.matchAll(/^\s*(?:- )?(\w+):\s*(.*)$/gm)].map((m) => [m[1] ?? '', m[2]?.trim()])))
}

// The response has no DISTINCT flag: a property count is plain only under its derived name; any other name reads as DISTINCT.
function plainCountNames(text: string): string[] {
  return columnFields(text)
    .filter(({ function: fn, name, target = '', property }) => fn === 'count' && name && (!property || name === `count_${target}_${property}`))
    .map((column) => column['name'] ?? '')
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

export type ScopeIdentity = { kind: 'group' | 'project'; id: string; path: string }
export type ScopeTotal = { key: string; total: number; identity?: ScopeIdentity }

export function scopeIdentity(json: string, kind: ScopeIdentity['kind']): ScopeIdentity | null {
  try {
    const body = JSON.parse(json) as { id?: unknown; full_path?: unknown; path_with_namespace?: unknown }
    const path = kind === 'group' ? body.full_path : body.path_with_namespace
    if (typeof path !== 'string' || !path || (typeof body.id !== 'number' && typeof body.id !== 'string')) return null
    return { kind, id: String(body.id), path }
  } catch {
    return null
  }
}

export function identityLookups(flags: readonly string[]): { kind: ScopeIdentity['kind']; argv: string[] }[] {
  const [flag, value] = flags
  if (!value) return []
  const lookup = (kind: ScopeIdentity['kind'], collection: string) => ({ kind, argv: ['glab', 'api', collection + '/' + encodeURIComponent(value)] })
  if (flag === '--namespace-id') return [lookup('group', 'groups')]
  if (flag === '--project-id') return [lookup('project', 'projects')]
  return [lookup('group', 'groups'), lookup('project', 'projects')]
}

export function countedTotal(scopes: readonly ScopeTotal[]): number {
  const distinct = new Map<string, ScopeTotal>()
  for (const scope of scopes) distinct.set(scope.identity ? scope.identity.kind + ':' + scope.identity.id : scope.key, scope)
  const groups = [...distinct.values()].flatMap((scope) => (scope.identity?.kind === 'group' ? [scope.identity] : []))
  const isInsideCountedGroup = ({ identity }: ScopeTotal) =>
    identity !== undefined && groups.some((group) => identity.path.startsWith(group.path + '/'))
  return sumBy([...distinct.values()].filter((scope) => !isInsideCountedGroup(scope)), (scope) => scope.total)
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
