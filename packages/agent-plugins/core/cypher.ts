import { orbitInvocations } from './command.ts'
import { sumBy, uniqBy } from './lib.js'

export function queryOf(command: string): string {
  for (const words of orbitInvocations(command)) {
    if (words[0] !== 'query' || words.includes('--file')) continue
    for (let i = 1; i < words.length; i++) {
      const word = words[i] ?? ''
      if (word === '--response-format') i++
      else if (!word.startsWith('-')) return word
    }
  }
  return ''
}

export type Segment = { kind: 'prose' | 'code'; text: string }

const CYPHER_FENCE = /^```[ \t]*(?:cypher|opencypher|gql)[ \t]*\n([\s\S]*?)^```[ \t]*$/gim

export function cypherSegments(markdown: string): Segment[] {
  const out: Segment[] = []
  let at = 0
  for (const m of markdown.matchAll(CYPHER_FENCE)) {
    const prose = markdown.slice(at, m.index).trim()
    if (prose) out.push({ kind: 'prose', text: prose })
    out.push({ kind: 'code', text: m[1] ?? '' })
    at = (m.index ?? 0) + m[0].length
  }
  const rest = markdown.slice(at).trim()
  if (rest) out.push({ kind: 'prose', text: rest })
  return out
}

export function hopsOf(query: string): number {
  const patterns = query.split(/\bMATCH\b/i).slice(1).map((clause) => clause.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? '')
  const hops = patterns.flatMap((pattern) => [...pattern.matchAll(/<?-(?:\[([^\]]*)\])?->?/g)])
  return sumBy(hops, (hop) => {
    const range = (hop[1] ?? '').match(/\*\s*(\d+)?/)
    return range ? Number(range[1] ?? 1) : 1
  })
}

type Hop = 'left' | 'right' | 'none'

type PathNode = { type: string; one: boolean }

type Path = { nodes: PathNode[]; text: string }

export const NO_ENTITIES: ReadonlySet<string> = new Set()

const UNIQUE_KEYS = new Set(['id', 'full_path', 'username'])

type Pin = { score: number; one: boolean; implied?: string }

function entityOfKey(key: string, known: ReadonlySet<string>): string | undefined {
  const m = key.match(/^(\w+)_id$/)
  if (!m?.[1]) return undefined
  const name = m[1].split('_').map((w) => (w[0]?.toUpperCase() ?? '') + w.slice(1)).join('')
  return known.has(name) ? name : undefined
}

function pinOf(keys: readonly string[], equals: readonly string[], label: string, known: ReadonlySet<string>): Pin {
  const one = equals.some((key) => UNIQUE_KEYS.has(key))
  if (keys.length === 0) return { score: 0, one }
  const implied = keys.map((key) => entityOfKey(key, known)).find((name) => name && name !== label)
  return { score: keys.includes('id') ? 2 : 1, one, ...(implied ? { implied } : {}) }
}

export function pluralize(word: string): string {
  if (/[^aeiou]y$/i.test(word)) return word.slice(0, -1) + 'ies'
  if (/(s|x|z|ch|sh)$/i.test(word)) return word + 'es'
  return word + 's'
}

export function displayOf({ type, one }: PathNode): string {
  if (type === 'any') return 'Anything'
  const words = type.split(/(?<=[a-z0-9])(?=[A-Z])/).map((w, i) => (i === 0 ? w : w.toLowerCase()))
  if (!one) words[words.length - 1] = pluralize(words[words.length - 1] ?? '')
  return words.join(' ')
}

function pathOf(run: readonly PathNode[], separator = ' → '): Path {
  const nodes: PathNode[] = []
  for (const node of run) {
    const last = nodes[nodes.length - 1]
    if (last?.type === node.type) last.one &&= node.one
    else nodes.push({ ...node })
  }
  return { nodes, text: nodes.map(displayOf).join(separator) }
}

export function entitiesOf(text: string, known: ReadonlySet<string> = NO_ENTITIES): string {
  const labels: Record<string, string> = {}
  for (const m of text.matchAll(/\(\s*(\w+)\s*:\s*(\w+)/g)) {
    if (m[1] && m[2]) labels[m[1]] ??= m[2]
  }
  const whereKeys: Record<string, string[]> = {}
  const whereEquals: Record<string, string[]> = {}
  for (const clause of text.split(/\bWHERE\b/i).slice(1)) {
    const where = clause.split(/\b(?:MATCH|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT)\b/i)[0] ?? ''
    for (const m of where.matchAll(/\b(\w+)\.(\w+)\s*(=|<>|!=|<=|>=|<|>|\bIN\b|\bSTARTS\b|\bENDS\b|\bCONTAINS\b)/gi)) {
      if (!m[1] || !m[2]) continue
      ;(whereKeys[m[1]] ??= []).push(m[2])
      if (m[3] === '=') (whereEquals[m[1]] ??= []).push(m[2])
    }
  }
  const paths: Path[] = []
  for (const segment of text.split(/\bMATCH\b/i).slice(1)) {
    const pattern = segment.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? ''
    for (const part of splitTopLevel(pattern)) {
      const nodes: string[] = []
      const pins: Pin[] = []
      const hops: Hop[] = []
      for (const m of part.matchAll(/\(([^)]*)\)|(<)?-(?:\[[^\]]*\])?-(>)?/g)) {
        if (m[1] !== undefined) {
          const [, name = '', label] = m[1].match(/^\s*(\w*)\s*(?::\s*(\w+))?/) ?? []
          const node = label || labels[name] || 'any'
          const inline = [...(m[1].match(/\{([^}]*)\}/)?.[1] ?? '').matchAll(/(\w+)\s*:/g)].map((k) => k[1] ?? '')
          const where = name ? (whereKeys[name] ?? []) : []
          const equals = name ? (whereEquals[name] ?? []) : []
          nodes.push(node)
          pins.push(pinOf([...inline, ...where], [...inline, ...equals], node, known))
        } else hops.push(m[2] ? 'left' : m[3] ? 'right' : 'none')
      }
      const typed = nodes.map((type, i) => ({ type, one: pins[i]?.one ?? false }))
      paths.push(...(readFromPin(typed, pins) ?? readForward(typed, hops)))
    }
  }
  const unique = uniqBy(paths, (p) => p.text)
  const walks = unique.filter((p) => p.nodes.length > 1)
  const kept = unique.filter((p) => p.nodes.length > 1 || !walks.some((w) => w.nodes.some((n) => n.type === p.nodes[0]?.type)))
  if (kept.length) return kept.map((p) => p.text).join(', ')
  const names = uniqBy([...text.matchAll(/\\?"entity\\?"\s*:\s*\\?"(\w+)/g)].map((m) => m[1] ?? ''), (name) => name).filter(Boolean)
  return names.map((type) => displayOf({ type, one: false })).join(' → ')
}

function readFromPin(nodes: readonly PathNode[], pins: readonly Pin[]): Path[] | null {
  const best = Math.max(0, ...pins.map((p) => p.score))
  const at = pins.findIndex((p) => p.score === best)
  if (best === 0 || pins.filter((p) => p.score === best).length > 1) return null
  const implied = pins[at]?.implied
  const sides: PathNode[][] = []
  if (at < nodes.length - 1) sides.push(nodes.slice(at))
  if (at > 0) sides.push(nodes.slice(0, at + 1).reverse())
  if (sides.length === 0) sides.push(nodes.slice(at, at + 1))
  return sides.map((side) => pathOf(implied ? [{ type: implied, one: true }, ...side] : side))
}

function readForward(nodes: readonly PathNode[], hops: Hop[]): Path[] {
  if (hops.length === 0) return nodes[0] ? [pathOf([nodes[0]])] : []
  const paths: Path[] = []
  let start = 0
  for (let i = 1; i <= hops.length; i++) {
    if (i < hops.length && hops[i] === hops[start]) continue
    const run = nodes.slice(start, i + 1)
    if (hops[start] === 'left') run.reverse()
    paths.push(pathOf(run, hops[start] === 'none' ? ' — ' : ' → '))
    start = i
  }
  return paths
}

function splitTopLevel(text: string): string[] {
  const parts = ['']
  let depth = 0
  for (const ch of text) {
    if ('([{'.includes(ch)) depth++
    else if (')]}'.includes(ch)) depth--
    if (ch === ',' && depth === 0) parts.push('')
    else parts[parts.length - 1] += ch
  }
  return parts.map((p) => p.trim()).filter(Boolean)
}

export type GraphScope = { key: string; flags: [string, string] }

function listed(text: string): string[] {
  return [...text.matchAll(/'([^']*)'|"([^"]*)"|(\d+)/g)].map((m) => m[1] ?? m[2] ?? m[3] ?? '')
}

export function scopesOf(query: string): GraphScope[] {
  const ids: string[] = []
  const groupIds: string[] = []
  const paths: string[] = []
  for (const m of query.matchAll(/\bproject_id\s*(?::|=)\s*(\d+)/g)) ids.push(m[1] ?? '')
  for (const m of query.matchAll(/\bproject_id\s+IN\s*\[([^\]]*)\]/gi)) ids.push(...listed(m[1] ?? ''))
  for (const m of query.matchAll(/\(\s*(\w*)\s*:\s*(Project|Group)\b\s*(\{[^}]*\})?/g)) {
    const idsOf = m[2] === 'Group' ? groupIds : ids
    const map = m[3] ?? ''
    idsOf.push(...[...map.matchAll(/\bid\s*:\s*(\d+)/g)].map((x) => x[1] ?? ''))
    paths.push(...[...map.matchAll(/\bfull_path\s*:\s*['"]([^'"]+)['"]/g)].map((x) => x[1] ?? ''))
    const name = m[1]
    if (!name) continue
    for (const w of query.matchAll(new RegExp(`\\b${name}\\.(id|full_path)\\s*(=|IN)\\s*(\\[[^\\]]*\\]|'[^']*'|"[^"]*"|\\d+)`, 'gi'))) {
      ;(w[1]?.toLowerCase() === 'id' ? idsOf : paths).push(...listed(w[3] ?? ''))
    }
  }
  const numeric = (id: string) => /^\d+$/.test(id)
  const scopes: GraphScope[] = [
    ...ids.filter(numeric).map((id): GraphScope => ({ key: 'project-id:' + id, flags: ['--project-id', id] })),
    ...groupIds.filter(numeric).map((id): GraphScope => ({ key: 'namespace-id:' + id, flags: ['--namespace-id', id] })),
    ...paths.filter(Boolean).map((path): GraphScope => ({ key: 'full-path:' + path, flags: ['--full-path', path] })),
  ]
  return uniqBy(scopes, (scope) => scope.key)
}
