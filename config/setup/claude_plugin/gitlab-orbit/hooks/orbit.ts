export const BLUE = '#949dcb'
export const FADE_STOPS = ['#b5dedc', BLUE, '#6851ac'] as const

export const MARK = '◎'

export const QUERY_LABEL = 'Explored Orbit'

/** A tool call that reads Orbit: its finished label, its running label, and what it read. */
export type OrbitCall = { label: string; active: string; detail: string }

type Labels = readonly [label: string, active: string]

function channels(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}

/** The color at `t` along FADE_STOPS, from the first stop at 0 to the last at 1. */
export function fade(t: number): string {
  const span = Math.min(1, Math.max(0, t)) * (FADE_STOPS.length - 1)
  const i = Math.min(FADE_STOPS.length - 2, Math.floor(span))
  const a = channels(FADE_STOPS[i] ?? BLUE)
  const b = channels(FADE_STOPS[i + 1] ?? BLUE)
  const f = span - i
  return '#' + a.map((c, k) => Math.round(c + ((b[k] ?? c) - c) * f).toString(16).padStart(2, '0')).join('')
}

const QUERY: Labels = [QUERY_LABEL, 'Exploring Orbit']
const SCHEMA: Labels = ['Read Orbit schema', 'Reading Orbit schema']
const SYNTAX: Labels = ['Read query syntax', 'Reading query syntax']
const TOOLS: Labels = ['Listed Orbit tools', 'Listing Orbit tools']

export const SUBCOMMANDS: Readonly<Record<string, Labels>> = {
  query: QUERY,
  ontology: SCHEMA,
  dsl: SYNTAX,
  tools: TOOLS,
  status: ['Checked Orbit', 'Checking Orbit'],
  'graph-status': ['Checked indexing', 'Checking indexing'],
  grep: ['Searched code', 'Searching code'],
  context: ['Read code', 'Reading code'],
  sql: ['Queried local graph', 'Querying local graph'],
  schema: ['Read local schema', 'Reading local schema'],
  list: ['Listed repos', 'Listing repos'],
  'repo-map': ['Mapped repo', 'Mapping repo'],
  index: ['Indexed repo', 'Indexing repo'],
  skills: ['Read Orbit guide', 'Reading Orbit guide'],
  setup: ['Set up Orbit', 'Setting up Orbit'],
  config: ['Read Orbit settings', 'Reading Orbit settings'],
  version: ['Checked version', 'Checking version'],
}

const MCP_COMMANDS: Readonly<Record<string, Labels>> = {
  query_graph: QUERY,
  get_graph_schema: SCHEMA,
  get_query_dsl: SYNTAX,
  get_response_format: ['Read response format', 'Reading response format'],
  list_commands: TOOLS,
}

function clip(text: string, max = 60): string {
  const flat = text.replace(/\s+/g, ' ').trim()
  return flat.length > max ? flat.slice(0, max - 1) + '…' : flat
}

type Hop = 'left' | 'right' | 'none'

/** A path as its nodes and the text it reads as. */
type Path = { nodes: string[]; text: string }

// The data a graph query explores, one path per MATCH pattern, read from source to target, as
// `User → MergeRequest → Project`. Repeated node types collapse into one, so a call graph reads
// `Definition`. A pattern written right to left reads reversed; one with arrows both ways splits
// into its left-to-right pieces. `—` is an undirected hop. For the older JSON DSL, the entity
// names in order.
export function entitiesOf(text: string): string {
  const labels: Record<string, string> = {}
  for (const m of text.matchAll(/\(\s*(\w+)\s*:\s*(\w+)/g)) {
    if (m[1] && m[2]) labels[m[1]] ??= m[2]
  }
  const paths: Path[] = []
  for (const segment of text.split(/\bMATCH\b/i).slice(1)) {
    const pattern = segment.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? ''
    for (const part of splitTopLevel(pattern)) {
      const nodes: string[] = []
      const hops: Hop[] = []
      for (const m of part.matchAll(/\(([^)]*)\)|(<)?-(?:\[[^\]]*\])?-(>)?/g)) {
        if (m[1] !== undefined) {
          const [, name = '', label] = m[1].match(/^\s*(\w*)\s*(?::\s*(\w+))?/) ?? []
          nodes.push(label || labels[name] || 'any')
        } else hops.push(m[2] ? 'left' : m[3] ? 'right' : 'none')
      }
      for (const path of readForward(nodes, hops)) {
        if (!paths.some((p) => p.text === path.text)) paths.push(path)
      }
    }
  }
  // A lone node that a longer path already walks adds nothing
  const walks = paths.filter((p) => p.nodes.length > 1)
  const kept = paths.filter((p) => p.nodes.length > 1 || !walks.some((w) => w.nodes.includes(p.text)))
  if (kept.length) return kept.map((p) => p.text).join(', ')
  const names: string[] = []
  for (const m of text.matchAll(/\\?"entity\\?"\s*:\s*\\?"(\w+)/g)) {
    if (m[1] && !names.includes(m[1])) names.push(m[1])
  }
  return names.join(' → ')
}

// One pattern's nodes and hops as paths that read left to right: a run of hops pointing the same
// way is one path, and a run pointing left is turned around
function readForward(nodes: string[], hops: Hop[]): Path[] {
  if (hops.length === 0) return nodes[0] ? [{ nodes: [nodes[0]], text: nodes[0] }] : []
  const paths: Path[] = []
  let start = 0
  for (let i = 1; i <= hops.length; i++) {
    if (i < hops.length && hops[i] === hops[start]) continue
    const run = nodes.slice(start, i + 1)
    if (hops[start] === 'left') run.reverse()
    const distinct = run.filter((node, k) => node !== run[k - 1])
    paths.push({ nodes: distinct, text: distinct.join(hops[start] === 'none' ? ' — ' : ' → ') })
    start = i
  }
  return paths
}

// A MATCH clause's comma-separated patterns, split only outside (), [], and {}
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

function firstArg(rest: string): string {
  const m = rest.match(/^\s*(?:-[\w-]+(?:[ =]\S+)?\s+)*(?:'([^']*)'|"([^"]*)"|(\S+))/)
  return m ? (m[1] ?? m[2] ?? m[3] ?? '') : ''
}

function detailFor(sub: string, rest: string): string {
  if (sub === 'query') {
    const schema = rest.match(/CALL\s+db\.schema\(\s*'?(\w*)/i)
    if (schema) return schema[1] ?? ''
    const entities = entitiesOf(rest)
    if (entities) return entities
    return rest.match(/(?:--file|-f)[ =](\S+)/)?.[1] ?? ''
  }
  if (['grep', 'context', 'sql', 'repo-map', 'graph-status', 'skills'].includes(sub)) {
    const arg = firstArg(sub === 'skills' ? rest.replace(/^\s*get\b/, '') : rest)
    return arg && !arg.startsWith('-') ? clip(arg, 48) : ''
  }
  return ''
}

function stringField(input: unknown, key: string): string | undefined {
  if (typeof input !== 'object' || input === null) return undefined
  const value = (input as Record<string, unknown>)[key]
  return typeof value === 'string' ? value : undefined
}

/** The Orbit call a tool call makes, or null when it isn't one. */
export function describeOrbitCall(tool: string, input: unknown): OrbitCall | null {
  const command = stringField(input, 'command')
  if (tool === 'Bash' && command !== undefined) {
    const m = command.match(/\bglab\s+orbit\s+([\w-]+)?([\s\S]*)/)
    if (!m) return null
    const sub = m[1] ?? ''
    const rest = m[2] ?? ''
    const isSchemaQuery = sub === 'query' && /CALL\s+db\.schema\(/i.test(rest)
    const [label, active] = isSchemaQuery ? SCHEMA : (SUBCOMMANDS[sub] ?? QUERY)
    return { label, active, detail: detailFor(sub, rest) }
  }
  const mcp = tool.match(/^mcp__(.+?)__(.+)$/)
  const server = mcp?.[1] ?? ''
  const name = mcp?.[2] ?? tool
  const isOrbit = /orbit|knowledge[-_]?graph|gkg/i.test(server) || /orbit|query_graph|get_graph_schema/i.test(name)
  if (!isOrbit) return null
  const invoked = stringField(input, 'command_name') ?? command ?? name
  const key = Object.keys(MCP_COMMANDS).find((k) => invoked.endsWith(k))
  const [label, active] = (key && MCP_COMMANDS[key]) || QUERY
  const detail = input === undefined ? '' : entitiesOf(JSON.stringify(input))
  return { label, active, detail }
}
