import { orbitInvocations, subcommandOf } from './command.ts'

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
const INDEX: Labels = ['Indexed repo', 'Indexing repo']
const SQL: Labels = ['Queried local graph', 'Querying local graph']
const REPOS: Labels = ['Listed repos', 'Listing repos']
const GUIDE: Labels = ['Read Orbit guide', 'Reading Orbit guide']
const SETTINGS: Labels = ['Read Orbit settings', 'Reading Orbit settings']

// Each key is an orbit command or alias, with its subcommand after a space; a Rust test checks
// the keys against the CLI. Commands not listed here are not Orbit rows.
export const SUBCOMMANDS: Readonly<Record<string, Labels>> = {
  'query': QUERY,
  'schema': SCHEMA,
  'ontology': SCHEMA,
  'status': ['Checked Orbit', 'Checking Orbit'],
  'graph-status': ['Checked indexing', 'Checking indexing'],
  'grep': ['Searched code', 'Searching code'],
  'context': ['Read code', 'Reading code'],
  'workspace index': INDEX,
  'workspace sql': SQL,
  'workspace schema': ['Read local schema', 'Reading local schema'],
  'workspace list': REPOS,
  'index': INDEX,
  'sql': SQL,
  'list': REPOS,
  'skills': GUIDE,
  'skill': GUIDE,
  'setup': ['Set up Orbit', 'Setting up Orbit'],
  'uninstall': ['Removed Orbit setup', 'Removing Orbit setup'],
  'config get': SETTINGS,
  'config list': SETTINGS,
  'config set': ['Changed Orbit settings', 'Changing Orbit settings'],
  'version': ['Checked version', 'Checking version'],
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

// Flags whose value is the next word, so it is not read as the argument
const VALUE_FLAGS = new Set(['--db', '--file', '--response-format', '--full-path', '--namespace-id', '--project-id'])

function firstArg(args: readonly string[]): string {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i] ?? ''
    if (arg === '-e' || arg === '--regexp') return args[i + 1] ?? ''
    if (VALUE_FLAGS.has(arg)) i++
    else if (!arg.startsWith('-')) return arg
  }
  return ''
}

const SHOWS_ARGUMENT = new Set(['grep', 'context', 'workspace sql', 'sql', 'schema', 'ontology', 'graph-status', 'skills', 'skill'])

function detailFor(key: string, args: readonly string[]): string {
  if (key === 'query') {
    const text = args.join(' ')
    const schema = text.match(/CALL\s+db\.schema\(\s*'?(\w*)/i)
    if (schema) return schema[1] ?? ''
    const entities = entitiesOf(text)
    if (entities) return entities
    const file = args.indexOf('--file')
    return file >= 0 ? (args[file + 1] ?? '') : ''
  }
  if (!SHOWS_ARGUMENT.has(key)) return ''
  const arg = firstArg(key.startsWith('skill') && args[0] === 'get' ? args.slice(1) : args)
  return arg ? clip(arg, 48) : ''
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
    for (const { words } of orbitInvocations(command)) {
      const sub = subcommandOf(words, (key) => Object.hasOwn(SUBCOMMANDS, key))
      if (!sub) continue
      const isSchemaQuery = sub.key === 'query' && sub.args.some((arg) => /^\s*CALL\s+db\.schema\(/i.test(arg))
      const [label, active] = isSchemaQuery ? SCHEMA : (SUBCOMMANDS[sub.key] ?? QUERY)
      return { label, active, detail: detailFor(sub.key, sub.args) }
    }
    return null
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
