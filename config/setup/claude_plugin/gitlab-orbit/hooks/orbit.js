export const BLUE = '#949dcb'
export const FADE_STOPS = ['#b5dedc', BLUE, '#6851ac']

export const MARK = '◎'

function channels(hex) {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}

export function fade(t) {
  const span = Math.min(1, Math.max(0, t)) * (FADE_STOPS.length - 1)
  const i = Math.min(FADE_STOPS.length - 2, Math.floor(span))
  const a = channels(FADE_STOPS[i])
  const b = channels(FADE_STOPS[i + 1])
  const f = span - i
  return '#' + a.map((c, k) => Math.round(c + (b[k] - c) * f).toString(16).padStart(2, '0')).join('')
}

const SUBCOMMANDS = {
  query: ['Explored Orbit', 'Exploring Orbit'],
  ontology: ['Read Orbit schema', 'Reading Orbit schema'],
  dsl: ['Read query syntax', 'Reading query syntax'],
  tools: ['Listed Orbit tools', 'Listing Orbit tools'],
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

const MCP_COMMANDS = {
  query_graph: SUBCOMMANDS.query,
  get_graph_schema: SUBCOMMANDS.ontology,
  get_query_dsl: SUBCOMMANDS.dsl,
  get_response_format: ['Read response format', 'Reading response format'],
  list_commands: SUBCOMMANDS.tools,
}

function clip(text, max = 60) {
  const flat = String(text).replace(/\s+/g, ' ').trim()
  return flat.length > max ? flat.slice(0, max - 1) + '…' : flat
}

// The paths a graph query walks, one per MATCH pattern, as `Definition → Definition` or
// `Project ← MergeRequest`, arrows pointing the way the relationship does. Reads Cypher patterns,
// and for the older JSON DSL the entity names in order.
function entitiesOf(text) {
  const query = String(text)
  const labels = {}
  for (const m of query.matchAll(/\(\s*(\w+)\s*:\s*(\w+)/g)) labels[m[1]] ??= m[2]
  const paths = []
  for (const segment of query.split(/\bMATCH\b/i).slice(1)) {
    const pattern = segment.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0]
    for (const part of splitTopLevel(pattern)) {
      let path = ''
      for (const m of part.matchAll(/\(([^)]*)\)|(<)?-(?:\[[^\]]*\])?-(>)?/g)) {
        if (m[1] !== undefined) {
          const [, name, label] = m[1].match(/^\s*(\w*)\s*(?::\s*(\w+))?/) ?? []
          path += label || labels[name] || 'any'
        } else path += m[2] ? ' ← ' : m[3] ? ' → ' : ' — '
      }
      if (path && !paths.includes(path)) paths.push(path)
    }
  }
  // A lone node that a longer path already walks adds nothing
  const walks = paths.filter((p) => / [←→—] /.test(p))
  const kept = paths.filter((p) => / [←→—] /.test(p) || !walks.some((w) => w.split(/ [←→—] /).includes(p)))
  if (kept.length) return kept.join(', ')
  const names = []
  for (const m of query.matchAll(/\\?"entity\\?"\s*:\s*\\?"(\w+)/g)) if (!names.includes(m[1])) names.push(m[1])
  return names.join(' → ')
}

// A MATCH clause's comma-separated patterns, split only outside (), [], and {}
function splitTopLevel(text) {
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

function firstArg(rest) {
  const m = rest.match(/^\s*(?:-[\w-]+(?:[ =]\S+)?\s+)*(?:'([^']*)'|"([^"]*)"|(\S+))/)
  return m ? m[1] ?? m[2] ?? m[3] : ''
}

function detailFor(sub, rest) {
  if (sub === 'query') {
    const schema = rest.match(/CALL\s+db\.schema\(\s*'?(\w*)/i)
    if (schema) return schema[1]
    const entities = entitiesOf(rest)
    if (entities) return entities
    const file = rest.match(/(?:--file|-f)[ =](\S+)/)
    return file ? file[1] : ''
  }
  if (['grep', 'context', 'sql', 'repo-map', 'graph-status', 'skills'].includes(sub)) {
    const arg = firstArg(sub === 'skills' ? rest.replace(/^\s*get\b/, '') : rest)
    return arg && !arg.startsWith('-') ? clip(arg, 48) : ''
  }
  return ''
}

export function describeOrbitCall(tool, input) {
  if (tool === 'Bash' && input && typeof input.command === 'string') {
    const m = input.command.match(/\bglab\s+orbit\s+([\w-]+)?([\s\S]*)/)
    if (!m) return null
    const sub = m[1] || ''
    const isSchemaQuery = sub === 'query' && /CALL\s+db\.schema\(/i.test(m[2] || '')
    const [label, active] = isSchemaQuery ? SUBCOMMANDS.ontology : SUBCOMMANDS[sub] || SUBCOMMANDS.query
    return { label, active, detail: detailFor(sub, m[2] || '') }
  }
  const mcp = tool.match(/^mcp__(.+?)__(.+)$/)
  const server = mcp ? mcp[1] : ''
  const name = mcp ? mcp[2] : tool
  const isOrbit = /orbit|knowledge[-_]?graph|gkg/i.test(server) || /orbit|query_graph|get_graph_schema/i.test(name)
  if (!isOrbit) return null
  const command = (input && (input.command_name || input.command)) || name
  const key = Object.keys(MCP_COMMANDS).find((k) => String(command).endsWith(k))
  const [label, active] = key ? MCP_COMMANDS[key] : SUBCOMMANDS.query
  const detail = input ? entitiesOf(JSON.stringify(input)) : ''
  return { label, active, detail }
}
