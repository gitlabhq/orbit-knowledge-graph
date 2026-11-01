import { spawnSync } from 'node:child_process'
import { appendFileSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { COMMAND_LABELS, MARK, QUERY_LABEL, describeOrbitCall, othersText, type OrbitCall } from '../core/calls.ts'
import { hopsOf, queryOf, scopesOf } from '../core/cypher.ts'
import { sumBy } from '../core/lib.js'
import { apiCallsFor, compact, countedTotal, graphStatus, grouped, identityLookups, parseResponse, scopeIdentity, type ScopeIdentity, type ScopeTotal } from '../core/stats.ts'
import { colorsOn, paint } from './style.ts'

export type HookInput = {
  hook_event_name?: unknown
  session_id?: unknown
  turn_id?: unknown
  tool_name?: unknown
  tool_input?: unknown
  tool_response?: unknown
}

export type Tally = { calls: number; nodes: number; edges: number; apiCalls: number }

// A count file holds a tally line per Orbit call, a marker after each summary shown, and the graph
// lookups: a line when a scope is first asked for, then its graph total and entity names.
type Line = Partial<Tally> & { shown?: boolean; asked?: string; scope?: string; total?: number; identity?: ScopeIdentity; entities?: string[] }

export type Run = (argv: readonly string[]) => { exitCode: number; stdout: string }

const STALE_MS = 7 * 24 * 60 * 60 * 1000
const MAX_PROJECT_TOTALS = 8
const SCHEMA_LABEL = COMMAND_LABELS['schema']?.done

// Codex keeps the head and tail of long output and says so before the head.
const CUT_HEADER = /^(?:Warning: truncated output \(.*\)\n)?(?:Total output lines: \d+\n)?(?:\.\.\. \d+ bytes omitted \.\.\.\n)?\n?/

// Query and schema output starts with `query_type:`, `domains[`, or JSON. These are how the Orbit CLI's
// errors start (crates/orbit-cli/src/remote/error.rs), so they mark a failed call without guessing.
const ORBIT_ERROR = /^\s*(?:Orbit |Error: |error: |not authenticated|rate limited|\w+ not found\b)/

function text(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function isTally(line: Line): boolean {
  return typeof line.calls === 'number'
}

// Codex hands shell output over as a string and MCP results as a CallToolResult.
function responseText(response: unknown): string | null {
  if (typeof response === 'string') return response
  if (typeof response !== 'object' || response === null) return ''
  const { content, structuredContent, isError } = response as { content?: unknown; structuredContent?: unknown; isError?: unknown }
  if (isError === true) return null
  const blocks: unknown[] = Array.isArray(content) ? content : []
  const joined = blocks
    .flatMap((block) => {
      const blockText = (block as { text?: unknown } | null)?.text
      return typeof blockText === 'string' ? [blockText] : []
    })
    .join('\n')
  return joined || (structuredContent === undefined || structuredContent === null ? '' : JSON.stringify(structuredContent))
}

// Null for a call that is not to Orbit or that failed: as in Claude Code, a failed call is not counted.
export function tallyOf(input: HookInput): Tally | null {
  const call = describeOrbitCall(text(input.tool_name), input.tool_input)
  if (!call) return null
  const response = responseText(input.tool_response)
  if (response === null) return null
  const isGraphRead = call.label === QUERY_LABEL || call.label === SCHEMA_LABEL
  if (isGraphRead && ORBIT_ERROR.test(response.replace(CUT_HEADER, ''))) return null
  // Codex leaves the exit code out of shell results, so other failed Orbit commands count as calls without nodes.
  const tally: Tally = { calls: 1, nodes: 0, edges: 0, apiCalls: 0 }
  if (call.label !== QUERY_LABEL) return tally
  const command = text((input.tool_input as { command?: unknown } | null | undefined)?.command)
  // Codex keeps only the head and tail of long output, so headers in the cut middle are lost and the
  // counts are a lower bound. They never overcount, because the cut only removes text.
  const parsed = parseResponse(response)
  tally.nodes = parsed.nodes + parsed.aggregated
  tally.edges = parsed.edges + parsed.aggregated * hopsOf(queryOf(command))
  tally.apiCalls = apiCallsFor(parsed)
  return tally
}

// Codex has no way to redraw its tool rows, so each Orbit call gets a message with the row the Claude
// Code plugin draws in place of the command.
export function rowOf(call: OrbitCall, failed: boolean, colors: boolean): string {
  return [
    paint(MARK, { color: failed ? 'error' : 'blue', bold: true }, colors),
    ' ',
    paint(call.label, { color: 'blue', bold: true }, colors),
    call.detail ? paint('  ' + call.detail, { dim: true }, colors) : '',
    paint(othersText(call), { color: 'warning', dim: true }, colors),
  ].join('')
}

// The words of the Claude Code plugin's status line.
export function summaryOf({ calls, nodes, edges, apiCalls }: Tally, colors: boolean, graphTotal = 0): string {
  const words: [string, string][] = [
    [compact(calls), calls === 1 ? ' query' : ' queries'],
    ['~' + compact(apiCalls), apiCalls === 1 ? ' API call replaced' : ' API calls replaced'],
    graphTotal ? [compact(nodes) + ' / ' + grouped(graphTotal), ' nodes'] : [compact(nodes), nodes === 1 ? ' node' : ' nodes'],
    [compact(edges), edges === 1 ? ' edge' : ' edges'],
  ]
  const parts = words.flatMap(([value, unit], i) => [
    ...(i > 0 ? [paint('  ·  ', { dim: true }, colors)] : []),
    paint(value, { color: 'blue' }, colors),
    paint(unit, { dim: true }, colors),
  ])
  return parts.join('') + paint('  ' + MARK + ' GitLab Orbit', { color: 'blue' }, colors)
}

export function stateDir(env: Record<string, string | undefined> = process.env): string {
  const data = env['PLUGIN_DATA']
  return data ? join(data, 'sessions') : join(tmpdir(), 'gitlab-orbit-codex')
}

function sessionFile(dir: string, session: string): string | null {
  return /^[\w-]{1,128}$/.test(session) ? join(dir, session + '.jsonl') : null
}

function readLines(file: string): Line[] {
  let raw = ''
  try {
    raw = readFileSync(file, 'utf8')
  } catch {
    return []
  }
  return raw.split('\n').flatMap((line) => {
    try {
      return line ? [JSON.parse(line) as Line] : []
    } catch {
      return []
    }
  })
}

function append(dir: string, file: string, lines: readonly object[]): void {
  mkdirSync(dir, { recursive: true })
  appendFileSync(file, lines.map((line) => JSON.stringify(line) + '\n').join(''))
}

// Stop reports every call since the last summary rather than the turn's own calls: a subagent shares
// the session but runs its tools under its own turn and ends with SubagentStop, not Stop.
function summary(dir: string, file: string, colors: boolean): string | null {
  const lines = readLines(file)
  if (!lines.slice(lines.findLastIndex((line) => line.shown) + 1).some(isTally)) return null
  const tallies = lines.filter(isTally)
  const sum = (key: keyof Tally) => sumBy(tallies, (tally) => Number(tally[key]) || 0)
  const scopes = lines.flatMap((line): ScopeTotal[] =>
    line.scope && typeof line.total === 'number' ? [{ key: line.scope, total: line.total, ...(line.identity ? { identity: line.identity } : {}) }] : [],
  )
  append(dir, file, [{ shown: true }])
  return summaryOf({ calls: sum('calls'), nodes: sum('nodes'), edges: sum('edges'), apiCalls: sum('apiCalls') }, colors, countedTotal(scopes))
}

// SessionEnd never runs for a session Codex did not shut down, so old count files go with the next one.
function removeSession(dir: string, file: string): void {
  rmSync(file, { force: true })
  let names: string[] = []
  try {
    names = readdirSync(dir)
  } catch {}
  for (const name of names) {
    const path = join(dir, name)
    try {
      if (name.endsWith('.jsonl') && Date.now() - statSync(path).mtimeMs > STALE_MS) rmSync(path, { force: true })
    } catch {}
  }
}

// One line per Orbit call: concurrent PostToolUse hooks append instead of rewriting a shared count.
export function handle(input: HookInput, dir: string, colors: boolean = colorsOn()): Record<string, unknown> | null {
  const file = sessionFile(dir, text(input.session_id))
  if (!file) return null
  const event = text(input.hook_event_name)
  if (event === 'PostToolUse') {
    const known = new Set(readLines(file).flatMap((line) => line.entities ?? []))
    const call = describeOrbitCall(text(input.tool_name), input.tool_input, known)
    if (!call) return null
    const tally = tallyOf(input)
    if (tally) append(dir, file, [tally])
    return { systemMessage: rowOf(call, tally === null, colors) }
  }
  if (event === 'Stop') {
    const message = summary(dir, file, colors)
    return message ? { systemMessage: message } : null
  }
  if (event === 'SessionEnd') removeSession(dir, file)
  return null
}

// The graph lookups of the Claude Code status line: the total of each project or group a query names,
// with its identity so nested scopes count once, and the entity names that sharpen each row's path.
// Codex runs this from an async hook, so the glab calls never hold up the agent.
export function lookUp(input: HookInput, dir: string, run: Run): void {
  const file = sessionFile(dir, text(input.session_id))
  if (!file || text(input.hook_event_name) !== 'PostToolUse') return
  if (describeOrbitCall(text(input.tool_name), input.tool_input)?.label !== QUERY_LABEL || !tallyOf(input)) return
  const asked = new Set(readLines(file).flatMap((line) => (typeof line.asked === 'string' ? [line.asked] : [])))
  const command = text((input.tool_input as { command?: unknown } | null | undefined)?.command)
  const lookUps: { key: string; flags: readonly string[] }[] = scopesOf(queryOf(command))
    .filter((scope) => !asked.has(scope.key))
    .slice(0, MAX_PROJECT_TOTALS)
  const entitiesAsked = asked.has('')
  if (!entitiesAsked && lookUps.length === 0) lookUps.push({ key: '', flags: [] })
  if (lookUps.length === 0) return
  // Recorded before the slow calls, so a parallel query does not look the same scope up again.
  append(dir, file, [...lookUps.map(({ key }) => ({ asked: key })), ...(entitiesAsked || lookUps.some(({ key }) => !key) ? [] : [{ asked: '' }])])
  for (const { key, flags } of lookUps) {
    let identity: ScopeIdentity | null = null
    for (const { kind, argv } of key ? identityLookups(flags) : []) {
      const result = run(argv)
      identity = result.exitCode === 0 ? scopeIdentity(result.stdout, kind) : null
      if (identity) break
    }
    for (const launcher of [['glab', 'orbit'], ['orbit']]) {
      const result = run([...launcher, 'graph-status', ...flags, '--response-format', 'raw'])
      const graph = result.exitCode === 0 ? graphStatus(result.stdout) : null
      if (!graph) continue
      append(dir, file, [{ scope: key, ...(key ? { total: graph.total } : {}), ...(identity ? { identity } : {}), entities: graph.entities }])
      break
    }
  }
}

export const runCommand: Run = (argv) => {
  const result = spawnSync(argv[0] ?? '', argv.slice(1), { encoding: 'utf8', timeout: 30_000 })
  return { exitCode: result.status ?? 1, stdout: result.stdout ?? '' }
}

export function main(stdin: string, dir: string = stateDir(), colors: boolean = colorsOn()): string {
  try {
    const output = handle(JSON.parse(stdin) as HookInput, dir, colors)
    return output ? JSON.stringify(output) : ''
  } catch {
    // A hook failure must never fail or block the agent's tool call.
    return ''
  }
}

export function lookUpMain(stdin: string, dir: string = stateDir(), run: Run = runCommand): void {
  try {
    lookUp(JSON.parse(stdin) as HookInput, dir, run)
  } catch {}
}
