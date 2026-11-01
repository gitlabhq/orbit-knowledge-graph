import { afterAll, beforeEach, expect, test } from 'bun:test'
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { handle, lookUp, main, stateDir, summaryOf, tallyOf, type HookInput, type Run } from './hook.ts'
import { colorsOn } from './style.ts'

const root = mkdtempSync(join(tmpdir(), 'orbit-codex-hook-'))
let dir = ''

beforeEach(() => {
  dir = mkdtempSync(join(root, 'data-'))
})

afterAll(() => rmSync(root, { recursive: true, force: true }))

const TRAVERSAL = `query_type: traversal
nodes:
  MergeRequest[20]{id,iid}:
    1,2
  Project[1]{id}:
    3
edges[20]{from,from_id,type,to,to_id}:
  MergeRequest,1,IN_PROJECT,Project,3
pagination:
  has_more: false`

const AGGREGATION = `query_type: aggregation
columns[1]{name,function,target}:
  n,count,mr
group_columns[1]{name,kind,node,property,entity}:
  mr_state,property,mr,state,MergeRequest
rows[2]{mr_state,n}:
  merged,2449
  opened,73
pagination:
  has_more: false`

const QUERY = "glab orbit query 'MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN mr, p'"
const BLUE = '\x1b[38;2;148;157;203m'
const DIM = '\x1b[2m'
const RESET = '\x1b[0m'

// The PostToolUse input Codex 0.166 sends for a shell command: the raw command and its output as a string.
function bash(command: string, output: string, turn = 'turn-1'): HookInput {
  return { hook_event_name: 'PostToolUse', session_id: 'ses-1', turn_id: turn, tool_name: 'Bash', tool_input: { command }, tool_response: output }
}

function stop(turn = 'turn-1'): HookInput {
  return { hook_event_name: 'Stop', session_id: 'ses-1', turn_id: turn }
}

test('a graph query counts its nodes, edges, and the API pages it replaced', () => {
  expect(tallyOf(bash(QUERY, TRAVERSAL))).toEqual({ calls: 1, nodes: 21, edges: 20, apiCalls: 1 })
})

test('an aggregation counts the rows it covers and one edge per hop for each', () => {
  const tally = tallyOf(bash("glab orbit query 'MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN mr.state, count(mr) AS n'", AGGREGATION))
  expect(tally).toEqual({ calls: 1, nodes: 2522, edges: 2522, apiCalls: 26 })
})

test('output Codex cut down counts the headers that survive the cut', () => {
  const head = 'Warning: truncated output (original token count: 20510)\nTotal output lines: 1010\n\nquery_type: traversal\nnodes:\n  MergeRequest[500]{id}:\n    1\n'
  const cut = head + '    2…9000 tokens truncated…99\n    500\npagination:\n  has_more: false'
  expect(tallyOf(bash(QUERY, cut))).toEqual({ calls: 1, nodes: 500, edges: 0, apiCalls: 5 })
})

test('other Orbit commands count as calls without nodes', () => {
  expect(tallyOf(bash("glab orbit grep 'auth|login'", 'src/a.rs:1: fn login()'))).toEqual({ calls: 1, nodes: 0, edges: 0, apiCalls: 0 })
})

test('shell commands that do not run Orbit are ignored', () => {
  expect(tallyOf(bash('rg orbit src', 'src/a.rs: orbit'))).toBeNull()
  expect(tallyOf({ ...bash('ls', ''), tool_name: 'apply_patch' })).toBeNull()
})

test('an Orbit MCP tool counts its structured result and ignores an error result', () => {
  const result = { result: { nodes: [{ type: 'User' }, { type: 'User' }], edges: [{}] } }
  const call: HookInput = {
    hook_event_name: 'PostToolUse',
    session_id: 'ses-1',
    turn_id: 'turn-1',
    tool_name: 'mcp__orbit__query_graph',
    tool_input: { query: 'MATCH (u:User) RETURN u' },
    tool_response: { content: [], structuredContent: result, isError: false },
  }
  expect(tallyOf(call)).toEqual({ calls: 1, nodes: 2, edges: 1, apiCalls: 1 })
  const text = { ...call, tool_response: { content: [{ type: 'text', text: JSON.stringify(result) }], isError: false } }
  expect(tallyOf(text)).toMatchObject({ nodes: 2 })
  expect(tallyOf({ ...call, tool_response: { content: [{ type: 'text', text: 'denied' }], isError: true } })).toBeNull()
  expect(tallyOf({ ...call, tool_name: 'mcp__github__search_code' })).toBeNull()
})

test('Stop reports the session total after a turn with new Orbit calls', () => {
  handle(bash(QUERY, TRAVERSAL, 'turn-1'), dir, false)
  expect(handle(stop('turn-1'), dir, false)).toEqual({ systemMessage: '1 query  ·  ~1 API call replaced  ·  21 nodes  ·  20 edges  ◎ GitLab Orbit' })
  handle(bash("glab orbit grep 'x'", '', 'turn-2'), dir, false)
  expect(handle(stop('turn-2'), dir, false)).toEqual({ systemMessage: '2 queries  ·  ~1 API call replaced  ·  21 nodes  ·  20 edges  ◎ GitLab Orbit' })
})

test('Stop stays quiet when no Orbit call happened since the last summary', () => {
  expect(handle(stop('turn-1'), dir, false)).toBeNull()
  handle(bash("glab orbit grep 'x'", '', 'turn-1'), dir, false)
  expect(handle(stop('turn-1'), dir, false)).not.toBeNull()
  handle(bash('ls', 'a', 'turn-2'), dir, false)
  expect(handle(stop('turn-2'), dir, false)).toBeNull()
})

test('calls a subagent makes under its own turn show at the next Stop', () => {
  handle(bash(QUERY, TRAVERSAL, 'subagent-turn'), dir, false)
  expect(handle(stop('parent-turn'), dir, false)).toEqual({ systemMessage: '1 query  ·  ~1 API call replaced  ·  21 nodes  ·  20 edges  ◎ GitLab Orbit' })
})

test('SessionEnd removes the session count and counts older than a week', () => {
  handle(bash("glab orbit grep 'x'", ''), dir, false)
  for (const [name, days] of [['old', 8], ['recent', 1]] as const) {
    writeFileSync(join(dir, name + '.jsonl'), '{"calls":1}\n')
    const at = new Date(Date.now() - days * 24 * 60 * 60 * 1000)
    utimesSync(join(dir, name + '.jsonl'), at, at)
  }
  expect(readdirSync(dir).sort()).toEqual(['old.jsonl', 'recent.jsonl', 'ses-1.jsonl'])
  handle({ hook_event_name: 'SessionEnd', session_id: 'ses-1' }, dir, false)
  expect(readdirSync(dir)).toEqual(['recent.jsonl'])
})

test('SessionEnd of a session without Orbit calls is quiet', () => {
  const missing = join(dir, 'never-created')
  expect(handle({ hook_event_name: 'SessionEnd', session_id: 'ses-1' }, missing, false)).toBeNull()
  expect(existsSync(missing)).toBe(false)
})

test('a session id that is not a plain name writes nothing', () => {
  handle({ ...bash("glab orbit grep 'x'", ''), session_id: '../escape' }, dir, false)
  handle({ ...bash("glab orbit grep 'x'", ''), session_id: 7 }, dir, false)
  expect(readdirSync(dir)).toEqual([])
  expect(existsSync(join(dir, '..', 'escape.jsonl'))).toBe(false)
})

test('main prints the hook output as JSON and nothing for bad input', () => {
  handle(bash("glab orbit grep 'x'", ''), dir, false)
  expect(JSON.parse(main(JSON.stringify(stop()), dir, false))).toEqual({ systemMessage: '1 query  ·  ~0 API calls replaced  ·  0 nodes  ·  0 edges  ◎ GitLab Orbit' })
  expect(main('not json', dir)).toBe('')
  expect(main('', dir)).toBe('')
})

test('counts live in the plugin data directory Codex provides', () => {
  expect(stateDir({ PLUGIN_DATA: '/data/gitlab-orbit' })).toBe('/data/gitlab-orbit/sessions')
  expect(stateDir({})).toBe(join(tmpdir(), 'gitlab-orbit-codex'))
})

test('the summary reads like the Claude Code status line, with compact numbers and singular units', () => {
  expect(summaryOf({ calls: 1, nodes: 1, edges: 1, apiCalls: 1 }, false)).toBe('1 query  ·  ~1 API call replaced  ·  1 node  ·  1 edge  ◎ GitLab Orbit')
  expect(summaryOf({ calls: 12, nodes: 15300, edges: 0, apiCalls: 1200 }, false)).toBe('12 queries  ·  ~1.2k API calls replaced  ·  15k nodes  ·  0 edges  ◎ GitLab Orbit')
  expect(summaryOf({ calls: 1, nodes: 1, edges: 1, apiCalls: 1 }, true)).toStartWith(`${BLUE}1${RESET}${DIM} query${RESET}${DIM}  ·  ${RESET}`)
  expect(summaryOf({ calls: 1, nodes: 1, edges: 1, apiCalls: 1 }, true)).toEndWith(`${BLUE}  ◎ GitLab Orbit${RESET}`)
})

test('each Orbit call gets the row the Claude Code plugin draws in place of the command', () => {
  expect(handle(bash(QUERY, TRAVERSAL), dir, false)).toEqual({ systemMessage: '◎ Explored Orbit  Merge requests → Projects' })
  expect(handle(bash("glab orbit grep 'auth|login' | head -5", ''), dir, false)).toEqual({ systemMessage: '◎ Searched code  auth|login' })
  expect(handle(bash("glab orbit grep 'x' && make test", ''), dir, false)).toEqual({ systemMessage: '◎ Searched code  x  + make' })
  expect(handle(bash('glab orbit status', ''), dir, false)).toEqual({ systemMessage: '◎ Checked Orbit' })
  expect(handle(bash('ls -la', 'a'), dir, false)).toBeNull()
})

test('the row is colored like the Claude Code row, with the mark in red for a failed MCP call', () => {
  const bold = `\x1b[1;38;2;148;157;203m`
  expect(handle(bash(QUERY, TRAVERSAL), dir, true)).toEqual({
    systemMessage: `${bold}◎${RESET} ${bold}Explored Orbit${RESET}${DIM}  Merge requests → Projects${RESET}`,
  })
  const failed: HookInput = {
    hook_event_name: 'PostToolUse',
    session_id: 'ses-1',
    tool_name: 'mcp__orbit__query_graph',
    tool_input: {},
    tool_response: { content: [{ type: 'text', text: 'denied' }], isError: true },
  }
  expect(handle(failed, dir, true)).toEqual({ systemMessage: `\x1b[1;31m◎${RESET} ${bold}Explored Orbit${RESET}` })
})

test('colors follow the terminal the hook runs in', () => {
  expect(colorsOn({ TERM: 'xterm-256color' })).toBe(true)
  expect(colorsOn({ TERM: 'dumb' })).toBe(false)
  expect(colorsOn({})).toBe(false)
  expect(colorsOn({ TERM: 'xterm-256color', NO_COLOR: '1' })).toBe(false)
})

test('a query the Orbit CLI rejected is drawn in red and not counted, as in Claude Code', () => {
  const rejected = 'Orbit API error (HTTP 400): {"code":"compile_error","message":"unknown entity"}\n\n\ndomains[9]:\n'
  expect(tallyOf(bash(QUERY, rejected))).toBeNull()
  expect(tallyOf(bash(QUERY, 'Warning: truncated output (original token count: 20510)\nTotal output lines: 900\n\n' + rejected))).toBeNull()
  expect(tallyOf(bash(QUERY, 'not authenticated\n\nThe Orbit API rejected the request with HTTP 401.'))).toBeNull()
  expect(tallyOf(bash("glab orbit query \"CALL db.schema('Project')\"", 'Orbit access denied\n'))).toBeNull()
  expect(tallyOf(bash("glab orbit query \"CALL db.schema('Project')\"", 'domains[1]:\n  - name: core\n'))).toEqual({ calls: 1, nodes: 0, edges: 0, apiCalls: 0 })
  expect(handle(bash(QUERY, rejected), dir, true)).toEqual({
    systemMessage: `\x1b[1;31m◎${RESET} \x1b[1;38;2;148;157;203mExplored Orbit${RESET}${DIM}  Merge requests → Projects${RESET}`,
  })
  expect(handle(stop(), dir, false)).toBeNull()
})

function fakeRun(answers: Record<string, string>): { run: Run; ran: string[][] } {
  const ran: string[][] = []
  const run: Run = (argv) => {
    ran.push([...argv])
    const stdout = answers[argv.join(' ')]
    return stdout === undefined ? { exitCode: 1, stdout: '' } : { exitCode: 0, stdout }
  }
  return { run, ran }
}

const GRAPH = JSON.stringify({ itemCounts: { domains: [{ entities: [{ name: 'MergeRequest', count: '1675399' }, { name: 'Project', count: '1' }] }] } })
const SCOPED = "glab orbit query 'MATCH (mr:MergeRequest {project_id: 7})-[:IN_PROJECT]->(p:Project) RETURN mr, p'"

test('the lookups find the graph total of the project a query names, once, and Stop shows it', () => {
  const { run, ran } = fakeRun({
    'glab api projects/7': JSON.stringify({ id: 7, path_with_namespace: 'gitlab-org/orbit/knowledge-graph' }),
    'glab orbit graph-status --project-id 7 --response-format raw': GRAPH,
  })
  handle(bash(SCOPED, TRAVERSAL), dir, false)
  lookUp(bash(SCOPED, TRAVERSAL), dir, run)
  lookUp(bash(SCOPED, TRAVERSAL), dir, run)
  expect(ran).toEqual([
    ['glab', 'api', 'projects/7'],
    ['glab', 'orbit', 'graph-status', '--project-id', '7', '--response-format', 'raw'],
  ])
  expect(handle(stop(), dir, false)).toEqual({ systemMessage: '1 query  ·  ~1 API call replaced  ·  21 / 1,675,400 nodes  ·  20 edges  ◎ GitLab Orbit' })
})

test('the first query without a scope looks up the entity names, which sharpen later rows', () => {
  const pinned = "glab orbit query 'MATCH (mr:MergeRequest) WHERE mr.project_id = 7 RETURN mr'"
  expect(handle(bash(pinned, TRAVERSAL), dir, false)).toEqual({ systemMessage: '◎ Explored Orbit  Merge requests' })
  const { run, ran } = fakeRun({ 'glab orbit graph-status --response-format raw': GRAPH })
  lookUp(bash(QUERY, TRAVERSAL), dir, run)
  lookUp(bash(QUERY, TRAVERSAL), dir, run)
  expect(ran).toEqual([['glab', 'orbit', 'graph-status', '--response-format', 'raw']])
  expect(handle(bash(pinned, TRAVERSAL), dir, false)).toEqual({ systemMessage: '◎ Explored Orbit  Project → Merge requests' })
})

test('the lookups fall back to the orbit binary and skip failed and non-query calls', () => {
  const { run, ran } = fakeRun({ 'orbit graph-status --response-format raw': GRAPH })
  lookUp(bash("glab orbit grep 'x'", ''), dir, run)
  lookUp(bash(QUERY, 'Orbit API error (HTTP 400): {}'), dir, run)
  expect(ran).toEqual([])
  lookUp(bash(QUERY, TRAVERSAL), dir, run)
  expect(ran).toEqual([
    ['glab', 'orbit', 'graph-status', '--response-format', 'raw'],
    ['orbit', 'graph-status', '--response-format', 'raw'],
  ])
  expect(readFileSync(join(dir, 'ses-1.jsonl'), 'utf8')).toContain('"entities":["MergeRequest","Project"]')
})
