import { afterAll, beforeEach, expect, test } from 'bun:test'
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { tallyOf, type HookInput } from './hook.ts'

const WRAPPER = join(import.meta.dir, 'orbit.sh')
const root = mkdtempSync(join(tmpdir(), 'orbit-codex-sh-'))
const log = join(root, 'node.log')

function fakeNode(name: string, script: string): string {
  const dir = join(root, name)
  mkdirSync(dir)
  writeFileSync(join(dir, 'node'), '#!/bin/sh\n' + script)
  chmodSync(join(dir, 'node'), 0o755)
  return `${dir}:/usr/bin:/bin`
}

// Stand-ins for node: one records the mode and input it is started with, one fails like an unset version shim.
const WITH_NODE = fakeNode('with-node', `printf '%s\\n' "$2" >> '${log}'\ncat >> '${log}'\nprintf '\\n' >> '${log}'\n`)
const BROKEN_NODE = fakeNode('broken-node', "cat > /dev/null\necho 'No version is set for shim: node' >&2\nexit 1\n")

// Only the tools the wrapper needs, since a system directory may hold a real node.
const WITHOUT_NODE = join(root, 'without-node')
mkdirSync(WITHOUT_NODE)
for (const tool of ['cat', 'grep', 'dirname']) symlinkSync(Bun.which(tool) ?? `/usr/bin/${tool}`, join(WITHOUT_NODE, tool))

beforeEach(() => rmSync(log, { force: true }))
afterAll(() => rmSync(root, { recursive: true, force: true }))

function run(input: HookInput, args: string[], path: string) {
  const result = Bun.spawnSync(['/bin/sh', WRAPPER, ...args], { stdin: Buffer.from(JSON.stringify(input)), env: { PATH: path } })
  return { exitCode: result.exitCode, stdout: result.stdout.toString(), nodeInput: existsSync(log) ? readFileSync(log, 'utf8') : '' }
}

// Laid out as Codex sends it, in a checkout whose path names Orbit.
const call = (tool_name: string, tool_input: unknown, tool_response: unknown = ''): HookInput & Record<string, unknown> => ({
  session_id: 'ses-1',
  turn_id: 'turn-1',
  transcript_path: '/home/dev/.codex/sessions/2026/10/31/rollout-ses-1.jsonl',
  cwd: '/home/dev/src/Orbit/orbit-knowledge-graph',
  hook_event_name: 'PostToolUse',
  model: 'gpt-5',
  permission_mode: 'bypassPermissions',
  tool_name,
  tool_input,
  tool_response,
  tool_use_id: 'call-1',
})

const ORBIT_CALLS = [
  call('Bash', { command: "glab orbit query 'MATCH (n) RETURN n'" }),
  call('Bash', { command: "cd src && /usr/local/bin/orbit grep 'auth'" }),
  call('Bash', { command: 'GLAB_HOST=x glab orbit context mr:1' }),
  call('Bash', { command: 'glab "orbit" status' }),
  call('Bash', { command: "x=1; orbit query 'MATCH (n) RETURN n'" }),
  call('Bash', { command: 'glab orbit query <<EOF\nMATCH (n) RETURN n\nEOF' }),
  call('mcp__orbit__run', { query: 'x' }),
  call('mcp__Knowledge-Graph__run', {}),
  call('mcp__gkg__run', {}),
  call('mcp__gitlab__query_graph', {}),
  call('mcp__gitlab__get_graph_schema', {}),
]

test('every call the hook counts starts Node with the input unchanged, for the row and for the lookups', () => {
  for (const mode of ['calls', 'lookups']) {
    for (const input of ORBIT_CALLS) {
      expect(tallyOf(input)).not.toBeNull()
      rmSync(log, { force: true })
      expect(run(input, [mode], WITH_NODE)).toEqual({ exitCode: 0, stdout: '', nodeInput: `${mode}\n${JSON.stringify(input)}\n` })
    }
  }
})

test('a tool call that cannot name Orbit never starts Node, even in an Orbit checkout', () => {
  const others = [
    call('Bash', { command: 'ls -la' }, 'orbit-knowledge-graph/\nOrbit/\n'),
    call('Bash', { command: 'cat scripts/orbit.sh ../orbit-cli/x' }),
    call('mcp__github__search_code', { q: 'x' }),
  ]
  for (const input of others) {
    expect(tallyOf(input)).toBeNull()
    expect(run(input, ['calls'], WITH_NODE)).toEqual({ exitCode: 0, stdout: '', nodeInput: '' })
    expect(run(input, ['lookups'], WITH_NODE)).toEqual({ exitCode: 0, stdout: '', nodeInput: '' })
  }
})

test('Stop and SessionEnd always reach the hook', () => {
  run({ hook_event_name: 'Stop', session_id: 'ses-1', turn_id: 'turn-1' }, [], WITH_NODE)
  run({ hook_event_name: 'SessionEnd', session_id: 'ses-1' }, [], WITH_NODE)
  const lines = readFileSync(log, 'utf8').split('\n')
  expect(lines.filter((line) => line.startsWith('{')).map((line) => JSON.parse(line).hook_event_name)).toEqual(['Stop', 'SessionEnd'])
  expect(lines.filter((line) => line && !line.startsWith('{'))).toEqual([])
})

test('without a working Node the hook exits quietly', () => {
  const quiet = { exitCode: 0, stdout: '', nodeInput: '' }
  for (const path of [WITHOUT_NODE, BROKEN_NODE]) {
    expect(run(ORBIT_CALLS[0] ?? {}, ['calls'], path)).toEqual(quiet)
    expect(run({ hook_event_name: 'Stop', session_id: 'ses-1' }, [], path)).toEqual(quiet)
  }
})
