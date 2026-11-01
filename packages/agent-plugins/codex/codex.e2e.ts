import { afterAll, beforeAll, expect, test } from 'bun:test'
import { chmodSync, cpSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { Subprocess } from 'bun'

// Installs the built plugin into a real Codex with `codex plugin add` and drives it against a scripted
// model on localhost, so the hooks see the payloads Codex really sends. It needs no API key or network.
// CODEX_BIN is an installed Codex. A package runner such as `bun x` cannot download it here, because the
// test cuts Codex off from the network.
const CODEX = process.env['CODEX_BIN'] || 'codex'
const PLUGIN = join(import.meta.dir, '../../../config/setup/agents/codex_plugin')
const MARKETPLACE = 'orbit-e2e'
const TIMEOUT_MS = 120_000
// Nothing listens on the discard port, so any request Codex sends past the local model fails fast.
const NO_NETWORK = 'http://127.0.0.1:9'

const ORBIT_PROMPT = 'orbit-e2e: ask the graph'
const SEARCH_PROMPT = 'orbit-e2e: search the code'
const PLAIN_PROMPT = 'orbit-e2e: answer without tools'
const QUERY = "glab orbit query 'MATCH (mr:MergeRequest {project_id: 7})-[:IN_PROJECT]->(p:Project) RETURN mr, p'"
const SEARCH = "glab orbit grep 'retry'"
// The fake glab answers this one with more output than Codex passes on, so Codex cuts it.
const LARGE_QUERY = "glab orbit query 'MATCH (mr:MergeRequest) RETURN mr LIMIT 9000'"
// The graph total comes from an async hook, so the first summary may come before it.
const FIRST_SUMMARY = /^2 queries {2}· {2}~91 API calls replaced {2}· {2}9k( \/ 1,675,400)? nodes {2}· {2}2 edges {2}◎ GitLab Orbit$/
const SUMMARY = '3 queries  ·  ~91 API calls replaced  ·  9k / 1,675,400 nodes  ·  2 edges  ◎ GitLab Orbit'
const GRAPH = JSON.stringify({ itemCounts: { domains: [{ entities: [{ name: 'MergeRequest', count: '1675399' }, { name: 'Project', count: '1' }] }] } })
const LARGE_ROW = '◎ Explored Orbit  Merge requests'
// Once a parallel call's lookup has learned the entity names, the project_id key reads as a pinned Project.
const QUERY_ROWS = ['◎ Explored Orbit  Merge requests → Projects', '◎ Explored Orbit  Project → Merge requests → Projects']
// The plugin's blue, #949dcb, in an ANSI color sequence.
const BLUE = '38;2;148;157;203'

const TRAVERSAL = `query_type: traversal
nodes:
  MergeRequest[2]{id,iid}:
    1,10
    2,11
  Project[1]{id}:
    7
edges[2]{from,from_id,type,to,to_id}:
  MergeRequest,1,IN_PROJECT,Project,7
  MergeRequest,2,IN_PROJECT,Project,7`

type Item = { type?: string; role?: string; content?: { text?: string }[] }

let root = ''
let work = ''
let codexHome = ''
let sessions = ''
let glabLog = ''
let env: Record<string, string> = {}
let model: ReturnType<typeof Bun.serve> | undefined

function shellCall(callId: string, cmd: string) {
  // A non-login shell keeps the fake glab first on PATH.
  return { type: 'function_call', call_id: callId, name: 'exec_command', arguments: JSON.stringify({ cmd, login: false }) }
}

function message(text: string) {
  return { type: 'message', role: 'assistant', id: 'msg-1', content: [{ type: 'output_text', text }] }
}

function replyTo(input: readonly Item[]): object[] {
  const textOf = (item: Item) => (item.content ?? []).map((part) => part.text ?? '').join('')
  const at = input.findLastIndex((item) => item.role === 'user' && textOf(item).includes('orbit-e2e:'))
  const prompt = at >= 0 ? textOf(input[at] ?? {}) : ''
  const answered = input.slice(at + 1).some((item) => item.type === 'function_call_output')
  if (prompt.includes(ORBIT_PROMPT) && !answered) {
    return [shellCall('call-orbit', QUERY), shellCall('call-large', LARGE_QUERY), shellCall('call-plain', 'echo plain')]
  }
  if (prompt.includes(SEARCH_PROMPT) && !answered) return [shellCall('call-search', SEARCH)]
  return [message('Done.')]
}

function sse(id: string, items: readonly object[]): string {
  const usage = { input_tokens: 0, input_tokens_details: null, output_tokens: 0, output_tokens_details: null, total_tokens: 0 }
  const events = [
    { type: 'response.created', response: { id } },
    ...items.map((item) => ({ type: 'response.output_item.done', item })),
    { type: 'response.completed', response: { id, usage } },
  ]
  return events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join('')
}

function startModel(): ReturnType<typeof Bun.serve> {
  let responses = 0
  return Bun.serve({
    port: 0,
    hostname: '127.0.0.1',
    async fetch(request) {
      if (request.method !== 'POST' || !new URL(request.url).pathname.endsWith('/responses')) return new Response('not found', { status: 404 })
      const body = (await request.json()) as { input?: Item[] }
      responses += 1
      return new Response(sse('resp-' + responses, replyTo(body.input ?? [])), { headers: { 'content-type': 'text/event-stream' } })
    },
  })
}

async function codex(args: readonly string[]): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const proc = Bun.spawn([CODEX, ...args], { cwd: work, env, stdin: 'ignore', stdout: 'pipe', stderr: 'pipe' })
  const [stdout, stderr, exitCode] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited])
  return { exitCode, stdout, stderr }
}

async function mustRun(args: readonly string[]): Promise<{ stdout: string; stderr: string }> {
  const run = await codex(args)
  if (run.exitCode !== 0) throw new Error(`codex ${args.join(' ')} exited ${run.exitCode}\n${run.stdout}\n${run.stderr}`)
  return run
}

beforeAll(async () => {
  root = realpathSync(mkdtempSync(join(tmpdir(), 'orbit-codex-e2e-')))
  const home = join(root, 'home')
  const bin = join(root, 'bin')
  const marketplace = join(root, 'marketplace')
  work = join(root, 'work')
  codexHome = join(root, 'codex')
  // Codex gives each plugin's hooks a data directory named after the plugin and its marketplace.
  sessions = join(codexHome, 'plugins/data', 'gitlab-orbit-' + MARKETPLACE, 'sessions')
  glabLog = join(root, 'glab.log')
  for (const dir of [home, bin, work, codexHome, join(marketplace, '.agents/plugins')]) mkdirSync(dir, { recursive: true })

  // Stands in for glab so the test never reaches GitLab, and logs each call to prove it ran.
  writeFileSync(
    join(bin, 'glab'),
    [
      '#!/bin/sh',
      `printf '%s\\n' "$*" >> '${glabLog}'`,
      `case "$1 $2" in`,
      `  'orbit grep') echo 'src/retry.rs'; exit 0 ;;`,
      `  'orbit graph-status') printf '%s\\n' '${GRAPH}'; exit 0 ;;`,
      `  'api projects/7') echo '{"id":7,"path_with_namespace":"gitlab-org/orbit/knowledge-graph"}'; exit 0 ;;`,
      `  'orbit query') ;;`,
      `  *) exit 2 ;;`,
      `esac`,
      `case "$3" in *'LIMIT 9000'*) printf 'query_type: traversal\\nnodes:\\n  MergeRequest[9000]{id}:\\n'; seq 1 9000 | sed 's/^/    /'; exit 0 ;; esac`,
      `cat <<'EOF'\n${TRAVERSAL}\nEOF`,
      '',
    ].join('\n'),
  )
  chmodSync(join(bin, 'glab'), 0o755)

  cpSync(PLUGIN, join(marketplace, 'gitlab-orbit'), { recursive: true })
  writeFileSync(
    join(marketplace, '.agents/plugins/marketplace.json'),
    JSON.stringify({ name: MARKETPLACE, plugins: [{ name: 'gitlab-orbit', source: './gitlab-orbit' }] }),
  )

  model = startModel()
  writeFileSync(
    join(codexHome, 'config.toml'),
    [
      'model = "orbit-e2e"',
      'model_provider = "orbit-e2e"',
      `chatgpt_base_url = "http://127.0.0.1:${model.port}/backend-api/"`,
      'check_for_update_on_startup = false',
      '',
      '[model_providers.orbit-e2e]',
      'name = "Orbit e2e"',
      `base_url = "http://127.0.0.1:${model.port}/v1"`,
      'wire_api = "responses"',
      'request_max_retries = 0',
      'stream_max_retries = 0',
      '',
    ].join('\n'),
  )

  // Credentials, proxies, and the plugin variables of an agent running this test stay out of Codex.
  const inherited = Object.entries(process.env).filter(([key, value]) => value !== undefined && !/^(OPENAI_|CODEX_|PLUGIN_|CLAUDE_|NO_COLOR$)|proxy$/i.test(key))
  env = Object.fromEntries(inherited) as Record<string, string>
  for (const proxy of ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']) env[proxy] = env[proxy.toLowerCase()] = NO_NETWORK
  Object.assign(env, {
    NO_PROXY: '127.0.0.1,localhost',
    no_proxy: '127.0.0.1,localhost',
    HOME: home,
    CODEX_HOME: codexHome,
    CODEX_SQLITE_HOME: codexHome,
    CODEX_APP_SERVER_MANAGED_CONFIG_PATH: join(codexHome, 'managed_config.toml'),
    PATH: bin + ':' + (process.env['PATH'] ?? ''),
    // The hook colors its messages only under a terminal, as the TUI runs it.
    TERM: 'xterm-256color',
    RUST_LOG: 'warn',
  })

  await mustRun(['plugin', 'marketplace', 'add', marketplace])
  await mustRun(['plugin', 'add', 'gitlab-orbit@' + MARKETPLACE])
}, TIMEOUT_MS)

afterAll(() => {
  model?.stop(true)
  rmSync(root, { recursive: true, force: true })
})

test(
  'codex plugin add installs the plugin with its hooks',
  async () => {
    const config = readFileSync(join(codexHome, 'config.toml'), 'utf8')
    expect(config).toContain(`[plugins."gitlab-orbit@${MARKETPLACE}"]`)
    const installed = join(codexHome, 'plugins/cache', MARKETPLACE, 'gitlab-orbit/0.0.0')
    expect(readdirSync(join(installed, 'hooks')).sort()).toEqual(['hooks.json', 'orbit.mjs', 'orbit.sh'])
  },
  TIMEOUT_MS,
)

test(
  'codex exec runs the hooks on every shell call and clears the count when the session ends',
  async () => {
    rmSync(glabLog, { force: true })
    const args = ['exec', '--color', 'never', '--skip-git-repo-check', '--dangerously-bypass-hook-trust', '--dangerously-bypass-approvals-and-sandbox', '-C', work, ORBIT_PROMPT]
    const { stderr } = await mustRun(args)
    const runs = (event: string, status: string) => stderr.split('\n').filter((line) => line.trim() === `hook: ${event} ${status}`).length
    // Each shell call runs the row hook and the async lookup hook; exec may exit before an async one reports.
    expect(runs('PostToolUse', 'Completed')).toBeGreaterThanOrEqual(3)
    expect(runs('PostToolUse', 'Completed')).toBeLessThanOrEqual(6)
    expect({ stop: runs('Stop', 'Completed'), failed: runs('PostToolUse', 'Failed') + runs('Stop', 'Failed') }).toEqual({ stop: 1, failed: 0 })
    expect(readFileSync(glabLog, 'utf8')).toContain('orbit query MATCH (mr:MergeRequest')
    // SessionEnd removes the count file; a lookup that outlives the session may leave only graph totals.
    const counts = readdirSync(sessions).map((name) => readFileSync(join(sessions, name), 'utf8'))
    expect(counts.filter((content) => content.includes('"calls"'))).toEqual([])
  },
  TIMEOUT_MS,
)

type Notification = { method: string; params: Record<string, unknown> }

// The interactive TUI is an app-server client, so a `hook/completed` notification is what it renders.
class AppServer {
  private readonly proc: Subprocess<'pipe', 'pipe', 'pipe'>
  private readonly stderr: Promise<string>
  private readonly waiting = new Map<number, (message: Record<string, unknown>) => void>()
  readonly notifications: Notification[] = []
  private nextId = 0
  private wake: () => void = () => {}
  private closed = false
  private stopping: Promise<string> | undefined

  constructor() {
    this.proc = Bun.spawn([CODEX, 'app-server'], { cwd: work, env, stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' })
    // Drained from the start so a full pipe never stalls Codex.
    this.stderr = new Response(this.proc.stderr).text()
    void this.read()
  }

  private async read(): Promise<void> {
    let buffered = ''
    for await (const chunk of this.proc.stdout.pipeThrough(new TextDecoderStream())) {
      buffered += chunk
      for (let end = buffered.indexOf('\n'); end >= 0; end = buffered.indexOf('\n')) {
        const line = buffered.slice(0, end).trim()
        buffered = buffered.slice(end + 1)
        if (line) this.receive(JSON.parse(line) as Record<string, unknown>)
      }
    }
    this.closed = true
    for (const resolve of this.waiting.values()) resolve({ error: 'app-server exited' })
    this.wake()
  }

  private receive(message: Record<string, unknown>): void {
    const { id, method } = message
    if (typeof method !== 'string') {
      if (typeof id === 'number') this.waiting.get(id)?.(message)
    } else if (id !== undefined) {
      // A request from Codex, such as an approval, would stall the turn, so it fails at once.
      this.send({ id, error: { code: -32601, message: 'not supported by the Orbit e2e client' } })
    } else {
      this.notifications.push({ method, params: (message['params'] ?? {}) as Record<string, unknown> })
      this.wake()
    }
  }

  private send(message: Record<string, unknown>): void {
    if (this.closed || this.stopping) return
    this.proc.stdin.write(JSON.stringify(message) + '\n')
    void this.proc.stdin.flush()
  }

  async request(method: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
    const id = this.nextId++
    const answer = new Promise<Record<string, unknown>>((resolve) => this.waiting.set(id, resolve))
    this.send({ id, method, params })
    const message = await answer
    if (message['error']) throw new Error(`${method} failed: ${JSON.stringify(message['error'])}\n${await this.stop()}`)
    return message['result'] as Record<string, unknown>
  }

  notify(method: string): void {
    this.send({ method })
  }

  async until(done: (notification: Notification) => boolean): Promise<void> {
    while (!this.notifications.some(done)) {
      if (this.closed) throw new Error('app-server exited early\n' + (await this.stderr))
      await new Promise<void>((resolve) => (this.wake = resolve))
    }
  }

  stop(): Promise<string> {
    this.stopping ??= (async () => {
      this.proc.stdin.end()
      // The app-server exits when its input closes; the kill only covers a stuck process.
      const exited = await Promise.race([this.proc.exited.then(() => true), Bun.sleep(10_000).then(() => false)])
      if (!exited) this.proc.kill()
      await this.proc.exited
      return this.stderr
    })()
    return this.stopping
  }
}

type HookRun = { eventName: string; executionMode: string; status: string; entries: { kind: string; text: string }[] }

const plain = (text: string) => text.replace(/\x1b\[[\d;]*m/g, '')

test(
  'the TUI shows the Orbit total after a turn that used Orbit and nothing after one that did not',
  async () => {
    const server = new AppServer()
    try {
      await server.request('initialize', { clientInfo: { name: 'orbit-e2e', title: null, version: '0.0.0' }, capabilities: null })
      server.notify('initialized')
      const started = await server.request('thread/start', {
        cwd: work,
        approvalPolicy: 'never',
        sandbox: 'danger-full-access',
        config: { bypass_hook_trust: true },
      })
      const threadId = (started['thread'] as { id: string }).id

      const runTurn = async (prompt: string): Promise<HookRun[]> => {
        const turn = await server.request('turn/start', { threadId, input: [{ type: 'text', text: prompt, text_elements: [] }] })
        const turnId = (turn['turn'] as { id: string }).id
        await server.until((n) => n.method === 'turn/completed' && (n.params['turn'] as { id?: string } | undefined)?.id === turnId)
        return server.notifications
          .filter((n) => n.method === 'hook/completed' && n.params['turnId'] === turnId)
          .map((n) => n.params['run'] as HookRun)
      }
      // The async lookup runs report no messages, so only the row hooks are checked.
      const messages = (runs: readonly HookRun[], event: string) =>
        runs
          .filter((run) => run.eventName === event && run.executionMode === 'sync')
          .flatMap((run) => (run.status === 'completed' ? run.entries : [{ kind: run.status, text: '' }]))

      const orbit = await runTurn(ORBIT_PROMPT)
      // One row per Orbit query, none for the plain command; Codex may run the calls in parallel.
      const rows = messages(orbit, 'postToolUse')
      expect(orbit.filter((run) => run.eventName === 'postToolUse' && run.executionMode === 'sync')).toHaveLength(3)
      expect(rows.map((row) => row.kind)).toEqual(['warning', 'warning'])
      const [largeRow, queryRow] = rows.map((row) => plain(row.text)).sort()
      expect(largeRow).toBe(LARGE_ROW)
      expect(QUERY_ROWS).toContain(queryRow ?? '')
      expect(rows.every((row) => row.text.includes(BLUE))).toBe(true)
      const [first, ...more] = messages(orbit, 'stop')
      expect(more).toEqual([])
      expect(first?.kind).toBe('warning')
      expect(plain(first?.text ?? '')).toMatch(FIRST_SUMMARY)
      expect(first?.text).toContain(BLUE)

      const [file, ...others] = readdirSync(sessions)
      expect(others).toEqual([])
      const count = join(sessions, file ?? '')
      const lines = () => readFileSync(count, 'utf8').trimEnd().split('\n').map((line) => JSON.parse(line) as Record<string, unknown>)
      // Codex may run the two queries in parallel, so their lines come in either order.
      const tallies = lines().filter((line) => 'calls' in line).map((line) => JSON.stringify(line))
      expect(tallies.sort()).toEqual(['{"calls":1,"nodes":3,"edges":2,"apiCalls":1}', '{"calls":1,"nodes":9000,"edges":0,"apiCalls":90}'])
      for (let waited = 0; !lines().some((line) => line['total'] === 1675400); waited += 100) {
        if (waited > 30_000) throw new Error('no graph total after 30s:\n' + readFileSync(count, 'utf8'))
        await Bun.sleep(100)
      }
      const glabCalls = readFileSync(glabLog, 'utf8').split('\n')
      expect(glabCalls).toContain('api projects/7')
      expect(glabCalls).toContain('orbit graph-status --project-id 7 --response-format raw')

      const search = await runTurn(SEARCH_PROMPT)
      expect(messages(search, 'postToolUse').map((row) => plain(row.text))).toEqual(['◎ Searched code  retry'])
      expect(messages(search, 'stop').map((row) => plain(row.text))).toEqual([SUMMARY])

      const later = await runTurn(PLAIN_PROMPT)
      expect(later.filter((run) => run.eventName === 'stop').map((run) => [run.status, run.entries])).toEqual([['completed', []]])
    } finally {
      await server.stop()
    }
  },
  TIMEOUT_MS,
)
