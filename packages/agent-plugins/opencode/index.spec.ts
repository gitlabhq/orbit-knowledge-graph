import { expect, mock, test } from 'bun:test'

import type { PluginContext, ToolExecuteAfter, ToolExecuteBefore } from '@opencode/plugin'

import type { Activity } from './rpc.ts'

// OpenCode resolves @opencode/plugin at runtime. Stand in for the two helpers the plugin calls.
mock.module('@opencode/plugin', () => ({ Plugin: { define: <T>(definition: T) => definition } }))
mock.module('@opencode/plugin/rpc', () => ({ Rpc: { define: <T>(definition: T) => definition } }))

const { default: plugin } = await import('./index.ts')

type Hooks = {
  before: (event: ToolExecuteBefore) => Promise<void> | void
  after: (event: ToolExecuteAfter) => Promise<void> | void
}

async function load(emit: (activity: Activity) => Promise<void> = async () => {}) {
  const sent: Activity[] = []
  const hooks: Partial<Hooks> = {}
  const ctx = {
    rpc: {
      register: async () => ({
        events: {
          emit: (_name: string, activity: Activity) => {
            sent.push(activity)
            return emit(activity)
          },
        },
      }),
    },
    tool: {
      hook: async (name: string, callback: unknown) => {
        if (name === 'execute.before') hooks.before = callback as Hooks['before']
        if (name === 'execute.after') hooks.after = callback as Hooks['after']
      },
    },
  } as unknown as PluginContext
  await plugin.setup(ctx)
  return { sent, hooks: hooks as Hooks }
}

function call(command: string, tool = 'shell', id = 'call-1'): ToolExecuteBefore {
  return { tool, id, sessionID: 'ses-1', messageID: 'msg-1', agent: 'build', input: { command } }
}

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

test('the default export is a V2 plugin definition', () => {
  expect(plugin.id).toBe('orbit-ui')
  expect(typeof plugin.setup).toBe('function')
})

test('an orbit call sends a start and an end with the same key and session', async () => {
  const { sent, hooks } = await load()
  const started = call("glab orbit grep 'auth|login'")
  await hooks.before(started)
  await hooks.after({ ...started, status: 'completed', result: { content: [{ type: 'text', text: 'src/a.rs' }] } })
  expect(sent).toEqual([
    { key: 'call-1', state: 'start', active: 'Searching code', done: 'Searched code', detail: 'auth|login', sessionID: 'ses-1' },
    {
      key: 'call-1',
      state: 'end',
      active: 'Searching code',
      done: 'Searched code',
      detail: 'auth|login',
      sessionID: 'ses-1',
      nodes: 0,
      edges: 0,
    },
  ])
})

test('OpenCode 2 shell calls and OpenCode 1 bash calls are both recognized', async () => {
  const { sent, hooks } = await load()
  await hooks.before(call('orbit context src/main.rs:10-40', 'shell'))
  await hooks.before(call('orbit context src/main.rs:10-40', 'bash'))
  expect(sent.map((activity) => activity.active)).toEqual(['Reading code', 'Reading code'])
})

test('the end counts the nodes and edges the response returned', async () => {
  const { sent, hooks } = await load()
  const query = call('glab orbit query "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN mr"')
  await hooks.after({ ...query, status: 'completed', result: { content: [{ type: 'text', text: TRAVERSAL }] } })
  await hooks.after({ ...query, id: 'call-2', status: 'completed', result: { output: AGGREGATION } })
  expect(sent.map(({ nodes, edges }) => ({ nodes, edges }))).toEqual([
    { nodes: 21, edges: 20 },
    { nodes: 2522, edges: 0 },
  ])
})

test('a string content result is counted', async () => {
  const { sent, hooks } = await load()
  const query = call('glab orbit query "MATCH (mr:MergeRequest) RETURN mr"')
  await hooks.after({ ...query, status: 'completed', result: { content: TRAVERSAL } })
  expect(sent.map(({ nodes, edges }) => ({ nodes, edges }))).toEqual([{ nodes: 21, edges: 20 }])
})

test('a truncated result is left uncounted instead of counted as zero', async () => {
  const { sent, hooks } = await load()
  const query = call('glab orbit query "MATCH (mr:MergeRequest) RETURN mr"')
  const result = { output: { output: '{"nodes":[', truncated: true }, content: [{ type: 'text', text: '{"nodes":[' }] }
  await hooks.after({ ...query, status: 'completed', result })
  expect(sent[0]?.nodes).toBeUndefined()
  expect(sent[0]?.edges).toBeUndefined()
})

test('a failed call is marked failed and counts nothing', async () => {
  const { sent, hooks } = await load()
  await hooks.after({ ...call('glab orbit query "MATCH (p:Project) RETURN p"'), status: 'error', error: new Error('boom') })
  expect(sent).toHaveLength(1)
  expect(sent[0]).toMatchObject({ state: 'end', failed: true })
  expect(sent[0]?.nodes).toBeUndefined()
})

test('other commands and other tools send nothing', async () => {
  const { sent, hooks } = await load()
  for (const command of ['git status', "echo 'glab orbit grep x'", 'glab orbit dsl', 'glab orbit grep --help']) {
    await hooks.before(call(command))
  }
  await hooks.before(call('glab orbit grep x', 'read'))
  expect(sent).toEqual([])
})

test('the hook returns before the event is delivered', async () => {
  let release = () => {}
  const delivered = new Promise<void>((resolve) => (release = resolve))
  const { hooks } = await load(() => delivered)
  const before = hooks.before(call('orbit grep x'))
  const waited = new Promise((resolve) => setTimeout(() => resolve('waited'), 50))
  expect(await Promise.race([Promise.resolve(before).then(() => 'returned'), waited])).toBe('returned')
  release()
})

test('an event that throws or rejects never fails the call', async () => {
  const failures = [
    () => {
      throw new Error('no tui')
    },
    async () => {
      throw new Error('no tui')
    },
  ]
  for (const emit of failures) {
    const { hooks } = await load(emit)
    await hooks.before(call('orbit grep x'))
  }
  await new Promise((resolve) => setTimeout(resolve, 0))
})
