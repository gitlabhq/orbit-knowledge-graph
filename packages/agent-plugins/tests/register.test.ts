import { expect, test, type TestBody } from 'claude-code/testing'

const SITE = { plugin: 'gitlab-orbit', requestId: 'orbit-test', viewport: { columns: 120, rows: 30 }, surface: 'terminal' } as const

const QUERY = 'glab orbit query "MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest) RETURN u"'

function toolUse(command: string, state: { isRunning?: boolean; isErrored?: boolean } = {}) {
  return {
    ...SITE,
    component: 'ToolUse',
    props: {
      tool_use_id: 'toolu_1',
      tool: 'Bash',
      input: { command },
      isRunning: state.isRunning ?? false,
      isErrored: state.isErrored ?? false,
      isInterrupted: false,
    },
  } as const
}

test('a finished Orbit call draws the mark, the label, and the path', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['drawn by Claude Code'] }))
  const ui = await $.ui.mount(toolUse(QUERY))
  expect(await ui.find({ type: 'Text', text: 'Explored Orbit' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: '  Users → Merge requests' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: 'drawn by Claude Code' })).toBeUndefined()
})

test('a failed Orbit call says so', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['drawn by Claude Code'] }))
  const ui = await $.ui.mount(toolUse(QUERY, { isErrored: true }))
  expect(await ui.find({ type: 'Text', text: '  · failed' })).toBeDefined()
})

test('other tool calls keep Claude Code row', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['drawn by Claude Code'] }))
  const ui = await $.ui.mount(toolUse('ls -la'))
  expect(await ui.find({ type: 'Text', text: 'drawn by Claude Code' })).toBeDefined()
})

test('the status line counts finished Orbit queries and passes every call through', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['hint'] }))
  on('tool.call', () => ({ result: { stdout: 'nodes:\n  User[3]{id}:\n    1\nedges[2]:\n' } }))
  const answer = await $.tool.call({ tool: 'Bash', command: QUERY })
  expect(answer).toMatchObject({ result: { stdout: expect.stringContaining('User[3]') } })
  await $.tool.call({ tool: 'Bash', command: 'ls' })

  const ui = await $.ui.mount({ ...SITE, component: 'PromptHint', props: { isDraft: false, isWorking: false, hint: '' } })
  expect(await ui.find({ type: 'Text', text: '1' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: ' query' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: ' nodes' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: ' edges' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: '  ◎ GitLab Orbit' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: ' API call replaced' })).toBeDefined()
})

test('the status line shows the nodes out of the project graph total once graph-status answers', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['hint'] }))
  on('tool.call', () => ({ result: { stdout: 'nodes:\n  User[3]{id}:\n    1\n' } }))
  const graph = { itemCounts: { domains: [{ entities: [{ name: 'Job', count: '1675399' }, { name: 'Project', count: '1' }] }] } }
  on('process.run', () => ({
    value: { exitCode: 0, stdout: JSON.stringify(graph), stderr: '', isStdoutTruncated: false, isStderrTruncated: false },
  }))
  await $.tool.call({ tool: 'Bash', command: QUERY })
  for (let i = 0; i < 10; i++) await Promise.resolve()

  const ui = await $.ui.mount({ ...SITE, component: 'PromptHint', props: { isDraft: false, isWorking: false, hint: '' } })
  expect(await ui.find({ type: 'Text', text: '3 / 1,675,400' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: ' nodes' })).toBeDefined()
})

test('a cypher block in a reply draws highlighted, and the prose before it stays Claude Code', async ($, on) => {
  on('ui.render', (_$, e) => ({ type: 'Text', props: {}, children: ['drawn by Claude Code: ' + (e.props as { text: string }).text] }))
  const text = 'Here is the query.\n\n```cypher\n-- count by state\nMATCH (mr:MergeRequest {project_id: 1})\nRETURN mr.state, count(mr) AS n\n```'
  const ui = await $.ui.mount({ ...SITE, component: 'AssistantMessage', props: { text, isFirstOfReply: true } })
  expect(await ui.find({ type: 'Text', text: 'drawn by Claude Code: Here is the query.' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: 'MATCH' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: 'MergeRequest' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: '-- count by state' })).toBeDefined()
})

test('a reply without cypher stays Claude Code', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['drawn by Claude Code'] }))
  const ui = await $.ui.mount({ ...SITE, component: 'AssistantMessage', props: { text: '```bash\nls\n```', isFirstOfReply: true } })
  expect(await ui.find({ type: 'Text', text: 'drawn by Claude Code' })).toBeDefined()
})

test('a count query adds the edges its counted rows walked', async ($, on) => {
  on('ui.render', () => ({ type: 'Text', props: {}, children: ['hint'] }))
  const aggregation = 'query_type: aggregation\ncolumns[1]{name,function,target}:\n  n,count,mr\nrows[1]{n}:\n  40\npagination:\n  has_more: false\n'
  on('tool.call', () => ({ result: { stdout: aggregation } }))
  await $.tool.call({ tool: 'Bash', command: 'glab orbit query "MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) RETURN count(mr) AS n"' })

  const ui = await $.ui.mount({ ...SITE, component: 'PromptHint', props: { isDraft: false, isWorking: false, hint: '' } })
  expect(await ui.find({ type: 'Text', text: '80' })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: ' edges' })).toBeDefined()
})

const RAN = { exitCode: 1, stdout: '', stderr: '', isStdoutTruncated: false, isStderrTruncated: false }

async function graphStatusRuns(
  $: Parameters<TestBody>[0],
  on: Parameters<TestBody>[1],
  command: string,
  answer: { result: { stdout: string } } | { deny: string },
) {
  const runs: (readonly string[])[] = []
  on('tool.call', () => answer)
  on('process.run', (_$, e) => {
    runs.push(e.argv)
    return { value: RAN }
  })
  await $.tool.call({ tool: 'Bash', command })
  for (let i = 0; i < 10; i++) await Promise.resolve()
  return runs
}

test('graph-status runs through a fixed launcher after a successful query', async ($, on) => {
  const runs = await graphStatusRuns($, on, './orbit query "MATCH (u:User {id: 1}) RETURN u"', { result: { stdout: 'nodes:\n' } })
  expect(runs).toEqual([['glab', 'orbit', 'graph-status', '--response-format', 'raw'], ['orbit', 'graph-status', '--response-format', 'raw']])
})

test('graph-status never runs after a denied call', async ($, on) => {
  expect(await graphStatusRuns($, on, QUERY, { deny: 'the user said no' })).toEqual([])
})

test('graph-status never runs after a local call', async ($, on) => {
  expect(await graphStatusRuns($, on, 'orbit grep foo', { result: { stdout: 'x' } })).toEqual([])
})
