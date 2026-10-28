import { expect, test } from 'claude-code/testing'

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
  expect(await ui.find({ type: 'Text', text: '  User → MergeRequest' })).toBeDefined()
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
  expect(await ui.find({ type: 'Text', text: '  ◎ GitLab Orbit' })).toBeDefined()
})
