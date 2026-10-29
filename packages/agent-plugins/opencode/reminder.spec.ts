import { afterAll, beforeAll, expect, test } from 'bun:test'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'

const REMINDER = 'REMINDER-TEXT'
const TEMPLATE = join(import.meta.dir, '../../../config/setup/opencode_plugin.js')

type Result = { content?: unknown; output?: unknown }
type Event = { tool: string; status: string; result?: Result }
type Plugin = {
  setup(ctx: { tool: { hook(name: string, callback: (event: Event) => void): Promise<void> } }): Promise<void>
  server(): Promise<{ 'tool.execute.after': (input: { tool: string }, output: { output: string }) => Promise<void> }>
}

let dir: string
let plugin: Plugin
const previousDataDir = process.env.ORBIT_DATA_DIR

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), 'orbit-reminder-'))
  const source = (await Bun.file(TEMPLATE).text()).replace('{{reminder}}', REMINDER)
  writeFileSync(join(dir, 'orbit.js'), source)
  plugin = (await import(join(dir, 'orbit.js'))).default
})

afterAll(() => {
  rmSync(dir, { recursive: true, force: true })
  if (previousDataDir === undefined) delete process.env.ORBIT_DATA_DIR
  else process.env.ORBIT_DATA_DIR = previousDataDir
})

function withGraph(present: boolean) {
  const root = mkdtempSync(join(dir, 'data-'))
  if (present) {
    mkdirSync(join(root, 'local'))
    writeFileSync(join(root, 'local', 'manifest.duckdb'), '')
  }
  process.env.ORBIT_DATA_DIR = root
}

async function hookedSetup() {
  let callback!: (event: Event) => void
  await plugin.setup({ tool: { hook: async (_name, cb) => void (callback = cb) } })
  return callback
}

const completed = (tool: string, result: Result): Event => ({ tool, status: 'completed', result })

test('setup appends the reminder to an array content, keeping the output', async () => {
  withGraph(true)
  const hook = await hookedSetup()
  const event = completed('shell', { content: [{ type: 'text', text: 'out' }] })
  hook(event)
  expect(event.result?.content).toEqual([
    { type: 'text', text: 'out' },
    { type: 'text', text: REMINDER },
  ])
})

test('setup appends the reminder to a string content, keeping the output', async () => {
  withGraph(true)
  const hook = await hookedSetup()
  const event = completed('bash', { content: 'out' })
  hook(event)
  expect(event.result?.content).toBe(`out\n\n${REMINDER}`)
})

test('setup appends the reminder to a string output when there is no content', async () => {
  withGraph(true)
  const hook = await hookedSetup()
  const event = completed('shell', { output: 'out' })
  hook(event)
  expect(event.result?.output).toBe(`out\n\n${REMINDER}`)
})

test('setup keeps a structured output when there is no content', async () => {
  withGraph(true)
  const hook = await hookedSetup()
  const event = completed('shell', { output: { output: 'out', exit: 0 } })
  hook(event)
  expect(event.result?.content).toEqual([
    { type: 'text', text: '{"output":"out","exit":0}' },
    { type: 'text', text: REMINDER },
  ])
})

test('setup reminds once per session, only for shell tools, only when completed, only with a graph', async () => {
  withGraph(false)
  const noGraph = await hookedSetup()
  const skipped = completed('shell', { content: 'out' })
  noGraph(skipped)
  expect(skipped.result?.content).toBe('out')

  withGraph(true)
  const hook = await hookedSetup()
  const read = completed('read', { content: 'out' })
  const failed: Event = { tool: 'shell', status: 'error' }
  hook(read)
  hook(failed)
  expect(read.result?.content).toBe('out')
  expect(failed.result).toBeUndefined()

  const first = completed('shell', { content: 'a' })
  const second = completed('shell', { content: 'b' })
  hook(first)
  hook(second)
  expect(first.result?.content).toBe(`a\n\n${REMINDER}`)
  expect(second.result?.content).toBe('b')
})

test('server appends the reminder once, only for shell tools, only with a graph', async () => {
  withGraph(false)
  const noGraph = (await plugin.server())['tool.execute.after']
  const untouched = { output: 'out' }
  await noGraph({ tool: 'bash' }, untouched)
  expect(untouched.output).toBe('out')

  withGraph(true)
  const hook = (await plugin.server())['tool.execute.after']
  const read = { output: 'out' }
  await hook({ tool: 'read' }, read)
  expect(read.output).toBe('out')

  const first = { output: 'a' }
  const second = { output: 'b' }
  await hook({ tool: 'bash' }, first)
  await hook({ tool: 'bash' }, second)
  expect(first.output).toBe(`a\n\n${REMINDER}`)
  expect(second.output).toBe('b')
})
