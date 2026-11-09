import { expect, mock, test } from 'bun:test'

import type { Component } from '@earendil-works/pi-tui'
import type { ExtensionApi, ExtensionContext, RenderContext, Theme, ToolRenderers, ToolResultEvent } from 'pi'

mock.module('@earendil-works/pi-tui', () => ({
  Text: class {
    constructor(public text = '') {}
    setText(text: string) {
      this.text = text
    }
  },
  Container: class {},
}))

const { default: extension } = await import('./index.ts')

const theme: Theme = { fg: (_color, text) => text, bold: (text) => text }
const QUERY = `orbit query '{"query_type":"traversal","nodes":[{"id":"p","entity":"Project"}]}'`

type Resolver = Parameters<ExtensionApi['registerToolRenderer']>[0]

function load() {
  const handlers: Record<string, (event: never, ctx: ExtensionContext) => void> = {}
  let resolver: Resolver | undefined
  extension({
    on: (name: string, handler: (event: never, ctx: ExtensionContext) => void) => {
      handlers[name] = handler
    },
    registerToolRenderer: (given) => {
      resolver = given
    },
  } as ExtensionApi)
  const statuses: (string | undefined)[] = []
  const ctx: ExtensionContext = { hasUI: true, ui: { theme, setStatus: (_key, text) => statuses.push(text) } }
  const rendererFor = (tool: string, theirs?: ToolRenderers) => resolver!(tool, () => theirs)
  return { handlers, statuses, ctx, rendererFor }
}

const textOf = (component: Component) => (component as unknown as { text: string }).text
const context = (over: Partial<RenderContext> = {}): RenderContext => ({ args: undefined, expanded: false, isError: false, isPartial: false, ...over })
const bash = (command: string) => ({ command })

test('an Orbit shell call shows its running and finished label', () => {
  const renderer = load().rendererFor('bash')!
  const args = bash(QUERY)
  expect(textOf(renderer.renderCall!(args, theme, context({ args })))).toContain('◎ Exploring Orbit')
  expect(textOf(renderer.renderCall!(args, theme, context({ args, durationMs: 5 })))).toContain('◎ Explored Orbit')
  expect(textOf(renderer.renderCall!(args, theme, context({ args, durationMs: 5, isError: true })))).toContain('· failed')
})

test('an expanded Orbit call adds the query and shows the raw result, a collapsed one hides it', () => {
  const raw = { render: () => ['raw output'], invalidate() {} }
  const renderer = load().rendererFor('bash', { renderResult: () => raw })!
  const args = bash(QUERY)
  const expanded = textOf(renderer.renderCall!(args, theme, context({ args, expanded: true })))
  expect(expanded).toContain('query_type')
  expect(textOf(renderer.renderCall!(args, theme, context({ args })))).not.toContain('query_type')
  expect(renderer.renderResult!({}, { expanded: true, isPartial: false }, theme, context({ args }))).toBe(raw)
  expect(renderer.renderResult!({}, { expanded: false, isPartial: false }, theme, context({ args }))).not.toBe(raw)
})

test('a shell call that is not Orbit goes to the default renderer', () => {
  const theirs = { render: () => ['theirs'], invalidate() {} }
  const renderer = load().rendererFor('bash', { renderCall: () => theirs, renderResult: () => theirs })!
  const args = bash('ls -la')
  expect(renderer.renderCall!(args, theme, context({ args }))).toBe(theirs)
  expect(renderer.renderResult!({}, { expanded: false, isPartial: false }, theme, context({ args }))).toBe(theirs)
})

test('tools that are neither a shell nor Orbit keep their own renderer', () => {
  const theirs: ToolRenderers = {}
  expect(load().rendererFor('read', theirs)).toBe(theirs)
})

test('each finished Orbit query updates the usage line and a new session clears it', () => {
  const { handlers, statuses, ctx } = load()
  const event: ToolResultEvent = {
    toolName: 'bash',
    input: bash(QUERY),
    content: [{ type: 'text', text: JSON.stringify({ result: { query_type: 'traversal', nodes: [{ type: 'Project' }, { type: 'Project' }], edges: [{}] } }) }],
  }
  handlers['tool_result']!(event as never, ctx)
  const line = statuses.at(-1) ?? ''
  expect(line).toContain('1 call')
  expect(line).toContain('2 nodes')
  expect(line).toContain('1 edge')
  handlers['tool_result']!({ ...event, isError: true } as never, ctx)
  handlers['tool_result']!({ ...event, input: bash('ls') } as never, ctx)
  expect(statuses).toHaveLength(1)
  handlers['session_start']!(undefined as never, ctx)
  expect(statuses.at(-1)).toBeUndefined()
})

test('a result that cannot be read never throws into Pi', () => {
  const { handlers, ctx } = load()
  expect(() => handlers['tool_result']!({ toolName: 'bash', input: bash(QUERY), content: undefined } as never, ctx)).not.toThrow()
})
