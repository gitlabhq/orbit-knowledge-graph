import type { Elements, Register, RenderElement } from 'claude-code'

import { QUERY_LABEL, describeOrbitCall, type OrbitCall } from '../core/calls.ts'
import { cypherSegments, hopsOf, queryOf } from '../core/cypher.ts'
import { apiCallsFor, compact, graphStatus, grouped, parseResponse } from '../core/stats.ts'
import { codeLines, layout, type Token } from './highlight.ts'
import type { MarkProps } from './orbit-mark.ts'
import { isPendingCall, mergeRuns } from './rows.ts'
import { BLUE, MARK, TOKEN_STYLE } from './theme.ts'

type Ui = Pick<Elements['terminal'], 'Box' | 'Text'>
type Resolved = Elements[keyof Elements]

function clientOf(ui: Resolved): Elements['terminal']['Client'] | undefined {
  return 'Client' in ui ? ui.Client : undefined
}
type RowState = { isErrored?: boolean; isInterrupted?: boolean }

const stats = { calls: 0, nodes: 0, edges: 0, apiCalls: 0 }
let graphTotal: number | null = null
let graphTotalAsked = false
let knownEntities: ReadonlySet<string> = new Set()

function outputText(result: unknown): string {
  if (typeof result === 'string') return result
  if (typeof result !== 'object' || result === null) return ''
  const { stdout, content } = result as { stdout?: unknown; content?: unknown }
  if (typeof stdout === 'string') return stdout
  const blocks: unknown[] = Array.isArray(result) ? result : Array.isArray(content) ? content : []
  return blocks
    .map((b) => (typeof b === 'string' ? b : ((b as { text?: unknown } | null)?.text ?? '')))
    .map(String)
    .join('\n')
}

async function responseText(read: (path: string) => Promise<unknown>, answer: unknown): Promise<string | null> {
  const { result, isError, deny } = (answer ?? {}) as { result?: unknown; isError?: boolean; deny?: string }
  if (!answer || isError || deny !== undefined) return null
  const saved = (result as { persistedOutputPath?: unknown } | null)?.persistedOutputPath
  return typeof saved === 'string' ? String(await read(saved)) : outputText(result)
}

function finishedRow({ Box, Text }: Ui, call: OrbitCall, state: RowState): RenderElement {
  const children: RenderElement[] = [
    Text({ color: state.isErrored ? 'error' : BLUE, bold: true, children: [MARK] }),
    Text({ children: [' '] }),
    Text({ color: BLUE, bold: true, children: [call.label] }),
  ]
  if (call.detail) children.push(Text({ dimColor: true, children: ['  ' + call.detail] }))
  if (state.isInterrupted) children.push(Text({ color: 'error', children: ['  · interrupted'] }))
  else if (state.isErrored) children.push(Text({ color: 'error', children: ['  · failed'] }))
  return Box({ flexDirection: 'row', children })
}

function markProps(call: OrbitCall): MarkProps {
  return { label: call.active, detail: call.detail }
}

function highlighted({ Box, Text }: Ui, query: string): RenderElement {
  return Box({
    flexDirection: 'column',
    paddingLeft: 2,
    children: layout(query).map((line) =>
      Text({ children: line.map((t) => Text({ ...TOKEN_STYLE[t.kind], children: [t.text] })) }),
    ),
  })
}

function tokenLine({ Text }: Ui, line: readonly Token[]): RenderElement {
  return Text({ children: line.length ? line.map((t) => Text({ ...TOKEN_STYLE[t.kind], children: [t.text] })) : [' '] })
}

function codeBlock(ui: Ui, code: string): RenderElement {
  return ui.Box({ flexDirection: 'column', children: codeLines(code).map((line) => tokenLine(ui, line)) })
}

export const register: Register = (on) => {
  on('tool.call', async ($, e, next) => {
    const call = describeOrbitCall(e.tool, e)
    if (!call) return next(e)
    const raw = (e as { command?: unknown }).command
    const command = typeof raw === 'string' ? raw : ''
    let answer: unknown
    try {
      answer = await next(e)
      return answer as Awaited<ReturnType<typeof next>>
    } finally {
      try {
        const text = await responseText((path) => $.fs.read(path), answer)
        if (text !== null) stats.calls += 1
        if (call.label === QUERY_LABEL && text !== null) {
          const parsed = parseResponse(text)
          stats.nodes += parsed.nodes + parsed.aggregated
          stats.apiCalls += apiCallsFor(parsed)
          stats.edges += parsed.edges + parsed.aggregated * hopsOf(queryOf(command))
          if (!graphTotalAsked) {
            graphTotalAsked = true
            void (async () => {
              for (const launcher of [['glab', 'orbit'], ['orbit']]) {
                const run = await $.process.run([...launcher, 'graph-status', '--response-format', 'raw'], { timeoutMs: 30000 }).catch(() => null)
                const graph = run?.exitCode === 0 ? graphStatus(run.stdout) : null
                if (!graph) continue
                graphTotal = graph.total
                knownEntities = new Set(graph.entities)
                return $.ui.invalidate('ui.render')
              }
            })()
          }
        }
      } catch {}
      $.ui.invalidate('ui.render')
    }
  }).catch((_$, e, next) => next(e))

  on('ui.render', { component: 'ToolUse' }, async ($, e, next) => {
    const call = describeOrbitCall(e.props.tool, e.props.input, knownEntities)
    if (!call) return next(e)
    const ui = $.ui.resolve(e)
    const { Box, Text } = ui
    const Client = clientOf(ui)
    const command = (e.props.input as { command?: unknown } | undefined)?.command
    const query = typeof command === 'string' ? queryOf(command) : ''
    if (e.props.isExpanded) {
      if (!query || typeof command !== 'string') return next(e)
      const short = command.replace(query, '…').replace(query.replace(/(["\\$`])/g, '\\$1'), '…')
      const theirs = await next({ ...e, props: { ...e.props, input: { ...(e.props.input as object), command: short } } })
      return Box({
        flexDirection: 'column',
        children: [finishedRow({ Box, Text }, call, e.props), highlighted({ Box, Text }, query), theirs],
      })
    }
    if (e.props.isRunning && Client) {
      return Client({ key: 'orbit-row-' + e.props.tool_use_id, module: './orbit-mark.ts', props: markProps(call) })
    }
    return finishedRow({ Box, Text }, call, e.props)
  }).catch((_$, e, next) => next(e))

  on('ui.render', { component: 'ToolGroup' }, async ($, e, next) => {
    if (e.props.isExpanded) return next(e)
    const orbitCalls = e.props.calls.flatMap((props, i) => {
      const call = describeOrbitCall(props.tool, props.input, knownEntities)
      return call ? [{ props, i, call }] : []
    })
    if (orbitCalls.length === 0) return next(e)
    const ui = $.ui.resolve(e)
    const { Box, Text } = ui
    const Client = clientOf(ui)
    const rows = mergeRuns(orbitCalls).map(({ props, call, isRunning }, n) =>
      isRunning && Client
        ? Client({ key: 'orbit-run-' + n, module: './orbit-mark.ts', props: markProps(call) })
        : finishedRow({ Box, Text }, call, props),
    )
    const onlyOrbit = e.props.calls.every((props) => describeOrbitCall(props.tool, props.input) || isPendingCall(props))
    if (onlyOrbit) return Box({ flexDirection: 'column', children: rows })
    const theirs = await next(e)
    return Box({ flexDirection: 'column', children: [theirs, Box({ flexDirection: 'column', paddingLeft: 2, children: rows })] })
  }).catch((_$, e, next) => next(e))

  on('ui.render', { component: 'PromptHint' }, async ($, e, next) => {
    const theirs = await next(e)
    if (stats.calls === 0) return theirs
    const { Box, Text } = $.ui.resolve(e)
    const blue = (text: string) => Text({ color: BLUE, children: [text] })
    const dim = (text: string) => Text({ dimColor: true, children: [text] })
    const words: [string, string][] = [
      [compact(stats.calls), stats.calls === 1 ? ' query' : ' queries'],
      [compact(stats.apiCalls), stats.apiCalls === 1 ? ' API call replaced' : ' API calls replaced'],
      graphTotal
        ? [compact(stats.nodes) + ' / ' + grouped(graphTotal), ' nodes']
        : [compact(stats.nodes), stats.nodes === 1 ? ' node' : ' nodes'],
      [compact(stats.edges), stats.edges === 1 ? ' edge' : ' edges'],
    ]
    const children = words.flatMap(([value, unit], i) => [...(i > 0 ? [dim('  ·  ')] : []), blue(value), dim(unit)])
    children.push(blue('  ' + MARK + ' GitLab Orbit'))
    const line = Box({ flexDirection: 'row', children })
    return Box({
      flexDirection: 'column',
      children: [theirs, Box({ flexDirection: 'row', children: [Box({ flexGrow: 1 }), line, Text({ children: [' '] })] })],
    })
  }).catch((_$, e, next) => next(e))

  on('ui.render', { component: 'AssistantMessage' }, async ($, e, next) => {
    const segments = cypherSegments(e.props.text)
    if (!segments.some((part) => part.kind === 'code')) return next(e)
    const ui = $.ui.resolve(e)
    const { Box, Markdown } = ui
    const [first, ...rest] = segments
    const leadsWithProse = first?.kind === 'prose'
    const theirs = leadsWithProse ? await next({ ...e, props: { ...e.props, text: first.text } }) : null
    const body = (leadsWithProse ? rest : segments).map((part, i) =>
      part.kind === 'code' ? codeBlock(ui, part.text) : Markdown({ key: 'orbit-prose-' + i, text: part.text }),
    )
    const blocks = Box({ flexDirection: 'column', paddingLeft: 2, rowGap: 1, children: body })
    return Box({ flexDirection: 'column', rowGap: 1, children: theirs ? [theirs, blocks] : [blocks] })
  }).catch((_$, e, next) => next(e))
}
