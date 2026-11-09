import { readFileSync } from 'node:fs'

import { Container, Text } from '@earendil-works/pi-tui'
import type { Color, ExtensionApi, ExtensionContext, RenderContext, Theme, ToolRenderers, ToolResultEvent } from 'pi'

import { MARK, QUERY_LABEL, describeOrbitCall, othersText } from '../core/calls.ts'
import { hopsOf, queryOf } from '../core/cypher.ts'
import { apiCallsFor, compact, parseResponse } from '../core/stats.ts'
import { layout, type TokenKind } from '../hooks/highlight.ts'

const TOKEN_COLOR: Partial<Record<TokenKind, Color>> = {
  keyword: 'accent',
  label: 'accent',
  relationship: 'accent',
  string: 'success',
  number: 'warning',
  punctuation: 'dim',
  comment: 'dim',
}

const stats = { calls: 0, nodes: 0, edges: 0, apiCalls: 0 }

function commandOf(input: unknown): string {
  const command = (input as { command?: unknown } | null)?.command
  return typeof command === 'string' ? command : ''
}

function rowText(theme: Theme, call: NonNullable<ReturnType<typeof describeOrbitCall>>, context: RenderContext): string {
  const finished = !context.isPartial
  const tone: Color = context.isError ? 'error' : 'accent'
  let text = theme.fg(tone, theme.bold(MARK + ' ' + (finished ? call.label : call.active)))
  if (call.detail) text += theme.fg('muted', '  ' + call.detail)
  const others = othersText(call)
  if (others) text += theme.fg('warning', others)
  if (context.isError) text += theme.fg('error', '  · failed')
  return text
}

function queryLines(theme: Theme, query: string): string {
  return layout(query)
    .map((line) => '  ' + line.map((token) => {
      const color = TOKEN_COLOR[token.kind]
      return color ? theme.fg(color, token.text) : token.text
    }).join(''))
    .join('\n')
}

function orbitRenderers(toolName: string, theirs: ToolRenderers | undefined): ToolRenderers {
  return {
    renderShell: theirs?.renderShell,
    renderCall(args, theme, context) {
      const call = describeOrbitCall(toolName, args)
      if (!call) return theirs?.renderCall?.(args, theme, context) ?? new Text(theme.fg('toolTitle', toolName), 0, 0)
      const query = queryOf(commandOf(args))
      const body = context.expanded && query ? rowText(theme, call, context) + '\n' + queryLines(theme, query) : rowText(theme, call, context)
      const text = (context.lastComponent as Text | undefined) ?? new Text('', 0, 0)
      text.setText(body)
      return text
    },
    renderResult(result, options, theme, context) {
      if (!options.expanded && describeOrbitCall(toolName, context.args)) return new Container()
      return theirs?.renderResult?.(result, options, theme, context) ?? new Container()
    },
  }
}

// Pi keeps only the tail of a long result and names the full file in `details`. The output guard of
// the nightly channel shortens it first and names its spill file only in the text it leaves.
const FULL_OUTPUT = /Full output: ([^\s\]]+)/

function responseText(event: ToolResultEvent): string {
  const shown = (event.content ?? []).map((block) => block.text ?? '').join('\n')
  const path = event.details?.fullOutputPath ?? shown.match(FULL_OUTPUT)?.[1]
  if (!path) return shown
  try {
    return readFileSync(path, 'utf8')
  } catch {
    return shown
  }
}

function record(event: ToolResultEvent): boolean {
  const call = describeOrbitCall(event.toolName, event.input)
  if (!call || event.isError) return false
  stats.calls += 1
  if (call.label !== QUERY_LABEL) return true
  const command = commandOf(event.input)
  const parsed = parseResponse(responseText(event))
  stats.nodes += parsed.nodes + parsed.aggregated
  stats.apiCalls += apiCallsFor(parsed, queryOf(command))
  stats.edges += parsed.edges + parsed.aggregated * hopsOf(queryOf(command))
  return true
}

function usageStatus(theme: Theme): string | undefined {
  if (stats.calls === 0) return undefined
  const dim = (text: string) => theme.fg('dim', text)
  const blue = (text: string) => theme.fg('accent', text)
  const part = (n: number, one: string, many: string) => blue(compact(n)) + dim(n === 1 ? one : many)
  return [
    part(stats.calls, ' call', ' calls'),
    blue('~' + compact(stats.apiCalls)) + dim(stats.apiCalls === 1 ? ' API call replaced' : ' API calls replaced'),
    part(stats.nodes, ' node', ' nodes'),
    part(stats.edges, ' edge', ' edges'),
  ].join(dim('  ·  ')) + blue('  ' + MARK + ' GitLab Orbit')
}

function show(ctx: ExtensionContext): void {
  if (ctx.hasUI) ctx.ui.setStatus('orbit', usageStatus(ctx.ui.theme))
}

export default function (pi: ExtensionApi) {
  // Older Pi has no renderer hook, and an extension that throws on load is reported at every start.
  if (typeof pi.registerToolRenderer !== 'function') return
  pi.registerToolRenderer((toolName, next) => {
    const theirs = next()
    const isShell = toolName.toLowerCase() === 'bash'
    return isShell || describeOrbitCall(toolName, undefined) ? orbitRenderers(toolName, theirs) : theirs
  })
  pi.on('session_start', (_event, ctx) => {
    Object.assign(stats, { calls: 0, nodes: 0, edges: 0, apiCalls: 0 })
    show(ctx)
  })
  pi.on('tool_result', (event, ctx) => {
    try {
      if (record(event)) show(ctx)
    } catch {}
  })
}
