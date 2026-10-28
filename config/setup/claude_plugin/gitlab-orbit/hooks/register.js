import { BLUE, MARK, describeOrbitCall } from './orbit.js'
import { compact, parseResponse } from './stats.js'
import { TOKEN_STYLE, layout, queryOf } from './cypher.js'

const stats = { calls: 0, nodes: 0, edges: 0 }

function outputText(answer) {
  const result = answer && answer.result
  if (!result) return ''
  if (typeof result === 'string') return result
  if (typeof result.stdout === 'string') return result.stdout
  const blocks = Array.isArray(result) ? result : Array.isArray(result.content) ? result.content : []
  return blocks.map((b) => (typeof b === 'string' ? b : b && b.text) || '').join('\n')
}

// Count one finished Orbit call; only graph queries carry nodes and edges.
// A large output reaches the hook as a stub, with the full text saved to a file.
async function record($, call, answer) {
  stats.calls += 1
  if (call.label !== 'Explored Orbit' || !answer || answer.isError) return
  const saved = answer.result && answer.result.persistedOutputPath
  const text = saved ? String(await $.fs.read(saved)) : outputText(answer)
  const parsed = parseResponse(text)
  stats.nodes += parsed.nodes + parsed.aggregated
  stats.edges += parsed.edges
  const shape = answer.result && typeof answer.result === 'object' ? Object.keys(answer.result).join(',') : typeof answer.result
  $.ui.log('orbit-ui: ' + call.label + ' read ' + text.length + ' chars (result: ' + shape + '), ' + parsed.nodes + ' nodes, ' + parsed.edges + ' edges, ' + parsed.aggregated + ' counted', { to: 'debug' })
}

function labelText(Text, label) {
  return [Text({ color: BLUE, bold: true, children: [label] })]
}

function finishedRow(Box, Text, call, props) {
  const markColor = props.isErrored ? 'error' : BLUE
  const children = [Text({ color: markColor, bold: true, children: [MARK] }), Text({ children: [' '] })]
  children.push(...labelText(Text, call.label))
  if (call.detail) children.push(Text({ dimColor: true, children: ['  ' + call.detail] }))
  if (props.isInterrupted) children.push(Text({ color: 'error', children: ['  · interrupted'] }))
  else if (props.isErrored) children.push(Text({ color: 'error', children: ['  · failed'] }))
  return Box({ flexDirection: 'row', children })
}

// Back-to-back calls with the same label and detail, as one row each: the latest call's state,
// running while any of them runs, and how many there were
function mergeRepeats(calls) {
  const merged = []
  for (const x of calls) {
    const last = merged[merged.length - 1]
    if (last && last.call.label === x.call.label && last.call.detail === x.call.detail) {
      last.count += 1
      last.isRunning = last.isRunning || x.props.isRunning
      last.props = x.props
    } else {
      merged.push({ ...x, count: 1, isRunning: x.props.isRunning })
    }
  }
  return merged
}

function withCount(detail, count) {
  if (count < 2) return detail
  return (detail ? detail + '  ' : '') + '×' + count
}

function highlighted(Box, Text, query) {
  return Box({
    flexDirection: 'column',
    paddingLeft: 2,
    children: layout(query).map((line) =>
      Text({ children: line.map((t) => Text({ ...TOKEN_STYLE[t.kind], children: [t.text] })) }),
    ),
  })
}

// The expanded view of an Orbit query: its row, the highlighted query, then Claude Code's own row
// with the query elided from the command, since it is drawn above
async function expandedQuery($, e, next, call) {
  const command = e.props.input && e.props.input.command
  const query = queryOf(command)
  if (!query) return next(e)
  const { Box, Text } = $.ui.resolve(e)
  const short = command.replace(query, '…').replace(query.replace(/(["\\$`])/g, '\\$1'), '…')
  const theirs = await next({ ...e, props: { ...e.props, input: { ...e.props.input, command: short } } })
  return Box({ flexDirection: 'column', children: [finishedRow(Box, Text, call, e.props), highlighted(Box, Text, query), theirs] })
}

export function register(on) {
  on('tool.call', async ($, e, next) => {
    const call = describeOrbitCall(e.tool, e)
    if (!call) return next(e)
    let answer
    try {
      answer = await next(e)
      return answer
    } finally {
      // A parsing slip must not fail the hook after the tool has run
      try {
        await record($, call, answer)
      } catch {}
      $.ui.invalidate('ui.render')
    }
  }).catch(($, e, next) => next(e)) // A failure here must never block the tool call

  on('ui.render', { component: 'ToolUse' }, async ($, e, next) => {
    const call = describeOrbitCall(e.props.tool, e.props.input)
    if (!call) return next(e)
    // ctrl+o and --verbose: a query shows highlighted above Claude Code's full row
    if (e.props.isExpanded) return expandedQuery($, e, next, call)
    const { Box, Text, Client } = $.ui.resolve(e)
    if (e.props.isRunning && Client) {
      return Client({ key: 'orbit-row-' + e.props.tool_use_id, module: './orbit-mark.js', props: { kind: 'row', label: call.active, detail: call.detail} })
    }
    return finishedRow(Box, Text, call, e.props)
  })

  // Claude Code folds runs of shell calls into one group line. Orbit calls in the group get
  // their own rows; a group of nothing but Orbit calls draws as those rows alone.
  on('ui.render', { component: 'ToolGroup' }, async ($, e, next) => {
    if (e.props.isExpanded) return next(e)
    const orbitCalls = e.props.calls
      .map((props, i) => ({ props, i, call: describeOrbitCall(props.tool, props.input) }))
      .filter((x) => x.call)
    if (orbitCalls.length === 0) return next(e)
    const { Box, Text, Client } = $.ui.resolve(e)
    const rows = mergeRepeats(orbitCalls).map(({ props, i, call, count, isRunning }) =>
      isRunning && Client
        ? Client({ key: 'orbit-row-' + (props.tool_use_id || i), module: './orbit-mark.js', props: { kind: 'row', label: call.active, detail: withCount(call.detail, count) } })
        : finishedRow(Box, Text, { ...call, detail: withCount(call.detail, count) }, props),
    )
    const onlyOrbit = orbitCalls.length === e.props.calls.length && e.props.isCallsOnly !== false
    if (onlyOrbit) return Box({ flexDirection: 'column', children: rows })
    const theirs = await next(e)
    return Box({ flexDirection: 'column', children: [theirs, Box({ flexDirection: 'column', paddingLeft: 2, children: rows })] })
  })

  on('ui.render', { component: 'PromptHint' }, async ($, e, next) => {
    const theirs = await next(e)
    if (stats.calls === 0) return theirs
    const { Box, Text } = $.ui.resolve(e)
    const blue = (text) => Text({ color: BLUE, children: [text] })
    const dim = (text) => Text({ dimColor: true, children: [text] })
    const sep = () => dim('  ·  ')
    const words = [
      [compact(stats.calls), stats.calls === 1 ? ' query' : ' queries'],
      [compact(stats.nodes), stats.nodes === 1 ? ' node' : ' nodes'],
      [compact(stats.edges), stats.edges === 1 ? ' edge' : ' edges'],
    ]
    const children = []
    words.forEach(([value, unit], i) => {
      if (i > 0) children.push(sep())
      children.push(blue(value))
      children.push(dim(unit))
    })
    children.push(blue('  ' + MARK + ' GitLab Orbit'))
    const line = Box({ flexDirection: 'row', children })
    // Right-aligned under Claude Code's hint. The hint's own row takes no layout change from a mod
    // and its tail slot draws dim text only, so the colored numbers get a row of their own.
    return Box({ flexDirection: 'column', children: [theirs, Box({ flexDirection: 'row', children: [Box({ flexGrow: 1 }), line, Text({ children: [' '] })] })] })
  })
}
