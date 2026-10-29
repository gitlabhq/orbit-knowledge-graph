import { Plugin, type ToolExecuteAfter, type ToolExecuteBefore, type ToolResult } from '@opencode/plugin'

import { describeOrbitCall, othersText } from '../core/calls.ts'
import { parseResponse } from '../core/stats.ts'
import { type Activity, OrbitUi } from './rpc.ts'

function resultText(result: ToolResult): string {
  if (typeof result.content === 'string' && result.content) return result.content
  if (Array.isArray(result.content)) {
    const text = result.content.flatMap((part) => (part.type === 'text' && typeof part.text === 'string' ? [part.text] : []))
    if (text.length) return text.join('\n')
  }
  return typeof result.output === 'string' ? result.output : ''
}

function isTruncated(result: ToolResult): boolean {
  return typeof result.output === 'object' && result.output !== null && result.output.truncated === true
}

function activityOf(event: ToolExecuteBefore | ToolExecuteAfter, state: Activity['state']): Activity | null {
  const call = describeOrbitCall(event.tool, event.input)
  if (!call) return null
  const activity: Activity = {
    key: event.id,
    state,
    active: call.active,
    done: call.label,
    detail: call.detail + othersText(call),
    sessionID: event.sessionID,
  }
  if (!('status' in event)) return activity
  if (event.status === 'error') return { ...activity, failed: true }
  if (isTruncated(event.result)) return activity
  const counts = parseResponse(resultText(event.result))
  return { ...activity, nodes: counts.nodes + counts.aggregated, edges: counts.edges }
}

export default Plugin.define({
  id: 'orbit-ui',
  async setup(ctx) {
    const rpc = await ctx.rpc.register(OrbitUi, {})
    // Never awaited and never thrown: the UI must not slow down or fail a tool call.
    const send = (event: ToolExecuteBefore | ToolExecuteAfter, state: Activity['state']) => {
      try {
        const activity = activityOf(event, state)
        if (activity) rpc.events.emit('activity', activity).catch(() => {})
      } catch {}
    }
    await ctx.tool.hook('execute.before', (event) => send(event, 'start'))
    await ctx.tool.hook('execute.after', (event) => send(event, 'end'))
  },
})
