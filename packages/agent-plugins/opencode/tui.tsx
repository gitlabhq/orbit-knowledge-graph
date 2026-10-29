/** @jsxImportSource @opentui/solid */
import { Plugin } from '@opencode/plugin/tui'

import { type Activity, OrbitUi } from './rpc.ts'

// The TUI part of the OpenCode 2 plugin. It ships as source, not bundled, because OpenCode
// compiles TUI JSX itself, so it imports nothing but rpc.ts.
//
// Activity line, above the prompt (session.composer.top), one blank line above the input:
//   running:  ◎ Exploring Orbit  Merge requests → Project   (light Orbit blue)
//   finished: ◎ Explored Orbit  Merge requests → Project    (Orbit blue, then clears)
//   failed:   ◎ Explored Orbit  Merge requests → Project · failed   (red mark)
//
// Usage bar, in the prompt footer (prompt.footer.status), once the session calls Orbit:
//   3 queries · 3.2k nodes · 20 edges  ◎ GitLab Orbit

const MARK = '◎'
const ORBIT_BLUE = '#949dcb'
const ORBIT_LIGHT = '#b5dedc'
const LINGER_MS = 2500
// A start whose end never arrives clears after this, or when its session stops running.
const STALE_MS = 60_000
const SESSION_STOPPED = ['session.execution.succeeded', 'session.execution.failed', 'session.execution.interrupted']

type Usage = { calls: number; nodes: number; edges: number }

// The session a server event belongs to, when it names one.
function sessionOf(event: unknown): string | undefined {
  const record = (value: unknown) => (typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {})
  const id = record(record(event)['data'])['sessionID'] ?? record(event)['sessionID']
  return typeof id === 'string' ? id : undefined
}

// 950, 1.2k, 12k, 1.2M. Same as compact() in core/stats.ts.
function compact(n: number): string {
  if (n < 1000) return String(n)
  if (n < 10000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  if (n < 1e6) return Math.round(n / 1000) + 'k'
  return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M'
}

export default Plugin.define({
  id: 'orbit-ui.tui',
  setup(context) {
    const [state, update] = context.storage.memory('activity', {
      initial: { lines: [] as Activity[], usage: {} as Record<string, Usage>, lastSessionID: '' },
    })
    const timers = new Map<string, ReturnType<typeof setTimeout>>()
    // Calls whose end arrived. Events are not awaited, so a fast call's end can arrive first.
    const ended = new Set<string>()

    // Memory storage outlives a plugin reload but these timers do not, so lines left by the
    // previous instance would never clear. Start without them; the usage counts stay.
    update((draft) => {
      draft.lines = []
    })

    const expire = (key: string, ms: number) => {
      clearTimeout(timers.get(key))
      timers.set(
        key,
        setTimeout(() => {
          timers.delete(key)
          ended.delete(key)
          update((draft) => {
            draft.lines = draft.lines.filter((line) => line.key !== key)
          })
        }, ms),
      )
    }

    const stop = context.client.rpc(OrbitUi).events.on('activity', ({ data }) => {
      if (data.state === 'start' && ended.has(data.key)) return
      if (data.state === 'end') ended.add(data.key)
      update((draft) => {
        draft.lines = [...draft.lines.filter((line) => line.key !== data.key), data]
        draft.lastSessionID = data.sessionID
        if (data.state === 'end') {
          const usage = draft.usage[data.sessionID] ?? { calls: 0, nodes: 0, edges: 0 }
          draft.usage[data.sessionID] = {
            calls: usage.calls + 1,
            nodes: usage.nodes + (data.nodes ?? 0),
            edges: usage.edges + (data.edges ?? 0),
          }
        }
      })
      expire(data.key, data.state === 'start' ? STALE_MS : LINGER_MS)
    })

    // An interrupted call may never send its end. When a session stops, its running lines go.
    const stopSessions = SESSION_STOPPED.map((type) =>
      context.data.on(type, (event) => {
        const sessionID = sessionOf(event)
        update((draft) => {
          draft.lines = draft.lines.filter(
            (line) => line.state === 'end' || (sessionID !== undefined && line.sessionID !== sessionID),
          )
        })
      }),
    )

    const linesFor = (sessionID?: string) => state.lines.filter((line) => !sessionID || line.sessionID === sessionID)
    // A slot that is not given a session shows the session of the latest Orbit call.
    const usageFor = (sessionID?: string) => state.usage[sessionID || state.lastSessionID]

    const Line = (props: { line: Activity }) => {
      const t = context.theme
      const tone = () => (props.line.state === 'start' ? ORBIT_LIGHT : ORBIT_BLUE)
      const red = () => t.text.feedback.error?.base ?? t.text.feedback.warning.base
      return (
        <box flexDirection="row" paddingLeft={1}>
          <text fg={props.line.failed ? red() : tone()}>{MARK + ' '}</text>
          <text fg={tone()}>{props.line.state === 'start' ? props.line.active : props.line.done}</text>
          {props.line.detail ? <text fg={t.text.muted}>{'  ' + props.line.detail}</text> : null}
          {props.line.failed ? <text fg={t.text.muted}>{' · failed'}</text> : null}
        </box>
      )
    }

    // Numbers and the name in Orbit blue, the words between them grey.
    const UsageBar = (props: { usage: Usage }) => {
      const muted = context.theme.text.muted
      const count = (n: number, one: string, many: string) => [
        <text fg={ORBIT_BLUE}>{compact(n)}</text>,
        <text fg={muted}>{' ' + (n === 1 ? one : many)}</text>,
      ]
      return (
        <box flexDirection="row">
          {count(props.usage.calls, 'query', 'queries')}
          <text fg={muted}>{' · '}</text>
          {count(props.usage.nodes, 'node', 'nodes')}
          <text fg={muted}>{' · '}</text>
          {count(props.usage.edges, 'edge', 'edges')}
          <text fg={ORBIT_BLUE}>{'  ' + MARK + ' GitLab Orbit'}</text>
        </box>
      )
    }

    // render runs once; the reads inside the JSX expressions keep each slot reactive.
    const removeLines = context.ui.slot({
      append: 'session.composer.top',
      render: (props) => (
        <box flexDirection="column" marginBottom={linesFor(props.sessionID).length > 0 ? 1 : 0}>
          {linesFor(props.sessionID).map((line) => (
            <Line line={line} />
          ))}
        </box>
      ),
    })
    const removeUsage = context.ui.slot({
      append: 'prompt.footer.status',
      render: (props) => (
        <box flexDirection="row">
          {(usageFor(props.sessionID)?.calls ?? 0) > 0 ? <UsageBar usage={usageFor(props.sessionID)!} /> : null}
        </box>
      ),
    })

    return () => {
      stop()
      stopSessions.forEach((unsubscribe) => unsubscribe())
      removeLines?.()
      removeUsage?.()
      timers.forEach(clearTimeout)
      timers.clear()
    }
  },
})
