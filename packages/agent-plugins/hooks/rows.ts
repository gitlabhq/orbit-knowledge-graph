import { describeOrbitCall, type OrbitCall } from '../core/calls.ts'
import { isOrbitPrefix, runsOrbit } from '../core/command.ts'

export type GroupCall<P extends { isRunning: boolean }> = { props: P; i: number; call: OrbitCall }

export type Row<P extends { isRunning: boolean }> = GroupCall<P> & { isRunning: boolean }

export type TranscriptMessage = { role: string; text: string; toolUses: readonly { tool: string; input: unknown }[] }

export function mergeRuns<P extends { isRunning: boolean; isErrored?: boolean }>(calls: readonly GroupCall<P>[]): Row<P>[] {
  const groups: GroupCall<P>[][] = []
  for (const x of calls) {
    const last = groups[groups.length - 1]
    const first = last?.[0]
    if (last && first?.call.label === x.call.label && first.call.detail === x.call.detail && !first.props.isErrored === !x.props.isErrored) last.push(x)
    else groups.push([x])
  }
  return groups.flatMap((group): Row<P>[] => {
    const shown = [...group].reverse().find((x) => x.props.isRunning) ?? group[group.length - 1]
    if (!shown) return []
    const also = [...new Set(group.flatMap((x) => x.call.also ?? []))]
    return [{ ...shown, isRunning: shown.props.isRunning, call: { ...shown.call, ...(also.length ? { also } : {}) } }]
  })
}

export function isPendingCall(props: { tool: string; input: unknown; output?: unknown }): boolean {
  if (props.tool !== 'Bash' || props.output !== undefined) return false
  const input = props.input as { command?: unknown } | undefined
  return isOrbitPrefix(typeof input?.command === 'string' ? input.command : '')
}

export function usesOrbit(tool: string, input: unknown): boolean {
  if (describeOrbitCall(tool, input)) return true
  const { command, skill } = (input ?? {}) as { command?: unknown; skill?: unknown }
  if (tool === 'Skill') return skill === 'orbit'
  return tool === 'Bash' && typeof command === 'string' && runsOrbit(command)
}

export function isProgressText(text: string): boolean {
  const flat = text.trim()
  return flat.length > 0 && flat.length <= 160 && !flat.includes('\n') && !/[?\d]/.test(flat) && /\b(next|now|let me|i'll|i will)\b/i.test(flat)
}

export function leadsIntoOrbit(messages: readonly TranscriptMessage[], text: string): boolean {
  if (!isProgressText(text)) return false
  const at = messages.findLastIndex((m) => m.role === 'assistant' && m.text.trim() === text)
  if (at < 0) return false
  const next = messages.slice(at).find((m, k) => m.toolUses.length > 0 || m.role !== 'assistant' || (k > 0 && m.text.trim() !== ''))
  return next?.role === 'assistant' && next.toolUses.some((use) => usesOrbit(use.tool, use.input))
}
