import { QUERY_LABEL, describeOrbitCall, type OrbitCall } from '../core/calls.ts'
import { isOrbitPrefix, runsOrbit } from '../core/command.ts'

export type GroupCall<P extends { isRunning: boolean }> = { props: P; i: number; call: OrbitCall }

export type Row<P extends { isRunning: boolean }> = GroupCall<P> & { isRunning: boolean }

export type TranscriptMessage = { role: string; text: string; toolUses: readonly { tool: string; input: unknown }[] }

function typeKey(name: string): string {
  const lower = name.toLowerCase()
  if (lower.endsWith('ies')) return lower.slice(0, -3) + 'y'
  if (/(s|x|z|ch|sh)es$/.test(lower)) return lower.slice(0, -2)
  return lower.endsWith('s') ? lower.slice(0, -1) : lower
}

function wider(a: string, b: string): string {
  return typeKey(a) === a.toLowerCase() ? b : a
}

export function joinPaths(details: readonly string[]): string {
  const chains: string[][] = []
  const at = (chain: string[], nodes: string[]) =>
    chain.findIndex((_, i) => nodes.every((node, k) => chain[i + k] !== undefined && typeKey(chain[i + k] ?? '') === typeKey(node)))
  const same = (a: string | undefined, b: string | undefined) => a !== undefined && b !== undefined && typeKey(a) === typeKey(b)
  const absorb = (chain: string[], nodes: string[], from: number) =>
    nodes.forEach((node, k) => {
      chain[from + k] = wider(chain[from + k] ?? node, node)
    })
  for (const detail of details) {
    for (const path of detail.split(', ').filter(Boolean)) {
      const nodes = path.split(' → ')
      const holder = chains.find((chain) => at(chain, nodes) >= 0)
      if (holder) {
        absorb(holder, nodes, at(holder, nodes))
        continue
      }
      const within = chains.findIndex((chain) => at(nodes, chain) >= 0)
      if (within >= 0) {
        const merged = [...nodes]
        absorb(merged, chains[within] ?? [], at(nodes, chains[within] ?? []))
        chains[within] = merged
        continue
      }
      const after = chains.find((chain) => same(chain[chain.length - 1], nodes[0]))
      if (after) {
        after[after.length - 1] = wider(after[after.length - 1] ?? '', nodes[0] ?? '')
        after.push(...nodes.slice(1))
        continue
      }
      const before = chains.find((chain) => same(chain[0], nodes[nodes.length - 1]))
      if (before) {
        before[0] = wider(before[0] ?? '', nodes[nodes.length - 1] ?? '')
        before.unshift(...nodes.slice(0, -1))
        continue
      }
      chains.push([...nodes])
    }
  }
  return chains.map((chain) => chain.join(' → ')).join(', ')
}

function queryDetails<P extends { isRunning: boolean }>(calls: readonly GroupCall<P>[]): string[] {
  return calls.filter((x) => x.call.label === QUERY_LABEL).map((x) => x.call.detail)
}

function detailsOf<P extends { isRunning: boolean }>(group: readonly GroupCall<P>[]): string {
  if (group[0]?.call.label === QUERY_LABEL) return joinPaths(queryDetails(group))
  return [...new Set(group.map((x) => x.call.detail).filter(Boolean))].join(', ')
}

export function mergeRuns<P extends { isRunning: boolean }>(calls: readonly GroupCall<P>[]): Row<P>[] {
  const groups: GroupCall<P>[][] = []
  for (const x of calls) {
    const last = groups[groups.length - 1]
    if (last && last[0]?.call.label === x.call.label) last.push(x)
    else groups.push([x])
  }
  return groups.flatMap((group): Row<P>[] => {
    const latest = group[group.length - 1]
    if (!latest) return []
    const running = [...group].reverse().find((x) => x.props.isRunning)
    const detail = detailsOf(group)
    if (running) return [{ ...running, isRunning: true, call: { ...running.call, detail } }]
    return [{ ...latest, isRunning: false, call: { ...latest.call, detail } }]
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
