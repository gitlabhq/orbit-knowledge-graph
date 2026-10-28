import { QUERY_LABEL, type OrbitCall } from '../core/calls.ts'
import { isOrbitPrefix } from '../core/command.ts'

export type GroupCall<P extends { isRunning: boolean }> = { props: P; i: number; call: OrbitCall }

export type Row<P extends { isRunning: boolean }> = GroupCall<P> & { isRunning: boolean }

export function joinPaths(details: readonly string[]): string {
  const chains: string[][] = []
  const holds = (chain: string[], nodes: string[]) =>
    chain.some((_, i) => nodes.every((node, k) => chain[i + k] === node))
  for (const detail of details) {
    for (const path of detail.split(', ').filter(Boolean)) {
      const nodes = path.split(' → ')
      if (chains.some((chain) => holds(chain, nodes))) continue
      const within = chains.findIndex((chain) => holds(nodes, chain))
      if (within >= 0) {
        chains[within] = [...nodes]
        continue
      }
      const after = chains.find((chain) => chain[chain.length - 1] === nodes[0])
      if (after) {
        after.push(...nodes.slice(1))
        continue
      }
      const before = chains.find((chain) => chain[0] === nodes[nodes.length - 1])
      if (before) {
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

function startsNewPath<P extends { isRunning: boolean }>(row: readonly GroupCall<P>[], x: GroupCall<P>): boolean {
  if (x.call.label !== QUERY_LABEL || !x.call.detail) return false
  const details = queryDetails(row)
  if (details.length === 0) return false
  const chains = (list: string[]) => joinPaths(list).split(', ').filter(Boolean).length
  return chains([...details, x.call.detail]) > chains(details)
}

export function mergeRuns<P extends { isRunning: boolean }>(calls: readonly GroupCall<P>[]): Row<P>[] {
  const runs: GroupCall<P>[][] = []
  for (const x of calls) {
    const last = runs[runs.length - 1]
    const follows = last !== undefined && x.i === (last[last.length - 1]?.i ?? -2) + 1
    if (last && follows && !startsNewPath(last, x)) last.push(x)
    else runs.push([x])
  }
  return runs.flatMap((run): Row<P>[] => {
    const latest = run[run.length - 1]
    if (!latest) return []
    const running = [...run].reverse().find((x) => x.props.isRunning)
    const walked = joinPaths(queryDetails(run))
    if (running) {
      const detail = running.call.label === QUERY_LABEL ? walked : running.call.detail
      return [{ ...running, isRunning: true, call: { ...running.call, detail } }]
    }
    if (!walked) return [{ ...latest, isRunning: false }]
    return [{ ...latest, isRunning: false, call: { ...latest.call, label: QUERY_LABEL, detail: walked } }]
  })
}

export function isPendingCall(props: { tool: string; input: unknown; output?: unknown }): boolean {
  if (props.tool !== 'Bash' || props.output !== undefined) return false
  const input = props.input as { command?: unknown } | undefined
  return isOrbitPrefix(typeof input?.command === 'string' ? input.command : '')
}
