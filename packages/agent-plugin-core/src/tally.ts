import { QUERY_LABEL, type OrbitCall } from './calls.ts'
import { hopsOf, queryOf } from './cypher.ts'
import { apiCallsFor, parseResponse } from './stats.ts'

/** A session's Orbit calls, and what the graph queries among them returned. */
export class Tally {
  calls = 0
  nodes = 0
  edges = 0
  apiCalls = 0

  /** Counts one finished call. `response` is its output, or null when the call failed. */
  record(call: OrbitCall, command: string, response: string | null): void {
    this.calls += 1
    if (call.label !== QUERY_LABEL || response === null) return
    const parsed = parseResponse(response)
    this.nodes += parsed.nodes + parsed.aggregated
    this.apiCalls += apiCallsFor(parsed)
    // A count answers with totals and no edges, yet every counted row matched the whole pattern
    this.edges += parsed.edges + parsed.aggregated * hopsOf(queryOf(command))
  }
}
