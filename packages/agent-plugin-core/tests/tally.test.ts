import { expect, test } from 'bun:test'

import { describeCommand } from '../src/calls.ts'
import { Tally } from '../src/tally.ts'

const traversal = ['query_type: traversal', 'nodes:', '  MergeRequest[3]{id}:', '    1', '    2', '    3', 'edges[2]:'].join('\n')

test('every Orbit call counts, and only graph queries add nodes and edges', () => {
  const tally = new Tally()
  const query = 'glab orbit query "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN mr"'
  tally.record(describeCommand(query)!, query, traversal)
  tally.record(describeCommand('orbit grep foo')!, 'orbit grep foo', traversal)
  expect(tally).toMatchObject({ calls: 2, nodes: 3, edges: 2, apiCalls: 1 })
})

test('a failed query counts as a call with nothing returned', () => {
  const tally = new Tally()
  tally.record(describeCommand('orbit query "MATCH (n) RETURN n"')!, 'orbit query "MATCH (n) RETURN n"', null)
  expect(tally).toMatchObject({ calls: 1, nodes: 0, edges: 0, apiCalls: 0 })
})
