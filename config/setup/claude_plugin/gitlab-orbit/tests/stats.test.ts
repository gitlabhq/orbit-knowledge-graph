import { expect, test } from 'claude-code/testing'

import { layout, queryOf, tokenize } from '../hooks/cypher.ts'
import { compact, parseResponse } from '../hooks/stats.ts'

test('the llm format counts nodes by entity and edges', () => {
  const text = [
    'query_type: traversal',
    'nodes:',
    '  MergeRequest[10]{id,iid,title}:',
    '    1,2,"a"',
    '  Project[1]{id}:',
    '    5',
    'edges[10]:',
  ].join('\n')
  expect(parseResponse(text)).toMatchObject({ nodes: 11, edges: 10, byEntity: { MergeRequest: 10, Project: 1 } })
})

test('the raw JSON format counts nodes and edges', () => {
  const text = JSON.stringify({ result: { nodes: [{ type: 'User' }, { type: 'User' }], edges: [{}] } })
  expect(parseResponse(text)).toMatchObject({ nodes: 2, edges: 1, byEntity: { User: 2 } })
})

test('an aggregation counts the nodes its count columns cover', () => {
  const text = [
    'query_type: aggregation',
    'columns[2]{name,function,type}:',
    '  state,group,string',
    '  mrs,count,int',
    'rows[2]{state,mrs}:',
    '  opened,40',
    '  merged,60',
  ].join('\n')
  expect(parseResponse(text).aggregated).toBe(100)
})

test('schema and other output reads as empty', () => {
  expect(parseResponse('domains[1]:\n  - name: code_review')).toMatchObject({ nodes: 0, edges: 0, aggregated: 0 })
})

test('counts compact for the status line', () => {
  expect([950, 1200, 12000, 1_200_000].map(compact)).toEqual(['950', '1.2k', '12k', '1.2M'])
})

test('a query lays out one clause per line', () => {
  const lines = layout('MATCH (mr:MergeRequest) WHERE mr.state = "opened" RETURN mr.iid LIMIT 10')
  expect(lines.map((line) => line.map((t) => t.text).join(''))).toEqual([
    'MATCH (mr:MergeRequest)',
    'WHERE mr.state = "opened"',
    'RETURN mr.iid',
    'LIMIT 10',
  ])
})

test('labels and relationship types are told apart', () => {
  const kinds = Object.fromEntries(tokenize('(mr:MergeRequest)-[:IN_PROJECT]->(p)').map((t) => [t.text, t.kind]))
  expect(kinds['MergeRequest']).toBe('label')
  expect(kinds['IN_PROJECT']).toBe('relationship')
})

test('the query is read from the command, quotes undone', () => {
  expect(queryOf('glab orbit query "MATCH (n) WHERE n.x = \\"a\\" RETURN n"')).toBe('MATCH (n) WHERE n.x = "a" RETURN n')
  expect(queryOf("glab orbit query 'MATCH (n) RETURN n' | head -3")).toBe('MATCH (n) RETURN n')
  expect(queryOf('glab orbit grep foo')).toBe('')
})
