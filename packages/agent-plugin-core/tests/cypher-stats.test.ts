import { expect, test } from 'bun:test'

import { codeLines, cypherSegments, hopsOf, layout, queryOf, tokenize } from '../src/cypher.ts'
import { apiCallsFor, compact, graphNodes, grouped, parseResponse } from '../src/stats.ts'

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
  expect(queryOf('orbit query --response-format raw "MATCH (n) RETURN n"')).toBe('MATCH (n) RETURN n')
  expect(queryOf('orbit query --file q.json')).toBe('')
  expect(queryOf('echo "glab orbit query \\"MATCH (n) RETURN n\\""')).toBe('')
})

test('the graph total sums every entity in every domain of graph-status', () => {
  const json = JSON.stringify({
    indexing: { state: 'READY' },
    itemCounts: { domains: [{ entities: [{ name: 'Job', count: '658940' }, { name: 'Stage', count: '147995' }] }, { entities: [{ name: 'Project', count: '1' }] }] },
  })
  expect(graphNodes(json)).toBe(806936)
  expect(graphNodes('not json')).toBeNull()
  expect(graphNodes('{}')).toBeNull()
})

test('a graph total is written in full with thousands separators', () => {
  expect(grouped(1675400)).toBe('1,675,400')
  expect(grouped(950)).toBe('950')
})

test('comments are their own tokens: //, /* */, and a line that starts with --', () => {
  expect(tokenize('MATCH (n) // trailing').at(-1)).toEqual({ text: '// trailing', kind: 'comment' })
  expect(tokenize('/* note */ RETURN n')[0]).toEqual({ text: '/* note */', kind: 'comment' })
  const lines = codeLines('-- 1. Check the schema\nCALL db.schema(\'Vulnerability\')\n')
  expect(lines[0]).toEqual([{ text: '-- 1. Check the schema', kind: 'comment' }])
  expect(lines[1]?.map((t) => t.kind)).toEqual(['keyword', 'space', 'variable', 'punctuation', 'function', 'punctuation', 'string', 'punctuation'])
  expect(lines).toHaveLength(2)
})

test('a reply splits around its cypher blocks and leaves other code alone', () => {
  const reply = 'Here it is.\n\n```cypher\nMATCH (n:Project {id: 1})\nRETURN n\n```\n\n```bash\nls\n```\nDone.'
  expect(cypherSegments(reply)).toEqual([
    { kind: 'prose', text: 'Here it is.' },
    { kind: 'code', text: 'MATCH (n:Project {id: 1})\nRETURN n\n' },
    { kind: 'prose', text: '```bash\nls\n```\nDone.' },
  ])
  expect(cypherSegments('No code here.')).toEqual([{ kind: 'prose', text: 'No code here.' }])
  expect(cypherSegments('```cypher\nMATCH (n)\n')).toEqual([{ kind: 'prose', text: '```cypher\nMATCH (n)' }])
})

test('hops count each relationship in the MATCH patterns, a variable-length one by its lower bound', () => {
  expect(hopsOf('MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) RETURN count(mr) AS n')).toBe(1)
  expect(hopsOf('MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 1}) RETURN u, count(mr) AS n')).toBe(2)
  expect(hopsOf('MATCH (c:Definition)-[:EXTENDS*2..3]->(d:Definition {id: 1}) RETURN count(c) AS n')).toBe(2)
  expect(hopsOf('MATCH (mr:MergeRequest {id: 1})--(n) RETURN n')).toBe(1)
  expect(hopsOf('MATCH (p:Project {id: 1}) RETURN count(p) AS n')).toBe(0)
  expect(hopsOf("CALL db.schema('Project')")).toBe(0)
})

test('API calls replaced follow the GraphQL minimum: pages of 100, joins nested', () => {
  const traversal = (n: number) => parseResponse(`query_type: traversal\nnodes:\n  MergeRequest[${n}]{id}:\n  Project[1]{id}:\nedges[${n}]{from}:\n`)
  expect(apiCallsFor(traversal(5))).toBe(1)
  expect(apiCallsFor(traversal(250))).toBe(3)
  const counts = (grouping: string, rows: string) =>
    parseResponse(`query_type: aggregation\ncolumns[1]{name,function,target}:\n  n,count,mr\n${grouping}rows[3]{mr_state,n}:\n${rows}pagination:\n  has_more: false\n`)
  const byState = counts('group_columns[1]{name,kind,node,property,entity}:\n  mr_state,property,mr,state,MergeRequest\n', '  merged,2416\n  closed,682\n  opened,67\n')
  expect(byState).toMatchObject({ kind: 'aggregation', grouped: true, aggregated: 3165 })
  expect(apiCallsFor(byState)).toBe(32)
  const total = parseResponse('query_type: aggregation\ncolumns[1]{name,function,target}:\n  n,count,mr\nrows[1]{n}:\n  3165\npagination:\n  has_more: false\n')
  expect(total).toMatchObject({ kind: 'aggregation', grouped: false })
  expect(apiCallsFor(total)).toBe(1)
  expect(apiCallsFor(parseResponse('domains[1]:\n  - name: code_review'))).toBe(0)
  expect(apiCallsFor(parseResponse(JSON.stringify({ result: { nodes: [{ type: 'User' }], edges: [] } })))).toBe(1)
})
