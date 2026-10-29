import { expect, test } from 'claude-code/testing'

import { cypherSegments, hopsOf, queryOf } from '../core/cypher.ts'
import { apiCallsFor, compact, graphStatus, grouped, parseResponse } from '../core/stats.ts'

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

test('an aggregation with several aggregates lists its columns and still counts', () => {
  const text = [
    'query_type: aggregation',
    'columns[2]:',
    '  - name: n',
    '    function: count',
    '    target: mr',
    '  - name: s',
    '    function: sum',
    '    target: mr',
    '    property: iid',
    'group_columns[1]{name,kind,node,property,entity}:',
    '  mr_state,property,mr,state,MergeRequest',
    'rows[3]{mr_state,n,s}:',
    '  merged,2450,3785384',
    '  closed,690,1161896',
    '  opened,76,225656',
    'pagination:',
    '  has_more: false',
    '  truncated: false',
  ].join('\n')
  expect(parseResponse(text)).toMatchObject({ aggregated: 3216, grouped: true })
})

test('an ungrouped aggregation with several aggregates counts too', () => {
  const text = 'query_type: aggregation\ncolumns[2]:\n  - name: n\n    function: count\n    target: mr\n  - name: s\n    function: sum\n    target: mr\n    property: iid\nrows[1]{n,s}:\n  3216,5172936\npagination:\n  has_more: false\n'
  expect(parseResponse(text).aggregated).toBe(3216)
})

const listedCount = (name: string, target: string, property?: string) =>
  `  - name: ${name}\n    function: count\n    target: ${target}\n` + (property ? `    property: ${property}\n` : '')
const aggregation = (columns: string, header: string, row: string) =>
  `query_type: aggregation\n${columns}rows[1]{${header}}:\n  ${row}\npagination:\n  has_more: false\n  truncated: false\n`

test('a DISTINCT count next to a plain count is left out', () => {
  const columns = `columns[2]:\n${listedCount('count_mr', 'mr')}${listedCount('count_distinct_mr_author_id', 'mr', 'author_id')}`
  expect(parseResponse(aggregation(columns, 'count_mr,count_distinct_mr_author_id', '3222,111')).aggregated).toBe(3222)
})

test('a response whose only count is DISTINCT in the tabled form reads as zero', () => {
  const columns = 'columns[2]{name,function,target,property}:\n  count_distinct_mr_author_id,count,mr,author_id\n  total,sum,mr,iid\n'
  expect(parseResponse(aggregation(columns, 'count_distinct_mr_author_id,total', '111,5000')).aggregated).toBe(0)
})

test('a DISTINCT property count next to a DISTINCT node count reads as the node count', () => {
  const columns = `columns[2]:\n${listedCount('count_distinct_mr', 'mr')}${listedCount('count_distinct_mr_author_id', 'mr', 'author_id')}`
  expect(parseResponse(aggregation(columns, 'count_distinct_mr,count_distinct_mr_author_id', '3,111')).aggregated).toBe(3)
})

test('a plain property count next to a DISTINCT node count reads as the property count', () => {
  const columns = `columns[2]:\n${listedCount('count_distinct_mr', 'mr')}${listedCount('count_mr_merged_at', 'mr', 'merged_at')}`
  expect(parseResponse(aggregation(columns, 'count_distinct_mr,count_mr_merged_at', '3,3000')).aggregated).toBe(3000)
})

test('a plain count of a property reads as counted', () => {
  const columns = 'columns[1]{name,function,target,property}:\n  count_mr_merged_at,count,mr,merged_at\n'
  expect(parseResponse(aggregation(columns, 'count_mr_merged_at', '3000')).aggregated).toBe(3000)
})

test('a DISTINCT count of a node, with no property, reads as counted', () => {
  const columns = 'columns[1]{name,function,target}:\n  count_distinct_mr,count,mr\n'
  expect(parseResponse(aggregation(columns, 'count_distinct_mr', '3222')).aggregated).toBe(3222)
})

test('an aliased count of a property reads as DISTINCT', () => {
  const columns = 'columns[1]{name,function,target,property}:\n  authors,count,mr,author_id\n'
  expect(parseResponse(aggregation(columns, 'authors', '111')).aggregated).toBe(0)
})

test('two plain counts on different targets read as the larger one', () => {
  const columns = 'columns[2]{name,function,target}:\n  mrs,count,mr\n  users,count,u\n'
  expect(parseResponse(aggregation(columns, 'mrs,users', '3222,111')).aggregated).toBe(3222)
})

test('schema and other output reads as empty', () => {
  expect(parseResponse('domains[1]:\n  - name: code_review')).toMatchObject({ nodes: 0, edges: 0, aggregated: 0 })
})

test('counts compact for the status line', () => {
  expect([950, 1200, 12000, 1_200_000].map(compact)).toEqual(['950', '1.2k', '12k', '1.2M'])
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
  expect(graphStatus(json)?.total).toBe(806936)
  expect(graphStatus('not json')).toBeNull()
  expect(graphStatus('{}')).toBeNull()
})

test('a graph total is written in full with thousands separators', () => {
  expect(grouped(1675400)).toBe('1,675,400')
  expect(grouped(950)).toBe('950')
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
