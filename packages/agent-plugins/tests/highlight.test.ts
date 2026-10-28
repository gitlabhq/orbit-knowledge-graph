import { expect, test } from 'claude-code/testing'

import { codeLines, layout } from '../hooks/highlight.ts'

const text = (lines: { text: string }[][]) => lines.map((line) => line.map((t) => t.text).join(''))

test('a query lays out one clause per line', () => {
  expect(text(layout('MATCH (mr:MergeRequest) WHERE mr.state = "opened" RETURN mr.iid LIMIT 10'))).toEqual([
    'MATCH (mr:MergeRequest)',
    'WHERE mr.state = "opened"',
    'RETURN mr.iid',
    'LIMIT 10',
  ])
  expect(text(layout('OPTIONAL MATCH (n) CALL db.schema()'))).toEqual(['OPTIONAL MATCH (n)', 'CALL db.schema()'])
})

test('Shiki scopes keywords, relationship types, literals, and functions', () => {
  const kinds = Object.fromEntries(
    layout('MATCH (a)-[:IN_PROJECT]->(p {id: 1}) WHERE a.x = "y" RETURN count(a)').flat().map((t) => [t.text.trim(), t.kind]),
  )
  expect(kinds).toMatchObject({ MATCH: 'keyword', IN_PROJECT: 'relationship', '1': 'number', y: 'string', count: 'function' })
})

test('comments are their own tokens, including a line that starts with --', () => {
  expect(codeLines('MATCH (n) // trailing')[0]?.at(-1)).toEqual({ text: '// trailing', kind: 'comment' })
  const lines = codeLines("-- 1. Check the schema\nCALL db.schema('Vulnerability')\n")
  expect(lines[0]).toEqual([{ text: '-- 1. Check the schema', kind: 'comment' }])
  expect(lines[1]?.[0]).toEqual({ text: 'CALL', kind: 'keyword' })
  expect(lines).toHaveLength(2)
})
