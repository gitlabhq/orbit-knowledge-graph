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
  expect(text(layout('MATCH (n) RETURN n.x ORDER BY n.x DESC SKIP 0 LIMIT 5'))).toEqual([
    'MATCH (n)',
    'RETURN n.x',
    'ORDER BY n.x DESC',
    'SKIP 0',
    'LIMIT 5',
  ])
})

test('keywords, labels, relationship types, literals, and functions are told apart', () => {
  const kinds = Object.fromEntries(
    layout('MATCH (a:User)-[:IN_PROJECT]->(p {id: 1}) WHERE a.x = "y" RETURN count(a)').flat().map((t) => [t.text, t.kind]),
  )
  expect(kinds).toMatchObject({ MATCH: 'keyword', User: 'label', IN_PROJECT: 'relationship', '1': 'number', '"y"': 'string', count: 'function' })
})

test('operators and sort directions read as keywords, and a property named like one stays a property', () => {
  const words = layout("MATCH (n) WHERE NOT n.a IS NULL AND n.b IN [1] AND n.c STARTS WITH 'x' RETURN n.not ORDER BY n.d ASC")
    .flat()
    .filter((t) => t.kind === 'keyword')
    .map((t) => t.text.trim())
  expect(words).toEqual(['MATCH', 'WHERE', 'NOT', 'IS', 'NULL', 'AND', 'IN', 'AND', 'STARTS', 'WITH', 'RETURN', 'ORDER', 'BY', 'ASC'])
})

test('comments are their own tokens, including a line that starts with --', () => {
  expect(codeLines('MATCH (n) // trailing')[0]?.at(-1)).toEqual({ text: '// trailing', kind: 'comment' })
  const lines = codeLines("-- 1. Check the schema\nCALL db.schema('Vulnerability')\n")
  expect(lines[0]).toEqual([{ text: '-- 1. Check the schema', kind: 'comment' }])
  expect(lines[1]?.[0]).toEqual({ text: 'CALL', kind: 'keyword' })
  expect(lines).toHaveLength(2)
})
