import { BLUE, FADE_STOPS } from './orbit.ts'

export type TokenKind =
  | 'keyword'
  | 'label'
  | 'relationship'
  | 'string'
  | 'number'
  | 'function'
  | 'property'
  | 'variable'
  | 'punctuation'
  | 'comment'
  | 'space'

export type Token = { text: string; kind: TokenKind }

type TokenStyle = { color?: string; bold?: boolean; italic?: boolean; dimColor?: boolean }

const KEYWORDS = new Set([
  'MATCH', 'WHERE', 'RETURN', 'ORDER', 'BY', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'AS', 'AND', 'NOT',
  'IN', 'IS', 'NULL', 'TRUE', 'FALSE', 'ASC', 'DESC', 'STARTS', 'ENDS', 'WITH', 'CONTAINS', 'ANY',
  'SHORTEST', 'ALL', 'DISTINCT', 'OPTIONAL', 'UNION', 'UNWIND', 'OR', 'XOR',
])
// The keywords a new line starts at; ORDER starts one too, as ORDER BY
const CLAUSES = new Set(['MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'ORDER', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'UNION'])

export const TOKEN_STYLE: Readonly<Record<TokenKind, TokenStyle>> = {
  keyword: { color: BLUE, bold: true },
  label: { color: FADE_STOPS[0] },
  relationship: { color: FADE_STOPS[0], italic: true },
  string: { color: 'success' },
  number: { color: 'warning' },
  function: { bold: true },
  property: {},
  variable: {},
  punctuation: { dimColor: true },
  comment: { dimColor: true, italic: true },
  space: {},
}

const TOKEN = /\/\/[^\n]*|\/\*[\s\S]*?\*\/|\s+|'(?:[^'\\]|\\.)*'?|"(?:[^"\\]|\\.)*"?|-?\d+(?:\.\d+)?|[A-Za-z_][\w]*|\*\d*(?:\.\.\d+)?|<-|->|<>|!=|<=|>=|[^\s\w]/g

export function tokenize(query: string): Token[] {
  const raw = query.match(TOKEN) ?? []
  const out: Token[] = []
  for (let i = 0; i < raw.length; i++) {
    const text = raw[i] ?? ''
    const prev = out[out.length - 1]?.text ?? ''
    const next = raw[i + 1] ?? ''
    let kind: TokenKind = 'variable'
    if (/^\/[/*]/.test(text)) kind = 'comment'
    else if (/^\s+$/.test(text)) kind = 'space'
    else if (/^['"]/.test(text)) kind = 'string'
    else if (/^-?\d/.test(text)) kind = 'number'
    else if (/^[A-Za-z_]/.test(text)) {
      // A name after a colon inside () is a label; inside [] it is a relationship type
      const opener = lastOpener(out)
      if (prev === ':' && opener === '[') kind = 'relationship'
      else if (prev === ':' && opener === '(') kind = 'label'
      else if (next === '(') kind = 'function'
      else if (prev === '.') kind = 'property'
      else if (KEYWORDS.has(text.toUpperCase())) kind = 'keyword'
    } else kind = 'punctuation'
    out.push({ text, kind })
  }
  return out
}

function lastOpener(tokens: readonly Token[]): '(' | '[' | '' {
  const depth = { '(': 0, '[': 0 }
  for (let i = tokens.length - 1; i >= 0; i--) {
    const t = tokens[i]?.text
    if (t === ')') depth['(']++
    else if (t === ']') depth['[']++
    else if (t === '(' || t === '[') {
      if (depth[t] === 0) return t
      depth[t]--
    }
  }
  return ''
}

/** The query as lines of tokens, a new line at each clause keyword outside brackets. */
export function layout(query: string): Token[][] {
  const tokens = tokenize(query.replace(/\s+/g, ' ').trim())
  const lines: Token[][] = [[]]
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i]
    if (!t) continue
    const word = t.text.toUpperCase()
    // MATCH right after OPTIONAL stays on OPTIONAL's line
    const afterOptional = word === 'MATCH' && tokens[i - 2]?.text.toUpperCase() === 'OPTIONAL'
    const startsClause = t.kind === 'keyword' && CLAUSES.has(word) && !afterOptional && lastOpener(tokens.slice(0, i)) === ''
    const current = lines[lines.length - 1] ?? []
    if (startsClause && current.some((x) => x.kind !== 'space')) lines.push([])
    const line = lines[lines.length - 1] ?? []
    if (t.kind === 'space' && line.every((x) => x.kind === 'space')) continue
    line.push(t)
  }
  return lines.map(trimEnd)
}

function trimEnd(line: Token[]): Token[] {
  while (line[line.length - 1]?.kind === 'space') line.pop()
  return line
}

/** The query a `glab orbit query` command runs, unquoted, or '' when there is none. */
export function queryOf(command: string): string {
  const m = command.match(/\bglab\s+orbit\s+query\s+(?:--[\w-]+(?:[ =][^\s'"]+)?\s+)*(?:"((?:[^"\\]|\\.)*)"|'([^']*)')/)
  if (!m) return ''
  return m[1] !== undefined ? m[1].replace(/\\(["\\$`])/g, '$1') : (m[2] ?? '')
}

/**
 * Code as the author laid it out, one token list per line, for a code block in a reply. A line
 * that starts with `-- ` is a comment too: agents write SQL-style comments in Cypher.
 */
export function codeLines(code: string): Token[][] {
  return code.replace(/\s+$/, '').split('\n').map((line) =>
    /^\s*--(\s|$)/.test(line) ? [{ text: line, kind: 'comment' as const }] : tokenize(line),
  )
}

export type Segment = { kind: 'prose' | 'code'; text: string }

const CYPHER_FENCE = /^```[ \t]*(?:cypher|opencypher|gql)[ \t]*\n([\s\S]*?)^```[ \t]*$/gim

/** A reply's markdown split around its closed ```cypher, ```opencypher, and ```gql blocks. */
export function cypherSegments(markdown: string): Segment[] {
  const out: Segment[] = []
  let at = 0
  for (const m of markdown.matchAll(CYPHER_FENCE)) {
    const prose = markdown.slice(at, m.index).trim()
    if (prose) out.push({ kind: 'prose', text: prose })
    out.push({ kind: 'code', text: m[1] ?? '' })
    at = (m.index ?? 0) + m[0].length
  }
  const rest = markdown.slice(at).trim()
  if (rest) out.push({ kind: 'prose', text: rest })
  return out
}
