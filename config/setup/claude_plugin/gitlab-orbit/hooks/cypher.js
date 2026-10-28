import { BLUE, FADE_STOPS } from './orbit.js'

const KEYWORDS = new Set([
  'MATCH', 'WHERE', 'RETURN', 'ORDER', 'BY', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'AS', 'AND', 'NOT',
  'IN', 'IS', 'NULL', 'TRUE', 'FALSE', 'ASC', 'DESC', 'STARTS', 'ENDS', 'WITH', 'CONTAINS', 'ANY',
  'SHORTEST', 'ALL', 'DISTINCT', 'OPTIONAL', 'UNION', 'UNWIND', 'OR', 'XOR',
])
// The keywords a new line starts at; ORDER starts one too, as ORDER BY
const CLAUSES = new Set(['MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'ORDER', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'UNION'])

export const TOKEN_STYLE = {
  keyword: { color: BLUE, bold: true },
  label: { color: FADE_STOPS[0] },
  relationship: { color: FADE_STOPS[0], italic: true },
  string: { color: 'success' },
  number: { color: 'warning' },
  function: { bold: true },
  property: {},
  variable: {},
  punctuation: { dimColor: true },
  space: {},
}

const TOKEN = /\s+|'(?:[^'\\]|\\.)*'?|"(?:[^"\\]|\\.)*"?|-?\d+(?:\.\d+)?|[A-Za-z_][\w]*|\*\d*(?:\.\.\d+)?|<-|->|<>|!=|<=|>=|[^\s\w]/g

export function tokenize(query) {
  const raw = query.match(TOKEN) ?? []
  const out = []
  for (let i = 0; i < raw.length; i++) {
    const text = raw[i]
    const prev = out.length ? out[out.length - 1].text : ''
    const next = raw[i + 1] ?? ''
    let kind = 'variable'
    if (/^\s+$/.test(text)) kind = 'space'
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

function lastOpener(tokens) {
  let depth = { '(': 0, '[': 0 }
  for (let i = tokens.length - 1; i >= 0; i--) {
    const t = tokens[i].text
    if (t === ')') depth['(']++
    else if (t === ']') depth['[']++
    else if (t === '(' || t === '[') {
      if (depth[t] === 0) return t
      depth[t]--
    }
  }
  return ''
}

export function layout(query) {
  const tokens = tokenize(query.replace(/\s+/g, ' ').trim())
  const lines = [[]]
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i]
    const word = t.text.toUpperCase()
    // MATCH right after OPTIONAL stays on OPTIONAL's line
    const afterOptional = word === 'MATCH' && tokens[i - 2]?.text.toUpperCase() === 'OPTIONAL'
    const startsClause = t.kind === 'keyword' && CLAUSES.has(word) && !afterOptional && lastOpener(tokens.slice(0, i)) === ''
    if (startsClause && lines[lines.length - 1].some((x) => x.kind !== 'space')) lines.push([])
    const line = lines[lines.length - 1]
    if (t.kind === 'space' && line.every((x) => x.kind === 'space')) continue
    line.push(t)
  }
  return lines.map((line) => trimEnd(line))
}

function trimEnd(line) {
  while (line.length && line[line.length - 1].kind === 'space') line.pop()
  return line
}

export function queryOf(command) {
  const m = String(command).match(/\bglab\s+orbit\s+query\s+(?:--[\w-]+(?:[ =][^\s'"]+)?\s+)*(?:"((?:[^"\\]|\\.)*)"|'([^']*)')/)
  if (!m) return ''
  return m[1] !== undefined ? m[1].replace(/\\(["\\$`])/g, '$1') : m[2]
}
