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

const KEYWORDS = new Set([
  'MATCH', 'WHERE', 'RETURN', 'ORDER', 'BY', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'AS', 'AND', 'NOT',
  'IN', 'IS', 'NULL', 'TRUE', 'FALSE', 'ASC', 'DESC', 'STARTS', 'ENDS', 'WITH', 'CONTAINS', 'ANY',
  'SHORTEST', 'ALL', 'DISTINCT', 'OPTIONAL', 'UNION', 'UNWIND', 'OR', 'XOR',
])
const CLAUSES = new Set(['MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'ORDER', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'UNION'])

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

export function layout(query: string): Token[][] {
  const tokens = tokenize(query.replace(/\s+/g, ' ').trim())
  const lines: Token[][] = [[]]
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i]
    if (!t) continue
    const word = t.text.toUpperCase()
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

export function codeLines(code: string): Token[][] {
  return code
    .replace(/\s+$/, '')
    .split('\n')
    .map((line) => (/^\s*--(\s|$)/.test(line) ? [{ text: line, kind: 'comment' as const }] : tokenize(line)))
}
