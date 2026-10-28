import { cypherPieces } from './shiki.js'

export type TokenKind = 'keyword' | 'relationship' | 'string' | 'number' | 'function' | 'variable' | 'punctuation' | 'comment' | 'space'

export type Token = { text: string; kind: TokenKind }

const KINDS: readonly [RegExp, TokenKind][] = [
  [/^comment/, 'comment'],
  [/^string/, 'string'],
  [/^constant\.numeric/, 'number'],
  [/^entity\.name\.class\.relationship\.type/, 'relationship'],
  [/^keyword\.(control\.clause|control\.function\.boolean|operator\.(logical|in)|other\.order)/, 'keyword'],
  [/^support\.function\.(?!relationship)/, 'function'],
  [/^(keyword|support)\./, 'punctuation'],
]

const CLAUSES = new Set(['MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'ORDER', 'SKIP', 'OFFSET', 'LIMIT', 'CALL', 'UNION'])
const OPERATOR_WORDS = new Set(['NOT', 'OR', 'XOR', 'STARTS', 'ENDS', 'CONTAINS', 'DISTINCT'])

function kindOf(text: string, scope: string): TokenKind {
  if (!text.trim()) return 'space'
  return KINDS.find(([pattern]) => pattern.test(scope))?.[1] ?? 'variable'
}

function tokens(code: string): Token[][] {
  try {
    return cypherPieces(code).map((line) =>
      line.map(({ text, scope }, i) => {
        const kind = kindOf(text, scope)
        const isOperator = kind === 'variable' && OPERATOR_WORDS.has(text.trim().toUpperCase()) && !line[i - 1]?.text.endsWith('.')
        return { text, kind: isOperator ? 'keyword' : kind }
      }),
    )
  } catch {
    return code.split('\n').map((line) => [{ text: line, kind: 'variable' }])
  }
}

export function layout(query: string): Token[][] {
  const lines: Token[][] = [[]]
  let depth = 0
  let previous = ''
  for (const token of tokens(query.replace(/\s+/g, ' ').trim()).flat()) {
    const word = token.text.toUpperCase()
    const line = lines[lines.length - 1] ?? []
    if (token.kind === 'keyword' && depth === 0 && CLAUSES.has(word.trim().split(/\s+/)[0] ?? '') && previous !== 'OPTIONAL' && line.length) lines.push([])
    const current = lines[lines.length - 1] ?? []
    const text = current.length ? token.text : token.text.trimStart()
    if (text) current.push({ ...token, text })
    depth += (token.text.match(/[([{]/g)?.length ?? 0) - (token.text.match(/[)\]}]/g)?.length ?? 0)
    if (token.kind !== 'space') previous = word
  }
  return lines.map((line) => {
    const last = line[line.length - 1]
    if (last) last.text = last.text.trimEnd()
    return line.filter((token) => token.text)
  })
}

export function codeLines(code: string): Token[][] {
  return code
    .replace(/\s+$/, '')
    .split('\n')
    .map((line) => (/^\s*--(\s|$)/.test(line) ? [{ text: line, kind: 'comment' as const }] : (tokens(line)[0] ?? [])))
}
