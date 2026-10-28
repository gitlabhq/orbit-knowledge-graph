import { orbitInvocations } from './command.ts'

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

export function queryOf(command: string): string {
  for (const words of orbitInvocations(command)) {
    if (words[0] !== 'query' || words.includes('--file')) continue
    for (let i = 1; i < words.length; i++) {
      const word = words[i] ?? ''
      if (word === '--response-format') i++
      else if (!word.startsWith('-')) return word
    }
  }
  return ''
}

export function codeLines(code: string): Token[][] {
  return code.replace(/\s+$/, '').split('\n').map((line) =>
    /^\s*--(\s|$)/.test(line) ? [{ text: line, kind: 'comment' as const }] : tokenize(line),
  )
}

export type Segment = { kind: 'prose' | 'code'; text: string }

const CYPHER_FENCE = /^```[ \t]*(?:cypher|opencypher|gql)[ \t]*\n([\s\S]*?)^```[ \t]*$/gim

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

export function hopsOf(query: string): number {
  let hops = 0
  for (const clause of query.split(/\bMATCH\b/i).slice(1)) {
    const pattern = clause.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? ''
    for (const m of pattern.matchAll(/<?-(?:\[([^\]]*)\])?->?/g)) {
      const range = (m[1] ?? '').match(/\*\s*(\d+)?/)
      hops += range ? Number(range[1] ?? 1) : 1
    }
  }
  return hops
}

type Hop = 'left' | 'right' | 'none'

type Path = { nodes: string[]; text: string }

export const NO_ENTITIES: ReadonlySet<string> = new Set()

type Pin = { score: number; implied?: string }

function entityOfKey(key: string, known: ReadonlySet<string>): string | undefined {
  const m = key.match(/^(\w+)_id$/)
  if (!m?.[1]) return undefined
  const name = m[1].split('_').map((w) => (w[0]?.toUpperCase() ?? '') + w.slice(1)).join('')
  return known.has(name) ? name : undefined
}

function pinOf(keys: readonly string[], label: string, known: ReadonlySet<string>): Pin {
  if (keys.length === 0) return { score: 0 }
  const implied = keys.map((key) => entityOfKey(key, known)).find((name) => name && name !== label)
  return { score: keys.includes('id') ? 2 : 1, ...(implied ? { implied } : {}) }
}

export function entitiesOf(text: string, known: ReadonlySet<string> = NO_ENTITIES): string {
  const labels: Record<string, string> = {}
  for (const m of text.matchAll(/\(\s*(\w+)\s*:\s*(\w+)/g)) {
    if (m[1] && m[2]) labels[m[1]] ??= m[2]
  }
  const whereKeys: Record<string, string[]> = {}
  for (const clause of text.split(/\bWHERE\b/i).slice(1)) {
    const where = clause.split(/\b(?:MATCH|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT)\b/i)[0] ?? ''
    for (const m of where.matchAll(/\b(\w+)\.(\w+)\s*(?:=|<>|!=|<=|>=|<|>|\bIN\b|\bSTARTS\b|\bENDS\b|\bCONTAINS\b)/gi)) {
      if (m[1] && m[2]) (whereKeys[m[1]] ??= []).push(m[2])
    }
  }
  const paths: Path[] = []
  for (const segment of text.split(/\bMATCH\b/i).slice(1)) {
    const pattern = segment.split(/\b(?:WHERE|RETURN|ORDER\s+BY|SKIP|OFFSET|LIMIT|CALL)\b/i)[0] ?? ''
    for (const part of splitTopLevel(pattern)) {
      const nodes: string[] = []
      const pins: Pin[] = []
      const hops: Hop[] = []
      for (const m of part.matchAll(/\(([^)]*)\)|(<)?-(?:\[[^\]]*\])?-(>)?/g)) {
        if (m[1] !== undefined) {
          const [, name = '', label] = m[1].match(/^\s*(\w*)\s*(?::\s*(\w+))?/) ?? []
          const node = label || labels[name] || 'any'
          const inline = [...(m[1].match(/\{([^}]*)\}/)?.[1] ?? '').matchAll(/(\w+)\s*:/g)].map((k) => k[1] ?? '')
          nodes.push(node)
          pins.push(pinOf([...inline, ...(name ? (whereKeys[name] ?? []) : [])], node, known))
        } else hops.push(m[2] ? 'left' : m[3] ? 'right' : 'none')
      }
      for (const path of readFromPin(nodes, pins) ?? readForward(nodes, hops)) {
        if (!paths.some((p) => p.text === path.text)) paths.push(path)
      }
    }
  }
  const walks = paths.filter((p) => p.nodes.length > 1)
  const kept = paths.filter((p) => p.nodes.length > 1 || !walks.some((w) => w.nodes.includes(p.text)))
  if (kept.length) return kept.map((p) => p.text).join(', ')
  const names: string[] = []
  for (const m of text.matchAll(/\\?"entity\\?"\s*:\s*\\?"(\w+)/g)) {
    if (m[1] && !names.includes(m[1])) names.push(m[1])
  }
  return names.join(' → ')
}

function readFromPin(nodes: readonly string[], pins: readonly Pin[]): Path[] | null {
  const best = Math.max(0, ...pins.map((p) => p.score))
  const at = pins.findIndex((p) => p.score === best)
  if (best === 0 || pins.filter((p) => p.score === best).length > 1) return null
  const implied = pins[at]?.implied
  const sides: string[][] = []
  if (at < nodes.length - 1) sides.push(nodes.slice(at))
  if (at > 0) sides.push(nodes.slice(0, at + 1).reverse())
  if (sides.length === 0) sides.push(nodes.slice(at, at + 1))
  return sides.map((side) => {
    const run = implied ? [implied, ...side] : [...side]
    const distinct = run.filter((node, k) => node !== run[k - 1])
    return { nodes: distinct, text: distinct.join(' → ') }
  })
}

function readForward(nodes: string[], hops: Hop[]): Path[] {
  if (hops.length === 0) return nodes[0] ? [{ nodes: [nodes[0]], text: nodes[0] }] : []
  const paths: Path[] = []
  let start = 0
  for (let i = 1; i <= hops.length; i++) {
    if (i < hops.length && hops[i] === hops[start]) continue
    const run = nodes.slice(start, i + 1)
    if (hops[start] === 'left') run.reverse()
    const distinct = run.filter((node, k) => node !== run[k - 1])
    paths.push({ nodes: distinct, text: distinct.join(hops[start] === 'none' ? ' — ' : ' → ') })
    start = i
  }
  return paths
}

function splitTopLevel(text: string): string[] {
  const parts = ['']
  let depth = 0
  for (const ch of text) {
    if ('([{'.includes(ch)) depth++
    else if (')]}'.includes(ch)) depth--
    if (ch === ',' && depth === 0) parts.push('')
    else parts[parts.length - 1] += ch
  }
  return parts.map((p) => p.trim()).filter(Boolean)
}
