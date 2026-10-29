export type Subcommand = { key: string; args: string[] }

type Token = { word: string } | { operator: ';' | '&&' | '||' | '|' }

const UNSAFE_UNQUOTED = new Set(['$', '`', '(', ')', '{', '}', '<', '>', '#', '&', '!'])
const HARMLESS_REDIRECT = /^(?:\d?>>?|&>>?)(?:&\d+|\s*\/dev\/null)(?![^\s;&|])/
const ENV_ASSIGNMENT = /^(?:ORBIT_[A-Z0-9_]+|GLAB_HOST|GLAB_TOKEN|GLAB_CONFIG_DIR)=/
const COUNT_FLAG = /^-[nc]?\d+$/

function tokenize(command: string): Token[] | null {
  const tokens: Token[] = []
  let word = ''
  let inWord = false
  const endWord = () => {
    if (inWord) tokens.push({ word })
    word = ''
    inWord = false
  }
  const operator = (op: '&&' | '||' | '|' | ';') => {
    endWord()
    tokens.push({ operator: op })
    return op.length - 1
  }
  for (let i = 0; i < command.length; i++) {
    const c = command[i] ?? ''
    if (c === "'") {
      const end = command.indexOf("'", i + 1)
      if (end < 0) return null
      word += command.slice(i + 1, end)
      inWord = true
      i = end
    } else if (c === '"') {
      inWord = true
      for (i++; command[i] !== '"'; i++) {
        const inner = command[i]
        if (inner === undefined || inner === '$' || inner === '`') return null
        if (inner === '\\' && '"\\$`\n'.includes(command[i + 1] ?? 'x')) {
          i++
          if (command[i] !== '\n') word += command[i]
        } else word += inner
      }
    } else if (c === '\\') {
      const next = command[++i]
      if (next === undefined) return null
      if (next !== '\n') {
        word += next
        inWord = true
      }
    } else if (!inWord && HARMLESS_REDIRECT.test(command.slice(i))) {
      i += (command.slice(i).match(HARMLESS_REDIRECT)?.[0].length ?? 1) - 1
    } else if (c === ' ' || c === '\t') endWord()
    else if (c === '\n' || c === ';') operator(';')
    else if (c === '&' && command[i + 1] === '&') i += operator('&&')
    else if (c === '|') i += operator(command[i + 1] === '|' ? '||' : '|')
    else if (UNSAFE_UNQUOTED.has(c) || c < ' ' || c === '\x7f') return null
    else {
      word += c
      inWord = true
    }
  }
  endWord()
  return tokens
}

function orbitWords(words: readonly string[]): string[] | null {
  let i = 0
  while (ENV_ASSIGNMENT.test(words[i] ?? '')) i++
  if (words[i] === 'glab' && words[i + 1] === 'orbit') i += 2
  else if (words[i] === 'orbit') i += 1
  else return null
  while (words[i]?.startsWith('-')) i++
  return words.slice(i)
}

function isCountFilter(words: readonly string[]): boolean {
  const [program, ...args] = words
  if (program === 'wc') return args.every((arg) => /^-[lwcm]$/.test(arg))
  if (program !== 'head' && program !== 'tail') return false
  for (let i = 0; i < args.length; i++) {
    if (/^-[nc]$/.test(args[i] ?? '') && /^\d+$/.test(args[i + 1] ?? '')) i++
    else if (!COUNT_FLAG.test(args[i] ?? '')) return false
  }
  return true
}

function isHarmless(words: readonly string[]): boolean {
  const [program, dir] = words
  return (program === 'cd' && words.length === 2 && !dir?.startsWith('-')) || (program === 'true' && words.length === 1)
}

type Segment = { words: string[]; piped: boolean }

function segmentsOf(tokens: readonly Token[]): Segment[] | null {
  const segments: Segment[] = []
  let words: string[] = []
  let piped = false
  const end = (next: string): boolean => {
    if (words.length === 0) return next === ';' && !piped
    segments.push({ words, piped })
    words = []
    piped = next === '|'
    return true
  }
  for (const token of tokens) {
    if ('word' in token) words.push(token.word)
    else if (!end(token.operator)) return null
  }
  return end(';') ? segments : null
}

// Fails closed: null unless the whole command is Orbit calls, `cd <dir>`, `true`, and count-only filters, so a row never hides a program or an effect.
export function orbitCalls(command: string): string[][] | null {
  const tokens = tokenize(command)
  const segments = tokens && segmentsOf(tokens)
  if (!segments) return null
  const calls: string[][] = []
  let afterOrbit = false
  for (const { words, piped } of segments) {
    if (piped) {
      if (!afterOrbit || !isCountFilter(words)) return null
      continue
    }
    const orbit = orbitWords(words)
    if (orbit) calls.push(orbit)
    else if (!isHarmless(words)) return null
    afterOrbit = orbit !== null
  }
  return calls.length > 0 ? calls : null
}

export function subcommandOf(words: readonly string[], known: (key: string) => boolean): Subcommand | null {
  const [sub, ...rest] = words
  if (sub === undefined) return null
  const nested = rest.findIndex((word) => known(sub + ' ' + word))
  if (nested >= 0) return { key: sub + ' ' + rest[nested], args: rest.slice(nested + 1) }
  return known(sub) ? { key: sub, args: rest } : null
}

export function isOrbitPrefix(command: string): boolean {
  const start = command.trimStart()
  return start === '' || ['glab orbit ', 'orbit '].some((launcher) => launcher.startsWith(start))
}
