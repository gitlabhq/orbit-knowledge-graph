export type Subcommand = { key: string; args: string[] }

const HARMLESS_REDIRECT = /^(?:&>>?|>>?)(?:&\d+|\s*\/dev\/null)(?=[\s;&|)]|$)/
const SUBSTITUTION = /\$\(|`/

type Heredoc = { delimiter: string; stripTabs: boolean; quoted: boolean }

function arithmeticEnd(command: string, open: number): number {
  let depth = 2
  for (let i = open + 2; i < command.length; i++) {
    if (command[i] === '(') depth++
    else if (command[i] === ')') {
      if (depth === 2 && command[i + 1] === ')') return i + 2
      if (--depth < 2) return -1
    }
  }
  return -1
}

function heredocOperand(command: string, from: number): { delimiter: string; quoted: boolean; end: number } {
  let i = from
  let delimiter = ''
  let quoted = false
  while (i < command.length && !/[\s;&|<>()]/.test(command[i] ?? '')) {
    const c = command[i] ?? ''
    if (c === "'" || c === '"') {
      const close = command.indexOf(c, i + 1)
      quoted = true
      delimiter += command.slice(i + 1, close < 0 ? command.length : close)
      i = close < 0 ? command.length : close + 1
    } else if (c === '\\') {
      quoted = true
      delimiter += command[i + 1] ?? ''
      i += 2
    } else {
      delimiter += c
      i++
    }
  }
  return { delimiter, quoted, end: i }
}

export function parseCommands(command: string): { commands: string[][]; unparsed: boolean } {
  const commands: string[][] = []
  let words: string[] = []
  let word = ''
  let inWord = false
  let quote = ''
  let ansiQuote = false
  let dollarAt = -1
  let piped = false
  let unparsed = false
  const heredocs: Heredoc[] = []
  const endWord = () => {
    if (inWord) words.push(word)
    word = ''
    inWord = false
  }
  const endCommand = () => {
    endWord()
    if (words.length) commands.push(piped ? ['|', ...words] : words)
    words = []
    piped = false
  }
  const skipHeredocBodies = (newline: number): number => {
    let at = newline + 1
    for (const { delimiter, stripTabs, quoted } of heredocs.splice(0)) {
      let closed = false
      while (at < command.length && !closed) {
        const end = command.indexOf('\n', at)
        const line = command.slice(at, end < 0 ? command.length : end)
        at = end < 0 ? command.length : end + 1
        closed = (stripTabs ? line.replace(/^\t+/, '') : line) === delimiter
        if (!closed && !quoted && SUBSTITUTION.test(line)) words.push('$(')
      }
      if (!closed) unparsed = true
    }
    return at - 1
  }
  for (let i = 0; i < command.length; i++) {
    const c = command[i] ?? ''
    if (quote !== "'" && (c === '`' || (c === '$' && command[i + 1] === '('))) words.push(c === '`' ? '`' : '$(')
    if (quote) {
      if (c === quote) {
        quote = ''
        ansiQuote = false
      } else if (c === '\\' && (quote === '"' || ansiQuote) && i + 1 < command.length) word += command[++i]
      else word += c
    } else if (c === "'" || c === '"') {
      ansiQuote = c === "'" && inWord && dollarAt === i - 1
      quote = c
      inWord = true
    } else if (c === '\\' && command[i + 1] === '\n') {
      i++
      endWord()
    } else if (c === '\\' && i + 1 < command.length) {
      word += command[++i]
      inWord = true
    } else if (
      (c === '$' && command[i + 1] === '(' && command[i + 2] === '(' && arithmeticEnd(command, i + 1) >= 0) ||
      (c === '(' && command[i + 1] === '(' && !inWord && (words.length === 0 || words[0] === 'for') && arithmeticEnd(command, i) >= 0)
    ) {
      const end = arithmeticEnd(command, c === '$' ? i + 1 : i)
      if (SUBSTITUTION.test(command.slice(i, end))) words.push('$(')
      if (c === '$') inWord = true
      i = end - 1
    } else if ('&>'.includes(c) && HARMLESS_REDIRECT.test(command.slice(i))) {
      if (/^\d+$/.test(word) && /\d/.test(command[i - 1] ?? '')) {
        word = ''
        inWord = false
      } else endWord()
      i += (command.slice(i).match(HARMLESS_REDIRECT)?.[0].length ?? 1) - 1
    } else if (c === '<' || c === '>' || (c === '&' && command[i + 1] === '>')) {
      if (inWord && /^\d+$/.test(word) && /\d/.test(command[i - 1] ?? '')) {
        word = ''
        inWord = false
      } else endWord()
      if (c === '&') i++
      const operator = command[i] ?? c
      if (operator === '<' && command[i + 1] === '<' && command[i + 2] === '<') {
        i += 2
        words.push('<')
      } else if (operator === '<' && command[i + 1] === '<') {
        const stripTabs = command[i + 2] === '-'
        const operand = heredocOperand(command, i + (stripTabs ? 3 : 2) + (command.slice(i + (stripTabs ? 3 : 2)).match(/^[ \t]*/)?.[0].length ?? 0))
        if (!operand.delimiter) unparsed = true
        else heredocs.push({ delimiter: operand.delimiter, stripTabs, quoted: operand.quoted })
        i = operand.end - 1
      } else {
        if (operator === '>' && command[i + 1] === '>') i++
        if (operator === '>' && (command[i + 1] === '|' || command[i + 1] === '&')) i++
        words.push(operator)
      }
    } else if (c === '|' && command[i + 1] !== '|') {
      endCommand()
      piped = true
      if (command[i + 1] === '&') i++
    } else if (c === '\n' || ';&|()'.includes(c)) {
      if (c === '\n' && heredocs.length) i = skipHeredocBodies(i)
      endCommand()
      if ('&|'.includes(c) && command[i + 1] === c) i++
    } else if (c === '#' && !inWord) {
      while (i + 1 < command.length && command[i + 1] !== '\n') i++
    } else if (/\s/.test(c)) {
      endWord()
    } else {
      if (c === '$') dollarAt = i
      word += c
      inWord = true
    }
  }
  endCommand()
  return { commands, unparsed: unparsed || quote !== '' }
}

export function simpleCommands(command: string): string[][] {
  return parseCommands(command).commands
}

function orbitWords(words: readonly string[]): string[] | null {
  let i = 0
  while (/^(?:ORBIT|GLAB)_\w*=/.test(words[i] ?? '')) i++
  if (words[i] === 'glab' && words[i + 1] === 'orbit') i += 2
  else if (words[i]?.split('/').pop() === 'orbit') i += 1
  else return null
  while (words[i]?.startsWith('-')) i++
  return words.slice(i)
}

const PREFIX_WORDS = new Set(['do', 'then', 'else', 'elif', 'if', 'while', 'until', '!', 'time', '{'])
const SUBSTITUTION_MARKERS = ['$(', '`']

type Runnable = { piped: boolean; words: string[]; readsInput: boolean; writes: string[]; substitutes: boolean }

function withoutRedirects(words: readonly string[]): Pick<Runnable, 'words' | 'readsInput' | 'writes'> {
  const kept: string[] = []
  const writes: string[] = []
  let readsInput = false
  for (let i = 0; i < words.length; i++) {
    const word = words[i] ?? ''
    if (word !== '<' && word !== '>') {
      kept.push(word)
      continue
    }
    const target = words[i + 1]
    const hasTarget = target !== undefined && !['<', '>', ...SUBSTITUTION_MARKERS].includes(target)
    if (word === '<') readsInput = true
    else if (hasTarget) writes.push(target)
    if (hasTarget) i++
  }
  return { words: kept, readsInput, writes }
}

function runnable(command: string): { runs: Runnable[]; unparsed: boolean } {
  const { commands, unparsed } = parseCommands(command)
  const runs: Runnable[] = []
  for (const raw of commands) {
    const piped = raw[0] === '|'
    const { words, readsInput, writes } = withoutRedirects(piped ? raw.slice(1) : raw)
    let i = 0
    while (PREFIX_WORDS.has(words[i] ?? '')) i++
    const substitutes = raw.some((word) => SUBSTITUTION_MARKERS.includes(word))
    if (i < words.length || writes.length || substitutes) runs.push({ piped, words: words.slice(i), readsInput, writes, substitutes })
  }
  return { runs, unparsed }
}

export function orbitInvocations(command: string): string[][] {
  return runnable(command)
    .runs.map(({ words }) => orbitWords(words))
    .filter((words) => words !== null)
}

const SHELL_WORDS = new Set(['cd', 'pushd', 'popd', 'true', ':', 'fi', 'done', 'esac', '}'])
const FILTER_MAX_OPERANDS: Readonly<Record<string, number>> = { head: 0, tail: 0, wc: 0, cut: 0, grep: 1, jq: 1 }
const FILTER_FILE_OPTIONS: Readonly<Record<string, RegExp>> = {
  grep: /^-[A-Za-z]*[rRdf]|^--(?:recursive|dereference-recursive|directories|file)/,
  jq: /^-[A-Za-z]*[fL]|^--(?:from-file|slurpfile|rawfile|argfile)/,
}
const OPTION_TAKING_NUMBER = /^-[A-Za-z]*[nNcmABCfb]$|^--(?:lines|bytes|max-count|after-context|before-context|context)$/

function isQuietFilter(program: string, args: readonly string[], readsInput: boolean): boolean {
  const maxOperands = FILTER_MAX_OPERANDS[program]
  if (maxOperands === undefined || readsInput) return false
  let operands = 0
  for (const [i, arg] of args.entries()) {
    if (arg.startsWith('-')) {
      if (FILTER_FILE_OPTIONS[program]?.test(arg)) return false
    } else if (!(/^\d+$/.test(arg) && OPTION_TAKING_NUMBER.test(args[i - 1] ?? ''))) operands++
  }
  return operands <= maxOperands
}

export function otherCommands(command: string, orbitExtras: readonly string[] = []): string[] {
  const { runs, unparsed } = runnable(command)
  const programs = new Set<string>(unparsed ? ['unparsed'] : [])
  const writes = new Set<string>()
  for (const run of runs) {
    if (run.substitutes) programs.add('$(...)')
    for (const target of run.writes) writes.add('> ' + target)
    if (orbitWords(run.words)) continue
    const at = run.words.findIndex((word) => !/^\w+=/.test(word))
    const program = run.words[at]
    if (program === undefined || SUBSTITUTION_MARKERS.includes(program)) continue
    const bare = !program.includes('/')
    if (bare && (SHELL_WORDS.has(program) || (run.piped && isQuietFilter(program, run.words.slice(at + 1), run.readsInput)))) continue
    programs.add(program)
  }
  return [...programs, ...orbitExtras, ...writes]
}

export function runsOrbit(command: string): boolean {
  return orbitInvocations(command).length > 0
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
