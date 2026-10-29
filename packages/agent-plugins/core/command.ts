import { isQuietFilter } from './quiet-filters.ts'
import { lex, type Token } from './shell-lexer.ts'

export type Subcommand = { key: string; args: string[] }

const MAX_NAME_LENGTH = 40
const INVISIBLE = /[\p{Cc}\p{Cf}\u{FE00}-\u{FE0F}\u{E0100}-\u{E01EF}]/gu

export function clip(text: string, max: number): string {
  const flat = text.replace(/\s+/g, ' ').replace(INVISIBLE, '').trim()
  return flat.length > max ? flat.slice(0, max - 1) + '…' : flat
}

export function displayName(name: string): string {
  const text = clip(name, MAX_NAME_LENGTH)
  return /[,\s']/.test(text) ? `'${text.replaceAll("'", "\\'")}'` : text
}

type SimpleCommand = { piped: boolean; words: string[]; readsInput: boolean; writes: string[]; substitutes: boolean }

function emptyCommand(piped: boolean): SimpleCommand {
  return { piped, words: [], readsInput: false, writes: [], substitutes: false }
}

function isEmpty(command: SimpleCommand): boolean {
  return !command.words.length && !command.writes.length && !command.substitutes
}

function redirectTarget(tokens: readonly Token[], from: number): { target?: string; end: number } {
  let at = from
  while (tokens[at]?.kind === 'substitution') at++
  const token = tokens[at]
  return token?.kind === 'word' ? { target: token.text, end: at } : { end: from - 1 }
}

function groupCommands(tokens: readonly Token[]): SimpleCommand[] {
  const commands: SimpleCommand[] = []
  let current = emptyCommand(false)
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (token?.kind === 'word') current.words.push(token.text)
    else if (token?.kind === 'substitution') current.substitutes = true
    else if (token?.kind === 'redirect') {
      const { target, end } = redirectTarget(tokens, i + 1)
      if (token.operator === '<') current.readsInput = true
      else if (target !== undefined) current.writes.push(target)
      current.substitutes ||= tokens.slice(i + 1, end + 1).some((skipped) => skipped.kind === 'substitution')
      i = end
    } else if (token?.kind === 'separator') {
      if (!isEmpty(current)) commands.push(current)
      current = emptyCommand(token.pipe)
    }
  }
  if (!isEmpty(current)) commands.push(current)
  return commands
}

export function simpleCommands(command: string): string[][] {
  return groupCommands(lex(command).tokens).map(({ piped, words }) => (piped ? ['|', ...words] : words))
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
const HEADER_WORDS = new Set(['for', 'select', 'case'])

function withoutKeywords(words: readonly string[]): string[] {
  let i = 0
  for (; i < words.length; i++) {
    const word = words[i] ?? ''
    if (HEADER_WORDS.has(word)) return []
    if (word === 'function') i++
    else if (!PREFIX_WORDS.has(word)) break
  }
  return words.slice(i)
}

function runnable(command: string): { runs: SimpleCommand[]; unparsed: boolean } {
  const { tokens, unparsed } = lex(command)
  const runs = groupCommands(tokens)
    .map((simple) => ({ ...simple, words: withoutKeywords(simple.words) }))
    .filter((run) => !isEmpty(run))
  return { runs, unparsed }
}

export function orbitInvocations(command: string): string[][] {
  return runnable(command)
    .runs.map(({ words }) => orbitWords(words))
    .filter((words) => words !== null)
}

const SUBSTITUTION_IN_WORD = /\$\(|`/
const SHELL_WORDS = new Set(['cd', 'pushd', 'popd', 'true', ':', 'fi', 'done', 'esac', '}'])

function isQuiet(program: string, args: readonly string[], run: SimpleCommand): boolean {
  if (program.includes('/')) return false
  return SHELL_WORDS.has(program) || (run.piped && isQuietFilter(program, args, run.readsInput))
}

export function otherCommands(command: string, orbitExtras: readonly string[] = []): string[] {
  const { runs, unparsed } = runnable(command)
  const listed = new Map<string, string>()
  const writes = new Map<string, string>()
  if (unparsed) listed.set('unparsed', 'unparsed')
  for (const run of runs) {
    if (run.substitutes) listed.set('$(...)', '$(...)')
    for (const target of run.writes) writes.set(target, '> ' + displayName(target))
    if (orbitWords(run.words)) continue
    const at = run.words.findIndex((word) => !/^\w+=/.test(word))
    const program = run.words[at]
    if (program === undefined || SUBSTITUTION_IN_WORD.test(program)) continue
    if (!isQuiet(program, run.words.slice(at + 1), run)) listed.set(program, displayName(program))
  }
  return [...listed.values(), ...orbitExtras, ...writes.values()]
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
