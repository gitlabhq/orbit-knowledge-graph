export type Subcommand = { key: string; args: string[] }

const PIPE_TARGETS = new Set(['head', 'tail'])
const SHELL_ONLY = new Set(['<', '>', '#', '$(', '`'])
const HARMLESS_REDIRECT = /^(?:&>>?|>>?)(?:&\d+|\s*\/dev\/null)(?=[\s;&|)]|$)/

export function simpleCommands(command: string): string[][] {
  const commands: string[][] = []
  let words: string[] = []
  let word = ''
  let inWord = false
  let quote = ''
  let piped = false
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
  for (let i = 0; i < command.length; i++) {
    const c = command[i] ?? ''
    if (quote !== "'" && (c === '`' || (c === '$' && command[i + 1] === '('))) words.push(c === '`' ? '`' : '$(')
    if (quote) {
      if (c === quote) quote = ''
      else if (c === '\\' && quote === '"' && i + 1 < command.length) word += command[++i]
      else word += c
    } else if (c === "'" || c === '"') {
      quote = c
      inWord = true
    } else if (c === '\\' && command[i + 1] === '\n') {
      i++
      endWord()
    } else if (c === '\\' && i + 1 < command.length) {
      word += command[++i]
      inWord = true
    } else if ('&>'.includes(c) && HARMLESS_REDIRECT.test(command.slice(i))) {
      if (/^\d+$/.test(word) && /\d/.test(command[i - 1] ?? '')) {
        word = ''
        inWord = false
      } else endWord()
      i += (command.slice(i).match(HARMLESS_REDIRECT)?.[0].length ?? 1) - 1
    } else if (c === '|' && command[i + 1] !== '|') {
      endCommand()
      piped = true
      if (command[i + 1] === '&') i++
    } else if (c === '\n' || ';&|()'.includes(c)) {
      endCommand()
      if ('&|'.includes(c) && command[i + 1] === c) i++
    } else if ('<>'.includes(c) || (c === '#' && !inWord)) {
      endWord()
      words.push(c)
    } else if (/\s/.test(c)) {
      endWord()
    } else {
      word += c
      inWord = true
    }
  }
  endCommand()
  return commands
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

export function orbitInvocations(command: string): string[][] {
  const commands = simpleCommands(command)
  const calls = commands.map(orbitWords)
  const helper = (words: string[]) =>
    words[0] === 'cd' || (words[0] === '|' && PIPE_TARGETS.has(words[1] ?? '')) || words.join(' ') === 'true'
  const plain = commands.every((words, i) => !words.some((w) => SHELL_ONLY.has(w)) && (calls[i] || helper(words)))
  return plain ? calls.filter((words) => words !== null) : []
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
