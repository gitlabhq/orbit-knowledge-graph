/** One orbit CLI call in a shell command: how it was launched and the words after the launcher. */
export type OrbitInvocation = { launcher: string[]; words: string[] }

/** A known subcommand path, such as `grep` or `workspace sql`, and the words after it. */
export type Subcommand = { key: string; args: string[] }

const LAUNCHERS = ['glab orbit ', 'orbit ']

// Splits only at unquoted separators, so orbit named inside an argument is not a call
export function simpleCommands(command: string): string[][] {
  const commands: string[][] = []
  let words: string[] = []
  let word = ''
  let inWord = false
  let quote = ''
  const endWord = () => {
    if (inWord) words.push(word)
    word = ''
    inWord = false
  }
  const endCommand = () => {
    endWord()
    if (words.length) commands.push(words)
    words = []
  }
  for (let i = 0; i < command.length; i++) {
    const c = command[i] ?? ''
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
    } else if (c === '\n' || ';&|()'.includes(c)) {
      endCommand()
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

/** Every `glab orbit` or standalone `orbit` call a shell command makes, flags before the subcommand skipped. */
export function orbitInvocations(command: string): OrbitInvocation[] {
  const found: OrbitInvocation[] = []
  for (const words of simpleCommands(command)) {
    let i = 0
    while (i < words.length && /^[A-Za-z_]\w*=/.test(words[i] ?? '')) i++
    const first = words[i] ?? ''
    let launcher: string[]
    if (first === 'glab' && words[i + 1] === 'orbit') launcher = ['glab', 'orbit']
    else if (first.split('/').pop() === 'orbit') launcher = [first]
    else continue
    i += launcher.length
    while (i < words.length && (words[i] ?? '').startsWith('-')) i++
    found.push({ launcher, words: words.slice(i) })
  }
  return found
}

/** The longest known subcommand path the words start, or null when the subcommand is unknown. */
export function subcommandOf(words: readonly string[], known: (key: string) => boolean): Subcommand | null {
  const [sub, ...rest] = words
  if (sub === undefined) return null
  // A nested subcommand can follow its parent's flags, as in `workspace --db x sql`
  const nested = rest.findIndex((word) => known(sub + ' ' + word))
  if (nested >= 0) return { key: sub + ' ' + rest[nested], args: rest.slice(nested + 1) }
  return known(sub) ? { key: sub, args: rest } : null
}

/** True while the model has written no command yet, or only the start of an orbit launcher. */
export function isOrbitPrefix(command: string): boolean {
  const start = command.trimStart()
  return start === '' || LAUNCHERS.some((launcher) => launcher.startsWith(start))
}
