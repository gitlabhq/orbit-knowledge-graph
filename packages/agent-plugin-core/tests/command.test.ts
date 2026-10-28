import { expect, test } from 'bun:test'

import { isOrbitPrefix, orbitInvocations, simpleCommands, subcommandOf } from '../src/command.ts'

const known = (key: string) => ['grep', 'query', 'workspace sql', 'config set'].includes(key)

test('a command splits at unquoted separators only', () => {
  expect(simpleCommands(`cd a && rg "x|y" 'p;q' | head -1; echo done\nls`)).toEqual([
    ['cd', 'a'],
    ['rg', 'x|y', 'p;q'],
    ['head', '-1'],
    ['echo', 'done'],
    ['ls'],
  ])
})

test('quotes and escapes come off the words', () => {
  expect(simpleCommands(`orbit query "MATCH (n) WHERE n.x = \\"a\\" RETURN n" it\\'s`)).toEqual([
    ['orbit', 'query', 'MATCH (n) WHERE n.x = "a" RETURN n', "it's"],
  ])
  expect(simpleCommands('orbit grep \\\n  foo')).toEqual([['orbit', 'grep', 'foo']])
})

test('glab orbit and the standalone binary are both launchers', () => {
  expect(orbitInvocations('glab orbit grep foo')).toEqual([{ launcher: ['glab', 'orbit'], words: ['grep', 'foo'] }])
  expect(orbitInvocations('orbit grep foo')).toEqual([{ launcher: ['orbit'], words: ['grep', 'foo'] }])
  expect(orbitInvocations('~/.local/bin/orbit grep foo')[0]?.launcher).toEqual(['~/.local/bin/orbit'])
  expect(orbitInvocations('ORBIT_DATA_DIR=/tmp orbit grep foo')[0]?.words).toEqual(['grep', 'foo'])
})

test('flags before the subcommand are skipped', () => {
  expect(orbitInvocations('glab orbit --yes grep foo')[0]?.words).toEqual(['grep', 'foo'])
})

test('orbit named inside an argument is not a call', () => {
  for (const command of [
    'rg "glab orbit grep" docs/',
    "echo 'run glab orbit setup'",
    'git commit -m "fix: glab orbit query output"',
    'rg "a|glab orbit grep" docs/',
    'cat orbit.md',
  ]) {
    expect(orbitInvocations(command)).toEqual([])
  }
})

test('every orbit call in a command is found', () => {
  expect(orbitInvocations('git pull && orbit grep a; glab orbit context b.rs').map((c) => c.words[0])).toEqual([
    'grep',
    'context',
  ])
})

test('the subcommand path is the longest known one', () => {
  expect(subcommandOf(['workspace', 'sql', 'SELECT 1'], known)).toEqual({ key: 'workspace sql', args: ['SELECT 1'] })
  expect(subcommandOf(['workspace', '--db', 'x.duckdb', 'sql', 'SELECT 1'], known)?.key).toBe('workspace sql')
  expect(subcommandOf(['grep', 'foo'], known)).toEqual({ key: 'grep', args: ['foo'] })
  expect(subcommandOf(['gerp', 'foo'], known)).toBe(null)
  expect(subcommandOf(['workspace'], known)).toBe(null)
  expect(subcommandOf([], known)).toBe(null)
})

test('a command still being written may become an orbit call', () => {
  expect(['', '  ', 'g', 'glab orb', 'orb', 'orbit '].map(isOrbitPrefix)).toEqual([true, true, true, true, true, true])
  expect(['ls', 'glab mr', 'orbit grep x'].map(isOrbitPrefix)).toEqual([false, false, false])
})
