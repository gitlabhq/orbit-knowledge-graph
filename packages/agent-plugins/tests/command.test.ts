import { expect, test } from 'claude-code/testing'

import { isOrbitPrefix, orbitCalls, subcommandOf } from '../core/command.ts'

const known = (key: string) => ['grep', 'query', 'workspace sql'].includes(key)

test('glab orbit and the standalone binary are launchers, with env and flags skipped', () => {
  for (const command of ['glab orbit grep foo', 'orbit grep foo', 'ORBIT_DATA_DIR=/tmp orbit --yes grep foo', 'GLAB_HOST=gitlab.com glab orbit grep foo']) {
    expect(orbitCalls(command)).toEqual([['grep', 'foo']])
  }
})

test('quotes and escapes come off the words', () => {
  expect(orbitCalls(`orbit query "n.x = \\"a\\"" it\\'s 'p;q|r'`)).toEqual([['query', 'n.x = "a"', "it's", 'p;q|r']])
  expect(orbitCalls('orbit grep \\\n  foo')).toEqual([['grep', 'foo']])
  expect(orbitCalls('orbit query "price \\$5 and \\`x\\`"')).toEqual([['query', 'price $5 and `x`']])
  expect(orbitCalls("orbit query 'MATCH (n) RETURN n // $(not run) `nor this`'")).toEqual([['query', 'MATCH (n) RETURN n // $(not run) `nor this`']])
})

test('cd, true, and a count-only pipe into head, tail, or wc keep a command an Orbit call', () => {
  expect(orbitCalls('cd repo && orbit grep a | head -5')).toEqual([['grep', 'a']])
  expect(orbitCalls('orbit grep a | tail -n 5 | wc -l')).toEqual([['grep', 'a']])
  expect(orbitCalls('orbit grep a | head -c 600')).toEqual([['grep', 'a']])
  expect(orbitCalls('orbit status || true')).toEqual([['status']])
  expect(orbitCalls('orbit status;\n')).toEqual([['status']])
})

test('stderr and /dev/null redirects keep a command an Orbit call', () => {
  for (const command of [
    'glab orbit query "MATCH (n) RETURN n" 2>&1',
    'glab orbit query "MATCH (n) RETURN n" 2>/dev/null | head -60',
    'glab orbit query "MATCH (n) RETURN n" >/dev/null 2>&1',
    'glab orbit query "MATCH (n) RETURN n" &>/dev/null || true',
    'glab orbit query "MATCH (n) RETURN n" 2> /dev/null',
  ]) {
    expect(orbitCalls(command)).toEqual([['query', 'MATCH (n) RETURN n']])
  }
})

test('several Orbit calls in one command are each listed', () => {
  expect(orbitCalls('orbit grep a; orbit grep b && cd x\norbit status')).toEqual([['grep', 'a'], ['grep', 'b'], ['status']])
})

test('anything the tokenizer does not model is not an Orbit call', () => {
  for (const command of [
    '',
    ';',
    'orbit status |',
    "orbit grep 'unterminated",
    'orbit grep "unterminated',
    'orbit grep trailing\\',
    'orbit status | orbit status',
    'cd repo | orbit status',
    'head -5 | orbit status',
    'rg "glab orbit grep" docs/',
    "echo 'glab orbit grep x'",
    'git commit -m "fix: glab orbit query output"',
  ]) {
    expect(orbitCalls(command)).toBe(null)
  }
})

test('the subcommand path is the longest known one', () => {
  expect(subcommandOf(['workspace', '--db', 'x', 'sql', 'SELECT 1'], known)).toEqual({ key: 'workspace sql', args: ['SELECT 1'] })
  expect(subcommandOf(['grep', 'foo'], known)).toEqual({ key: 'grep', args: ['foo'] })
  expect([['gerp'], ['workspace'], []].map((words) => subcommandOf(words, known))).toEqual([null, null, null])
})

test('a command still being written may become an Orbit call', () => {
  expect(['', 'g', 'glab orb', 'orb'].map(isOrbitPrefix)).toEqual([true, true, true, true])
  expect(['ls', 'glab mr', 'orbit grep x'].map(isOrbitPrefix)).toEqual([false, false, false])
})
