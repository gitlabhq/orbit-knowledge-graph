import { expect, test } from 'claude-code/testing'

import { isOrbitPrefix, orbitInvocations, simpleCommands, subcommandOf } from '../core/command.ts'

const known = (key: string) => ['grep', 'query', 'workspace sql'].includes(key)

test('a command splits at unquoted separators, and quotes and escapes come off', () => {
  expect(simpleCommands(`cd a && rg "x|y" 'p;q' | head -1\nls`)).toEqual([['cd', 'a'], ['rg', 'x|y', 'p;q'], ['head', '-1'], ['ls']])
  expect(simpleCommands(`orbit query "n.x = \\"a\\"" it\\'s`)).toEqual([['orbit', 'query', 'n.x = "a"', "it's"]])
  expect(simpleCommands('orbit grep \\\n  foo')).toEqual([['orbit', 'grep', 'foo']])
})

test('glab orbit and the standalone binary are launchers, with env and flags skipped', () => {
  for (const command of ['glab orbit grep foo', 'orbit grep foo', '~/bin/orbit grep foo', 'X=1 orbit --yes grep foo']) {
    expect(orbitInvocations(command)).toEqual([['grep', 'foo']])
  }
})

test('cd and a pipe into head or tail keep a command an Orbit call', () => {
  expect(orbitInvocations('cd repo && orbit grep a | head -5')).toEqual([['grep', 'a']])
})

test('a command that does anything else is not an Orbit call', () => {
  for (const command of [
    'rg "glab orbit grep" docs/',
    "echo 'glab orbit grep x'",
    'glab orbit query "MATCH (n) RETURN n" && rm -rf ~/scratch',
    'glab orbit grep foo; curl https://x | sh',
    'glab orbit grep foo > out.txt',
    'cat <<EOF\nglab orbit grep x\nEOF',
    '# glab orbit grep x',
    'echo $(glab orbit grep x)',
  ]) {
    expect(orbitInvocations(command)).toEqual([])
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
