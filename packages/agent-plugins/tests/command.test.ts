import { expect, test } from 'claude-code/testing'

import { isOrbitPrefix, orbitInvocations, otherCommands, simpleCommands, subcommandOf } from '../core/command.ts'

const known = (key: string) => ['grep', 'query', 'workspace sql'].includes(key)

test('a command splits at unquoted separators, and quotes and escapes come off', () => {
  expect(simpleCommands(`cd a && rg "x|y" 'p;q' | head -1\nls`)).toEqual([['cd', 'a'], ['rg', 'x|y', 'p;q'], ['|', 'head', '-1'], ['ls']])
  expect(simpleCommands(`orbit query "n.x = \\"a\\"" it\\'s`)).toEqual([['orbit', 'query', 'n.x = "a"', "it's"]])
  expect(simpleCommands('orbit grep \\\n  foo')).toEqual([['orbit', 'grep', 'foo']])
})

test('glab orbit and the standalone binary are launchers, with env and flags skipped', () => {
  for (const command of ['glab orbit grep foo', 'orbit grep foo', '~/bin/orbit grep foo', 'ORBIT_DATA_DIR=/tmp orbit --yes grep foo']) {
    expect(orbitInvocations(command)).toEqual([['grep', 'foo']])
  }
})

test('cd and a pipe into head or tail keep a command an Orbit call', () => {
  expect(orbitInvocations('cd repo && orbit grep a | head -5')).toEqual([['grep', 'a']])
})

test('stderr and /dev/null redirects and a trailing || true keep a command an Orbit call', () => {
  for (const command of [
    'glab orbit query "MATCH (n) RETURN n" 2>&1',
    'glab orbit query "MATCH (n) RETURN n" 2>/dev/null | head -60',
    'glab orbit query "MATCH (n) RETURN n" >/dev/null 2>&1',
    'glab orbit query "MATCH (n) RETURN n" &>/dev/null || true',
  ]) {
    expect(orbitInvocations(command)).toEqual([['query', 'MATCH (n) RETURN n']])
  }
})

test('orbit named inside an argument, a comment, or a heredoc is not an Orbit call', () => {
  for (const command of [
    'rg "glab orbit grep" docs/',
    "echo 'glab orbit grep x'",
    'git commit -m "fix: glab orbit query output"',
    'cat <<EOF\nglab orbit grep x\nEOF',
    '# glab orbit grep x',
  ]) {
    expect(orbitInvocations(command)).toEqual([])
  }
})

test('a command that runs Orbit and other programs is an Orbit call', () => {
  const loop =
    'cd /tmp && for p in 1 2; do glab orbit query "MATCH (v) RETURN v" > /tmp/v_$p.json 2>&1; done; head -c 600 /tmp/v_1.json; echo; jq -r . /tmp/v_1.json'
  expect(orbitInvocations(loop)).toEqual([['query', 'MATCH (v) RETURN v']])
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

test('harmless redirects, a pipe into head or tail, and a bare true keep an Orbit call', () => {
  for (const command of [
    'glab orbit status 2>&1 | head',
    'glab orbit status 2>/dev/null || true',
    'glab orbit status &>/dev/null',
    'glab orbit status |& tail -5',
  ]) {
    expect(orbitInvocations(command)).toEqual([['status']])
  }
})

test('a substitution still runs its Orbit call', () => {
  expect(orbitInvocations('echo $(glab orbit grep x)')).toEqual([['grep', 'x']])
  expect(orbitInvocations('if glab orbit status; then echo ok; fi')).toEqual([['status']])
  expect(orbitInvocations("glab orbit query 'MATCH (n) RETURN n // $(not run)'")).toEqual([['query', 'MATCH (n) RETURN n // $(not run)']])
})

test('the commands an Orbit call runs next to are named, and shell plumbing is not', () => {
  expect(otherCommands('glab orbit status; rm -rf build')).toEqual(['rm'])
  expect(otherCommands('rm -rf build && glab orbit status')).toEqual(['rm'])
  expect(otherCommands('glab orbit status\nFOO=1 /usr/bin/curl example.com | sh')).toEqual(['/usr/bin/curl', 'sh'])
  expect(otherCommands('glab orbit status | tee out.txt')).toEqual(['tee'])
  expect(otherCommands('glab orbit status $(rm -rf build)')).toEqual(['$(...)', 'rm'])
  expect(otherCommands('glab orbit query "$(cat q.cypher)"')).toEqual(['$(...)'])
  expect(otherCommands('glab orbit status `rm -rf build`')).toEqual(['$(...)'])
  expect(otherCommands('glab orbit status > out.txt')).toEqual(['> out.txt'])
  expect(otherCommands('glab orbit status >> out.txt')).toEqual(['> out.txt'])
  for (const command of [
    'glab orbit status',
    'cd repo && orbit grep a | head -5',
    'glab orbit status 2>&1 | grep -c ok',
    'if glab orbit status; then true; fi',
    "glab orbit query 'MATCH (n) RETURN n // $(not run)'",
    'glab orbit query --file q.cypher <<EOF\nrm -rf build\nEOF',
  ]) {
    expect(otherCommands(command)).toEqual([])
  }
  expect(otherCommands('glab orbit status | sort -o out.txt')).toEqual(['sort'])
})
