import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describeCommand, othersText } from '../core/calls.ts'

const STUBS = ['prog', 'tool', 'helper', 'inner', 'glab', 'orbit']
const SUBSTITUTION_MARKER = '$(...)'
const UNPARSED_MARKER = 'unparsed'

const ORBIT_CALLS = ['glab orbit status', 'glab orbit grep term', 'orbit query "MATCH (n) RETURN n"', 'glab orbit query <<EOF\nMATCH (n) RETURN n\nEOF']
const SEPARATORS = [' ; ', ' && ', ' || ', ' | ', '\n', ' & ', ' |& ']
const FRAGMENTS = [
  'prog', 'tool x', './prog', 'FOO=1 prog', '(prog)', '{ prog; }', '! prog', 'time prog',
  '$(inner)', '`inner`', '"$(inner)"', '<(inner)', '> >(inner)', ': ${x:-$(inner)}',
  '</dev/null prog', '>out.txt prog', '> out.txt', '2>&1', '&>out.txt',
  '<<EOF; prog', '<<< text; prog', 'cat <<EOF\n$(inner)\nEOF', "cat <<'EOF'\n$(inner)\nEOF", 'cat <<EOF\nit\'s\nEOF\nprog',
  '# it\'s a comment\nprog', "echo $'\\'' ; prog", '((x=1<<2)); prog', 'echo $((1<<2)); prog',
  'if ((x<<2)); then prog; fi', 'while ((i<1)); do prog; done', 'for i in 1 2; do prog; done',
  'for ((i=0;i<1;i++)); do prog; done', 'for ((i=0;i<1;i++)) do prog; done', 'for ((i=0;i<1;i++)) { prog; }', 'for i do prog; done',
  'pro\\\ng', ': a\\\n#; prog', '"pro\\\ng"', 'cat <(true)#; prog', ': ${x:- #}; prog', 'FOO+=1 prog', 'time -p prog',
  ': "${x:-"a #"}"; prog', ': "$(echo "a #")"; prog', ': "$(echo # (\n)"; prog; "x)"', 'case x in x) prog;; esac', 'f() { prog; }; f',
  'head -n 1', 'head file.txt', 'head -- -file', 'grep -c ok', 'grep -r pattern .', 'grep -epat file.txt',
  'grep -e pat file.txt', 'grep --regexp=pat file.txt', 'grep -- -x file.txt', 'grep x < file.txt',
  'jq .', 'jq . file.txt', 'jq -n env', 'cd .', 'true', ': noop',
  'echo "unclosed', "echo 'unclosed", 'cat <<EOF\nno terminator',
  'glab orbit workspace reset', 'orbit config set a b',
]

function prng(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function generate(random: () => number, count: number): string[] {
  const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)] as T
  return Array.from({ length: count }, () => {
    const parts = [pick(ORBIT_CALLS)]
    for (let n = 1 + Math.floor(random() * 3); n > 0; n--) parts.push(pick(SEPARATORS), pick(FRAGMENTS))
    return parts.join('')
  })
}

function setup(): { dir: string; bin: string } {
  const dir = mkdtempSync(join(tmpdir(), 'differential-'))
  const bin = join(dir, 'bin')
  mkdirSync(bin)
  for (const name of STUBS) {
    writeFileSync(join(bin, name), '#!/bin/sh\necho "${0##*/}" >> "$STUB_LOG"\n')
    chmodSync(join(bin, name), 0o755)
  }
  writeFileSync(join(dir, 'prog'), '#!/bin/sh\necho prog >> "$STUB_LOG"\n')
  chmodSync(join(dir, 'prog'), 0o755)
  writeFileSync(join(dir, 'file.txt'), 'text\n')
  writeFileSync(join(dir, '-file'), 'text\n')
  return { dir, bin }
}

let runs = 0

function ranStubs(command: string, dir: string, bin: string): string[] {
  const log = join(dir, `stub-${runs++}.log`)
  writeFileSync(log, '')
  Bun.spawnSync(['bash', '-c', `${command}\nwait`], {
    cwd: dir,
    env: { PATH: `${bin}:/usr/bin:/bin`, STUB_LOG: log, HOME: dir },
    stdin: 'ignore',
    stdout: 'ignore',
    stderr: 'ignore',
    timeout: 5000,
  })
  return [...new Set(readFileSync(log, 'utf8').split('\n').filter(Boolean))]
}

function unnamed(command: string, ran: readonly string[]): string[] {
  const call = describeCommand(command)
  if (!call) return ['<no Orbit row>']
  const listed = call.others ?? []
  if (listed.includes(UNPARSED_MARKER)) return []
  const named = (name: string) => listed.some((item) => item === name || item.endsWith('/' + name))
  const covered = (name: string) => name === 'inner' && listed.includes(SUBSTITUTION_MARKER)
  return ran.filter((name) => name !== 'glab' && name !== 'orbit' && !covered(name) && !named(name))
}

function main(): number {
  const flag = (name: string, fallback: number) => {
    const at = process.argv.indexOf(name)
    return at < 0 ? fallback : Number(process.argv[at + 1])
  }
  const seed = flag('--seed', 1)
  const count = flag('--count', 300)
  if (!(count > 0)) {
    console.log('--count must be a positive number')
    return 2
  }
  const { dir, bin } = setup()
  const failures: string[] = []
  try {
    if (!ranStubs('glab orbit status; prog', dir, bin).includes('prog')) {
      console.log('positive control failed: a stub did not log, so the check cannot see anything')
      return 2
    }
    let logged = 0
    const alone = FRAGMENTS.map((fragment) => `${ORBIT_CALLS[0]}${SEPARATORS[0]}${fragment}`)
    for (const command of [...alone, ...generate(prng(seed), count)]) {
      const ran = ranStubs(command, dir, bin)
      if (ran.some((name) => name !== 'glab' && name !== 'orbit')) logged++
      const missing = unnamed(command, ran)
      if (missing.length) failures.push(`${JSON.stringify(command)} ran unnamed: ${missing.join(', ')}  row: ${describeCommand(command) ? othersText(describeCommand(command)!) : ''}`)
    }
    if (logged < (alone.length + count) / 2) {
      console.log(`only ${logged} commands logged a stub program, so the check is not exercising the row`)
      return 2
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
  console.log(`seed ${seed}: ${count} commands, ${failures.length} ran a program the row does not name`)
  for (const failure of failures.slice(0, 10)) console.log('  ' + failure)
  return failures.length ? 1 : 0
}

process.exit(main())
