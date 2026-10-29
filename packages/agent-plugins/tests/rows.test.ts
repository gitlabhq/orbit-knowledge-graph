import { expect, test } from 'claude-code/testing'

import { QUERY_LABEL, describeOrbitCall } from '../core/calls.ts'
import { isPendingCall, leadsIntoOrbit, mergeRuns, usesOrbit } from '../hooks/rows.ts'

function calls(...commands: string[]) {
  return commands.map((command, i) => {
    const call = describeOrbitCall('Bash', { command })
    if (!call) throw new Error(`not an Orbit call: ${command}`)
    return { props: { isRunning: false }, i, call }
  })
}

const q = (cypher: string) => `glab orbit query "${cypher}"`
const MR_PROJECT = q('MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN mr')
const USER_MR = q('MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest) RETURN u')
const PIPELINE_JOB = q('MATCH (pl:Pipeline)-[:HAS_JOB]->(j:Job) RETURN j')
const JOB_RUNNER = q('MATCH (j:Job)-[:RAN_ON]->(r:Runner) RETURN r')

test('calls of one kind share a line that shows the latest detail', () => {
  const rows = mergeRuns(calls(MR_PROJECT, q("CALL db.schema('MergeRequest')"), USER_MR, PIPELINE_JOB, JOB_RUNNER))
  expect(rows.map((row) => [row.call.label, row.call.detail])).toEqual([
    [QUERY_LABEL, 'Jobs → Runners'],
    ['Read Orbit schema', 'MergeRequest'],
  ])
})

test('a call with no detail keeps the last detail on its line', () => {
  expect(mergeRuns(calls(q("CALL db.schema('Runner')"), q('CALL db.schema()'))).map((row) => row.call.detail)).toEqual(['Runner'])
})

test('a running call gets its own line under the finished one, and merges up when done', () => {
  const [first, second] = calls(MR_PROJECT, USER_MR)
  if (!first || !second) throw new Error('two calls expected')
  const rows = mergeRuns([first, { ...second, props: { isRunning: true } }])
  expect(rows.map((row) => [row.isRunning, row.call.detail])).toEqual([
    [false, 'Merge requests → Projects'],
    [true, 'Users → Merge requests'],
  ])
  expect(mergeRuns([first, second]).map((row) => [row.isRunning, row.call.detail])).toEqual([[false, 'Users → Merge requests']])
})

test('a shell call still being written does not break an Orbit group', () => {
  expect(isPendingCall({ tool: 'Bash', input: {} })).toBe(true)
  expect(isPendingCall({ tool: 'Bash', input: { command: 'glab orb' } })).toBe(true)
  expect(isPendingCall({ tool: 'Bash', input: { command: 'orb' } })).toBe(true)
  expect(isPendingCall({ tool: 'Bash', input: { command: 'ls' } })).toBe(false)
  expect(isPendingCall({ tool: 'Bash', input: {}, output: 'done' })).toBe(false)
})

test('Orbit work covers the skill, Orbit rows, and shell calls that run Orbit among other commands', () => {
  expect(usesOrbit('Skill', { skill: 'orbit' })).toBe(true)
  expect(usesOrbit('Bash', { command: MR_PROJECT })).toBe(true)
  expect(usesOrbit('Bash', { command: 'glab orbit --yes status 2>&1 | head -40; echo ---; git remote -v' })).toBe(true)
  expect(usesOrbit('Skill', { skill: 'commit' })).toBe(false)
  expect(usesOrbit('Bash', { command: 'git log --oneline -5' })).toBe(false)
  expect(usesOrbit('Bash', { command: 'rg "glab orbit" docs/' })).toBe(false)
})

test('a progress line is the text right before an Orbit call, and the answer after it is not', () => {
  const orbit = { tool: 'Bash', input: { command: MR_PROJECT } }
  const messages = [
    { role: 'user', text: 'analyse MRs', toolUses: [] },
    { role: 'assistant', text: '', toolUses: [] },
    { role: 'assistant', text: 'The schema is loaded. Next I will count MRs.', toolUses: [] },
    { role: 'assistant', text: '', toolUses: [orbit] },
    { role: 'user', text: '', toolUses: [] },
    { role: 'assistant', text: 'There are 75 open MRs.', toolUses: [] },
    { role: 'assistant', text: 'Next I will read the README.', toolUses: [] },
    { role: 'assistant', text: '', toolUses: [{ tool: 'Read', input: { file_path: 'README.md' } }] },
  ]
  expect(leadsIntoOrbit(messages, 'The schema is loaded. Next I will count MRs.')).toBe(true)
  expect(leadsIntoOrbit(messages, 'There are 75 open MRs.')).toBe(false)
  expect(leadsIntoOrbit(messages, 'Next I will read the README.')).toBe(false)
  expect(leadsIntoOrbit(messages, 'not in the transcript')).toBe(false)
})
