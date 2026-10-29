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
const CALLS = q('MATCH (c:Definition)-[:CALLS]->(d:Definition) RETURN c')

test('each different query gets its own row', () => {
  const rows = mergeRuns(calls(MR_PROJECT, USER_MR, PIPELINE_JOB, JOB_RUNNER))
  expect(rows.map((row) => row.call.detail)).toEqual(['Merge requests → Projects', 'Users → Merge requests', 'Pipelines → Jobs', 'Jobs → Runners'])
  expect(rows.every((row) => row.call.label === QUERY_LABEL)).toBe(true)
})

test('a query repeated back to back shares one row with a count', () => {
  const rows = mergeRuns(calls(CALLS, CALLS, CALLS))
  expect(rows.map((row) => row.call.detail)).toEqual(['Definitions  ×3'])
  const [ok, failed] = calls(CALLS, CALLS)
  if (!ok || !failed) throw new Error('two calls expected')
  expect(mergeRuns<{ isRunning: boolean; isErrored?: boolean }>([ok, { ...failed, props: { isRunning: false, isErrored: true } }]).length).toBe(2)
})

test('each schema read and each search gets its own row', () => {
  expect(mergeRuns(calls(q('CALL db.schema()'), q("CALL db.schema('MergeRequest')"))).map((row) => row.call.detail)).toEqual(['', 'MergeRequest'])
  expect(mergeRuns(calls('glab orbit grep foo', 'glab orbit grep bar')).map((row) => row.call.detail)).toEqual(['foo', 'bar'])
})

test('a running query shows its own row', () => {
  const [first, second] = calls(MR_PROJECT, USER_MR)
  if (!first || !second) throw new Error('two calls expected')
  const rows = mergeRuns([first, { ...second, props: { isRunning: true } }])
  expect(rows[1]).toMatchObject({ isRunning: true, call: { active: 'Exploring Orbit', detail: 'Users → Merge requests' } })
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
