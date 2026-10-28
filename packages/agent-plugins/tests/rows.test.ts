import { expect, test } from 'claude-code/testing'

import { QUERY_LABEL, describeOrbitCall } from '../core/calls.ts'
import { isPendingCall, joinPaths, leadsIntoOrbit, mergeRuns, usesOrbit } from '../hooks/rows.ts'

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

test('joined paths extend a chain at either end', () => {
  expect(joinPaths(['MergeRequest → Project', 'User → MergeRequest'])).toBe('User → MergeRequest → Project')
  expect(joinPaths(['Pipeline → Job', 'Job → Runner'])).toBe('Pipeline → Job → Runner')
  expect(joinPaths(['A → B', 'C → D'])).toBe('A → B, C → D')
})

test('a path the chain already holds is not repeated', () => {
  expect(joinPaths(['User → MergeRequest → Project', 'MergeRequest → Project'])).toBe('User → MergeRequest → Project')
})

test('connected queries share one row with the joined path', () => {
  const rows = mergeRuns(calls(MR_PROJECT, USER_MR))
  expect(rows.length).toBe(1)
  expect(rows[0]?.call).toMatchObject({ label: QUERY_LABEL, detail: 'Users → Merge requests → Projects' })
})

test('back-to-back queries share one row, with disconnected paths listed side by side', () => {
  const rows = mergeRuns(calls(MR_PROJECT, PIPELINE_JOB, JOB_RUNNER))
  expect(rows.map((row) => row.call.detail)).toEqual(['Merge requests → Projects, Pipelines → Jobs → Runners'])
})

test('a repeated call graph query stays on one row', () => {
  const rows = mergeRuns(calls(CALLS, CALLS))
  expect(rows.length).toBe(1)
  expect(rows[0]?.call.detail).toBe('Definitions')
})

test('a schema read keeps its own line between queries', () => {
  const rows = mergeRuns(calls(MR_PROJECT, q("CALL db.schema('MergeRequest')"), USER_MR))
  expect(rows.map((row) => row.call.label)).toEqual([QUERY_LABEL, 'Read Orbit schema', QUERY_LABEL])
})

test('similar commands in a row share one line with their details', () => {
  const rows = mergeRuns(calls('glab orbit grep foo', 'glab orbit grep bar'))
  expect(rows.length).toBe(1)
  expect(rows[0]?.call.detail).toBe('foo, bar')
})

test('a running query shows the path walked so far', () => {
  const [first, second] = calls(MR_PROJECT, USER_MR)
  if (!first || !second) throw new Error('two calls expected')
  const rows = mergeRuns([first, { ...second, props: { isRunning: true } }])
  expect(rows[0]).toMatchObject({ isRunning: true, call: { active: 'Exploring Orbit', detail: 'Users → Merge requests → Projects' } })
})

test('a shell call still being written does not break an Orbit group', () => {
  expect(isPendingCall({ tool: 'Bash', input: {} })).toBe(true)
  expect(isPendingCall({ tool: 'Bash', input: { command: 'glab orb' } })).toBe(true)
  expect(isPendingCall({ tool: 'Bash', input: { command: 'orb' } })).toBe(true)
  expect(isPendingCall({ tool: 'Bash', input: { command: 'ls' } })).toBe(false)
  expect(isPendingCall({ tool: 'Bash', input: {}, output: 'done' })).toBe(false)
})

test('a longer path that walks a whole chain replaces it', () => {
  expect(joinPaths(['Project → MergeRequest', 'Project → MergeRequest → User'])).toBe('Project → MergeRequest → User')
  expect(joinPaths(['Project → MergeRequest → User', 'Project → MergeRequest'])).toBe('Project → MergeRequest → User')
})

test('paths join on the node type whether it reads singular or plural', () => {
  expect(joinPaths(['Project → Merge requests', 'Merge request → Users'])).toBe('Project → Merge requests → Users')
  expect(joinPaths(['Project → Merge requests', 'Merge requests → Users'])).toBe('Project → Merge requests → Users')
  expect(joinPaths(['Pipeline → Jobs', 'Job → Runners'])).toBe('Pipeline → Jobs → Runners')
  expect(joinPaths(['Merge request → Users', 'Project → Merge requests'])).toBe('Project → Merge requests → Users')
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
