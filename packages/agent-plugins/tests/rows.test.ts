import { expect, test } from 'claude-code/testing'

import { QUERY_LABEL, describeOrbitCall } from '../core/calls.ts'
import { isPendingCall, joinPaths, mergeRuns } from '../hooks/rows.ts'

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
  expect(rows[0]?.call).toMatchObject({ label: QUERY_LABEL, detail: 'User → MergeRequest → Project' })
})

test('a disconnected query starts its own row', () => {
  const rows = mergeRuns(calls(MR_PROJECT, PIPELINE_JOB, JOB_RUNNER))
  expect(rows.map((row) => row.call.detail)).toEqual(['MergeRequest → Project', 'Pipeline → Job → Runner'])
})

test('a repeated call graph query stays on one row', () => {
  const rows = mergeRuns(calls(CALLS, CALLS))
  expect(rows.length).toBe(1)
  expect(rows[0]?.call.detail).toBe('Definition')
})

test('guide and schema calls stay on the row of the queries they follow', () => {
  const rows = mergeRuns(calls('glab orbit skills get orbit', q("CALL db.schema('MergeRequest')"), MR_PROJECT))
  expect(rows.length).toBe(1)
  expect(rows[0]?.call).toMatchObject({ label: QUERY_LABEL, detail: 'MergeRequest → Project' })
})

test('a running query shows the path walked so far', () => {
  const [first, second] = calls(MR_PROJECT, USER_MR)
  if (!first || !second) throw new Error('two calls expected')
  const rows = mergeRuns([first, { ...second, props: { isRunning: true } }])
  expect(rows[0]).toMatchObject({ isRunning: true, call: { active: 'Exploring Orbit', detail: 'User → MergeRequest → Project' } })
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
