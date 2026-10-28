import { expect, test } from 'claude-code/testing'

import { SUBCOMMANDS, describeOrbitCall, entitiesOf, fade } from '../hooks/orbit.ts'

const query = (cypher: string) => describeOrbitCall('Bash', { command: `glab orbit query "${cypher}"` })

test('every subcommand has a short finished and running label', () => {
  for (const [sub, [label, active]] of Object.entries(SUBCOMMANDS)) {
    expect(label.split(' ').length <= 3).toBe(true)
    expect(active.split(' ').length <= 3).toBe(true)
    expect(describeOrbitCall('Bash', { command: `glab orbit ${sub}` })?.label).toBe(label)
  }
})

test('an unknown subcommand reads as a query, never as running Orbit', () => {
  expect(describeOrbitCall('Bash', { command: 'glab orbit frobnicate' })).toMatchObject({
    label: 'Explored Orbit',
    active: 'Exploring Orbit',
  })
})

test('calls that are not Orbit calls are left alone', () => {
  expect(describeOrbitCall('Bash', { command: 'ls -la' })).toBe(null)
  expect(describeOrbitCall('Read', { file_path: 'README.md' })).toBe(null)
  expect(describeOrbitCall('mcp__slack__send_message', { text: 'hi' })).toBe(null)
})

test('Orbit MCP tools map to the same labels', () => {
  expect(describeOrbitCall('mcp__gitlab-orbit__invoke_command', { command_name: 'get_graph_schema' })?.label).toBe(
    'Read Orbit schema',
  )
  expect(describeOrbitCall('mcp__gitlab-orbit__list_commands', {})?.label).toBe('Listed Orbit tools')
})

test('a schema call names the entity it reads', () => {
  expect(describeOrbitCall('Bash', { command: "glab orbit query \"CALL db.schema('Runner')\"" })).toMatchObject({
    label: 'Read Orbit schema',
    detail: 'Runner',
  })
})

test('the guide call shows the skill name, not the get subcommand', () => {
  expect(describeOrbitCall('Bash', { command: 'glab orbit skills get orbit' })?.detail).toBe('orbit')
})

test('paths read from source to target', () => {
  expect(query('MATCH (p:Project)<-[:IN_PROJECT]-(mr:MergeRequest) RETURN mr')?.detail).toBe('MergeRequest → Project')
  expect(query('MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN u')?.detail).toBe(
    'User → MergeRequest → Project',
  )
})

test('repeated node types collapse into one', () => {
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition {project_id: 1}) RETURN c')?.detail).toBe('Definition')
  expect(query('MATCH (d:Definition {project_id: 1})<-[:CALLS]-(c:Definition) RETURN c')?.detail).toBe('Definition')
  expect(query('MATCH (child:Definition)-[:EXTENDS*1..3]->(parent:Definition) RETURN child')?.detail).toBe('Definition')
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition)<-[:DEFINES]-(f:File) RETURN f')?.detail).toBe(
    'File → Definition',
  )
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition)-[:IN_FILE]->(f:File) RETURN f')?.detail).toBe(
    'Definition → File',
  )
})

test('arrows both ways split into separate paths, and repeats show once', () => {
  expect(query('MATCH (a:User)-[:AUTHORED]->(mr:MergeRequest)<-[:REVIEWED]-(r:User) RETURN mr')?.detail).toBe(
    'User → MergeRequest',
  )
})

test('an undirected hop and an untyped node read plainly', () => {
  expect(query('MATCH (mr:MergeRequest)--(x) RETURN x')?.detail).toBe('MergeRequest — any')
})

test('a reused variable keeps its label across MATCH clauses', () => {
  expect(
    query('MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) MATCH (u:User)-[:AUTHORED]->(mr) RETURN u')?.detail,
  ).toBe('MergeRequest → Project, User → MergeRequest')
})

test('the older JSON DSL falls back to entity names in order', () => {
  expect(entitiesOf('{"nodes":[{"entity":"Project"},{"entity":"MergeRequest"}]}')).toBe('Project → MergeRequest')
})

test('the fade runs from the light stop to the dark stop', () => {
  expect(fade(0)).toBe('#b5dedc')
  expect(fade(0.5)).toBe('#949dcb')
  expect(fade(1)).toBe('#6851ac')
})
