import { expect, test } from 'claude-code/testing'

import { COMMAND_LABELS, describeCommand, describeOrbitCall, othersText } from '../core/calls.ts'
import { entitiesOf, projectsOf } from '../core/cypher.ts'

const query = (cypher: string, known?: ReadonlySet<string>) =>
  describeOrbitCall('Bash', { command: `glab orbit query "${cypher}"` }, known)

test('every subcommand has a short finished and running label', () => {
  for (const [sub, { done: label, running: active }] of Object.entries(COMMAND_LABELS)) {
    expect(label.split(' ').length <= 3).toBe(true)
    expect(active.split(' ').length <= 3).toBe(true)
    expect(describeOrbitCall('Bash', { command: `glab orbit ${sub}` })?.label).toBe(label)
  }
})

test('unknown, removed, and internal subcommands are not Orbit rows', () => {
  for (const command of [
    'glab orbit',
    'glab orbit --help',
    'glab orbit frobnicate',
    'glab orbit dsl',
    'glab orbit tools',
    'glab orbit repo-map overview',
    'glab orbit hook-guard',
    'glab orbit mcp serve',
    'glab orbit workspace',
    'glab orbit constructor',
  ]) {
    expect(describeOrbitCall('Bash', { command })).toBe(null)
  }
})

test('the standalone orbit binary gets the same labels', () => {
  expect(describeOrbitCall('Bash', { command: "orbit grep 'auth|login'" })).toMatchObject({
    label: 'Searched code',
    detail: 'auth|login',
  })
  expect(describeOrbitCall('Bash', { command: 'cd repo && orbit context src/main.rs:1-40' })?.label).toBe('Read code')
})

test('the remote schema and the local graph commands are told apart', () => {
  const label = (command: string) => describeOrbitCall('Bash', { command })?.label
  expect(label('glab orbit schema MergeRequest')).toBe('Read Orbit schema')
  expect(label('glab orbit ontology')).toBe('Read Orbit schema')
  expect(label('glab orbit workspace schema')).toBe('Read local schema')
  expect(label("glab orbit workspace sql 'SELECT 1'")).toBe('Queried local graph')
  expect(label('glab orbit workspace index .')).toBe('Indexed repo')
  expect(describeOrbitCall('Bash', { command: 'glab orbit schema MergeRequest' })?.detail).toBe('MergeRequest')
})

test('flags before the subcommand and orbit inside an argument are handled', () => {
  expect(describeOrbitCall('Bash', { command: 'glab orbit --yes grep foo' })?.label).toBe('Searched code')
  expect(describeOrbitCall('Bash', { command: 'git commit -m "fix: glab orbit query output"' })).toBe(null)
  expect(describeOrbitCall('Bash', { command: "echo 'glab orbit grep x'" })).toBe(null)
})

test('a grep pattern given with -e is the detail', () => {
  expect(describeOrbitCall('Bash', { command: 'glab orbit grep -e foo src/' })?.detail).toBe('foo')
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
  expect(query('MATCH (p:Project)<-[:IN_PROJECT]-(mr:MergeRequest) RETURN mr')?.detail).toBe('Merge requests → Projects')
  expect(query('MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)-[:IN_PROJECT]->(p:Project) RETURN u')?.detail).toBe(
    'Users → Merge requests → Projects',
  )
})

test('repeated node types collapse into one', () => {
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition {project_id: 1}) RETURN c')?.detail).toBe('Definitions')
  expect(query('MATCH (d:Definition {project_id: 1})<-[:CALLS]-(c:Definition) RETURN c')?.detail).toBe('Definitions')
  expect(query('MATCH (child:Definition)-[:EXTENDS*1..3]->(parent:Definition) RETURN child')?.detail).toBe('Definitions')
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition)<-[:DEFINES]-(f:File) RETURN f')?.detail).toBe(
    'Files → Definitions',
  )
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition)-[:IN_FILE]->(f:File) RETURN f')?.detail).toBe(
    'Definitions → Files',
  )
})

test('arrows both ways split into separate paths, and repeats show once', () => {
  expect(query('MATCH (a:User)-[:AUTHORED]->(mr:MergeRequest)<-[:REVIEWED]-(r:User) RETURN mr')?.detail).toBe(
    'Users → Merge requests',
  )
})

test('an undirected hop and an untyped node read plainly', () => {
  expect(query('MATCH (mr:MergeRequest)--(x) RETURN x')?.detail).toBe('Merge requests — Anything')
})

test('a reused variable keeps its label across MATCH clauses', () => {
  expect(
    query('MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) MATCH (u:User)-[:AUTHORED]->(mr) RETURN u')?.detail,
  ).toBe('Merge requests → Projects, Users → Merge requests')
})

test('the older JSON DSL falls back to entity names in order', () => {
  expect(entitiesOf('{"nodes":[{"entity":"Project"},{"entity":"MergeRequest"}]}')).toBe('Projects → Merge requests')
})

test('a path starts from the node the query pins and reads outward', () => {
  const mrs = 'Project → Merge requests'
  expect(query("MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'g/p'}) RETURN mr.iid")?.detail).toBe(mrs)
  expect(query("MATCH (p:Project {full_path: 'g/p'})<-[:IN_PROJECT]-(mr:MergeRequest) RETURN mr.iid")?.detail).toBe(mrs)
  expect(query("MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project) WHERE p.full_path = 'g/p' RETURN mr.iid")?.detail).toBe(mrs)
  expect(query("MATCH (u:User {username: 'a'})-[:AUTHORED]->(mr:MergeRequest {id: 5}) RETURN u")?.detail).toBe('Merge request → User')
})

test('with no single pinned node, a path reads source to target', () => {
  expect(query("MATCH (u:User {username: 'a'})-[:AUTHORED]->(mr:MergeRequest {state: 'merged'}) RETURN mr")?.detail).toBe(
    'User → Merge requests',
  )
})

test('an entity id filter puts that entity first, once graph-status has listed it', () => {
  const fk = 'MATCH (mr:MergeRequest {project_id: 1}) RETURN mr.iid'
  expect(query(fk)?.detail).toBe('Merge requests')
  const known = new Set(['Project', 'MergeRequest', 'MergeRequestDiff', 'User', 'Definition'])
  expect(query(fk, known)?.detail).toBe('Project → Merge requests')
  expect(query('MATCH (c:Definition)-[:CALLS]->(d:Definition {project_id: 1}) RETURN d.name', known)?.detail).toBe('Project → Definitions')
  expect(query('MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {project_id: 1})-[:HAS_DIFF]->(d:MergeRequestDiff) RETURN d', known)?.detail).toBe(
    'Project → Merge requests → Merge request diffs, Project → Merge requests → Users',
  )
  expect(query('MATCH (mr:MergeRequest {pipeline_id: 1}) RETURN mr', known)?.detail).toBe('Merge requests')
})

test('a node pinned to one item is singular and everything else plural, in plain words', () => {
  const known = new Set(['Project', 'Vulnerability'])
  expect(query('MATCH (j:Job)-[:IN_PIPELINE]->(pl:Pipeline {id: 9}) RETURN j.name')?.detail).toBe('Pipeline → Jobs')
  expect(query('MATCH (mr:MergeRequest {id: 5})<-[:AUTHORED]-(u:User) RETURN u')?.detail).toBe('Merge request → Users')
  expect(query("MATCH (mr:MergeRequest {state: 'merged'}) RETURN mr")?.detail).toBe('Merge requests')
  expect(query('MATCH (mr:MergeRequest) WHERE mr.id IN [1, 2] RETURN mr')?.detail).toBe('Merge requests')
  expect(query('MATCH (mr:MergeRequest) WHERE mr.id = 7 RETURN mr')?.detail).toBe('Merge request')
  expect(query('MATCH (v:Vulnerability {project_id: 1}) RETURN v', known)?.detail).toBe('Project → Vulnerabilities')
  expect(query('MATCH (f:MergeRequestDiffFile {project_id: 1}) RETURN f')?.detail).toBe('Merge request diff files')
  expect(query('MATCH (d:Directory {project_id: 1}) RETURN d')?.detail).toBe('Directories')
  expect(query('MATCH (b:Branch {project_id: 1}) RETURN b')?.detail).toBe('Branches')
})

test('the projects a query asks about come from project_id, Project ids, and full paths', () => {
  const keys = (query: string) => projectsOf(query).map((scope) => scope.key)
  expect(keys('MATCH (mr:MergeRequest {project_id: 7}) RETURN mr')).toEqual(['project-id:7'])
  expect(keys('MATCH (mr:MergeRequest) WHERE mr.project_id IN [7, 8] RETURN mr')).toEqual(['project-id:7', 'project-id:8'])
  expect(keys("MATCH (p:Project {full_path: 'a/b'})<-[:IN_PROJECT]-(mr:MergeRequest) RETURN mr")).toEqual(['full-path:a/b'])
  expect(keys("MATCH (p:Project) WHERE p.id = 3 AND p.full_path IN ['c/d'] RETURN p")).toEqual(['project-id:3', 'full-path:c/d'])
  expect(keys("MATCH (u:User {username: 'x'})-[:AUTHORED]->(mr:MergeRequest) RETURN mr")).toEqual([])
})

test('schema reads chained in one command share a row with every node named', () => {
  const call = describeOrbitCall('Bash', { command: `glab orbit query 'CALL db.schema()'; glab orbit query "CALL db.schema('Vulnerability')"; glab orbit query "CALL db.schema('Pipeline')"` })
  expect(call).toMatchObject({ label: 'Read Orbit schema', detail: 'Vulnerability, Pipeline' })
})

test('a help call is not an Orbit row', () => {
  expect(describeOrbitCall('Bash', { command: 'glab orbit query --help | head -60' })).toBe(null)
})

test('a long list of chained details is clipped', () => {
  const command = Array.from({ length: 30 }, (_, i) => `glab orbit grep term${i}`).join('; ')
  expect(describeOrbitCall('Bash', { command })?.detail.length).toBeLessThanOrEqual(96)
})

test('an Orbit call that runs other commands names them', () => {
  expect(describeOrbitCall('Bash', { command: 'glab orbit status; rm -rf build' })).toMatchObject({ label: 'Checked Orbit', others: ['rm'] })
  expect(describeOrbitCall('Bash', { command: 'orbit grep login && ls' })).toMatchObject({ label: 'Searched code', detail: 'login', others: ['ls'] })
  expect(describeOrbitCall('Bash', { command: 'glab orbit status' })?.others).toBeUndefined()
  expect(describeOrbitCall('mcp__gitlab-orbit__list_commands', {})?.others).toBeUndefined()
})

const rowOf = (command: string) => {
  const call = describeCommand(command)
  return call ? call.label + othersText(call) : null
}

test('every shape that runs something besides the Orbit call names it or shows a marker', () => {
  const shapes: [string, string][] = [
    ['glab orbit status <<EOF; prog', '+ prog'],
    ['glab orbit status <<< x; prog', '+ prog'],
    ['glab orbit status; ((x=1<<2)); prog', '+ prog'],
    ['glab orbit status; echo $((1<<2)); prog', ', prog'],
    ['glab orbit query <<EOF\n$(prog)\nEOF', '+ $(...)'],
    ["glab orbit status # it's done\nprog", '+ prog'],
    ["glab orbit status $'\\'' ; prog", '+ prog'],
    ["glab orbit query <<EOF\nit's\nEOF\nprog", '+ prog'],
    ['glab orbit status; </dev/null prog', '+ prog'],
    ['glab orbit status; >/tmp/x prog', '+ prog, > /tmp/x'],
    ['glab orbit status; ./true', '+ ./true'],
    ['glab orbit status; x/cd', '+ x/cd'],
    ['glab orbit status | ./grep x', '+ ./grep'],
    ['glab orbit status | ./x/jq .', '+ ./x/jq'],
    ['glab orbit status | head file', '+ head'],
    ['glab orbit status | grep -r pattern dir', '+ grep'],
    ['glab orbit status | jq . file', '+ jq'],
    ['glab orbit status | grep x < file', '+ grep'],
    ['glab orbit status | grep -epat file', '+ grep'],
    ['glab orbit status | grep --regexp=pat file', '+ grep'],
    ['glab orbit status | grep -e pat file', '+ grep'],
    ['glab orbit status | jq "include \\"f\\"; ." ', '+ jq'],
    ['glab orbit status; if ((x<<2)); then prog; fi', '+ prog'],
    ['glab orbit status; glab orbit workspace reset --all', '+ orbit workspace reset'],
    ['glab orbit status && orbit config set foo bar', '+ orbit config set'],
    ['glab orbit status; glab orbit reset', '+ orbit reset'],
    ["glab orbit status; echo \"unclosed", '+ unparsed'],
    ['glab orbit query <<EOF\nno terminator', '+ unparsed'],
  ]
  for (const [command, expected] of shapes) expect(rowOf(command)).toContain(expected)
})

test('quiet shell words and filters with only a pattern or count stay out of the list', () => {
  for (const command of [
    'glab orbit status | head -n 5',
    'glab orbit status | grep -c ok',
    'glab orbit status | jq .',
    'glab orbit status | grep -epat',
    'glab orbit status | grep -e pat',
    'glab orbit status; if ((x<<2)); then true; fi',
    'glab orbit status; cd repo; true',
    "glab orbit query <<'EOF'\n$(not run)\nEOF",
  ]) {
    expect(rowOf(command)).not.toContain('+')
  }
})

test('the list shows programs before redirects, clips each name, and counts what it drops', () => {
  const programs = Array.from({ length: 9 }, (_, i) => `p${i}`).join('; ')
  expect(rowOf(`glab orbit status > out.txt; ${programs}`)).toBe('Checked Orbit  + p0, p1, p2, p3, p4, p5, … +4 more')
  expect(rowOf(`glab orbit status; ${'a'.repeat(60)}`)).toBe(`Checked Orbit  + ${'a'.repeat(39)}…`)
  expect(rowOf("glab orbit status; 'ls, pwd' x")).toBe("Checked Orbit  + 'ls, pwd'")
  expect(rowOf('glab orbit status; "it\'s, x"')).toBe("Checked Orbit  + 'it\\'s, x'")
  expect(rowOf("glab orbit status; '> out.txt' x")).toBe("Checked Orbit  + '> out.txt'")
})

test('control characters are dropped from the names in the list', () => {
  expect(rowOf('glab orbit status; \u001b[8mprog\u009b')).toBe('Checked Orbit  + [8mprog')
})

test('bidirectional and zero-width characters are dropped from the names in the list', () => {
  expect(rowOf('glab orbit status; a\u202eb\u200bc\u2066d')).toBe('Checked Orbit  + abcd')
})
