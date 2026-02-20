# GKG E2E Testing Runbook

Step-by-step guide to running the Knowledge Graph E2E test suite against a
local GDK + Tilt environment. Covers setup, test data, the redaction test, and
troubleshooting.

---

## Starting and stopping the environment

Use the bootstrap and teardown scripts to manage the full local stack:

```shell
# Start everything (Colima, GDK, Tilt, port-forwards)
cd ~/Desktop/Code/gkg
./scripts/bootstrap-dev.sh

# Stop everything (including Colima)
./scripts/teardown-dev.sh

# Stop everything except Colima (faster next bootstrap — skips VM boot)
./scripts/teardown-dev.sh --keep-colima
```

The bootstrap script is idempotent — safe to run if services are partially up.
It skips any step that is already running.

---

## Prerequisites

A working local environment as described in `local-setup-runbook.md`:

- GDK running (PostgreSQL, ClickHouse, NATS, Siphon)
- GKG server running (via Tilt **or** `cargo run`) and reachable at port 4200 / 50051
- Port-forwards active if using Tilt
- `gkg-development` ClickHouse database populated with graph tables and data
- GitLab on the feature branch with `:knowledge_graph` feature flag enabled
- No stale `target/debug/gkg-server` process on port 4200 (see setup runbook troubleshooting)

Verify before proceeding:

```shell
curl http://localhost:4200/health
# {"status":"ok","version":"0.1.0"}
```

---

## Test Suite Location

```
~/Desktop/Code/gkg/tests/e2e/
```

All tests run inside the GitLab Rails environment via `rails runner`:

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner <path/to/test.rb>
```

---

## 1. Confirm ClickHouse Has Data

Before running any tests, check that the key `gl_*` tables have rows:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
SELECT 'gl_merge_request' AS t, count() FROM \`gkg-development\`.gl_merge_request FINAL
UNION ALL SELECT 'gl_group',    count() FROM \`gkg-development\`.gl_group FINAL
UNION ALL SELECT 'gl_project',  count() FROM \`gkg-development\`.gl_project FINAL
UNION ALL SELECT 'gl_note',     count() FROM \`gkg-development\`.gl_note FINAL
UNION ALL SELECT 'gl_milestone',count() FROM \`gkg-development\`.gl_milestone FINAL
UNION ALL SELECT 'gl_work_item',count() FROM \`gkg-development\`.gl_work_item FINAL
UNION ALL SELECT 'gl_user',     count() FROM \`gkg-development\`.gl_user FINAL
UNION ALL SELECT 'gl_label',    count() FROM \`gkg-development\`.gl_label FINAL"
```

Expected counts for this GDK instance:

| Table | Expected |
|-------|----------|
| `gl_merge_request` | 188 |
| `gl_group` | 96 |
| `gl_project` | 4 |
| `gl_note` | 148 |
| `gl_milestone` | 83 |
| `gl_work_item` | 575 |
| `gl_user` | 73 |
| `gl_label` | 111 |

If any are 0, see `local-setup-runbook.md` → Siphon Troubleshooting.

---

## 2. Confirm Test Users and Memberships

The redaction test requires five specific users with specific group memberships.

### Check users exist

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner "
  [['root',1],['lois',70],['vickey.schmidt',71],['franklyn.mcdermott',72],['hanna',73]].each do |u,id|
    found = User.find_by(id: id)
    puts \"#{u} (id=#{id}): #{found ? 'OK' : 'MISSING'}\"
  end
" 2>/dev/null
```

### Check group memberships and JWT claims

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner "
Feature.enable(:knowledge_graph)
['lois','franklyn.mcdermott','vickey.schmidt','hanna'].each do |uname|
  u = User.find_by!(username: uname)
  ctx = Ai::KnowledgeGraph::AuthorizationContext.new(u)
  ids = ctx.reporter_plus_traversal_ids[:group_traversal_ids]
  puts \"#{uname}: #{ids.inspect}\"
end
" 2>/dev/null
```

Expected output:

```
lois: ["1/24/", "1/99/"]
franklyn.mcdermott: ["1/22/"]
vickey.schmidt: []
hanna: []
```

### If memberships are missing — set them up

The JWT `group_traversal_ids` claim is derived entirely from *group*
memberships at reporter level or above. Direct project memberships are **not**
included. Users who only have project memberships will have empty
`group_traversal_ids` and will see nothing.

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner "
Feature.enable(:knowledge_graph)

lois     = User.find_by!(username: 'lois')
franklyn = User.find_by!(username: 'franklyn.mcdermott')

# lois: reporter in group 24 (gitlab-org, contains projects 2 & 3)
#       developer in group 99 (kg-redaction-test-group, contains project 19)
Group.find(24).add_member(lois, Gitlab::Access::REPORTER)
Group.find(99).add_member(lois, Gitlab::Access::DEVELOPER)

# franklyn: reporter in group 22 (toolbox, contains project 1)
Group.find(22).add_member(franklyn, Gitlab::Access::REPORTER)

puts 'done'
" 2>/dev/null
```

Re-run the JWT check above to confirm.

### User reference table

| Username | ID | JWT group_traversal_ids | Sees |
|----------|----|------------------------|------|
| `root` | 1 | admin → `["1/"]` | everything |
| `lois` | 70 | `["1/24/", "1/99/"]` | projects 2, 3, 19 and their entities |
| `franklyn.mcdermott` | 72 | `["1/22/"]` | project 1 and its entities |
| `vickey.schmidt` | 71 | `[]` | nothing |
| `hanna` | 73 | `[]` | nothing |

---

## 3. Confirm the Private Test Group and Project Exist

The redaction test requires:

- Group `kg-redaction-test-group` (id=99, private, traversal `1/99/`)
- Project `kg-redaction-test-project` (id=19, private, in group 99, traversal `1/99/100/`)

Check:

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner "
puts Group.find_by(name: 'kg-redaction-test-group')&.inspect || 'MISSING'
puts Project.find_by(name: 'kg-redaction-test-project')&.inspect || 'MISSING'
" 2>/dev/null
```

If missing, create them:

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner \
  ~/Desktop/Code/gkg/tests/e2e/create_test_data.rb \
  2>/dev/null
```

After creating, the new group and project need to be indexed. Reset the
watermarks for the relevant entities and re-dispatch:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
ALTER TABLE \`gkg-development\`.namespace_indexing_watermark
DELETE WHERE entity IN ('Group', 'Project', 'WorkItem', 'Note', 'Milestone', 'MergeRequest')"

cd ~/Desktop/Code/gkg
GKG__JWT_SECRET=$(cat ~/Desktop/Code/gdk/gitlab/.gitlab_shell_secret) \
GKG__NATS__URL=localhost:4222 \
GKG__DATALAKE__URL=http://localhost:8123 \
GKG__DATALAKE__DATABASE=gitlab_clickhouse_development \
GKG__DATALAKE__USERNAME=default \
GKG__GRAPH__URL=http://localhost:8123 \
GKG__GRAPH__DATABASE=gkg-development \
GKG__GRAPH__USERNAME=default \
cargo run -q -p gkg-server -- --mode=dispatch-indexing
```

Then force deduplication before running tests:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
OPTIMIZE TABLE \`gkg-development\`.gl_group FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_project FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_merge_request FINAL"
```

---

## 4. Run the Redaction Test

The focused redaction test is the primary security validation for this GDK
instance. It verifies that every user sees exactly the entities their
`group_traversal_ids` JWT claim entitles them to — no more, no less.

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner \
  ~/Desktop/Code/gkg/tests/e2e/redaction_test.rb \
  2>/dev/null
```

Expected result: **38/38 passed**

### What the test covers

| Section | Tests |
|---------|-------|
| 1. Admin (root) sees everything | 4 |
| 2. lois scoped to `1/24/` + `1/99/` | 9 |
| 3. franklyn scoped to `1/22/` | 9 |
| 4. vickey & hanna — empty claims → zero results | 12 |
| 5. Cross-user isolation | 4 |

### Exact entity counts asserted

| User | Entity | Expected |
|------|--------|----------|
| lois | projects | 3 (ids 2, 3, 19) |
| lois | MRs | 13 |
| lois | notes | 96 |
| lois | work items | 82 |
| franklyn | projects | 1 (id 1) |
| franklyn | MRs | 8 |
| franklyn | notes | 52 |
| franklyn | work items | 38 |
| vickey | anything | 0 |
| hanna | anything | 0 |

---

## 5. Run the Mega Test (optional)

A broader test covering query types, filters, aggregations, traversals,
path-finding, ordering, and more. Less strict on exact counts.

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner \
  ~/Desktop/Code/gkg/tests/e2e/mega_test.rb \
  2>/dev/null
```

Expected: **~88/96** (some tests are loose lower-bounds that may vary with data).

Known failures in the mega test that are not bugs:

- **Groups ≥ 90** — server returns ~9 with limit:100 due to pagination behaviour
- **Milestones ≥ 80** — same cap (~27 returned)
- **MR count by state ≥ 2 states** — aggregation returns only `merged` (opened
  MRs have empty traversal_path and are excluded from the join)
- **MR/WorkItem count by project ≥ 5** — only 4 projects are indexed in
  `gl_project`; the aggregation JOIN returns at most 4 buckets

---

## Query DSL Reference

All tests call `Ai::KnowledgeGraph::GrpcClient#execute_query`. The `query_json`
argument is a Ruby Hash.

### Search

```ruby
{
  query_type: 'search',
  node: { id: 'p', entity: 'Project', columns: ['name', 'full_path'] },
  filters: { visibility_level: { op: 'eq', value: 20 } },  # integer, not string
  order_by: { node: 'p', property: 'name', direction: 'ASC' },
  limit: 20
}
```

### Traversal

```ruby
{
  query_type: 'traversal',
  nodes: [
    { id: 'u',  entity: 'User',         columns: ['username'], node_ids: [1] },
    { id: 'mr', entity: 'MergeRequest', columns: ['iid', 'state'] },
    { id: 'p',  entity: 'Project',      columns: ['name'] }
  ],
  relationships: [
    { type: 'AUTHORED',    from: 'u',  to: 'mr' },
    { type: 'IN_PROJECT',  from: 'mr', to: 'p' }
  ],
  limit: 20
}
```

### Aggregation

```ruby
# Group by a node:
{
  query_type: 'aggregation',
  nodes: [
    { id: 'p',  entity: 'Project',      columns: ['name'] },
    { id: 'mr', entity: 'MergeRequest', columns: ['iid'] }
  ],
  relationships: [{ type: 'IN_PROJECT', from: 'mr', to: 'p' }],
  aggregations: [{ function: 'count', target: 'mr', group_by: 'p', alias: 'mr_count' }],
  aggregation_sort: { agg_index: 0, direction: 'DESC' },
  limit: 10
}

# Group by a column on a single node:
{
  query_type: 'aggregation',
  nodes: [{ id: 'mr', entity: 'MergeRequest', columns: ['state'] }],
  aggregations: [{ function: 'count', target: 'mr', group_by_column: 'state', alias: 'cnt' }],
  limit: 10
}
```

### Neighbors

```ruby
{
  query_type: 'neighbors',
  node: { id: 'p', entity: 'Project', columns: ['name'], node_ids: [1] },
  neighbors: { node: 'p', direction: 'both' },   # incoming | outgoing | both
  limit: 50
}
# Optional: rel_types: ['AUTHORED', 'IN_PROJECT']  to restrict edge types
```

### Path Finding

```ruby
{
  query_type: 'path_finding',      # NOT 'path'
  nodes: [
    { id: 'u', entity: 'User',    columns: ['username'], node_ids: [1] },
    { id: 'p', entity: 'Project', columns: ['name'],     node_ids: [1] }
  ],
  path: { type: 'shortest', from: 'u', to: 'p', max_depth: 3 },
  limit: 10
}
# path.type options: shortest | any | all_shortest
```

### Filter operators

| Op | Meaning |
|----|---------|
| `eq` | Equals |
| `in` | In list |
| `gt` / `lt` | Greater / less than |
| `gte` / `lte` | Greater-or-equal / less-or-equal |
| `contains` | Substring match |
| `starts_with` | Prefix match |

Note: `like` is **not** a supported op — use `starts_with` or `contains`.

### Common gotchas

- **filters** is a Hash keyed by column name: `{ state: { op: 'eq', value: 'merged' } }` — NOT an array
- **visibility_level** is an integer: `20`=public, `10`=internal, `0`=private — NOT a string
- **columns `'*'`** is a string, not an array: `columns: '*'`
- **neighbors** requires `neighbors: { node: 'alias', direction: '...' }` at the top level
- **path_finding** query type is `'path_finding'`, not `'path'`
- **order_by** shape: `{ node: 'alias', property: 'column_name', direction: 'ASC' }`

### Relationship types

| Edge | Typical direction |
|------|-------------------|
| `AUTHORED` | User → MergeRequest / Note |
| `ASSIGNED` | User → MergeRequest |
| `REVIEWER` | MergeRequest → User |
| `MERGED_BY` | MergeRequest → User |
| `CLOSES` | MergeRequest → WorkItem |
| `IN_PROJECT` | MR / Pipeline / WorkItem / etc → Project |
| `IN_GROUP` | Project → Group |
| `HAS_NOTE` | MergeRequest / WorkItem → Note |
| `HAS_LABEL` | MergeRequest → Label |
| `IN_MILESTONE` | WorkItem → Milestone |
| `HAS_STAGE` | Pipeline → Stage |
| `HAS_JOB` | Stage → Job |
| `HAS_FINDING` | Vulnerability → Finding |

---

## Redaction Internals

The GKG server enforces access control entirely through **traversal path prefix
matching**, not through the Rails ability system. Understanding this is critical
for writing correct redaction tests.

### JWT claim: `group_traversal_ids`

Every request from GitLab Rails to the GKG server includes a signed JWT. For
non-admin users the JWT contains:

```json
{
  "user_id": 70,
  "admin": false,
  "group_traversal_ids": ["1/24/", "1/99/"],
  "organization_id": 1
}
```

`group_traversal_ids` is produced by
`Ai::KnowledgeGraph::AuthorizationContext#reporter_plus_traversal_ids` in Rails.
It contains the traversal paths of all groups where the user has reporter-level
access or above. The path format is `{org_id}/{group_traversal_ids_joined}/`.

**Key point:** only *group* memberships contribute to this list. Direct project
memberships (e.g. being added directly to a project, not via a group) are not
included. A user with only project memberships gets `group_traversal_ids: []`
and sees nothing from the GKG server.

### Server-side filtering

The GKG server's security stage (`query_pipeline/stages/security.rs`) converts
the JWT claim into a `SecurityContext`:

- Admin users get a single path `"{org_id}/"` covering the entire org
- Non-admin users get their `group_traversal_ids` directly
- If `group_traversal_ids` is empty, the security filter evaluates to `false`
  and all queries return zero rows

The query engine injects a `startsWith(traversal_path, path)` predicate on
every `gl_*` table scan. Entities whose `traversal_path` does not start with any
of the user's paths are excluded.

### Traversal path structure

Traversal paths encode the namespace hierarchy:

```
1/                          ← org 1 (admin sees everything with this prefix)
1/24/                       ← group 24 (gitlab-org) and everything under it
1/24/25/                    ← project 2 (gitlab-test), in group 24
1/99/                       ← group 99 (kg-redaction-test-group)
1/99/100/                   ← project 19 (kg-redaction-test-project), in group 99
```

A user with claim `["1/24/"]` sees all entities (MRs, notes, work items, etc.)
whose `traversal_path` starts with `1/24/` — including both `1/24/25/` (project
2) and `1/24/26/` (project 3).

### MergeRequests and empty traversal paths

`gl_merge_request` rows get their `traversal_path` from `hierarchy_merge_requests`,
which computes it via a Materialized View join against
`project_namespace_traversal_paths`. If a project is missing from that table at
the time the MR is first inserted, the MR gets `traversal_path = ''`.

Empty-path MRs are **invisible to all non-admin users** — the `startsWith`
filter never matches an empty string against a real path. They also cannot be
fixed by re-indexing alone; the `hierarchy_merge_requests` rows must be updated
directly. See `local-setup-runbook.md` → `gl_merge_request and hierarchy_merge_requests`.

### ReplacingMergeTree and stale duplicates

`gl_*` tables use `ReplacingMergeTree`. Part merges are lazy — until background
merges run, multiple versions of the same row can co-exist. The GKG server does
not use `FINAL` in its queries, so it may see these duplicates.

Symptom: a user sees 3-4× more rows than expected in non-admin queries.

Fix:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
OPTIMIZE TABLE \`gkg-development\`.gl_merge_request FINAL"
```

Run `OPTIMIZE TABLE ... FINAL` on any `gl_*` table where exact-count assertions
are failing.

---

## Troubleshooting

### All non-admin queries return 0

Check the user's JWT `group_traversal_ids`:

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner "
Feature.enable(:knowledge_graph)
u = User.find_by!(username: 'lois')
ctx = Ai::KnowledgeGraph::AuthorizationContext.new(u)
puts ctx.reporter_plus_traversal_ids.inspect
" 2>/dev/null
```

If it returns `{group_traversal_ids: []}`, the user has no reporter+ group
memberships. Add them to the appropriate group (see step 2 above).

### Non-admin user sees more rows than expected

Likely stale ReplacingMergeTree duplicates. Run:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
OPTIMIZE TABLE \`gkg-development\`.gl_merge_request FINAL"
```

### `compile_error: validation error: "like" is not valid`

The `like` filter op is not supported. Use `starts_with` or `contains` instead.

### `execution_error: Unknown table expression identifier 'gl_group'`

A stale local `gkg-server` binary is on port 4200. See `local-setup-runbook.md`.

### `compile_error: "query_type" is a required property`

The `query_json` hash is missing required fields. Minimum required:
`query_type`, `node`/`nodes`, and `limit`.

### `Unauthenticated: Invalid token`

The JWT TTL is 5 minutes. The test harness generates a fresh token per call;
this should not occur during normal test runs. If hitting it manually, generate
a new token with `Ai::KnowledgeGraph::JwtAuth.generate_token(user:, organization_id:)`.

### MR counts are wrong even with correct traversal paths

Check whether `project_namespace_traversal_paths` has an entry for the relevant
project:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
SELECT * FROM gitlab_clickhouse_development.project_namespace_traversal_paths
WHERE project_id = <id>"
```

If missing, add it and reseed `hierarchy_merge_requests` (see setup runbook).
