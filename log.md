# Work Log — chore/e2e-harness-1

Temporary log of the E2E harness setup session. Safe to delete once the work
is merged and the findings are captured in the runbooks.

---

## What was built

### `tests/redaction_test.rb` (in the external specs repo)

38-test focused redaction suite. Verifies that the GKG server scopes results
correctly to each user's `group_traversal_ids` JWT claim. Five sections:

1. Admin (root) sees everything
2. `lois` — scoped to `1/24/` + `1/99/` (projects 2, 3, 19)
3. `franklyn` — scoped to `1/22/` (project 1 only)
4. `vickey` + `hanna` — empty claims, zero results everywhere
5. Cross-user isolation — each user cannot traverse into the other's projects

Result: **38/38 passing**, survives a `main` merge.

### `scripts/bootstrap-dev.sh`

One-shot script to bring up the full local environment from a cold stop:

1. Starts Colima with Kubernetes (skips if already running)
2. Starts GDK (`gdk start`), waits for PostgreSQL + ClickHouse
3. Kills any stale local `gkg-server` binary that would shadow the k8s pod on
   port 4200
4. Launches Tilt in the background, waits for `gkg-webserver` pod ready
5. Starts `kubectl port-forward` for HTTP (:4200) + gRPC (:50051), waits for
   health endpoint
6. Confirms the pod is using `graph_database=gkg-development`

Flags: `--no-tilt`, `--no-gdk`. Logs/pids go to `/tmp` (not the repo).

### `scripts/teardown-dev.sh`

Reverse teardown: port-forwards → Tilt → GDK. Colima is left running by
default; pass `--stop-colima` for a full shutdown.

### `docs/dev/local-setup-runbook.md` — additions

- `hierarchy_merge_requests` and `project_namespace_traversal_paths`: explains
  the Materialized View chain, correct traversal path format for projects, SQL
  to derive paths from Postgres, pattern to reseed the table and reset watermarks
- ReplacingMergeTree deduplication: `OPTIMIZE TABLE ... FINAL` one-liner for
  all `gl_*` tables — needed after bulk inserts to avoid stale duplicate rows
  inflating non-admin query counts

### `docs/dev/e2e-testing-runbook.md` — full rewrite

Replaced stale content (wrong user IDs, old Rails redaction service model) with:

- How `group_traversal_ids` works (JWT claim, source, what it includes/excludes)
- Step-by-step setup: verify data, check/fix user memberships, confirm test group
- `redaction_test.rb` as the primary test with expected count table
- Updated Query DSL reference with correct shapes and a `like`-not-supported note
- Troubleshooting for all issues encountered during the session

---

## Key findings

### JWT `group_traversal_ids` — group memberships only

`Ai::KnowledgeGraph::AuthorizationContext#reporter_plus_traversal_ids` only
considers *group* memberships at reporter level or above. Direct project
memberships are invisible to it. A user added directly to a project (not via a
group) gets `group_traversal_ids: []` and sees nothing from the GKG server.

Fix for this GDK instance:
- `franklyn` (maintainer on project 1) → added as reporter to group 22 (toolbox)
  → JWT claim: `["1/22/"]`
- `lois` (developer on projects 2 & 3) → added as reporter to group 24
  (gitlab-org) → JWT claims: `["1/24/", "1/99/"]`

### ReplacingMergeTree deduplication

The GKG server queries `gl_*` tables without `FINAL`. Until ClickHouse merges
parts in the background, duplicate rows are visible, inflating counts. After
any bulk insert or watermark reset, run:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
OPTIMIZE TABLE \`gkg-development\`.gl_merge_request FINAL"
```

### `project_namespace_traversal_paths` must be complete

The `hierarchy_merge_requests` Materialized View joins against this table at
INSERT time. Missing project entries produce `traversal_path = ''`, making
those MRs invisible to all non-admin users. The table initially only had 4 of
19 projects; all 19 were populated using:

```sql
SELECT p.id, '1/' || array_to_string(pns.traversal_ids, '/') || '/'
FROM projects p
JOIN namespaces pns
  ON pns.type = 'Project'
  AND pns.path = p.path
  AND pns.parent_id = p.namespace_id
```

### `like` filter op is not supported

Using `{ op: 'like', value: '1/22/%' }` in a filter produces a compile error.
Use `starts_with` or `contains` instead, or restructure to use traversal queries.

### Config file change (from main merge)

The `bohdanpk/helm_config_files` merge switched non-secret config from env vars
to a ConfigMap mounted at `/app/config/default.yaml` in the webserver pod.
Passwords remain as env vars. No impact on test results — all 38 tests pass
after the merge.

---

## GDK data state (this instance)

| Table | Count | Notes |
|-------|-------|-------|
| `gl_merge_request` | 188 | 94 have empty traversal_path (projects not in `project_namespace_traversal_paths`) |
| `gl_group` | 96 | includes group 99 (kg-redaction-test-group) |
| `gl_project` | 4 | ids 1, 2, 3, 19 |
| `gl_note` | 148 | |
| `gl_milestone` | 83 | |
| `gl_work_item` | 575 | |
| `gl_user` | 73 | |
| `gl_label` | 111 | |

### Test users

| Username | ID | Group memberships (reporter+) | JWT `group_traversal_ids` |
|----------|----|-------------------------------|---------------------------|
| `root` | 1 | admin | `["1/"]` |
| `lois` | 70 | group 24 (gitlab-org), group 99 (kg-redaction-test-group) | `["1/24/", "1/99/"]` |
| `franklyn.mcdermott` | 72 | group 22 (toolbox) | `["1/22/"]` |
| `vickey.schmidt` | 71 | none | `[]` |
| `hanna` | 73 | none | `[]` |

### Private test data

| Resource | ID | Traversal path |
|----------|----|----------------|
| Group `kg-redaction-test-group` | 99 | `1/99/` |
| Project `kg-redaction-test-project` | 19 | `1/99/100/` |

---

## Running the tests

```shell
# Start everything (from cold)
cd ~/Desktop/Code/gkg
./scripts/bootstrap-dev.sh

# Run redaction tests
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails runner \
  ~/Desktop/Code/gkg/tests/e2e/redaction_test.rb

# Tear down
cd ~/Desktop/Code/gkg
./scripts/teardown-dev.sh
```
