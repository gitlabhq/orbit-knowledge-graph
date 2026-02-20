# Local GKG E2E Setup Runbook

Step-by-step commands to get a full local GKG development environment running
for manual E2E testing, based on an actual setup session.

## Prerequisites

- GDK installed at `~/Desktop/Code/gdk`
- `knowledge-graph` repo cloned at `~/Desktop/Code/gkg`
- Homebrew, mise installed

---

## 1. Start Colima with Kubernetes

```shell
colima start --kubernetes
```

Verify:

```shell
kubectl cluster-info
kubectl get nodes
```

---

## 2. Install tools via mise

```shell
cd ~/Desktop/Code/gkg
mise install
```

Installs Tilt (v0.36.1) and other tools defined in `mise.toml`.

---

## 3. Checkout correct branches

**GKG repo** — use `main`:

```shell
cd ~/Desktop/Code/gkg
git checkout main && git pull
```

**GitLab (GDK)** — use the feature branch:

```shell
cd ~/Desktop/Code/gdk/gitlab
git checkout gkg-feature-branch-working-copy && git pull
```

---

## 4. Enable GDK services

Edit `~/Desktop/Code/gdk/gdk.yml` to add:

```yaml
clickhouse:
  bin: "/opt/homebrew/bin/clickhouse"
  enabled: true
nats:
  enabled: true
siphon:
  enabled: true
  tables:
    - namespaces
    - projects
    - issues
    - merge_requests
    - users
    - members
    - labels
    - milestones
    - notes
```

Then reconfigure:

```shell
cd ~/Desktop/Code/gdk
gdk reconfigure
```

---

## 5. Enable PostgreSQL logical replication

Edit `~/Desktop/Code/gdk/postgresql/data/postgresql.conf`:

```
wal_level = logical
```

Restart PostgreSQL:

```shell
gdk restart postgresql
```

Verify:

```shell
psql -h ~/Desktop/Code/gdk/postgresql -p 5432 -d gitlabhq_development -c "SHOW wal_level;"
# should return: logical
```

---

## 6. Start GDK services

```shell
gdk start clickhouse nats siphon-producer-main-db siphon-clickhouse-consumer
```

---

## 7. Create ClickHouse databases

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query \
  "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development;
   CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_test;
   CREATE DATABASE IF NOT EXISTS \`gkg-development\`;"
```

---

## 8. Configure Rails ClickHouse connection

```shell
cp ~/Desktop/Code/gdk/gitlab/config/click_house.yml.example \
   ~/Desktop/Code/gdk/gitlab/config/click_house.yml
```

---

## 9. Run ClickHouse migrations

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rake gitlab:clickhouse:migrate
```

Run this again after switching to the feature branch (step 3) to pick up any
branch-specific migrations:

```shell
bundle exec rake gitlab:clickhouse:migrate
```

---

## 10. Expose Gitaly on network

Edit `~/Desktop/Code/gdk/gitaly/gitaly-0.praefect.toml` (not `gitaly.config.toml`):

```toml
listen_addr = "0.0.0.0:8075"
```

Restart Gitaly:

```shell
gdk restart gitaly
```

Verify:

```shell
nc -zv localhost 8075
# should print: Connection to localhost port 8075 succeeded
```

---

## 11. Configure .tilt-secrets

```shell
cp ~/Desktop/Code/gkg/.tilt-secrets.example ~/Desktop/Code/gkg/.tilt-secrets
```

Edit `.tilt-secrets` and set `GKG_JWT_SECRET` to the GDK shell secret:

```shell
cat ~/Desktop/Code/gdk/gitlab/.gitlab_shell_secret
```

Final `.tilt-secrets`:

```
POSTGRES_PASSWORD=
CLICKHOUSE_PASSWORD=
GKG_JWT_SECRET=<value from above>
```

---

## 12. Install Helm

```shell
brew install helm
```

---

## 13. Enable knowledge_graph in gitlab.yml

Edit `~/Desktop/Code/gdk/gitlab/config/gitlab.yml` and add under the
`development:` section (near the `elasticsearch:` block):

```yaml
knowledge_graph:
  enabled: true
  base_url: http://localhost:4200
  grpc_endpoint: localhost:50051
```

---

## 14. Enable the knowledge_graph feature flag

```shell
cd ~/Desktop/Code/gdk
echo "Feature.enable(:knowledge_graph)" | gdk rails console
```

---

## 15. Run pending Rails migrations

```shell
cd ~/Desktop/Code/gdk/gitlab
bundle exec rails db:migrate
```

---

## 16. Fix JS dependencies

```shell
cd ~/Desktop/Code/gdk/gitlab
yarn install
yarn add d3-time@3 d3-scale@4
```

---

## 17. Run GDK restart

```shell
cd ~/Desktop/Code/gdk
gdk restart
```

---

## 18. Apply GKG graph schema to ClickHouse

The `gkg-development` database needs the graph tables (`gl_group`, `gl_project`, etc.)
created from the fixture schema:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --database "gkg-development" \
  < ~/Desktop/Code/gkg/fixtures/schema/graph.sql
```

Verify:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --query "SHOW TABLES FROM \`gkg-development\`;"
```

---

## 19. Start the GKG server via Tilt

```shell
cd ~/Desktop/Code/gkg
mise exec -- tilt up
```

Tilt builds the Docker image, deploys to Colima/k8s, and manages the
webserver, indexer, and health-check pods. The Tilt UI is at
http://localhost:10350.

Wait for all pods to show green, then start port-forwards (see below).

> **Do not run `cargo run` or `target/debug/gkg-server` locally while Tilt
> is active.** A local binary will grab port 4200 before the port-forward
> can, and all traffic will silently go to the local process instead of the
> k8s pod. The local binary has no env vars set and will query the `default`
> ClickHouse database, producing `Unknown table expression identifier
> 'gl_group'` errors. See the troubleshooting section below.

To run locally instead of via Tilt (e.g. for rapid iteration):

```shell
cd ~/Desktop/Code/gkg
GKG__JWT_SECRET=$(cat ~/Desktop/Code/gdk/gitlab/.gitlab_shell_secret) \
  GKG__DATALAKE__DATABASE=gitlab_clickhouse_development \
  GKG__GRAPH__DATABASE=gkg-development \
  GKG__GITALY__ADDRESS=tcp://localhost:8075 \
  GKG__GITALY__STORAGE=default \
  cargo run -p gkg-server -- --mode=webserver
```

But **not both at the same time**.

Verify the server is up:

```shell
curl http://localhost:4200/health
# {"status":"ok","version":"0.1.0"}
```

---

## Known Issues Fixed During Setup

### `ProcessFdQuotaExceeded` during cross-compilation

When `cargo-zigbuild` cross-compiles for `aarch64-unknown-linux-gnu` on macOS,
the linker opens ~250 `.rlib` files simultaneously and can hit the process fd
limit. Fixed by adding `ulimit -n 65536` to `scripts/build-dev.sh`.

### `No CA certificates were loaded from the system`

The `ubi-micro` base image in `build-dev.sh` has no CA certificates, causing
`reqwest` to panic on startup. Fixed by switching the dev image base to
`ubi10/ubi-minimal` which includes `ca-certificates`.

### Kubernetes service env var injection / `try_parsing` config bug

See `docs/dev/tilt-k8s-service-links-bug.md` for full details. Fixed by:
1. Adding `enableServiceLinks: false` to all GKG pod specs
2. Removing `try_parsing(true)` from the `config` crate env source
3. Switching to `prefix_separator("__")` so only `GKG__*` vars are picked up
4. Adding a custom `deserialize_services` to handle comma-separated env vars

### `execution_error: Unknown table expression identifier 'gl_group'`

ClickHouse can't find `gl_group` because the query ran against the `default`
database instead of `gkg-development`.

**Most likely cause:** a stale `target/debug/gkg-server` process is running
locally and grabbed port 4200 before the `kubectl port-forward`. All traffic
goes to the local binary, which has no `GKG__GRAPH__DATABASE` env var and
defaults to the `default` database.

Check:

```shell
ps aux | grep "gkg-server\|target/debug" | grep -v grep
```

If you see a local binary, kill it:

```shell
kill -9 <pid>
```

Then restart the port-forwards:

```shell
kubectl port-forward svc/gkg-webserver 4200:8080 -n default &
kubectl port-forward svc/gkg-webserver 50051:50051 -n default &
```

Verify the response comes from the k8s pod (version field present):

```shell
curl http://localhost:4200/health
# {"status":"ok","version":"0.1.0"}
```

To confirm the pod is using the right database, check its startup log:

```shell
kubectl logs deployment/gkg-webserver -n default | grep "parsed ClickHouse config"
# graph_database should be "gkg-development"
```

---

## Port Forwarding (Tilt mode)

When running via Tilt, the GKG webserver runs inside Kubernetes and is not
directly accessible on the host. You need to port-forward to expose it to
GitLab Rails.

Run these in a terminal that stays open (they must remain running):

```shell
# HTTP — used by GitLab Rails (base_url in gitlab.yml)
kubectl port-forward svc/gkg-webserver 4200:8080

# gRPC — used by GitLab Rails (grpc_endpoint in gitlab.yml)
kubectl port-forward svc/gkg-webserver 50051:50051
```

Verify:

```shell
curl http://localhost:4200/health
# {"status":"ok","version":"0.1.0"}
```

`gitlab.yml` should point to:

```yaml
knowledge_graph:
  enabled: true
  base_url: http://localhost:4200
  grpc_endpoint: localhost:50051
```

---

## Siphon Troubleshooting

Siphon replicates Postgres tables into ClickHouse. Several `gl_*` graph tables
depend on Siphon data being present. Empty Siphon tables produce silent failures
(the indexer processes 0 rows and sets the watermark, so the problem never
resurfaces on its own).

### Diagnosing empty Siphon tables

Check which Siphon tables are missing rows:

```sql
-- Run against ClickHouse (port 9001)
SELECT 'siphon_namespaces'       AS tbl, count() FROM gitlab_clickhouse_development.siphon_namespaces
UNION ALL SELECT 'siphon_namespace_details', count() FROM gitlab_clickhouse_development.siphon_namespace_details
UNION ALL SELECT 'siphon_notes',             count() FROM gitlab_clickhouse_development.siphon_notes
UNION ALL SELECT 'siphon_milestones',        count() FROM gitlab_clickhouse_development.siphon_milestones
UNION ALL SELECT 'siphon_projects',          count() FROM gitlab_clickhouse_development.siphon_projects
UNION ALL SELECT 'siphon_members',           count() FROM gitlab_clickhouse_development.siphon_members
UNION ALL SELECT 'siphon_merge_requests',    count() FROM gitlab_clickhouse_development.siphon_merge_requests
UNION ALL SELECT 'siphon_labels',            count() FROM gitlab_clickhouse_development.siphon_labels
UNION ALL SELECT 'siphon_issues',            count() FROM gitlab_clickhouse_development.siphon_issues
UNION ALL SELECT 'siphon_users',             count() FROM gitlab_clickhouse_development.siphon_users
UNION ALL SELECT 'siphon_knowledge_graph_enabled_namespaces', count() FROM gitlab_clickhouse_development.siphon_knowledge_graph_enabled_namespaces
FORMAT PrettyCompact
```

Cross-reference with the Postgres counts:

```sql
-- Run against Postgres
SELECT 'namespaces'        AS tbl, count(*) FROM namespaces
UNION ALL SELECT 'namespace_details', count(*) FROM namespace_details
UNION ALL SELECT 'notes',             count(*) FROM notes
UNION ALL SELECT 'milestones',        count(*) FROM milestones
UNION ALL SELECT 'projects',          count(*) FROM projects
UNION ALL SELECT 'members',           count(*) FROM members
UNION ALL SELECT 'merge_requests',    count(*) FROM merge_requests
UNION ALL SELECT 'labels',            count(*) FROM labels
UNION ALL SELECT 'issues',            count(*) FROM issues
UNION ALL SELECT 'users',             count(*) FROM users;
```

### Why tables are empty

Three root causes:

1. **Table not in Siphon config** — `siphon/config_main.yml` (producer) and/or
   `siphon/consumer.yml` (consumer) don't mention the table.
2. **Table not in the PostgreSQL publication** — Siphon will never receive
   change events for it.
3. **Initial snapshot was skipped** — Siphon tracks which tables it has already
   snapshotted in its internal state. If the table was added to the config after
   Siphon completed its first snapshot pass, it won't be auto-backfilled.

Check what tables are in the publication:

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -c "SELECT tablename FROM pg_publication_tables WHERE pubname = 'siphon_publication_main_db' ORDER BY tablename"
```

Check that the Siphon consumer is subscribed to the right subjects (look for
`"jetstream consumer ready"` lines):

```shell
tail -100 ~/Desktop/Code/gdk/log/siphon-clickhouse-consumer/current
```

### Adding a missing table to Siphon

1. Add to producer config (`siphon/config_main.yml`):

```yaml
table_mapping:
  # ... existing entries ...
  - table: <table_name>
    schema: public
    subject: <table_name>
```

2. Add to consumer config (`siphon/consumer.yml`):

```yaml
streams:
  # ... existing entries ...
  - identifier: <table_name>
    subject: <table_name>
    target: siphon_<table_name>
```

3. Add to the PostgreSQL publication:

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -c "ALTER PUBLICATION siphon_publication_main_db ADD TABLE <table_name>"
```

4. Restart Siphon:

```shell
gdk restart siphon-producer-main-db siphon-clickhouse-consumer
```

5. Backfill existing rows (see below — Siphon won't re-snapshot).

### Backfilling a Siphon table manually

Siphon won't backfill tables that were already past the initial snapshot phase.
Use this pattern to copy data directly from Postgres into ClickHouse.

The key requirement is timestamp formatting: Postgres outputs timestamps with a
timezone offset (`2026-02-20 01:30:04.728514+00`) but ClickHouse expects bare
UTC (`2026-02-20 01:30:04.728514`). Use `to_char(...AT TIME ZONE 'UTC', ...)`.

General pattern:

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -t -A -F $'\t' -c "
SELECT
  col1,
  to_char(timestamp_col AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),
  COALESCE(nullable_col, '\N')
FROM <table_name>
" | /opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --query "INSERT INTO gitlab_clickhouse_development.siphon_<table_name>
    (col1, timestamp_col, nullable_col)
    FORMAT TabSeparated"
```

**Known backfills required for this GDK instance:**

`siphon_namespace_details` (needed for `gl_group` ETL — INNER JOIN):

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -t -A -F $'\t' -c "
SELECT
  namespace_id,
  to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),
  to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),
  COALESCE(cached_markdown_version::text, '\N'),
  COALESCE(description, '\N'),
  COALESCE(description_html, '\N'),
  COALESCE(creator_id::text, '\N'),
  COALESCE(to_char(deleted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'),
  state_metadata::text
FROM namespace_details
" | /opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --query "INSERT INTO gitlab_clickhouse_development.siphon_namespace_details
    (namespace_id, created_at, updated_at, cached_markdown_version,
     description, description_html, creator_id, deleted_at, state_metadata)
    FORMAT TabSeparated"
```

`siphon_notes` (needed for `gl_note`):

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -t -A -F $'\t' -c "
SELECT
  id,
  COALESCE(note, '\N'), COALESCE(noteable_type, '\N'),
  COALESCE(author_id::text, '\N'),
  COALESCE(to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'),
  COALESCE(to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'),
  COALESCE(project_id::text, '\N'), COALESCE(line_code, '\N'),
  COALESCE(commit_id, '\N'), COALESCE(noteable_id::text, '\N'),
  CASE WHEN system THEN '1' ELSE '0' END,
  COALESCE(to_char(resolved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'),
  COALESCE(resolved_by_id::text, '\N'), COALESCE(discussion_id, '\N'),
  COALESCE(confidential::text, '\N'),
  CASE WHEN internal THEN '1' ELSE '0' END,
  COALESCE(namespace_id::text, '\N')
FROM notes
" | /opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --query "INSERT INTO gitlab_clickhouse_development.siphon_notes
    (id, note, noteable_type, author_id, created_at, updated_at,
     project_id, line_code, commit_id, noteable_id, system, resolved_at,
     resolved_by_id, discussion_id, confidential, internal, namespace_id)
    FORMAT TabSeparated"
```

`siphon_milestones` (needed for `gl_milestone`):

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -t -A -F $'\t' -c "
SELECT
  id, COALESCE(title, ''),
  COALESCE(project_id::text, '\N'), COALESCE(description, '\N'),
  COALESCE(due_date::text, '\N'),
  COALESCE(to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'),
  COALESCE(to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\N'),
  COALESCE(state, '\N'), COALESCE(iid::text, '\N'),
  COALESCE(group_id::text, '\N'), COALESCE(lock_version::text, '0')
FROM milestones
" | /opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --query "INSERT INTO gitlab_clickhouse_development.siphon_milestones
    (id, title, project_id, description, due_date, created_at, updated_at,
     state, iid, group_id, lock_version)
    FORMAT TabSeparated"
```

### Watermark management

The GKG indexer tracks progress per (namespace, entity) pair in
`gkg-development.namespace_indexing_watermark`. After a backfill, if the
`_siphon_replicated_at` of the new rows is **older than** the existing
watermark, the indexer will skip them.

Check current watermarks:

```sql
SELECT entity, count() AS namespaces, max(watermark) AS latest
FROM `gkg-development`.namespace_indexing_watermark
GROUP BY entity
ORDER BY entity;
```

To force re-indexing for a specific entity after a backfill, delete its
watermark rows and re-run the dispatcher:

```shell
# Delete watermark for a specific entity (e.g. Group)
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
ALTER TABLE \`gkg-development\`.namespace_indexing_watermark
DELETE WHERE entity = 'Group'"

# Re-run dispatcher
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

To reset **all** entities (full re-index from scratch):

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
TRUNCATE TABLE \`gkg-development\`.namespace_indexing_watermark"
```

### `traversal_path` and group indexing

The `gl_group` ETL query requires all three of:

- `siphon_namespaces` — source group data
- `siphon_namespace_details` — joined for description (INNER JOIN — if empty,
  zero rows are produced)
- `namespace_traversal_paths` — maps namespace IDs to `{org_id}/{ns_id}/` paths
  (populated by the datalake-generator; check with
  `SELECT count() FROM gitlab_clickhouse_development.namespace_traversal_paths`)

The `traversal_path` columns in `siphon_notes` and `siphon_milestones` are
**computed** via ClickHouse dictionaries (`project_traversal_paths_dict`,
`namespace_traversal_paths_dict`) at INSERT time. Notes/milestones without a
matching project or namespace end up with `traversal_path = '0/'` and are
excluded from graph results.

### `gl_merge_request` and `hierarchy_merge_requests`

`gl_merge_request` is indexed from `hierarchy_merge_requests`, a
ReplacingMergeTree table backed by a Materialized View (MV) over
`siphon_merge_requests`. The MV computes `traversal_path` by joining against
`project_namespace_traversal_paths` at INSERT time.

**Two tables are involved:**

| Table | Location | Purpose |
|-------|----------|---------|
| `namespace_traversal_paths` | `gitlab_clickhouse_development` | Group namespace IDs → `{org_id}/{ns_id}/` paths. ~100 entries. |
| `project_namespace_traversal_paths` | `gitlab_clickhouse_development` | Project IDs → full traversal paths including the project's own namespace. Must have one row per project. |

**Traversal path format for projects:**

A project's traversal path is `1/{namespace_traversal_ids_joined}/` where the
IDs come from the project's *Project-type namespace* (not the group namespace or
the project ID itself). For example:

- Project 1 (`gitlab-smoke-tests`), in group namespace 22, has a Project
  namespace id 23 with `traversal_ids = [22, 23]` → path `1/22/23/`
- Project 2 (`gitlab-test`), in group namespace 24, has a Project namespace id
  25 with `traversal_ids = [24, 25]` → path `1/24/25/`

To get correct paths for all projects from Postgres:

```sql
SELECT p.id, '1/' || array_to_string(pns.traversal_ids, '/') || '/'
FROM projects p
JOIN namespaces pns
  ON pns.type = 'Project'
  AND pns.path = p.path
  AND pns.parent_id = p.namespace_id
ORDER BY p.id;
```

**If `project_namespace_traversal_paths` is incomplete**, newly-inserted MRs
get `traversal_path = ''` (the MV join produces NULL, stored as empty string).
They are then invisible to all non-admin users and do not appear in per-project
traversal queries.

Populate all projects:

```shell
psql -h ~/Desktop/Code/gdk/postgresql -d gitlabhq_development \
  -t -A -F $'\t' -c "
SELECT p.id, '1/' || array_to_string(pns.traversal_ids, '/') || '/'
FROM projects p
JOIN namespaces pns
  ON pns.type = 'Project'
  AND pns.path = p.path
  AND pns.parent_id = p.namespace_id
ORDER BY p.id
" | /opt/homebrew/bin/clickhouse client --port 9001 -u default \
  --query "INSERT INTO gitlab_clickhouse_development.project_namespace_traversal_paths
    (project_id, traversal_path) FORMAT TabSeparated"
```

After populating the table, existing MR rows with empty `traversal_path` must
be fixed by inserting corrected rows directly into `hierarchy_merge_requests`
and deleting the old empty-path rows. See the seeding pattern below.

**Seeding `hierarchy_merge_requests` with correct traversal paths:**

The safest approach is: insert corrected rows, then delete the stale ones.

```shell
# 1. Insert corrected rows (traversal_path computed from project_namespace_traversal_paths)
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
INSERT INTO gitlab_clickhouse_development.hierarchy_merge_requests
SELECT
  mr.id, mr.iid, mr.title, mr.state, mr.source_branch, mr.target_branch,
  mr.author_id, mr.assignee_id, mr.project_id, mr.created_at, mr.updated_at,
  mr.merged_at, mr.closed_at, mr.draft, mr.merge_status,
  mr.source_project_id, mr.target_project_id, mr.merge_user_id,
  COALESCE(tp.traversal_path, '') AS traversal_path,
  mr._siphon_replicated_at
FROM gitlab_clickhouse_development.siphon_merge_requests mr
LEFT JOIN gitlab_clickhouse_development.project_namespace_traversal_paths tp
  ON tp.project_id = mr.target_project_id
WHERE mr.traversal_path = '' OR mr.traversal_path IS NULL"

# 2. Delete the stale empty-traversal_path rows
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
ALTER TABLE gitlab_clickhouse_development.hierarchy_merge_requests
DELETE WHERE traversal_path = ''"
```

Then reset the MergeRequest watermark and re-dispatch:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
ALTER TABLE \`gkg-development\`.namespace_indexing_watermark
DELETE WHERE entity = 'MergeRequest'"
```

### ReplacingMergeTree deduplication (`OPTIMIZE TABLE`)

`gl_merge_request` (and other `gl_*` tables) use `ReplacingMergeTree`. Parts
are merged lazily in the background. The GKG server queries without `FINAL`,
which means it can see duplicate rows if parts have not yet been merged.

Symptoms: a user sees 3-4× more rows than expected; the excess rows disappear
after waiting or after forcing a merge.

Force immediate deduplication:

```shell
/opt/homebrew/bin/clickhouse client --port 9001 -u default --query "
OPTIMIZE TABLE \`gkg-development\`.gl_merge_request FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_group FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_project FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_note FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_milestone FINAL;
OPTIMIZE TABLE \`gkg-development\`.gl_work_item FINAL;"
```

Run this after any bulk insert into ClickHouse (backfills, watermark resets,
etc.) before running E2E tests that assert exact counts.

---

## Access Points

| Service       | URL                          |
|---------------|------------------------------|
| GitLab        | http://127.0.0.1:3000        |
| Orbit UI      | http://127.0.0.1:3000/dashboard/orbit |
| GKG Server    | http://localhost:4200        |
| GKG gRPC      | localhost:50051               |
| ClickHouse    | http://localhost:8123        |
| NATS          | localhost:4222               |
| Tilt UI       | http://localhost:10350 (when tilt is running) |
| Grafana       | http://localhost:30300       |
