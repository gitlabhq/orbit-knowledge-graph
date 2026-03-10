# Local development setup

Run the GKG server natively on your host against GDK-managed services.
No Kubernetes, Docker, or Tilt required.

## Prerequisites

1. **[mise](https://mise.jdx.dev/)** for tool version management
2. **[GDK](https://gitlab.com/gitlab-org/gitlab-development-kit)** with ClickHouse enabled
3. **[grpcurl](https://github.com/fullstorydev/grpcurl)** (optional, for testing gRPC)

If your GDK root is not `~/gitlab/gdk`, set the `GDK_ROOT` environment variable before
running any GKG commands:

```shell
export GDK_ROOT=~/workspace/gdk  # adjust to your GDK root
```

This overrides the default paths in `mise.toml` and the `.env.local` template below.

> **Note:** `GDK_ROOT` support in `mise.toml` is tracked in
> [!475](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/475)
> and may not be merged yet. Until it is, set `GKG_GITLAB__JWT__VERIFYING_KEY`
> explicitly in your `.env.local` as shown in the [Build and run](#build-and-run) section.

## GDK services setup

### 1. Enable ClickHouse

```shell
gdk config set clickhouse.enabled true
gdk reconfigure
gdk start clickhouse
```

Create the GKG graph database:

```shell
clickhouse client --port 9001 -u default \
  --query "CREATE DATABASE IF NOT EXISTS gkg_development"
```

The `gitlab_clickhouse_development` datalake database is created by the Rails ClickHouse
migrations in step 3, but you can also create it now if you want to start
`siphon-clickhouse-consumer` without it crash-looping:

```shell
clickhouse client --port 9001 -u default \
  --query "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development"
```

### 2. Enable PostgreSQL logical replication

GDK uses `$GDK_ROOT/postgresql/data/gitlab.conf` for local overrides (included from
`postgresql.conf`). Add `wal_level` there rather than editing `postgresql.conf` directly:

```shell
echo "wal_level = logical" >> $GDK_ROOT/postgresql/data/gitlab.conf
```

If a `replication.conf` file exists in that directory, add the line there instead.

Restart PostgreSQL:

```shell
gdk restart postgresql
```

Verify:

```shell
gdk psql -d gitlabhq_development -c "SHOW wal_level"
# Expected: logical
```

### 3. Enable NATS and Siphon

```shell
gdk config set nats.enabled true
gdk config set siphon.enabled true
```

Add the full Siphon table list to `$GDK_ROOT/gdk.yml`. Table order matters;
it follows the tiered dependency order that ClickHouse materialized views expect:

```yaml
siphon:
  enabled: true
  tables:
    # Tier 1: Foundation (namespace_traversal_paths_mv, dictionaries)
    - organizations
    - namespaces
    - namespace_details
    - users
    # Tier 2: Projects (project_namespace_traversal_paths_mv)
    - projects
    - knowledge_graph_enabled_namespaces
    # Tier 3: Work item associations (hierarchy_work_items_mv JOINs)
    - issue_assignees
    - work_item_current_statuses
    - work_item_parent_links
    # Tier 4: MR associations (hierarchy_merge_requests_mv JOINs)
    - merge_request_assignees
    - approvals
    - merge_request_metrics
    # Tier 5: Shared associations (both hierarchy MVs)
    - label_links
    - labels
    # Tier 6: Entity tables (trigger hierarchy MVs)
    - issues
    - merge_requests
    - group_audit_events
    # Tier 7: Other entity tables
    - milestones
    - members
    - environments
    - deployments
    # Tier 8: Detail tables
    - notes
    - award_emoji
    - issue_links
    - merge_request_diffs
    - merge_request_diff_files
    - merge_requests_closing_issues
    - deployment_merge_requests
    - project_authorizations
    # Tier 9: Misc
    - bulk_import_entities
    - events
  ci_tables:
    - p_ci_pipelines
    - p_ci_stages
    - p_ci_builds
```

Reconfigure and start:

```shell
gdk reconfigure
gdk start nats siphon-producer-main-db siphon-clickhouse-consumer
```

If you have CI database decomposition enabled, also start:

```shell
gdk start siphon-producer-ci-db
```

`siphon-clickhouse-consumer` will crash-loop at this point because the
`gitlab_clickhouse_development` database does not exist yet. This is expected —
it will recover automatically after the next step.

Before running Rails ClickHouse migrations, create the required database:

```shell
clickhouse client --port 9001 -u default \
  --query "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development"
```

If the `click_house.yml` config does not exist, create it from the example:

```shell
cp $GDK_ROOT/gitlab/config/click_house.yml.example \
   $GDK_ROOT/gitlab/config/click_house.yml
```

Run ClickHouse migrations in GDK to create `siphon_*` tables:

```shell
cd $GDK_ROOT/gitlab && bundle exec rake gitlab:clickhouse:migrate
```

See: [GDK Siphon guide](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/doc/howto/siphon.md)

### 4. Verify GDK services

```shell
gdk status | grep -E "(clickhouse|nats|siphon|postgresql)"
```

All should show `up`. Verify data is flowing:

```shell
clickhouse client --port 9001 \
  --query "SELECT count() FROM gitlab_clickhouse_development.siphon_projects FINAL"
```

### 5. Enable the feature flag and generate the JWT secret

In a Rails console (`gdk rails c`):

```ruby
Feature.enable(:knowledge_graph)
```

The Rails gRPC client defaults to `localhost:50054`, so no config changes are needed.
If `gdk reconfigure` wipes your `gitlab.yml` edits, the client falls back to
the `KNOWLEDGE_GRAPH_GRPC_ENDPOINT` env var (default: `localhost:50054`).

The JWT secret used by the GKG server (`$GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret`)
is not generated by enabling the feature flag. Generate it explicitly via Rails runner:

```shell
gdk rails runner \
  "Analytics::KnowledgeGraph::JwtAuth.ensure_secret!"
```

This creates the file only if it doesn't already exist.

## Start the GKG server

### Install Rust toolchain

```shell
cd /path/to/knowledge-graph
mise install
```

### Build and run

The `server:start` mise task handles the JWT secret automatically:

```shell
mise run server:start
```

This reads the JWT signing key from `$GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret`
(falling back to `.gitlab_shell_secret`) and starts the server with the default config
(`config/default.yaml`). The secret must exist first — see step 5 above.

Override the mise task to use the correct secret explicitly:

```shell
GKG_GITLAB__JWT__VERIFYING_KEY=$(cat ${GDK_ROOT:-~/gitlab/gdk}/gitlab/.gitlab_knowledge_graph_secret) \
GKG_DATALAKE__DATABASE=gitlab_clickhouse_development \
GKG_GRAPH__DATABASE=gkg_development \
cargo run -p gkg-server
```

The defaults bind to `127.0.0.1:4200` (HTTP) and `127.0.0.1:50054` (gRPC),
and connect to ClickHouse at `127.0.0.1:8123` with `database: default`.

For GDK, override the database names:

```shell
GKG_DATALAKE__DATABASE=gitlab_clickhouse_development \
GKG_GRAPH__DATABASE=gkg_development \
mise run server:start
```

Or set them in a `.env.local` file (git-ignored):

```shell
cat > .env.local << EOF
GDK_ROOT=~/gitlab/gdk
GKG_DATALAKE__DATABASE=gitlab_clickhouse_development
GKG_GRAPH__DATABASE=gkg_development
GKG_GITLAB__BASE_URL=http://127.0.0.1:3000
GKG_GITLAB__JWT__VERIFYING_KEY=$(cat ${GDK_ROOT:-~/gitlab/gdk}/gitlab/.gitlab_knowledge_graph_secret)
EOF
```

Adjust `GDK_ROOT` to your GDK installation path.

Then source it before starting:

```shell
set -a && source .env.local && set +a
mise run server:start
```

### Check it works

```shell
# HTTP health check
curl http://127.0.0.1:4200/health

# Cluster health (HTTP)
curl -s http://127.0.0.1:4200/api/v1/cluster_health | python3 -m json.tool
```

### Test gRPC with JWT auth

Requires `pyjwt` (`pip install pyjwt`). Generate a test JWT using the knowledge graph secret:

```shell
SECRET=$(cat ${GDK_ROOT:-~/gitlab/gdk}/gitlab/.gitlab_knowledge_graph_secret)

JWT=$(python3 -c "
import jwt, time, base64
secret = '${SECRET}'
try:
    decoded_secret = base64.b64decode(secret)
except:
    decoded_secret = secret.encode()
payload = {
    'iss': 'gitlab',
    'aud': 'gitlab-knowledge-graph',
    'sub': 'user',
    'iat': int(time.time()),
    'exp': int(time.time()) + 300,
    'user_id': 1,
    'username': 'root',
    'admin': True,
    'group_traversal_ids': []
}
print(jwt.encode(payload, decoded_secret, algorithm='HS256'))
")
```

Test the gRPC endpoints:

```shell
PROTO=crates/gkg-server/proto

# List available tools
grpcurl -plaintext \
  -import-path $PROTO -proto gkg.proto \
  -H "authorization: Bearer $JWT" \
  127.0.0.1:50054 gkg.v1.KnowledgeGraphService/ListTools

# Get graph schema
grpcurl -plaintext \
  -import-path $PROTO -proto gkg.proto \
  -H "authorization: Bearer $JWT" \
  127.0.0.1:50054 gkg.v1.KnowledgeGraphService/GetGraphSchema

# Get cluster health
grpcurl -plaintext \
  -import-path $PROTO -proto gkg.proto \
  -H "authorization: Bearer $JWT" \
  127.0.0.1:50054 gkg.v1.KnowledgeGraphService/GetClusterHealth
```

## Apply the GKG graph schema

Before running the indexer, apply the GKG graph schema to the `gkg_development` database.
This creates the `gl_*` tables and supporting structures:

```shell
clickhouse client --port 9001 -u default \
  --database gkg_development \
  --multiquery < fixtures/schema/graph.sql
```

Verify the tables were created:

```shell
clickhouse client --port 9001 --database gkg_development --query "SHOW TABLES"
```

## Run the indexer

The indexer runs in two steps. On the **first run**, start the indexer first to create the
required NATS stream (`GKG_INDEXER`), then run dispatch-indexing, then run the indexer again
to process the queued jobs. On subsequent runs, dispatch and then index in order.

**Step 1 — create the NATS stream** (first time only):

```shell
GKG_DATALAKE__DATABASE=gitlab_clickhouse_development \
GKG_GRAPH__DATABASE=gkg_development \
cargo run -p gkg-server -- --mode indexer
```

Wait for the log line `indexer started` then stop with Ctrl-C.

**Step 2 — dispatch indexing jobs:**

```shell
GKG_DATALAKE__DATABASE=gitlab_clickhouse_development \
GKG_GRAPH__DATABASE=gkg_development \
cargo run -p gkg-server -- --mode dispatch-indexing
```

**Step 3 — run the indexer worker** to process the queued jobs:

```shell
GKG_DATALAKE__DATABASE=gitlab_clickhouse_development \
GKG_GRAPH__DATABASE=gkg_development \
cargo run -p gkg-server -- --mode indexer
```

The indexer is long-running; stop it with Ctrl-C once you see the pipelines complete in
the logs (for example `pipeline completed pipeline=User`). On subsequent runs you can skip
Step 1 and go straight to dispatch then index.

## Architecture

```plaintext
GDK Host (localhost)
┌──────────────────────────────────────────────┐
│ PostgreSQL :5432                             │
│   ↓ logical replication                      │
│ siphon-producer (GDK service)                │
│   ↓                                          │
│ NATS :4222 (GDK service, JetStream)          │
│   ↓                                          │
│ siphon-consumer (GDK service)                │
│   ↓                                          │
│ ClickHouse :8123/:9001                       │
│   ├─ gitlab_clickhouse_development (datalake)│
│   └─ gkg_development (graph)                 │
│                                              │
│ gkg-server (cargo run)                       │
│   ├─ HTTP  127.0.0.1:4200                    │
│   ├─ gRPC  127.0.0.1:50054                   │
│   └─ health 0.0.0.0:4201                     │
│                                              │
│ Gitaly (GDK, Unix socket or :8075)           │
└──────────────────────────────────────────────┘
```

## Environment variables

The GKG server reads `config/default.yaml` for defaults, then overrides
with environment variables using the `GKG_` prefix and `__` as the
nesting separator.

| Variable | Default | Description |
|----------|---------|-------------|
| `GKG_DATALAKE__DATABASE` | `default` | ClickHouse datalake database |
| `GKG_GRAPH__DATABASE` | `default` | ClickHouse graph database |
| `GKG_DATALAKE__URL` | `http://127.0.0.1:8123` | ClickHouse HTTP URL |
| `GKG_GRAPH__URL` | `http://127.0.0.1:8123` | ClickHouse HTTP URL |
| `GKG_NATS__URL` | `localhost:4222` | NATS server URL |
| `GKG_GITLAB__BASE_URL` | (none) | GitLab instance URL |
| `GKG_GITLAB__JWT__VERIFYING_KEY` | (none) | JWT secret (from `.gitlab_knowledge_graph_secret`) |
| `GKG_BIND_ADDRESS` | `127.0.0.1:4200` | HTTP bind address |
| `GKG_GRPC_BIND_ADDRESS` | `127.0.0.1:50054` | gRPC bind address |

## Troubleshooting

**Port already in use:**

Check for stale processes (old caproni/kubectl port-forwards):

```shell
lsof -i :50054 -sTCP:LISTEN
lsof -i :4200 -sTCP:LISTEN
```

Kill any stale processes, or if you had a Colima cluster running:

```shell
colima stop <profile-name>
```

**NATS connection refused:**

```shell
gdk status nats
nc -zv localhost 4222
```

If NATS shows as down, start it: `gdk start nats`

**ClickHouse connection issues:**

```shell
gdk status clickhouse
curl "http://localhost:8123/ping"
```

**No data in siphon tables:**

```shell
gdk status siphon-producer-main-db siphon-clickhouse-consumer
gdk tail siphon-producer-main-db
```

Verify the publication exists in PostgreSQL:

```shell
gdk psql -d gitlabhq_development -c "SELECT * FROM pg_publication"
```

**JWT authentication failures:**

The GKG server base64-decodes the secret before using it (matching Rails behavior).
Ensure you're using the raw contents of `$GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret`
as the `GKG_GITLAB__JWT__VERIFYING_KEY` value. The JWT must include:

- `iss`: `"gitlab"`
- `aud`: `"gitlab-knowledge-graph"`
- Algorithm: `HS256`

**Siphon "limit_markers" error:**

GDK ships NATS >= 2.12 which supports this. If you see this error, update GDK:

```shell
cd $GDK_ROOT && gdk update
```
