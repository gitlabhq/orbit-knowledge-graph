# Local development setup

Run GKG components in Kubernetes while using NATS, Siphon, PostgreSQL, and ClickHouse from your GDK installation.

## Prerequisites

1. **[mise](https://mise.jdx.dev/)** for tool version management

2. **[Tilt](https://tilt.dev/)** for local Kubernetes development

3. **Kubernetes cluster** (one of):
   - Colima with k8s: `colima start --kubernetes`
   - Docker Desktop with Kubernetes enabled
   - minikube, kind, or rancher-desktop

4. **GDK with required services enabled:**

   Add the following to `$GDK_ROOT/gdk.yml`:

   ```yaml
   clickhouse:
     enabled: true
   nats:
     enabled: true
   siphon:
     enabled: true
   postgresql:
     host: localhost
   ```

   Setting `postgresql.host: localhost` makes PostgreSQL listen on TCP so that
   GKG services running in Kubernetes can connect via `host.docker.internal`.

5. **PostgreSQL logical replication and TCP access:**

   Edit `$GDK_ROOT/postgresql/data/postgresql.conf` (and `replication.conf` if it exists):

   ```plaintext
   listen_addresses = 'localhost'
   wal_level = logical
   ```

   Then restart PostgreSQL: `gdk restart postgresql`

6. **ClickHouse setup:**

   Create the Rails ClickHouse config from the example and run migrations:

   ```shell
   cp $GDK_ROOT/gitlab/config/click_house.yml.example $GDK_ROOT/gitlab/config/click_house.yml
   cd $GDK_ROOT/gitlab && bundle exec rake gitlab:clickhouse:migrate
   ```

   Then create the GKG graph database and apply the schema:

   ```shell
   clickhouse client --host localhost --port 9001 --query "CREATE DATABASE IF NOT EXISTS \`gkg-development\`"
   ```

   Apply the graph schema (each statement separately since ClickHouse
   doesn't support multi-statement execution):

   ```shell
   sed 's/--.*$//' config/graph.sql | tr '\n' ' ' | sed 's/;/;\n/g' | \
     while IFS= read -r stmt; do
       [ -n "$stmt" ] && clickhouse client --host localhost --port 9001 \
         --database gkg-development --query "$stmt"
     done
   ```

7. **Configure Siphon tables:**

   Run `gdk reconfigure` to generate the initial Siphon configs, then replace
   them with the correct format. The current Siphon binary expects a
   `producers:`/`consumers:` array structure, but GDK generates an older flat
   format.

   Create `$GDK_ROOT/siphon/config_main.yml`:

   ```yaml
   producers:
     - application_identifier: "gdkproducer_main"
       max_column_size_in_bytes: 1048576
       partitions_monitoring_interval_in_seconds: 30
       database:
         host: "localhost"
         port: 5432
         database: "gitlabhq_development"
         advisory_lock_id: 1
         advisory_lock_timeout_ms: 100
         advisory_lock_timeout_fuzziness_ms: 50
         lock_timeout_ms: 500
         lock_timeout_fuzziness_ms: 300
         application_name: "siphon_main"
       replication:
         publication_name: "siphon_publication_main_db"
         slot_name: "siphon_slot_main_db"
         initial_data_snapshot_threads_per_table: 3
         memory_buffer_size_in_bytes: 8388608
       queueing:
         driver: "nats"
         url: "localhost:4222"
         stream_name: "siphon_stream"
         temp_stream_name: "siphon_temp_stream_main"
         snapshot_stream_name: "siphon_snapshot_stream_main"
       table_mapping:
         - table: namespaces
           schema: public
           subject: namespaces
         - table: projects
           schema: public
           subject: projects
         # Add more tables as needed (issues, merge_requests, users, etc.)
   prometheus:
     port: 8081
   ```

   Create `$GDK_ROOT/siphon/consumer.yml`:

   ```yaml
   consumers:
     - type: "clickhouse"
       application_identifier: "gdkconsumer"
       queueing:
         driver: "nats"
         url: "localhost:4222"
         stream_name: "siphon_stream"
       streams:
         - identifier: namespaces
           subject: namespaces
           target: siphon_namespaces
         - identifier: projects
           subject: projects
           target: siphon_projects
         # Add matching entries for each table in the producer
       clickhouse:
         host: localhost
         port: 9001
         user: default
         database: gitlab_clickhouse_development
   prometheus:
     port: 8084
   ```

   The consumer also needs a wrapper script since GDK expects a separate binary.
   Create `$GDK_ROOT/siphon/bin/clickhouse_consumer`:

   ```shell
   #!/bin/sh
   exec "$(dirname "$0")/siphon" consumer "$@"
   ```

   ```shell
   chmod +x $GDK_ROOT/siphon/bin/clickhouse_consumer
   ```

   Protect these files from being overwritten by `gdk reconfigure` by adding
   to `$GDK_ROOT/gdk.yml`:

   ```yaml
   gdk:
     protected_config_files:
       - siphon/config_main.yml
       - siphon/consumer.yml
   ```

   Then restart siphon: `gdk restart siphon-producer-main-db siphon-clickhouse-consumer`

   See the [staging Siphon config](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-helmfiles/-/blob/master/releases/siphon/orbit-stg.yaml.gotmpl)
   for the full list of tables used in production.

8. **Enable Knowledge Graph and JWT auth:**

   Add the `knowledge_graph` section to `$GDK_ROOT/gitlab/config/gitlab.yml`
   under the `production:` / `development:` block (e.g. near the `elasticsearch:` section):

   ```yaml
     knowledge_graph:
       enabled: true
   ```

   Protect `gitlab.yml` from being overwritten by adding it to
   `gdk.protected_config_files` in `$GDK_ROOT/gdk.yml`:

   ```yaml
   gdk:
     protected_config_files:
       - gitlab/config/gitlab.yml
   ```

   Restart Rails to auto-generate the JWT secret file:

   ```shell
   gdk restart rails-web rails-background-jobs
   ```

   This creates `$GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret` which the
   Tiltfile reads automatically to configure the GKG webserver's JWT
   verifying key.

   Finally, enable the feature flags:

   ```shell
   cd $GDK_ROOT/gitlab
   bundle exec rails runner "Feature.enable(:knowledge_graph); Feature.enable(:knowledge_graph_infra)"
   ```

## Setup

1. **Install dependencies:**

   ```shell
   mise install
   ```

2. **Sync vendored Helm charts:**

   ```shell
   helm/sync.sh
   ```

   This fetches the official GKG chart via vendir and applies local patches.
   Requires `vendir` and `yq` (installed by `mise install`).

3. **Configure secrets:**

   ```shell
   cp .tilt-secrets.example .tilt-secrets
   ```

   Edit `.tilt-secrets` and set:
   - `GDK_ROOT`: Absolute path to your GDK installation
   - `CLICKHOUSE_PASSWORD`: Usually empty for local development

   The JWT secret is read automatically from `$GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret`
   (generated in prerequisite 8).

4. **Start local environment:**

   ```shell
   tilt up
   ```

## Quick start/stop script

Once prerequisites are installed, you can use `scripts/gkg-dev.sh` to manage
the full stack (K8s cluster, GDK, and Tilt) with a single command:

```shell
# Copy the config template and set your GDK path
cp .gkg-dev.conf.example .gkg-dev.conf
# Edit .gkg-dev.conf — at minimum, set GDK_ROOT if your GDK is not at ~/gdk

# Verify everything is installed and configured correctly
scripts/gkg-dev.sh check

# Start all services (K8s → GDK → Tilt)
scripts/gkg-dev.sh start

# Check what's running
scripts/gkg-dev.sh status

# Stop all services (Tilt → GDK → K8s)
scripts/gkg-dev.sh stop
```

## Alternative: quick start with mise

If you prefer using the repository's existing `mise` task runner, an additive
shortcut is also available:

```shell
mise run dev
```

This alternative is separate from the Tilt/Kubernetes workflow above. It starts
lightweight native Rust processes directly on your host and connects them to the
existing services in your GDK instance (for example NATS, ClickHouse, GitLab,
and Gitaly), without using Tilt, Helm, Colima, or minikube.

It starts all three GKG runtime modes in the foreground:

- 1 webserver (HTTP + gRPC)
- 1 indexer
- 1 dispatcher (dispatch-indexing)

`mise run dev` orchestrates these long-running processes directly via mise
tasks, so you get mise's built-in prefixed output and Ctrl+C stops
everything.

Useful companion tasks:

```shell
mise run dev:check    # validate prerequisites
mise run dev:setup    # create graph DB + apply schema
mise run dev:status   # show derived config
mise run dev:env      # print env vars
```

`mise run gdk` is also available as an alias for the same GDK-connected local
development workflow.

Port assignments and GDK connection settings can be overridden in a gitignored
`.env` file. The only required input is `GDK_ROOT` (or `GDK_DIR` as an alias),
and the script derives GDK service ports from `gdk.yml` automatically. Start from the checked-in template
if you want to override only the GKG-local listen ports:

```shell
cp .env.example .env
```

For example, you can change the webserver and indexer ports if you want to run
multiple isolated local clusters on the same machine. You do not need to copy
GDK connection details into `.env`; those are parsed from `gdk.yml`.

Prerequisites:

- A working GDK with `nats`, `clickhouse`, and `siphon` enabled in `gdk.yml`
- PostgreSQL `wal_level = logical` (required for Siphon CDC)
- `mise` shell activation so that `cargo`, `ruby`, and `clickhouse` are on `PATH`
- Run `mise run dev:check` to validate all prerequisites

Typical usage:

```shell
export GDK_ROOT=~/workspace/gdk
mise run dev
```

On the first run, `cargo` compiles the full workspace which takes several
minutes. Subsequent runs use the cached build and start in seconds.

`mise run dev:setup` creates the graph database (default `gkg-development`) and
applies `config/graph.sql` to the configured ClickHouse instance.

This lightweight path assumes NATS, ClickHouse, Siphon, PostgreSQL, and Gitaly
come from GDK.

See `.gkg-dev.conf.example` for all configuration options (K8s runtime,
resource allocation, Tilt streaming mode).

## Access Services

- **Tilt UI**: http://localhost:10350
- **GKG Webserver**: http://localhost:8080
- **Grafana**: http://localhost:3030 (login: admin/admin). Dashboards: GKG Overview, ETL Engine, SDLC Indexing, Query Pipeline.
- **Prometheus**: http://localhost:9090

## Architecture

```plaintext
GDK Host (localhost)                    Kubernetes Cluster
┌─────────────────────────┐            ┌─────────────────────────┐
│ PostgreSQL :5432        │            │                         │
│   ↓                     │            │ gkg-scheduler (cron)    │
│ siphon-producer         │            │   ↓ publishes indexing  │
│   ↓                     │            │   ↓ requests to NATS    │
│ NATS :4222 ─────────────┼────────────┼── gkg-indexer           │
│   ↓                     │            │   ↓ reads from CH       │
│ siphon-consumer         │            │   ↓ writes graph        │
│   ↓                     │            │                         │
│ ClickHouse :8123 ───────┼────────────┼── gkg-webserver         │
└─────────────────────────┘            └─────────────────────────┘
```

## Troubleshooting

**NATS connection refused:**

- Verify GDK NATS is running: `gdk status nats`
- Check if NATS port is accessible: `nc -zv localhost 4222`

**NATS limit_markers error:**

- Update `NATS_VERSION` in `$GDK_ROOT/support/makefiles/Makefile.nats.mk` to a version >= 2.11 (example `2.11.12`)
- Run `gkg-stop && rm -rf nats/nats-server`
- Run `make nats-setup && nats/nats-server -version`
- Run `gkg start`

**ClickHouse connection issues:**

- Verify ClickHouse is running: `gdk status clickhouse`
- Check HTTP port: `curl "http://localhost:8123/ping"`

**No data in graph:**

- Check siphon services: `gdk status siphon-producer-main-db siphon-clickhouse-consumer`
- Verify `siphon_*` tables have data: `clickhouse-client --port 9001 -q "SELECT count() FROM siphon_projects"`
- Check indexer logs: `kubectl logs -l app.kubernetes.io/name=gkg-indexer`

**host.docker.internal not resolving:**

- On Linux, add `--add-host=host.docker.internal:host-gateway` to Docker
- Or use your host's actual IP address in `helm/values/gkg-local.yaml`
