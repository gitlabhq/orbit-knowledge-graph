# Local development setup

Run GKG as native Rust processes connected to NATS, Siphon, PostgreSQL, and
ClickHouse from your GDK installation.

> Working on `orbit-local`, the ontology, language parsers, or docs only?
> You don't need GDK or any of the services below. See the
> [Orbit Local development quickstart](orbit-local-quickstart.md).

## Prerequisites

1. **[mise](https://mise.jdx.dev/)** for tool version management

1. **[ClickHouse](https://clickhouse.com/docs/install)** installed locally.
   On macOS, follow the
   [terminal process instructions](https://clickhouse.com/docs/install/macOS#terminal-process).
   After downloading, remove the binary from quarantine before running it:

   ```shell
   xattr -d com.apple.quarantine clickhouse
   ```

   > **Note:** GDK's ClickHouse listens on port **9001**, not the default 9000.
   > Always pass `--port 9001` when using `clickhouse client` to connect to the
   > GDK instance. Running `clickhouse client` without `--port 9001` connects to
   > a standalone ClickHouse instance if you have one installed.

1. **GDK with required services enabled:**

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

   Setting `postgresql.host: localhost` makes PostgreSQL listen on TCP, which
   Siphon requires for logical replication (GDK defaults to Unix sockets).

1. **PostgreSQL logical replication:**

   Edit `$GDK_ROOT/postgresql/data/postgresql.conf` (and `replication.conf` if it exists):

   ```plaintext
   listen_addresses = 'localhost'
   wal_level = logical
   ```

   Then restart PostgreSQL: `gdk restart postgresql`

1. **ClickHouse setup:**

   Create the Rails ClickHouse config from the example, create the database,
   and run migrations:

   ```shell
   cp $GDK_ROOT/gitlab/config/click_house.yml.example $GDK_ROOT/gitlab/config/click_house.yml
   clickhouse client --host localhost --port 9001 --query "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development"
   cd $GDK_ROOT/gitlab && bundle exec rake gitlab:clickhouse:migrate
   ```

   Then create the GKG graph database and apply the schema:

   ```shell
   clickhouse client --host localhost --port 9001 --query "CREATE DATABASE IF NOT EXISTS \`gkg-development\`"
   ```

   Apply the graph schema using the helper script (it applies each
   statement individually since ClickHouse does not support
   multi-statement DDL execution):

   ```shell
   scripts/apply-graph-schema.sh
   ```

   The script defaults to `localhost:9001` and database
   `gkg-development`. Override with `--host`, `--port`, or
   `--database` flags, or set `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`,
   `CLICKHOUSE_DATABASE` environment variables. Run with `--dry-run`
   to preview statements without executing.

   Or skip both steps and run `mise run dev:setup` later (see [Setup](#setup)).

1. **Configure Siphon tables:**

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
         stream_name: "siphon_stream_main_db"
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
         stream_name: "siphon_stream_main_db"
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

   Then restart siphon: `gdk restart siphon`

   See the [staging Siphon config](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-helmfiles/-/blob/master/bases/environments/orbit-stg.yaml.gotmpl)
   for the full list of tables used in production.

1. **Enable Knowledge Graph and JWT auth:**

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
   dev script reads automatically to configure the GKG webserver's JWT
   verifying key. Verify the file was created:

   ```shell
   ls $GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret
   ```

   If the file does not exist, restart Rails again. It may take a second
   restart for the secret to be generated.

   Enable the feature flags:

   ```shell
   cd $GDK_ROOT/gitlab
   bundle exec rails runner "Feature.enable(:knowledge_graph); Feature.enable(:knowledge_graph_infra)"
   ```

   Enable namespaces for indexing:

   ```shell
   cd $GDK_ROOT/gitlab
   bundle exec rails runner "Namespace.where(type: 'Group', parent_id: nil).find_each { |ns| Analytics::KnowledgeGraph::EnabledNamespace.find_or_create_by!(root_namespace_id: ns.id) }"
   ```

   The Knowledge Graph UI is available at
   `https://<gdk-hostname>:<gdk-port>/dashboard/orbit`.

## Setup

Clone this repository somewhere accessible (for example, next to your
`$GDK_ROOT` directory). The `GDK_ROOT` variable in `.env` (see step 2) is how
GKG locates your GDK installation, so the two directories do not need to be
adjacent.

1. **Install dependencies:**

   ```shell
   mise install
   ```

1. **Configure environment:**

   ```shell
   cp .env.example .env
   ```

   Edit `.env` and set `GDK_ROOT` to the absolute path to your GDK
   installation. The script derives GDK service ports from `gdk.yml`
   automatically, so you do not need to copy connection details into `.env`.

1. **Validate prerequisites:**

   ```shell
   mise run dev:check
   ```

1. **Create graph database and apply schema:**

   ```shell
   mise run dev:setup
   ```

1. **Start all services:**

   ```shell
   mise run dev
   ```

The GKG webserver is available at `http://localhost:8090` (HTTP) and
`localhost:50054` (gRPC) by default. Ports can be changed in `.env`.

This starts all three GKG runtime modes in the foreground:

- 1 webserver (HTTP + gRPC)
- 1 indexer
- 1 dispatcher (dispatch-indexing)

`mise run dev` runs these processes with prefixed output. Ctrl+C stops
everything.

Useful companion tasks:

```shell
mise run dev:check    # validate prerequisites
mise run dev:setup    # create graph DB + apply schema
mise run dev:status   # show derived config
mise run dev:env      # print env vars
```

`mise run gdk` is also available as an alias.

On the first run, `cargo` compiles the full workspace which takes several
minutes. Subsequent runs use the cached build and start in seconds.

Port assignments can be overridden in the `.env` file if you want to run
multiple isolated local clusters on the same machine.

### HTTPS and NGINX GDK setups

The dev script reads `hostname`, `port`, and `https.enabled` from `gdk.yml` to
derive `GKG_GITLAB__BASE_URL`. If your GDK has HTTPS enabled (for example
`https.enabled: true` with `hostname: gdk.test` and `port: 3443`), the script
automatically sets `GKG_GITLAB__BASE_URL=https://gdk.test:3443`.

For HTTPS to work, the GKG server's TLS stack (`rustls` via `reqwest`) must
trust the certificate. If you used `mkcert` to generate GDK certificates, run
`mkcert -install` to add the root CA to your system trust store.

### Siphon Prometheus port conflict

Siphon's default Prometheus port (8081) often conflicts with Elasticsearch. If
Siphon crash-loops with `listen tcp :8081: bind: address already in use`, change
the port in `$GDK_ROOT/siphon/config.yml`:

```yaml
prometheus:
  port: 8082
```

Protect the file from being overwritten by adding `siphon/config.yml` to
`gdk.protected_config_files` in `gdk.yml`, then `gdk restart siphon`.

## Troubleshooting

**NATS connection refused:**

- Verify GDK NATS is running: `gdk status nats`
- Check if NATS port is accessible: `nc -zv localhost 4222`

**NATS limit_markers error:**

- Update `NATS_VERSION` in `$GDK_ROOT/support/makefiles/Makefile.nats.mk` to a version >= 2.11 (example `2.11.12`)
- Run `cd $GDK_ROOT && rm -rf nats/nats-server`
- Run `make nats-setup && nats/nats-server -version`
- Restart GDK: `gdk restart nats`

**ClickHouse connection issues:**

ClickHouse exposes two ports: the **native TCP port** (`9001` in GDK)
used by `clickhouse client`, and the **HTTP port** (`8123`) used for
health checks and REST-style queries.

- Verify ClickHouse is running: `gdk status clickhouse`
- Check HTTP port: `curl "http://localhost:8123/ping"`
- Check native port: `clickhouse client --host localhost --port 9001 --query "SELECT 1"`

**MEMORY_LIMIT_EXCEEDED errors from ClickHouse:**

- Increase `max_server_memory_usage` (bytes) in `$GDK_ROOT/clickhouse/config.d/gdk.xml` e.g. `4294967296` for 4 GB:

```xml
<clickhouse>
  <!-- other existing settings ... -->
  <max_server_memory_usage>4294967296</max_server_memory_usage>
</clickhouse>
```

- Restart ClickHouse: `gdk restart clickhouse`

**403 Forbidden on the /dashboard/orbit page but JWT auth works:**

- The Knowledge Graph UI on the GDK (`/dashboard/orbit`) requires a Premium or Ultimate license.
- View instructions for configuring a license for the GDK: [Configure a developer license in GDK](https://gitlab-org.gitlab.io/gitlab-development-kit/#configure-developer-license-in-gdk)

**No data in graph:**

- Check siphon services: `gdk status siphon`
- Verify `siphon_*` tables have data: `clickhouse-client --port 9001 -q "SELECT count() FROM siphon_projects"`
- Check GKG indexer output in the `mise run dev` terminal

**`mise install` crashes with Rust toolchain errors:**

If `mise install` fails with errors related to parallel Rust toolchain installs,
reinstall the stable toolchain manually:

```shell
rustup toolchain uninstall stable
rustup toolchain install stable
```

Then re-run `mise install`.

**Datalake connection errors in the indexer:**

If the indexer logs errors like `datalake query failed: client error (Connect)`,
verify that ClickHouse is running and accessible:

```shell
gdk status clickhouse
curl "http://localhost:8123/ping"
```

Also confirm that the `gitlab_clickhouse_development` database exists and the
Siphon datalake tables have been created:

```shell
clickhouse client --host localhost --port 9001 --query "SHOW TABLES FROM gitlab_clickhouse_development"
```

If the tables are missing, check that Siphon is running (`gdk status siphon`)
and has been configured correctly (see [Configure Siphon tables](#prerequisites)).
