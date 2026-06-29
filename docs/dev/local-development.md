# Local development setup

Run GKG as native Rust processes connected to NATS, Siphon, PostgreSQL, and
ClickHouse from your GDK installation.

> Working on `orbit-local`, the ontology, language parsers, or docs only?
> You don't need GDK or any of the services below. See the
> [Orbit Local development quickstart](orbit-local-quickstart.md).

## Prerequisites

1. **[mise](https://mise.jdx.dev/)** for tool version management

1. **[ClickHouse](https://clickhouse.com/docs/install)** installed locally.

   GDK does **not** download the ClickHouse binary for you. When
   `clickhouse.enabled: true`, GDK templates the config and registers the
   service, but expects a binary to already exist at `clickhouse.bin` (default
   `/usr/bin/clickhouse`). If none is present, GDK silently skips the service —
   so install ClickHouse yourself first.

   On macOS, follow the
   [terminal process instructions](https://clickhouse.com/docs/install/macOS#terminal-process).
   After downloading, remove the binary from quarantine before running it:

   ```shell
   xattr -d com.apple.quarantine clickhouse
   ```

   On Linux, download the binary with the one-liner:

   ```shell
   curl -sSL "https://clickhouse.com/" | sh
   ```

   This downloads a `./clickhouse` binary into the current directory. Either
   point `clickhouse.bin` in `gdk.yml` at it, or move it onto your `PATH` (for
   example to `/usr/bin/clickhouse`); `sudo ./clickhouse install` is not
   required.

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

   The set of replicated tables is driven by per-table YAML files in
   `$GDK_ROOT/gitlab/db/siphon/tables/`. GDK reads them on `gdk reconfigure` and
   generates the entire Siphon config (`$GDK_ROOT/siphon/config.yml`) — both the
   producer and consumer sides — from that single source. In general you do
   **not** hand-write `config.yml`; add or remove table files and let GDK
   regenerate it. (One exception, the hardcoded Prometheus port, is covered under
   [Troubleshooting](#siphon-prometheus-port-conflict).)

   The GitLab repo already ships the tables the live indexing path needs,
   including the system-notes / commit-edge path: `notes`,
   `system_note_metadata`, `merge_requests`, `issues`, `users`, and `routes`
   (the transform reads these to resolve note bodies, the `action`
   discriminator, noteable and cross-reference targets, authors, and
   `path` → traversal-path lookups), alongside `namespaces` and `projects`.
   Confirm the ones you need exist:

   ```shell
   ls $GDK_ROOT/gitlab/db/siphon/tables/
   ```

   Each file maps one PostgreSQL table to a ClickHouse `siphon_*` target, for
   example `$GDK_ROOT/gitlab/db/siphon/tables/notes.yml`:

   ```yaml
   table: notes
   database: main
   replication_targets:
     - name: clickhouse_main
       target: siphon_notes
   ```

   To replicate a table that isn't shipped yet, add a file in the same shape,
   generate its ClickHouse migration, and run it:

   ```shell
   cd $GDK_ROOT/gitlab
   bundle exec rails generate gitlab:click_house:siphon <table_name>
   bundle exec rake gitlab:clickhouse:migrate
   ```

   Then regenerate the Siphon config and restart the service:

   ```shell
   gdk reconfigure
   gdk restart siphon
   ```

   For full details (multi-database support, re-syncing a single table) see the
   GDK [Siphon how-to](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/doc/howto/siphon.md).
   The [staging Siphon config](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-helmfiles/-/blob/master/bases/environments/orbit-stg.yaml.gotmpl)
   lists the tables used in production.

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

Siphon's Prometheus port (8081) often conflicts with Elasticsearch. GDK
hardcodes this port when it generates `$GDK_ROOT/siphon/config.yml` and exposes
no `gdk.yml` knob for it, so changing it is the one case where you override the
generated file. If Siphon crash-loops with
`listen tcp :8081: bind: address already in use`, change the port:

```yaml
prometheus:
  port: 8082
```

Then protect the file from being regenerated by adding `siphon/config.yml` to
`gdk.protected_config_files` in `gdk.yml`, and `gdk restart siphon`. Note that
while this file is protected, GDK will not pick up new table files until you
remove the protection and reconfigure.

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

ClickHouse's `max_server_memory_usage` is a **whole-host RSS** limit, not a
per-query budget. GDK sets it from `clickhouse.max_server_memory_usage` in
`gdk.yml` (default 3 GB), and on a busy GDK the baseline RSS can already exceed
that cap — so even a trivial `SELECT` fails with
`(total) memory limit exceeded ... (MEMORY_LIMIT_EXCEEDED)`.

GDK generates `$GDK_ROOT/clickhouse/config.d/gdk.xml` from `gdk.yml`, so editing
that file directly is overwritten on the next `gdk reconfigure`. Raise the limit
through `gdk.yml` instead:

```yaml
clickhouse:
  max_server_memory_usage: 8000000000
```

Then apply it and restart:

```shell
gdk reconfigure
gdk restart clickhouse
```

A value of `0` disables the absolute cap and falls back to a fraction of host
RAM (`max_server_memory_usage_to_ram_ratio`), which is a good choice when total
RAM, not a fixed number, is the right ceiling.

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
