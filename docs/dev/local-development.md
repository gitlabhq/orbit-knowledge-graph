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

   ```shell
   gdk config set nats.enabled true
   gdk config set siphon.enabled true
   gdk config set clickhouse.enabled true
   gdk reconfigure
   ```

5. **PostgreSQL logical replication:**

   Edit `$GDK_ROOT/postgresql/data/postgresql.conf` (and `replication.conf` if it exists):

   ```plaintext
   wal_level = logical
   ```

   Then restart PostgreSQL: `gdk restart postgresql`

6. **Create the GKG graph database in ClickHouse:**

   ```shell
   clickhouse-client --port 9001 -u default --query "CREATE DATABASE IF NOT EXISTS `gkg-development`"
   ```

7. **Configure Siphon tables:**

   GKG requires specific tables to be replicated. Add to `$GDK_ROOT/gdk.yml`:

   ```yaml
   siphon:
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

   Then run `gdk reconfigure`.

   **Note:** When adding new tables, run ClickHouse migrations in GDK to create the corresponding `siphon_*` tables:

   ```shell
   cd $GDK_ROOT/gitlab && bundle exec rake gitlab:clickhouse:migrate
   ```

   See: [GDK Siphon documentation](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/main/doc/howto/siphon.md)

8. **Expose Gitaly on network interface** (for code indexing):

   By default, GDK's Gitaly only listens on a Unix socket. To allow K8s pods to connect, edit `$GDK_ROOT/gitaly/gitaly.config.toml`:

   ```toml
   listen_addr = '0.0.0.0:8075'
   ```

   Then restart Gitaly: `gdk restart gitaly`

   See: https://docs.gitlab.com/administration/gitaly/configure_gitaly

## Setup

1. **Install dependencies:**

   ```shell
   mise install
   ```

2. **Configure secrets:**

   ```shell
   cp .tilt-secrets.example .tilt-secrets
   ```

   Edit `.tilt-secrets` and fill in:
   - `POSTGRES_PASSWORD`: Check `$GDK_ROOT/postgresql/.s.PGSQL.5432` or use empty string for trust auth
   - `CLICKHOUSE_PASSWORD`: Usually empty for local development
   - `GKG_JWT_SECRET`: Any 32+ character string (used as `gitlab.jwt.verifying_key` via K8s secret)

3. **Start local environment:**

   ```shell
   tilt up
   ```

## Access Services

- **Tilt UI**: http://localhost:10350
- **GKG Webserver**: http://localhost:8080
- **Grafana**: http://localhost:30300 (login: admin/admin)

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

**Gitaly connection refused:**

- Verify Gitaly is configured to listen on network: check `listen_addr` in `$GDK_ROOT/gitaly/gitaly.config.toml`
- Check if port is accessible: `nc -zv localhost 8075`
- Restart Gitaly after config changes: `gdk restart gitaly`

**No data in graph:**

- Check siphon services: `gdk status siphon-producer-main-db siphon-clickhouse-consumer`
- Verify `siphon_*` tables have data: `clickhouse-client --port 9001 -q "SELECT count() FROM siphon_projects"`
- Check indexer logs: `kubectl logs -l app.kubernetes.io/name=gkg-indexer`

**host.docker.internal not resolving:**

- On Linux, add `--add-host=host.docker.internal:host-gateway` to Docker
- Or use your host's actual IP address in `helm-dev/gkg/values-local.yaml`
