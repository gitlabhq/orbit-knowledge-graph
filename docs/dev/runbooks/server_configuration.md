# Server configuration runbook

Reference for all configurable knobs in the GKG server. All four modes (Webserver, Indexer, DispatchIndexing, HealthCheck) share the same `AppConfig` struct and loading mechanism.

## Configuration loading

Config is loaded in layers, each overriding the previous:

1. **Configuration file**: Example `config/default.yaml`
2. **Secrets**: Files in `/etc/secrets/` (Kubernetes secret mounts)
3. **Environment variables**: Prefixed with `GKG_`, using `__` as a separator for nested keys and `,` for lists

Environment variable examples:

```shell
GKG_NATS__URL=nats://gkg-nats:4222
GKG_GRAPH__DATABASE=gkg-sandbox
GKG_ENGINE__MAX_CONCURRENT_WORKERS=16
```

## Server modes

The binary (`gkg-server`) runs in one of four modes via `--mode`:

| Mode | Purpose | Key config sections |
|------|---------|---------------------|
| `Webserver` | HTTP/gRPC query server | `bind_address`, `grpc_bind_address`, `grpc`, `tls`, `query`, `graph`, `gitlab` |
| `Indexer` | Consumes NATS messages and runs indexing handlers | `nats`, `engine`, `graph`, `datalake`, `gitlab`, `schedule`, `schema` |
| `DispatchIndexing` | Runs the scheduler loop that publishes indexing requests | `nats`, `graph`, `datalake`, `schedule`, `schema` |
| `HealthCheck` | K8s readiness/liveness probes | `health_check`, `graph`, `datalake` |

All modes share the same configuration structure.

## NATS

### Connection

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `nats.url` | `GKG_NATS__URL` | `localhost:4222` | Broker address |
| `nats.username` | `GKG_NATS__USERNAME` | None | Auth username |
| `nats.password` | `GKG_NATS__PASSWORD` | None | Auth password |
| `nats.tls_ca_cert_path` | `GKG_NATS__TLS_CA_CERT_PATH` | None | CA cert (PEM) for TLS. Setting any TLS path enables TLS. |
| `nats.tls_cert_path` | `GKG_NATS__TLS_CERT_PATH` | None | Client cert (PEM) for mTLS. Must pair with `tls_key_path`. |
| `nats.tls_key_path` | `GKG_NATS__TLS_KEY_PATH` | None | Client key (PEM) for mTLS. Must pair with `tls_cert_path`. |
| `nats.connection_timeout_secs` | | `10` | Connection timeout |
| `nats.request_timeout_secs` | | `5` | Request timeout |

### Consumer

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `nats.consumer_name` | `GKG_NATS__CONSUMER_NAME` | None | Durable consumer name. `None` = ephemeral (lost on disconnect). Set in production for persistence across restarts. |
| `nats.ack_wait_secs` | | `300` | Seconds before unacked message is redelivered |
| `nats.max_deliver` | | `5` | Max redelivery attempts. `None` = unlimited. |
| `nats.batch_size` | | `10` | Messages fetched per batch |
| `nats.subscription_buffer_size` | | `100` | Internal channel buffer between fetch loop and handler |
| `nats.fetch_expires_secs` | | `5` | Server-side timeout for batch fetch (clamped to min 1s) |

### Stream

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `nats.auto_create_streams` | `GKG_NATS__AUTO_CREATE_STREAMS` | `true` | Create streams on startup |
| `nats.stream_replicas` | `GKG_NATS__STREAM_REPLICAS` | `1` | Replicas per stream. Use 3 in production for fault tolerance. |
| `nats.stream_max_age_secs` | `GKG_NATS__STREAM_MAX_AGE_SECS` | None | Max message age before deletion |
| `nats.stream_max_bytes` | `GKG_NATS__STREAM_MAX_BYTES` | None | Max stream size in bytes |
| `nats.stream_max_messages` | `GKG_NATS__STREAM_MAX_MESSAGES` | None | Max messages per stream |

The `GKG_INDEXER` stream is created with:

- Retention: `WorkQueue` (messages deleted after ack)
- Discard: `New` with `discard_new_per_subject: true`
- Max messages per subject: `1` (deduplication: rejects publishes while a handler hasn't acked)
- Storage: File

## ClickHouse

Two separate ClickHouse connections are required: one for the datalake (Siphon-replicated tables) and one for the graph (indexed property graph).

### Datalake

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `datalake.url` | `GKG_DATALAKE__URL` | `http://127.0.0.1:8123` | HTTP endpoint |
| `datalake.database` | `GKG_DATALAKE__DATABASE` | `default` | Database name |
| `datalake.username` | `GKG_DATALAKE__USERNAME` | `default` | Auth user |
| `datalake.password` | `GKG_DATALAKE__PASSWORD` | None | Auth password |
| `datalake.query_settings` | | `{}` | ClickHouse session settings (e.g., `max_rows_to_read`) |

### Graph

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `graph.url` | `GKG_GRAPH__URL` | `http://127.0.0.1:8123` | HTTP endpoint |
| `graph.database` | `GKG_GRAPH__DATABASE` | `default` | Database name |
| `graph.username` | `GKG_GRAPH__USERNAME` | `default` | Auth user |
| `graph.password` | `GKG_GRAPH__PASSWORD` | None | Auth password |
| `graph.query_settings` | | `{}` | ClickHouse session settings |

### Profiling (debug)

| Config path | Default | Description |
|-------------|---------|-------------|
| `graph.profiling.enabled` | `false` | Enable query profiling |
| `graph.profiling.explain` | `false` | Collect EXPLAIN output |
| `graph.profiling.query_log` | `false` | Log to system.query_log |
| `graph.profiling.processors` | `false` | Collect processor stats |
| `graph.profiling.instance_health` | `false` | Check instance health |

## Worker pool

The worker pool limits how many messages are processed concurrently. It uses a two-level semaphore: a global limit and optional per-group limits.

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `engine.max_concurrent_workers` | `GKG_ENGINE__MAX_CONCURRENT_WORKERS` | `16` | Global concurrency cap |
| `engine.concurrency_groups` | | `{}` | Named group limits |

### How concurrency groups work

Each handler can declare a `concurrency_group`. When a message arrives, the handler acquires the group semaphore first, then the global semaphore. Both are released after processing.

This prevents one handler type from monopolizing all workers. For example, with 16 global workers, you can cap SDLC at 12 and code at 4:

```yaml
engine:
  max_concurrent_workers: 16
  concurrency_groups:
    sdlc: 12
    code: 4
```

## Handler configuration

Each handler has retry and concurrency settings under `engine.handlers.<name>`:

| Config path | Default | Description |
|-------------|---------|-------------|
| `handlers.<name>.concurrency_group` | None | Which group semaphore to use |
| `handlers.<name>.max_attempts` | None | Total attempts (1 = no retry, 5 = 4 retries) |
| `handlers.<name>.retry_interval_secs` | None | Delay between retries (NATS nack delay) |

### Default handler settings

From `config/default.yaml`:

```yaml
engine:
  max_concurrent_workers: 16
  concurrency_groups:
    sdlc: 12
    code: 4
  handlers:
    global-handler:
      concurrency_group: sdlc
      max_attempts: 1
      retry_interval_secs: 60
    namespace-handler:
      concurrency_group: sdlc
      max_attempts: 1
      retry_interval_secs: 60
    code-indexing-task:
      concurrency_group: code
      max_attempts: 5
      retry_interval_secs: 60
      pipeline:
        max_file_size_bytes: 5000000
        max_files: 1000000
        respect_gitignore: true
        worker_threads: 0
        max_concurrent_languages: 0
    namespace-deletion:
      concurrency_group: code
      max_attempts: 1
```

### Handler-specific settings

#### SDLC global handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `engine.handlers.global-handler.datalake_batch_size` | `1,000,000` | Rows per datalake extraction query |

#### SDLC namespace handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `engine.handlers.namespace-handler.datalake_batch_size` | `1,000,000` | Rows per datalake extraction query |

#### Code indexing task handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `engine.handlers.code-indexing-task.pipeline.max_file_size_bytes` | `5,000,000` | Largest source file the v2 pipeline will parse |
| `engine.handlers.code-indexing-task.pipeline.max_files` | `1,000,000` | Maximum language-supported source files accepted for one pipeline run |
| `engine.handlers.code-indexing-task.pipeline.respect_gitignore` | `true` | Whether repository `.gitignore` rules apply during discovery |
| `engine.handlers.code-indexing-task.pipeline.worker_threads` | `0` | Rayon workers per language; `0` uses Rayon default |
| `engine.handlers.code-indexing-task.pipeline.max_concurrent_languages` | `0` | Concurrent language pipelines; `0` uses the pipeline default |

### Retry strategy by handler type

| Handler | max_attempts | DLQ | Rationale |
|---------|-------------|-----|-----------|
| Global (SDLC) | 1 | No | Re-dispatched every cycle. No need to retry. |
| Namespace (SDLC) | 1 | No | Re-dispatched every cycle. No need to retry. |
| Code indexing task | 5 | Yes | Event-driven. Won't be re-dispatched. Must retry and DLQ. |
| Namespace deletion | 1 | No | Re-dispatched on next scheduler cycle. |

## Scheduler configuration

Scheduled tasks run in `DispatchIndexing` mode. Each task has a 6-field cron expression (seconds, minutes, hours, day-of-month, month, day-of-week). Tasks without a cron expression fall back to a 60-second interval.

Distributed locking via NATS KV ensures only one dispatcher instance runs each task per interval.

| Task | Config path | Default cron | Description |
|------|-------------|-------------|-------------|
| Global dispatch | `schedule.tasks.global.cron` | `0 */1 * * * *` (every minute) | Publishes `GlobalIndexingRequest` |
| Namespace dispatch | `schedule.tasks.namespace.cron` | `0 */1 * * * *` (every minute) | Publishes per-namespace requests |
| Code task dispatch | `schedule.tasks.code-indexing-task.cron` | `0 */1 * * * *` (every minute) | Consumes Siphon CDC push events |
| Code backfill | `schedule.tasks.namespace-code-backfill.cron` | `0 */1 * * * *` (every minute) | Backfills newly enabled namespaces |
| Table cleanup | `schedule.tasks.table-cleanup.cron` | `0 0 3 * * *` (daily 03:00 UTC) | Runs `OPTIMIZE TABLE ... FINAL CLEANUP` |
| Namespace deletion | `schedule.tasks.namespace-deletion.cron` | `0 0 3 * * *` (daily 03:00 UTC) | Schedules and executes namespace deletions |
| Migration completion | `schedule.tasks.migration-completion.cron` | `0 */1 * * * *` (every minute) | Detects completed schema migrations |

### Code dispatch task settings

| Config path | Default | Description |
|-------------|---------|-------------|
| `schedule.tasks.code-indexing-task.events_stream_name` | `siphon_stream_main_db` | NATS stream for Siphon CDC events |
| `schedule.tasks.code-indexing-task.batch_size` | `100` | CDC events to process per cycle |

### Code backfill task settings

| Config path | Default | Description |
|-------------|---------|-------------|
| `schedule.tasks.namespace-code-backfill.events_stream_name` | `siphon_stream_main_db` | NATS stream for namespace events |
| `schedule.tasks.namespace-code-backfill.batch_size` | `100` | Events to process per cycle |

## GitLab client

Required for code indexing (repository archive download) and authorization.

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `gitlab.base_url` | `GKG_GITLAB__BASE_URL` | None | GitLab instance URL |
| `gitlab.jwt.signing_key` | `GKG_GITLAB__JWT__SIGNING_KEY` | None | JWT signing key (for creating tokens) |
| `gitlab.jwt.verifying_key` | `GKG_GITLAB__JWT__VERIFYING_KEY` | (required) | JWT verification key |
| `gitlab.resolve_host` | `GKG_GITLAB__RESOLVE_HOST` | None | Override DNS resolution for GitLab |

## Observability

### Logging

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `metrics.log_level` | `GKG_METRICS__LOG_LEVEL` | None | Rust log filter string |

Example: `info,gkg_server=debug,gkg_indexer=trace`

### OpenTelemetry

| Config path | Default | Description |
|-------------|---------|-------------|
| `metrics.otel.enabled` | `false` | Enable OTEL tracing |
| `metrics.otel.endpoint` | `http://localhost:4317` | OTEL gRPC collector endpoint |

### Prometheus

| Config path | Default | Description |
|-------------|---------|-------------|
| `metrics.prometheus.enabled` | `false` | Expose `/metrics` endpoint |
| `metrics.prometheus.port` | `9394` | Prometheus scrape port |

## Webserver

These settings are used by the Webserver mode.

### General

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `bind_address` | `GKG_BIND_ADDRESS` | `127.0.0.1:4200` | HTTP server bind address |
| `grpc_bind_address` | `GKG_GRPC_BIND_ADDRESS` | `127.0.0.1:50054` | gRPC server bind address |
| `jwt_clock_skew_secs` | `GKG_JWT_CLOCK_SKEW_SECS` | `60` | Allowed JWT clock skew in seconds |
| `health_check_url` | `GKG_HEALTH_CHECK_URL` | None | Optional health check URL |

### TLS

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `tls.cert_path` | `GKG_TLS__CERT_PATH` | None | TLS certificate path (PEM) |
| `tls.key_path` | `GKG_TLS__KEY_PATH` | None | TLS private key path (PEM) |

### gRPC tuning

| Config path | Default | Description |
|-------------|---------|-------------|
| `grpc.keepalive_interval_secs` | `20` | HTTP/2 keepalive ping interval |
| `grpc.keepalive_timeout_secs` | `20` | Keepalive ping timeout |
| `grpc.tcp_keepalive_secs` | `60` | TCP keepalive interval |
| `grpc.connection_window_size` | `2097152` (2 MB) | HTTP/2 connection flow control window |
| `grpc.stream_window_size` | `1048576` (1 MB) | HTTP/2 stream flow control window |
| `grpc.concurrency_limit` | `256` | Max concurrent requests |
| `grpc.max_connection_age_secs` | `300` (5 min) | Max connection age (for L4 ILB rebalancing) |
| `grpc.max_connection_age_grace_secs` | `30` | Graceful drain window after `max_connection_age_secs` fires. Must be non-zero to avoid a tonic 0.14.5 panic ([hyperium/tonic#2522](https://github.com/hyperium/tonic/issues/2522)). |
| `grpc.stream_timeout_secs` | `60` | Stream timeout |

### Query settings

Supports default settings and per-query-type overrides (e.g. `aggregation`, `traversal`, `search`):

```yaml
query:
  default:
    max_execution_time: 30
    max_memory_usage: 1073741824
    use_query_cache: false
    query_cache_ttl: 60
  aggregation:
    max_execution_time: 60
```

| Config path | Default | Description |
|-------------|---------|-------------|
| `query.default.max_execution_time` | `30` | ClickHouse `max_execution_time` in seconds |
| `query.default.max_memory_usage` | `1073741824` | ClickHouse `max_memory_usage` in bytes (1 GiB) |
| `query.default.use_query_cache` | `false` | Enable ClickHouse query cache |
| `query.default.query_cache_ttl` | `60` | Query cache TTL in seconds |

## Schema management

| Config path | Default | Description |
|-------------|---------|-------------|
| `schema.max_retained_versions` | `2` | Number of schema version table-sets to retain (min 2) |

## Analytics

Identifies the GitLab deployment hosting this GKG instance. Used to tag Snowplow product analytics and OTel telemetry so downstream dashboards can segment by instance type and environment without inferring it from the hostname. Future analytics settings (opt-in/opt-out, transport, auth) will live under the same `analytics` block.

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `analytics.deployment.type` | `GKG_ANALYTICS__DEPLOYMENT__TYPE` | `self_managed` | `com`, `dedicated`, or `self_managed` |
| `analytics.deployment.environment` | `GKG_ANALYTICS__DEPLOYMENT__ENVIRONMENT` | `development` | `development`, `staging`, or `production` |

Example for the .com staging cluster:

```yaml
analytics:
  deployment:
    type: com
    environment: staging
```

## Health check

| Config path | Default | Description |
|-------------|---------|-------------|
| `health_check.bind_address` | `0.0.0.0:4201` | HealthCheck mode bind address |
| `indexer_health_bind_address` | `0.0.0.0:4202` | Health check server address for Indexer mode |
| `dispatcher_health_bind_address` | `0.0.0.0:4203` | Health check server address for DispatchIndexing mode |

## Tuning guide

### Increase indexing throughput

Increase global and group concurrency:

```yaml
engine:
  max_concurrent_workers: 32
  concurrency_groups:
    sdlc: 24
    code: 8
```

Increase SDLC batch sizes for large namespaces:

```yaml
engine:
  handlers:
    namespace-handler:
      datalake_batch_size: 5000000
```

### Reduce NATS pressure

Increase ack wait for slow handlers:

```shell
GKG_NATS__ACK_WAIT_SECS=600  # 10 minutes instead of default 5
```

### Handle large CDC backlogs

Increase the code dispatch batch size:

```yaml
schedule:
  tasks:
    code-indexing-task:
      batch_size: 500
```

### Production NATS settings

```shell
GKG_NATS__CONSUMER_NAME=gkg-indexer       # Durable consumer (survives restarts)
GKG_NATS__STREAM_REPLICAS=3               # Fault tolerance
GKG_NATS__AUTO_CREATE_STREAMS=true        # Auto-create on startup
```

## Helm chart configuration

In production, GKG is deployed via the [gkg-helm-charts](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts). Most configuration is set through Helm values rather than raw YAML or environment variables.

### Key Helm values mapping

| Helm value | Application config | Description |
|------------|-------------------|-------------|
| `nats.url` | `nats.url` | NATS broker address |
| `nats.consumerName` | `nats.consumer_name` | Durable consumer name |
| `clickhouse.datalake.host` | `datalake.url` | Datalake ClickHouse host |
| `clickhouse.datalake.database` | `datalake.database` | Datalake database name |
| `clickhouse.graph.host` | `graph.url` | Graph ClickHouse host |
| `clickhouse.graph.database` | `graph.database` | Graph database name |
| `gitlab.baseUrl` | `gitlab.base_url` | GitLab instance URL |
| `indexer.logLevel` | `metrics.log_level` | Indexer log level |
| `secrets.existingSecret` | (secret mounts) | Kubernetes secret with credentials |

### Overriding configuration via Helm

```shell
helm upgrade gkg gkg-helm-charts/gkg \
  --set nats.consumerName=gkg-indexer \
  --set clickhouse.graph.database=gkg-production
```

For complex overrides, use a values file:

```shell
helm upgrade gkg gkg-helm-charts/gkg -f custom-values.yaml
```

## Troubleshooting with kubectl

### Check pod status and logs

```shell
kubectl -n gkg get pods
kubectl -n gkg logs deployment/gkg-indexer -f
kubectl -n gkg logs deployment/gkg-dispatcher -f

# Filter logs for a specific project
kubectl -n gkg logs deployment/gkg-indexer -f | grep 'project_id=<id>'
```

### Inspect running configuration

```shell
kubectl -n gkg exec deployment/gkg-indexer -- env | grep GKG_
```

### Troubleshoot NATS with nats-box

Spin up a [nats-box](https://github.com/nats-io/nats-box) pod to run NATS commands inside the cluster:

```shell
kubectl -n gkg run nats-box --image=natsio/nats-box:latest --restart=Never -- sleep infinity
kubectl -n gkg exec -it nats-box -- sh
```

From inside nats-box:

```shell
# Check stream health
nats -s nats://gkg-nats:4222 stream ls
nats -s nats://gkg-nats:4222 stream info GKG_INDEXER

# Inspect consumers
nats -s nats://gkg-nats:4222 consumer ls GKG_INDEXER

# Check dead letter queue
nats -s nats://gkg-nats:4222 stream info GKG_DEAD_LETTERS

# Purge a stuck subject
nats -s nats://gkg-nats:4222 stream purge GKG_INDEXER \
  --subject='sdlc.namespace.indexing.requested.<org>.<ns>'

# Inspect KV locks
nats -s nats://gkg-nats:4222 kv ls indexing_locks
```

Clean up when done:

```shell
kubectl -n gkg delete pod nats-box
```

## Example: production config

```yaml
nats:
  url: nats://gkg-nats:4222
  consumer_name: gkg-indexer
  ack_wait_secs: 300
  auto_create_streams: true
  stream_replicas: 3

datalake:
  url: http://clickhouse:8123
  database: gitlab_clickhouse_main_production
  username: default

graph:
  url: http://clickhouse:8123
  database: gkg-sandbox
  username: default

gitlab:
  base_url: https://gitlab.example.com

engine:
  max_concurrent_workers: 16
  concurrency_groups:
    sdlc: 12
    code: 4
  handlers:
    global-handler:
      concurrency_group: sdlc
      max_attempts: 1
      retry_interval_secs: 60
    namespace-handler:
      concurrency_group: sdlc
      max_attempts: 1
      retry_interval_secs: 60
    code-indexing-task:
      concurrency_group: code
      max_attempts: 5
      retry_interval_secs: 60
      pipeline:
        max_file_size_bytes: 5000000
        max_files: 1000000
        respect_gitignore: true
        worker_threads: 0
        max_concurrent_languages: 0
    namespace-deletion:
      concurrency_group: code
      max_attempts: 1

schedule:
  tasks:
    table-cleanup:
      cron: "0 0 3 * * *"
    namespace-deletion:
      cron: "0 0 3 * * *"
    migration-completion:
      cron: "0 */1 * * * *"

metrics:
  log_level: info,gkg_server=debug
  prometheus:
    enabled: true
    port: 9394
```
