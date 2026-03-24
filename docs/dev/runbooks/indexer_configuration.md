# Indexer configuration runbook

Reference for all configurable knobs in the GKG indexer and dispatcher.

## Configuration loading

Config is loaded in layers, each overriding the previous:

1. **Configuration file**: Example `config/default.yaml`
2. **Secrets**: Files in `/run/secrets/` (Kubernetes secret mounts)
3. **Environment variables**: Prefixed with `GKG_`, using `__` as a separator for nested keys and `,` for lists

Environment variable examples:

```shell
GKG_NATS__URL=nats://gkg-nats:4222
GKG_GRAPH__DATABASE=gkg-sandbox
GKG_ENGINE__MAX_CONCURRENT_WORKERS=16
```

## Server modes

The indexer binary (`gkg-server`) runs in one of four modes via `--mode`:

| Mode | Purpose |
|------|---------|
| `Indexer` | Consumes NATS messages and runs indexing handlers |
| `DispatchIndexing` | Runs the scheduler loop that publishes indexing requests |

The `Indexer` and `DispatchIndexing` modes share the same configuration structure but use different sections.

## NATS

### Connection

| Config path | Env var | Default | Description |
|-------------|---------|---------|-------------|
| `nats.url` | `GKG_NATS__URL` | `localhost:4222` | Broker address |
| `nats.username` | `GKG_NATS__USERNAME` | None | Auth username |
| `nats.password` | `GKG_NATS__PASSWORD` | None | Auth password |
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
    code-push-event:
      concurrency_group: code
      max_attempts: 5
      retry_interval_secs: 60
    code-project-reconciliation:
      concurrency_group: code
      max_attempts: 1
```

### Handler-specific settings

#### SDLC global handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `handlers.global_handler.datalake_batch_size` | `1,000,000` | Rows per datalake extraction query |

#### SDLC namespace handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `handlers.namespace_handler.datalake_batch_size` | `1,000,000` | Rows per datalake extraction query |

### Retry strategy by handler type

| Handler | max_attempts | DLQ | Rationale |
|---------|-------------|-----|-----------|
| Global (SDLC) | 1 | No | Re-dispatched every cycle. No need to retry. |
| Namespace (SDLC) | 1 | No | Re-dispatched every cycle. No need to retry. |
| Code push event | 5 | Yes | Event-driven. Won't be re-dispatched. Must retry and DLQ. |
| Code project reconciliation | 1 | No | Periodic backfill. Re-dispatched on next cycle. |
| Namespace deletion | 1 | No | Re-dispatched on next scheduler cycle. |

## Scheduler configuration

Scheduled tasks run in `DispatchIndexing` mode. Each task can have an `interval_secs` cadence. Tasks without an interval run on every scheduler loop iteration.

Distributed locking via NATS KV ensures only one dispatcher instance runs each task per interval.

| Task | Config path | Default interval | Description |
|------|-------------|-----------------|-------------|
| Global dispatch | `schedule.tasks.global.interval_secs` | None (every cycle) | Publishes `GlobalIndexingRequest` |
| Namespace dispatch | `schedule.tasks.namespace.interval_secs` | None (every cycle) | Publishes per-namespace requests |
| Code task dispatch | `schedule.tasks.code_indexing_task.interval_secs` | None (every cycle) | Consumes Siphon CDC push events |
| Code backfill | `schedule.tasks.namespace_code_backfill.interval_secs` | None (every cycle) | Backfills newly enabled namespaces |
| Table cleanup | `schedule.tasks.table_cleanup.interval_secs` | `86400` (24h) | Runs `OPTIMIZE TABLE ... FINAL CLEANUP` |
| Namespace deletion | `schedule.tasks.namespace_deletion.interval_secs` | `86400` (24h) | Schedules and executes namespace deletions |

### Code dispatch task settings

| Config path | Default | Description |
|-------------|---------|-------------|
| `schedule.tasks.code_indexing_task.events_stream_name` | `siphon_stream_main_db` | NATS stream for Siphon CDC events |
| `schedule.tasks.code_indexing_task.batch_size` | `100` | CDC events to process per cycle |

### Code backfill task settings

| Config path | Default | Description |
|-------------|---------|-------------|
| `schedule.tasks.namespace_code_backfill.events_stream_name` | `siphon_stream_main_db` | NATS stream for namespace events |
| `schedule.tasks.namespace_code_backfill.batch_size` | `100` | Events to process per cycle |

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

## Health check

| Config path | Default | Description |
|-------------|---------|-------------|
| `indexer_health_bind_address` | `0.0.0.0:4202` | Health check server address for Indexer mode |

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
    namespace_handler:
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
    code_indexing_task:
      batch_size: 500
```

### Production NATS settings

```shell
GKG_NATS__CONSUMER_NAME=gkg-indexer       # Durable consumer (survives restarts)
GKG_NATS__STREAM_REPLICAS=3               # Fault tolerance
GKG_NATS__AUTO_CREATE_STREAMS=true        # Auto-create on startup
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
    code-push-event:
      concurrency_group: code
      max_attempts: 5
      retry_interval_secs: 60
    code-project-reconciliation:
      concurrency_group: code
      max_attempts: 1

schedule:
  tasks:
    table_cleanup:
      interval_secs: 86400
    namespace_deletion:
      interval_secs: 86400

metrics:
  log_level: info,gkg_server=debug
  prometheus:
    enabled: true
    port: 9394
```
