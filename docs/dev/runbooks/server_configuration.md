# Server configuration runbook

Reference for all configurable knobs in the GKG server. All four modes (Webserver, Indexer, DispatchIndexing, HealthCheck) share the same `AppConfig` struct and loading mechanism.

## Configuration loading

Config is loaded in layers, each overriding the previous:

1. **Embedded defaults**: `config/default.yaml`, compiled into the binary. It declares every
   section and scalar the server reads; the Rust config structs carry no fallback values, so a
   key removed from this file fails startup with a "missing field" error. Optional keys are
   `Option` fields (passwords, TLS paths, values derived from container resources) and are
   commented out in the file. These are **deployment defaults**: a production pod runs on them for
   every key the Helm ConfigMap does not set, so the file holds no local-development tuning. Its
   maps stay empty unless an entry is a genuine universal default, because config-rs deep-merges
   maps and any entry here that a partial ConfigMap does not set would leak into production.
2. **On-disk `config/default.yaml`**, relative to the working directory, when present. This is
   the key the Helm chart's ConfigMap currently uses; treat it as a partial overlay.
3. **Overlay file**: the path given with `--config <path>`, otherwise `config/config.yaml` when it exists.
   An explicit `--config` path must exist; the default overlay is optional and Git ignores it.
   The mise dev tasks pass a generated file built from `config/dev.yaml`; see the layering below.
4. **Secrets**: Files in `/etc/secrets/` (Kubernetes secret mounts)

There is no environment-variable layer. Every override is a YAML overlay or a secret file.

Adding a setting means adding a field to the struct in `crates/orbit-server-config/` and its
value to `config/default.yaml`; nothing else. Tests that need a config start from
`AppConfig::embedded_defaults()` and override the fields they care about.

The mise dev tasks (`server:start`, `server:dispatch`, `dev:web`, `dev:indexer`, `dev:dispatcher`,
`dev:healthcheck`) run `scripts/orbit-native-dev.sh`, which writes `.dev/<mode>.yaml` on every start
and passes it as the single `--config` file. The file is a `yq` deep merge of, in increasing priority:

1. `config/dev.yaml`: the committed description of the dev environment: bind addresses, NATS URL and
   consumer name, database names and users, laptop tuning.
2. Values read from the GDK checkout: ClickHouse URLs from `gdk.yml`, the GitLab base URL, the Siphon
   stream name from GDK's Siphon config, JWT keys and the ClickHouse password from the GitLab secret files.
3. The mode's Prometheus port, so the processes `mise run dev` starts side by side do not collide once
   metrics are enabled.
4. `config/dev.local.yaml`: personal overrides, when the file exists. Git ignores it.

Passing `--config` disables the default `config/config.yaml` lookup.

Overlay example (`config/dev.local.yaml` or any `--config` file):

```yaml
bind_address: "127.0.0.1:8091"
graph:
  database: "gkg-sandbox"
engine:
  max_concurrent_workers: 16
```

## Server modes

The binary (`gkg-server`) runs in one of four modes via `--mode`:

| Mode | Purpose | Key config sections |
|------|---------|---------------------|
| `Webserver` | HTTP/gRPC query server | `bind_address`, `grpc_bind_address`, `grpc`, `tls`, `query`, `graph`, `gitlab` |
| `Indexer` | Consumes NATS messages and runs indexing handlers | `nats`, `engine`, `graph`, `datalake`, `gitlab`, `schedule`, `schema` |
| `DispatchIndexing` | Runs the scheduler loop that publishes indexing requests | `nats`, `graph`, `datalake`, `schedule`, `schema` |
| `HealthCheck` | Aggregate Kubernetes workload and ClickHouse health, plus NATS queue depth | `health_check`, `graph`, `datalake`, `nats` |

All modes share the same configuration structure.

## NATS

### Connection

| Config path | Default | Description |
|-------------|---------|-------------|
| `nats.url` | `localhost:4222` | Broker address |
| `nats.username` | None | Auth username |
| `nats.password` | None | Auth password |
| `nats.tls_ca_cert_path` | None | CA cert (PEM) for TLS. Setting any TLS path enables TLS. |
| `nats.tls_cert_path` | None | Client cert (PEM) for mTLS. Must pair with `tls_key_path`. |
| `nats.tls_key_path` | None | Client key (PEM) for mTLS. Must pair with `tls_cert_path`. |
| `nats.connection_timeout_secs` | `10` | Connection timeout |
| `nats.request_timeout_secs` | `5` | Request timeout |

### Consumer

| Config path | Default | Description |
|-------------|---------|-------------|
| `nats.consumer_name` | None | Durable consumer name. `None` = ephemeral (lost on disconnect). Set in production for persistence across restarts. |
| `nats.ack_wait_secs` | `300` | Seconds before unacked message is redelivered |
| `nats.max_deliver` | `5` | Max redelivery attempts. `None` = unlimited. |
| `nats.batch_size` | `10` | Messages fetched per batch |
| `nats.subscription_buffer_size` | `100` | Internal channel buffer between fetch loop and handler |
| `nats.fetch_expires_secs` | `5` | Server-side timeout for batch fetch (clamped to min 1s) |

### Stream

| Config path | Default | Description |
|-------------|---------|-------------|
| `nats.auto_create_streams` | `true` | Create streams on startup |
| `nats.stream_replicas` | `1` | Replicas per stream. Use 3 in production for fault tolerance. |
| `nats.stream_max_age_secs` | None | Max message age before deletion |
| `nats.stream_max_bytes` | None | Max stream size in bytes |
| `nats.stream_max_messages` | None | Max messages per stream |
| `nats.consumer_inactive_threshold_secs` | `3600` | Idle time after which NATS auto-deletes a versioned durable consumer (min 60) |
| `nats.release_gc_idle_threshold_secs` | `3600` | Idle time after which a starting dispatcher deletes another release's streams (min 600) |

The `GKG_INDEXER` stream is created with:

- Retention: `WorkQueue` (messages deleted after ack)
- Discard: `New` with `discard_new_per_subject: true`
- Max messages per subject: `1` (deduplication: rejects publishes while a handler hasn't acked)
- Storage: File

## ClickHouse

Two separate ClickHouse connections are required: one for the datalake (Siphon-replicated tables) and one for the graph (indexed property graph).

### Datalake

| Config path | Default | Description |
|-------------|---------|-------------|
| `datalake.url` | `http://127.0.0.1:8123` | HTTP endpoint |
| `datalake.database` | `default` | Database name |
| `datalake.username` | `default` | Auth user |
| `datalake.password` | None | Auth password |
| `datalake.session_settings` | `{}` | ClickHouse session-level settings (e.g., `max_execution_time`, `max_query_size`) |
| `datalake.replicated` | `false` | Self-managed replicated cluster; see [Self-managed replicated clusters](#self-managed-replicated-clusters) |

### Graph

| Config path | Default | Description |
|-------------|---------|-------------|
| `graph.url` | `http://127.0.0.1:8123` | HTTP endpoint |
| `graph.database` | `default` | Database name |
| `graph.username` | `default` | Auth user |
| `graph.password` | None | Auth password |
| `graph.session_settings` | `{}` | ClickHouse session-level settings (e.g., `optimize_on_insert`, `max_query_size`) |
| `graph.insert_settings` | `{}` | Settings applied to INSERT operations only (e.g., `async_insert`, `wait_for_async_insert`) |
| `graph.replicated` | `false` | Self-managed replicated cluster; see [Self-managed replicated clusters](#self-managed-replicated-clusters) |

### Self-managed replicated clusters

Set `replicated: true` on a connection that points at a self-managed ClickHouse cluster with more than one replica. Leave it `false` on a single node and on ClickHouse Cloud, where SharedMergeTree replicates and writes with quorum by itself.

```yaml
datalake:
  replicated: true
graph:
  replicated: true
```

The Helm chart sets both from one value, `clickhouse.ha.enabled: true`.

The database must use the `Replicated` database engine, created by the ClickHouse administrator:

```sql
CREATE DATABASE orbit ON CLUSTER '{cluster}'
ENGINE = Replicated('/clickhouse/databases/orbit', '{shard}', '{replica}');
```

On the graph connection the switch does three things:

1. Prefixes `Replicated` onto every `*MergeTree` engine in DDL, the same rewrite GitLab Rails applies on a `Replicated` database. A `Replicated` database replicates metadata only; without replicated table engines, rows stay on the replica that took the write. ClickHouse takes the ZooKeeper path and replica name from the server settings `default_replica_path` and `default_replica_name`, so the DDL carries no customer macros.
1. Applies the quorum session settings below.
1. Retries a request that fails with error 286 (`UNSATISFIED_QUORUM`), error 289 (`REPLICA_IS_NOT_IN_QUORUM`), or a Keeper session error. The backoff is linear, 100 ms per attempt, capped at 1 s, up to 20 attempts. All of these errors are transient by design. Serialized quorum inserts collide. A sequential-consistency read can land on a replica that has not received the last quorum write. A Keeper leader election after a node loss expires open sessions for a few seconds. A lagging replica catches up within seconds, and a load balancer that spreads requests moves the retry to another replica.

On the datalake connection only the session settings and the retry apply. GitLab Orbit never runs DDL on the datalake.

| Setting | Value | Reason |
|---------|-------|--------|
| `insert_quorum` | `auto` | Majority quorum that tracks the replica count. A fixed number stops being a majority once replicas are added. |
| `insert_quorum_parallel` | `0` | Required for `select_sequential_consistency` to take effect. Serializes quorum inserts per table. |
| `select_sequential_consistency` | `1` | A read on a lagging replica errors instead of returning stale rows. |
| `async_insert` | `0` | ClickHouse rejects an async insert that also carries `insert_quorum`, and servers since 26.x default `async_insert` on. Client-sent async-insert settings are also suppressed. |

Anything set in `session_settings` wins over these, so you can still pin a fixed quorum size:

```yaml
graph:
  replicated: true
  session_settings:
    insert_quorum: "2"
```

Expect two costs. Writes create more parts and more merge work, because async inserts coalesced the many small per-page writes and each one now becomes its own part. Write throughput drops, because `insert_quorum_parallel: 0` serializes quorum inserts per table.

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

| Config path | Default | Description |
|-------------|---------|-------------|
| `engine.max_concurrent_workers` | derived | Global concurrency cap |
| `engine.concurrency_groups` | derived | Named group limits |

### Resource-derived defaults

These fields derive from the container's resources at startup when left unset; an explicit config value always wins. Each derived value logs once at info level with its input, so an operator can read a pod's choice from its logs without exec'ing in.

- `max_concurrent_workers` unset → the container's available parallelism (`std::thread::available_parallelism`, which is cgroup-aware on Linux, so it tracks the pod's CPU limit), capped by the cgroup memory limit at 1.5 GiB per worker so a CPU-rich but memory-tight pod cannot derive more workers than it can feed. The budget is calibrated on production's hand-tuned pools (code: 16 workers in 24 GiB, sdlc: 20 in 32 GiB). No readable memory limit (unlimited cgroup, bare metal, macOS) means CPU alone decides.
- `concurrency_groups` empty → derived from the modules the pool registers (`engine.modules`) and the resolved worker cap. A single-group pool gives that group the whole cap; a pool spanning both the SDLC and code groups splits the cap 75% / 25% (the historical universal-pool ratio of sdlc 12 / code 4 out of 16). Namespace deletion shares the sdlc group.
- `handlers.entity-handler.datalake_batch_size` unset → the SDLC datalake page size scales with the cgroup memory limit, anchored at prod's 32 GiB sdlc pool (its hand-tuned 500k page), floored at 100k rows so a memory-scarce host can't OOM on a full page. No readable memory limit means the 500k anchor default. `batch_size_overrides` still apply on top per entity.

On a 16-core universal pool this reproduces the previous hardcoded defaults exactly (16 workers, sdlc 12 / code 4).

### How concurrency groups work

Each handler can declare a `concurrency_group`. When a message arrives, the handler acquires the group semaphore first, then the global semaphore. Both are released after processing.

This prevents one handler type from monopolizing all workers. To pin explicit values instead of deriving them, e.g. 16 global workers capped at SDLC 12 and code 4:

```yaml
engine:
  max_concurrent_workers: 16
  concurrency_groups:
    sdlc: 12
    code: 4
```

## Topic configuration

Each topic's default subscription policy (retry, DLQ, concurrency group) is
**declared in Rust** by the indexer module that owns the topic, next to the
handler it protects:

- `code-indexing-task` — `crates/indexer/src/modules/code/mod.rs`
- `global-handler`, `namespace-handler` — `crates/indexer/src/modules/sdlc/mod.rs`
- `namespace-deletion` — `crates/indexer/src/modules/namespace_deletion/mod.rs`

Because the policy lives in code, a deployment that omits config still gets the
correct policy — omitting `engine.topics` no longer silently disables
`code-indexing-task` retries and dead-lettering.

An `engine.topics.<name>` entry in YAML is a **sparse, field-wise override**
layered on top of the declared default: only the fields the entry sets change;
every unset field keeps the module default. `dead_letter_on_exhaustion` is
`Option<bool>`, so an entry can explicitly turn it off, not just on.

| Config path | Overrides | Description |
|-------------|-----------|-------------|
| `topics.<name>.concurrency_group` | declared default | Which group semaphore to use |
| `topics.<name>.max_attempts` | declared default | Total attempts (1 = no retry, 5 = 4 retries) |
| `topics.<name>.retry_interval_secs` | declared default | Delay between retries (NATS nack delay) |
| `topics.<name>.dead_letter_on_exhaustion` | declared default | Route exhausted retries to the DLQ |

### Default topic settings

These are the module-declared defaults (canonical values, matching the production
Helm chart). They apply with no `engine.topics` config at all:

| Topic | concurrency_group | max_attempts | retry_interval_secs | dead_letter_on_exhaustion |
|-------|-------------------|--------------|---------------------|---------------------------|
| `global-handler` | `sdlc` | 1 | — | — |
| `namespace-handler` | `sdlc` | 1 | — | — |
| `code-indexing-task` | `code` | 5 | 60 | true |
| `namespace-deletion` | `sdlc` | 1 | — | — |

To override a single field, e.g. raise code retries to 8:

```yaml
engine:
  topics:
    code-indexing-task:
      max_attempts: 8
```

### Handler-specific settings

#### SDLC entity handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `engine.handlers.entity-handler.datalake_batch_size` | derived | Rows per datalake extraction query (see [Resource-derived defaults](#resource-derived-defaults)) |
| `engine.handlers.entity-handler.batch_size_overrides.<Entity>` | None | Per-entity override for datalake batch size |

Initial-load partition parallelism is no longer configured here; a pipeline declares `extract.partition_count` in its ontology node YAML (e.g. `config/ontology/nodes/ci/job.yaml`).

#### Code indexing task handler

| Config path | Default | Description |
|-------------|---------|-------------|
| `engine.handlers.code-indexing-task.pipeline.max_file_size_bytes` | `5,000,000` | Largest source file the v2 pipeline will parse |
| `engine.handlers.code-indexing-task.pipeline.max_files` | `1,000,000` | Maximum language-supported source files accepted for one pipeline run |
| `engine.handlers.code-indexing-task.pipeline.worker_threads` | `0` | Rayon workers per language; `0` uses Rayon default |
| `engine.handlers.code-indexing-task.pipeline.max_concurrent_languages` | `0` | Concurrent language pipelines; `0` uses the pipeline default |

### Retry strategy by topic

| Topic | max_attempts | DLQ | Rationale |
|-------|-------------|-----|-----------|
| `global-handler` (SDLC) | 1 | No | Re-dispatched every cycle. No need to retry. |
| `namespace-handler` (SDLC) | 1 | No | Re-dispatched every cycle. No need to retry. |
| `code-indexing-task` | 5 | Yes | Event-driven. Won't be re-dispatched. Must retry and DLQ. |
| `namespace-deletion` | 1 | No | Re-dispatched on next scheduler cycle. |

## Scheduler configuration

Scheduled tasks run in `DispatchIndexing` mode. Each scheduled task has a 6-field cron expression (seconds, minutes, hours, day-of-month, month, day-of-week). Every task's default cron is declared in `config/default.yaml` under `schedule.tasks`; a `schedule.tasks.<name>` entry in an overlay replaces only the fields you set. The cron expression is required and is parsed when the configuration loads, so a missing or invalid expression fails startup.

Distributed locking via NATS KV ensures only one dispatcher instance runs each scheduled task per interval. Raw Siphon routing is a separate continuous trigger and does not use a cron expression.

| Task | Config path | Default cron | Description |
|------|-------------|-------------|-------------|
| Global dispatch | `schedule.tasks.global.cron` | `0 */1 * * * *` (every minute) | Publishes `GlobalIndexingRequest` |
| Namespace dispatch | `schedule.tasks.namespace.cron` | `*/30 * * * * *` (every 30 seconds) | Publishes requests for changed enabled root namespaces and performs the integrated namespace sweep when due |
| Code backfill | `schedule.tasks.code-backfill.cron` | `0 */1 * * * *` (every minute) | Backfills enabled namespaces whose projects do not yet have code checkpoints, then sweeps stale code rows for a bounded number of drained namespaces |
| Table cleanup | `schedule.tasks.table-cleanup.cron` | `0 0 3 * * 0` (weekly, Sunday 03:00 UTC) | Runs `APPLY DELETED MASK` on every graph table to physically remove lightweight-deleted rows |
| Namespace deletion | `schedule.tasks.namespace-deletion.cron` | `0 0 3 * * *` (daily 03:00 UTC) | Schedules and executes namespace deletions |
| Migration completion | `schedule.tasks.migration-completion.cron` | `0 */1 * * * *` (every minute) | Detects completed schema migrations and reconciles dead versions |
| Stale-edge reconciliation | `schedule.tasks.stale-edge-reconciliation.cron` | `0 */30 * * * *` (every 30 minutes) | Tombstones stale mutable-FK edges |

The namespace dispatcher is checkpoint-driven. With no change-detection checkpoint it
dispatches every enabled namespace once (cold start) and records a checkpoint;
every later tick queries Siphon-backed datalake tables for changes since that checkpoint,
however old it is. The same task re-dispatches every enabled namespace when its separate
sweep checkpoint is older than `schedule.tasks.namespace.sweep_interval_secs` (default
`3600`), backstopping migration backfill and missed windows.

`APPLY DELETED MASK` is idempotent. A failed or skipped run is safe — the next
run picks up all outstanding masks. Alert on
`gkg.scheduler.task.errors{task="maintenance.table_cleanup"}`; the task logs a
failed table and moves on.

### Continuous Siphon router settings

DispatchIndexing continuously polls the raw Siphon JetStream and routes code-task and enabled-namespace CDC events into Orbit's internal request stream. It drains each supported source-table subject until no pending messages remain, waits one second, and polls again.

| Config path | Default | Description |
|-------------|---------|-------------|
| `schedule.tasks.siphon.events_stream_name` | `siphon_stream_main_db` | Raw NATS stream containing Siphon CDC events |
| `schedule.tasks.siphon.batch_size` | `100` | Pending messages consumed per route and drain call |

### Scheduled task settings

| Config path | Default | Description |
|-------------|---------|-------------|
| `schedule.tasks.namespace.sweep_interval_secs` | `3600` | Age at which the namespace dispatcher performs a full enabled-namespace sweep instead of change-only dispatch |
| `schedule.tasks.stale-edge-reconciliation.lookback_secs` | `3600` | Recent node-version window rescanned on each stale-edge reconciliation run |
| `schedule.tasks.code-backfill.publish_window` | `200000` | Pending projects held per publish batch. Also the per-run budget shared between the namespaces that still have pending projects, so it bounds both dispatcher memory (about 70 bytes per project) and how much work one namespace can queue ahead of the others |
| `schedule.tasks.code-backfill.stale_sweeps_per_tick` | `10` | Drained namespaces whose stale code rows are swept per backfill tick. The sweep runs inside the tick, so the cap bounds how long dispatch pauses; the remaining namespaces are swept on later ticks. `0` pauses sweeping |

## GitLab client

Required for code indexing (repository archive download) and authorization.

| Config path | Default | Description |
|-------------|---------|-------------|
| `gitlab.base_url` | None | GitLab instance URL |
| `gitlab.jwt.signing_key` | None | JWT signing key (for creating tokens) |
| `gitlab.jwt.verifying_key` | (required) | JWT verification key |
| `gitlab.resolve_host` | None | Override DNS resolution for GitLab |

## Observability

### Logging

| Config path | Default | Description |
|-------------|---------|-------------|
| `metrics.log_level` | None | Rust log filter string |

Example: `info,orbit_server=debug,gkg_indexer=trace`

### OpenTelemetry

| Config path | Default | Description |
|-------------|---------|-------------|
| `metrics.otel.enabled` | `false` | Enable OTEL tracing |
| `metrics.otel.endpoint` | `http://localhost:4317` | OTEL gRPC collector endpoint |

### Prometheus

| Config path | Default | Description |
|-------------|---------|-------------|
| `metrics.prometheus.enabled` | `false` | Expose the `/-/metrics` scrape endpoint |
| `metrics.prometheus.port` | `9394` | Prometheus scrape port |

## Webserver

These settings are used by the Webserver mode.

### General

| Config path | Default | Description |
|-------------|---------|-------------|
| `bind_address` | `127.0.0.1:4200` | HTTP server bind address |
| `grpc_bind_address` | `127.0.0.1:50054` | gRPC server bind address |
| `jwt_clock_skew_secs` | `60` | Allowed JWT clock skew in seconds |
| `health_check_url` | None | Optional health check URL |

### TLS

| Config path | Default | Description |
|-------------|---------|-------------|
| `tls.cert_path` | None | TLS certificate path (PEM) |
| `tls.key_path` | None | TLS private key path (PEM) |

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
| `grpc.max_header_list_size_bytes` | `65536` (64 KiB) | HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` advertised to clients. tonic/hyper default of 16 KiB is too small for GitLab JWTs carrying many traversal IDs. |

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
| `query.default.max_memory_usage` | unset | ClickHouse `max_memory_usage` in bytes |
| `query.default.max_bytes_to_read` | unset | ClickHouse `max_bytes_to_read` in bytes |
| `query.default.max_rows_to_read` | unset | ClickHouse `max_rows_to_read` |
| `query.default.max_rows_in_set` | unset | ClickHouse `max_rows_in_set` (IN subquery cap) |
| `query.default.use_query_cache` | `false` | Enable ClickHouse query cache |
| `query.default.query_cache_ttl` | `60` | Query cache TTL in seconds |

## Schema management

| Config path | Default | Description |
|-------------|---------|-------------|
| `schema.max_retained_versions` | `2` | Active-plus-retired keep-set size (min 2); every migrating version is retained in addition |

## Analytics

Controls Snowplow product-analytics event emission. Events carry `orbit_common` and `orbit_query` contexts (consumer-owned, defined in `orbit-analytics`). Disabled by default -- Helm enables it for .com and Dedicated.

| Config path | Default | Description |
|-------------|---------|-------------|
| `analytics.enabled` | `false` | Enable Snowplow analytics event emission |
| `analytics.collector_url` | `""` | Snowplow collector endpoint (e.g. `https://events.gitlab.net`) |
| `analytics.deployment.type` | `self_managed` | `com`, `dedicated`, or `self_managed` |
| `analytics.deployment.environment` | `development` | `development`, `staging`, or `production` |

Example for the .com staging cluster:

```yaml
analytics:
  enabled: true
  collector_url: "https://events.gitlab.net"
  deployment:
    type: com
    environment: staging
```

## Billing

Controls Snowplow billing-event emission and the CDot quota gate that enforces GitLab credit limits on metered Orbit queries.

### Billing events

| Config path | Default | Description |
|-------------|---------|-------------|
| `billing.enabled` | `false` | Enable Snowplow billing-event emission |
| `billing.collector_url` | `""` | Snowplow collector endpoint |

### Quota gate

When enabled, every metered Orbit query (`mcp`, `rest` source types) is checked against CDot before execution. Requests from namespaces with exhausted credits are rejected with `codes.ResourceExhausted`.

| Config path | Default | Description |
|-------------|---------|-------------|
| `billing.quota.enabled` | `false` | Enable the CDot quota gate |
| `billing.quota.customers_dot_url` | `""` | CDot base URL (e.g. `https://customers.gitlab.com`) |
| `billing.quota.request_timeout_ms` | `1000` | CDot request timeout in milliseconds |
| `billing.quota.api_user` | None | CDot admin email. Mounted from `/etc/secrets/billing/quota/api_user`. |
| `billing.quota.api_token` | None | CDot admin token. Mounted from `/etc/secrets/billing/quota/api_token`. |

## Object storage

Names the bucket Orbit will use for cold storage and how to authenticate to it. Disabled by default; nothing reads the store yet. The `orbit-object-storage` crate turns this section into an `object_store` client for S3, S3-compatible stores, Google Cloud Storage, or a local directory.

| Config path | Default | Description |
|-------------|---------|-------------|
| `object_storage.enabled` | `false` | Enable the object store |
| `object_storage.provider` | `s3` | `s3` (AWS and S3-compatible), `gcs`, or `local` (a directory, for development and tests) |
| `object_storage.bucket` | `""` | Bucket name, or the directory path for `local` |
| `object_storage.prefix` | `""` | Key prefix under which every object is placed |
| `object_storage.auth` | `identity` | `identity` uses the runtime (IRSA, EC2 instance profile, `AWS_*` variables, GKE Workload Identity, GCE metadata server, `GOOGLE_APPLICATION_CREDENTIALS`, gcloud ADC); `static` uses the credentials below |
| `object_storage.region` | unset | S3 region; required for AWS, ignored by most S3-compatible stores |
| `object_storage.endpoint` | unset | S3-compatible store URL, or a GCS emulator or private endpoint |
| `object_storage.path_style` | `false` | S3 only. With a custom endpoint and `false`, the endpoint host must include the bucket |
| `object_storage.allow_http` | `false` | Permit `http://` endpoints; local development only |
| `object_storage.ca_cert_path` | unset | PEM bundle of extra root certificates for stores behind a private CA |
| `object_storage.access_key_id` | unset | S3 static credentials; mount at `/etc/secrets/object_storage/access_key_id` |
| `object_storage.secret_access_key` | unset | S3 static credentials; mount at `/etc/secrets/object_storage/secret_access_key` |
| `object_storage.session_token` | unset | S3 static credentials, optional |
| `object_storage.service_account_key` | unset | GCS static credentials, JSON key content; mount at `/etc/secrets/object_storage/service_account_key` |

Identity on GitLab.com and Dedicated:

```yaml
object_storage:
  enabled: true
  provider: gcs
  bucket: gitlab-orbit-stg-storage
  prefix: orbit
```

MinIO behind a private CA, keys mounted as secret files:

```yaml
object_storage:
  enabled: true
  provider: s3
  bucket: orbit
  auth: static
  endpoint: https://minio.internal:9000
  path_style: true
  ca_cert_path: /etc/ssl/private-ca.pem
```

Local directory for development and tests (the directory must exist):

```yaml
object_storage:
  enabled: true
  provider: local
  bucket: /tmp/orbit-store
  prefix: dev
```

Round-trip a config file against the bucket it names with the throwaway example:

```shell
cargo run -p orbit-object-storage --example roundtrip -- config.yaml [secrets-dir]
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
    entity-handler:
      datalake_batch_size: 5000000
```

### Reduce NATS pressure

Increase ack wait for slow handlers:

```yaml
nats:
  ack_wait_secs: 600  # 10 minutes instead of default 5
```

### Handle large CDC backlogs

Increase the continuous Siphon router batch size:

```yaml
schedule:
  tasks:
    siphon:
      batch_size: 500
```

### Production NATS settings

```yaml
nats:
  consumer_name: gkg-indexer   # Durable consumer (survives restarts)
  stream_replicas: 3           # Fault tolerance
  auto_create_streams: true    # Auto-create on startup
```

## Helm chart configuration

In production, GKG is deployed via the [`orbit-helm-charts`](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts). Most configuration is set through Helm values rather than raw YAML. The chart renders the values it knows about into a ConfigMap mounted at `/app/config`; every key the chart does not render comes from the embedded `config/default.yaml`.

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
helm upgrade gkg orbit-helm-charts/gkg \
  --set nats.consumerName=gkg-indexer \
  --set clickhouse.graph.database=gkg-production
```

For complex overrides, use a values file:

```shell
helm upgrade gkg orbit-helm-charts/gkg -f custom-values.yaml
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

The chart mounts its ConfigMap at `/app/config/default.yaml`; every key absent from it runs on the embedded default.

```shell
kubectl -n gkg exec deployment/gkg-indexer -- cat /app/config/default.yaml
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
  topics:
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
      dead_letter_on_exhaustion: true
    namespace-deletion:
      concurrency_group: code
      max_attempts: 1
  handlers:
    entity-handler:
      datalake_batch_size: 1000000
    code-indexing-task:
      pipeline:
        max_file_size_bytes: 5000000
        max_files: 1000000
        worker_threads: 0
        max_concurrent_languages: 0

schedule:
  tasks:
    table-cleanup:
      cron: "0 0 3 * * 0"
    namespace-deletion:
      cron: "0 0 3 * * *"
    migration-completion:
      cron: "0 */1 * * * *"

metrics:
  log_level: info,orbit_server=debug
  prometheus:
    enabled: true
    port: 9394
```
