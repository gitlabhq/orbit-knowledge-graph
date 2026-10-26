# Server configuration

`gkg-server` reads the same configuration in all modes. The default values are in
[`config/default.yaml`](../../../../config/default.yaml). The types and allowed values are in [`config/schemas/config.schema.json`](../../../../config/schemas/config.schema.json).

## Load order

1. The embedded `config/default.yaml`. It declares every key, so a removed key stops startup.
2. One overlay file: `--config <path>`, or else `config/config.yaml` when it exists.
3. Secret files under `/etc/secrets/`, for example `/etc/secrets/billing/quota/api_token`.

The server reads only two environment variables: `RUST_LOG`, when `metrics.log_level` is
unset, and `GKG_VERSION`. To add a setting, add the field to
`crates/orbit-server-config/` and its value to `config/default.yaml`. For the local overlay
files, see [local development](local-development.md).

## Modes

Select the mode with `--mode`. The default is `webserver`.

| Mode | Runs | Main sections |
| --- | --- | --- |
| `webserver` | gRPC query server | `grpc_bind_address`, `grpc`, `tls`, `query`, `graph`, `gitlab` |
| `indexer` | Consumes NATS messages and runs the indexing handlers | `nats`, `engine`, `graph`, `datalake`, `gitlab`, `schema` |
| `dispatch-indexing` | Scheduler that publishes indexing requests | `nats`, `schedule`, `graph`, `datalake`, `schema` |
| `health-check` | Kubernetes workload, ClickHouse and NATS queue health | `health_check`, `graph`, `datalake`, `nats` |
| `clickhouse-setup` | Applies `config/clickhouse-setup.sql`, then exits | `clickhouse_setup`, `graph`, `datalake` |

## NATS

| Key | Effect |
| --- | --- |
| `nats.url` | Broker address |
| `nats.username`, `nats.password` | Credentials |
| `nats.tls_ca_cert_path` | CA certificate (PEM). Any TLS path turns on TLS. |
| `nats.tls_cert_path`, `nats.tls_key_path` | Client certificate and key for mTLS. Set both. |
| `nats.connection_timeout_secs`, `nats.request_timeout_secs` | Connection and request timeouts |
| `nats.consumer_name` | Durable consumer name. Unset gives an ephemeral consumer. |
| `nats.ack_wait_secs` | Time before NATS sends an unacknowledged message again |
| `nats.max_deliver` | Maximum deliveries of one message. Unset means no limit. |
| `nats.batch_size` | Messages fetched per batch |
| `nats.subscription_buffer_size` | Buffer between the fetch loop and the handlers |
| `nats.fetch_expires_secs` | Server-side timeout of one batch fetch |
| `nats.auto_create_streams` | Create the streams at startup |
| `nats.stream_replicas` | Replicas per stream. |
| `nats.stream_max_age_secs`, `nats.stream_max_bytes`, `nats.stream_max_messages` | Stream limits |
| `nats.consumer_inactive_threshold_secs` | Idle time before NATS deletes a versioned durable consumer |
| `nats.release_gc_idle_threshold_secs` | Idle time before a dispatcher deletes the streams of another release |

The indexing stream is `GKG_INDEXER_V<release>`. To inspect it, see [NATS](../runbooks/nats.md).

## ClickHouse

Orbit uses two connections. `datalake` reads the tables that Siphon replicates. `graph` holds the
indexed graph. Both take the same keys.

| Key | Effect |
| --- | --- |
| `<connection>.url` | HTTP endpoint |
| `<connection>.database` | Database name |
| `<connection>.username`, `<connection>.password` | Credentials |
| `<connection>.session_settings` | ClickHouse settings for every query on the connection |
| `<connection>.insert_settings` | ClickHouse settings for inserts only |
| `<connection>.replicated` | Self-hosted ClickHouse cluster with more than one replica |
| `<connection>.profiling.*` | Debug only: EXPLAIN output, query log, processor statistics, instance health |

### Self-hosted replicated clusters

Set `replicated: true` on both connections when they point at a self-hosted cluster with more
than one replica. Keep `false` on a single node and on ClickHouse Cloud. The `orbit` Helm chart
sets both from `clickhouse.ha.enabled`.

The ClickHouse administrator must create the graph database with the `Replicated` engine:

```sql
CREATE DATABASE orbit ON CLUSTER '{cluster}'
ENGINE = Replicated('/clickhouse/databases/orbit', '{shard}', '{replica}');
```

On the graph connection, the switch adds `Replicated` to every `*MergeTree` engine in the DDL.
On both connections, it applies quorum settings and retries transient replica errors. Orbit sends
these session settings:

| Setting | Value | Reason |
| --- | --- | --- |
| `insert_quorum` | `auto` | A majority quorum that follows the replica count |
| `insert_quorum_parallel` | `0` | Necessary for `select_sequential_consistency` |
| `select_sequential_consistency` | `1` | A read on a replica that lags fails instead of returning old rows |
| `async_insert` | `0` | ClickHouse rejects an async insert with `insert_quorum` |

A value in `session_settings` overrides these, for example a fixed `insert_quorum: "2"`. Writes
become slower and make more parts, because quorum inserts run one at a time per table.

## Worker pool

| Key | Effect |
| --- | --- |
| `engine.modules` | Indexer modules that this process runs: `sdlc`, `code`, `namespace_deletion` |
| `engine.max_concurrent_workers` | Global limit of concurrent handlers |
| `engine.concurrency_groups.<group>` | Limit for one group, for example `sdlc` or `code` |

A handler takes its group slot first, then a global slot. So one handler type cannot use all
workers. When these keys are unset, the server derives them from the container resources:

- `max_concurrent_workers`: the CPU limit, capped by the memory limit.
- `concurrency_groups`: one group gets the full limit. `sdlc` and `code` together share it 75% and 25%.

Each derived value writes one log line at startup.

## Topics

Each indexer module declares the default policy of its topics in code, next to the handler. Only
`code-indexing-task` sends failed messages to the dead letter stream by default. An
`engine.topics.<topic>` entry changes only the fields that it sets.

| Key | Effect |
| --- | --- |
| `engine.topics.<topic>.concurrency_group` | Group limit that the handler uses |
| `engine.topics.<topic>.max_attempts` | Total attempts. `1` means no retry. |
| `engine.topics.<topic>.retry_interval_secs` | Delay before a retry |
| `engine.topics.<topic>.dead_letter_on_exhaustion` | Send the message to the dead letter stream after the last attempt |

The topics are `global-handler`, `namespace-handler`, `code-indexing-task` and `namespace-deletion`.

## Handlers

| Key | Effect |
| --- | --- |
| `engine.handlers.entity-handler.datalake_batch_size` | Rows per `datalake` query. Unset scales with the memory limit. |
| `engine.handlers.entity-handler.batch_size_overrides.<Entity>` | Rows per `datalake` query for one entity |
| `engine.handlers.code-indexing-task.pipeline.max_file_size_bytes` | Largest source file that the pipeline parses |
| `engine.handlers.code-indexing-task.pipeline.max_files` | Maximum source files in one pipeline run |
| `engine.handlers.code-indexing-task.pipeline.worker_threads` | Parser threads per language. `0` uses the Rayon default. |
| `engine.handlers.code-indexing-task.pipeline.max_concurrent_languages` | Languages parsed at the same time. `0` uses the pipeline default. |

The other `pipeline` keys set timeouts and memory budgets. See `config/default.yaml`.

## Scheduler

The `dispatch-indexing` mode runs the scheduled tasks. Each task has a 6-field cron expression
(seconds first) in `schedule.tasks.<task>.cron`. An invalid expression stops startup. A NATS
KV lock makes sure that only one dispatcher runs a task per interval.

| Task | Effect |
| --- | --- |
| `global` | Publishes the global indexing request |
| `namespace` | Publishes requests for enabled root namespaces that changed, and sweeps all of them when due |
| `code-backfill` | Indexes code for enabled namespaces without code checkpoints, then removes old code rows |
| `table-cleanup` | Runs `APPLY DELETED MASK` on every graph table |
| `namespace-deletion` | Schedules and runs namespace deletions |
| `migration-completion` | Finds completed schema migrations and removes old versions |
| `stale-edge-reconciliation` | Marks old edges of mutable foreign keys as deleted |
| `namespace-freshness` | Records how far indexed SDLC data trails the `datalake` tables |

| Key | Effect |
| --- | --- |
| `schedule.tasks.namespace.sweep_interval_secs` | Age of the last full sweep before the next one |
| `schedule.tasks.stale-edge-reconciliation.lookback_secs` | Time window that each run scans again |
| `schedule.tasks.code-backfill.publish_window` | Projects per publish batch and per run |
| `schedule.tasks.code-backfill.stale_sweeps_per_tick` | Namespaces cleaned of old code rows per run. `0` pauses the cleanup. |
| `schedule.tasks.siphon.events_stream_name` | Siphon stream that the router reads |
| `schedule.tasks.siphon.batch_size` | Messages per route and read |

The Siphon router has no cron. It reads the Siphon stream all the time and sends code tasks and
namespace changes to the indexing stream.

## GitLab client

| Key | Effect |
| --- | --- |
| `gitlab.base_url` | GitLab instance URL for repository downloads and authorization |
| `gitlab.jwt.verifying_key` | Key that verifies the JWT from GitLab. Necessary. |
| `gitlab.jwt.signing_key` | Key that signs the JWTs that Orbit sends to GitLab |
| `gitlab.resolve_host` | Address to use for the GitLab host name instead of DNS |

## Observability

| Key | Effect |
| --- | --- |
| `metrics.log_level` | Log filter, for example `info,orbit_server=debug` |
| `metrics.expanded_logs.global_override` | Write expanded fields (paths, query text, errors) on all log lines |
| `metrics.expanded_logs.root_namespace_ids` | Root namespaces whose work always gets expanded fields |
| `metrics.otel.enabled`, `metrics.otel.endpoint` | OpenTelemetry tracing and its gRPC collector |
| `metrics.prometheus.enabled` | Add `/-/metrics` to the probe server |
| `metrics.prometheus.port` | Deprecated. Use `probe_server.bind_address`. |
| `probe_server.bind_address` | Address of `/-/liveness`, `/-/readiness` and `/-/metrics` in every mode |

The server writes debug and trace lines only when expanded fields are on. A span directive, for
example `[orbit_request]=debug`, also turns on the lines of dependencies in that span.

## Webserver

| Key | Effect |
| --- | --- |
| `grpc_bind_address` | gRPC listen address |
| `jwt_clock_skew_secs` | Allowed JWT clock skew |
| `health_check_url` | URL of the health-check service |
| `grpc.keepalive_interval_secs`, `grpc.keepalive_timeout_secs` | HTTP/2 keepalive ping |
| `grpc.tcp_keepalive_secs` | TCP keepalive |
| `grpc.connection_window_size`, `grpc.stream_window_size` | HTTP/2 flow control windows |
| `grpc.concurrency_limit` | Maximum concurrent requests |
| `grpc.max_connection_age_secs` | Connection age before the server closes it, to balance the load balancer |
| `grpc.max_connection_age_grace_secs` | Drain time after the maximum age. Must not be `0`. |
| `grpc.stream_timeout_secs` | Stream timeout |
| `grpc.max_header_list_size_bytes` | Largest HTTP/2 header list. GitLab JWTs with many traversal IDs need more than 16 KiB. |
| `grpc.result_chunk_bytes` | Largest result chunk. 64 KiB to 4 MiB, because GitLab rejects messages over 8 MiB. |
| `grpc.max_result_bytes` | Largest chunked result. A larger result fails before the first chunk. |

### TLS

| Key | Effect |
| --- | --- |
| `tls.cert_path`, `tls.key_path` | Certificate and key (PEM). Both turn on TLS on the gRPC port. |
| `tls.internal.enabled` | TLS on the probe server and the health-check API |
| `tls.internal.cert_path`, `tls.internal.key_path` | Internal certificate. Unset uses `tls.cert_path` and `tls.key_path`. |

The server reads certificates at startup. Restart the pods after a rotation.

### Turn on internal TLS

FedRAMP SC-8 needs TLS between pods. When you turn on `tls.internal`, change the clients too:

1. Set `scheme: HTTPS` on the Kubernetes probes. Kubernetes does not verify the certificate.
2. Change `health_check_url` to `https://`. Give the certificate a SAN for the health-check
   service name. Put its CA in `/etc/pki/tls/certs`.
3. Change the KEDA scalers that read `/queue-depth` to HTTPS.
4. Set `scheme: https` on the PodMonitor. Trust the CA with `serverName`, or set `insecureSkipVerify`.

Check: the pods are ready and Prometheus scrapes `/-/metrics`.

### Query settings

`query.default` applies to all queries. A query type key, for example `query.aggregation`,
overrides it for that type.

| Key | Effect |
| --- | --- |
| `query.<type>.max_execution_time` | ClickHouse `max_execution_time` in seconds |
| `query.<type>.max_memory_usage`, `max_bytes_to_read`, `max_rows_to_read` | ClickHouse limits |
| `query.<type>.max_rows_in_set` | Limit of an `IN` subquery |
| `query.<type>.max_ast_elements`, `max_expanded_ast_elements`, `max_ast_depth` | ClickHouse query size limits |
| `query.<type>.use_query_cache` | ClickHouse query cache. Indexing reads and queries that pin every node by ID skip it. |
| `query.<type>.query_cache_ttl` | Maximum age of a cached result in seconds |

## Schema

| Key | Effect |
| --- | --- |
| `schema.max_retained_versions` | Active and retired schema versions to keep. Migrating versions are kept too. |
| `schema.version_poll_interval_secs` | Retry delay when the server cannot read the active schema version |
| `schema.indexer_schema_wait_timeout_secs` | Time an indexer waits for the dispatcher to prepare its schema version, then exits |

## Analytics

| Key | Effect |
| --- | --- |
| `analytics.enabled` | Snowplow product events. `null` follows the GitLab product usage data setting. |
| `analytics.collector_url` | Snowplow collector, without a path |
| `analytics.deployment.type` | `com`, `dedicated` or `self_managed` |
| `analytics.deployment.environment` | `development`, `staging` or `production` |

## Billing

| Key | Effect |
| --- | --- |
| `billing.enabled` | Snowplow billing events |
| `billing.collector_url` | Billing collector. GitLab Self-Managed and GitLab Dedicated use the host of the Cloud Connector token audience. |
| `billing.auth_mode` | `oidc` (GitLab.com workload identity) or `cloud_connector` (GitLab Self-Managed and GitLab Dedicated) |
| `billing.quota.enabled` | Check metered queries (`mcp`, `rest`) against CustomersDot before they run |
| `billing.quota.customers_dot_url` | CustomersDot URL |
| `billing.quota.auth_mode` | `admin_token` (GitLab.com) or `license_checksum` (GitLab Self-Managed and GitLab Dedicated) |
| `billing.quota.api_user`, `billing.quota.api_token` | CustomersDot credentials for `admin_token`, from `/etc/secrets/billing/quota/` |
| `billing.quota.request_timeout_ms` | CustomersDot request timeout |
| `billing.quota.fallback_cache_ttl_secs` | Cache time of a decision when CustomersDot sends no `max-age` |

When credits are exhausted, the query fails with `ResourceExhausted`. In `license_checksum` mode,
a request without a license checksum claim skips the check and logs a warning.

Images built with `ORBIT_BILLING_ENFORCED=true` do not start unless billing and the quota gate are
on with an approved URL pair. See [ADR 020](../adr/020_billing_sox_scope.md) and the
[SOX billing boundary](sox-billing-boundary.md).

## Object storage

| Key | Effect |
| --- | --- |
| `object_storage.enabled` | Turn on the object store |
| `object_storage.provider` | `s3` (AWS and S3-compatible), `gcs`, or `local` (a directory, for tests) |
| `object_storage.bucket` | Bucket name, or the directory for `local` |
| `object_storage.prefix` | Key prefix of every object |
| `object_storage.auth` | `identity` uses the runtime identity. `static` uses the keys below. |
| `object_storage.region` | S3 region. AWS needs it. |
| `object_storage.endpoint` | URL of an S3-compatible store or a GCS emulator |
| `object_storage.path_style` | S3 path-style URLs. With `false`, the endpoint host must include the bucket. |
| `object_storage.allow_http` | Allow `http://` endpoints. Local development only. |
| `object_storage.ca_cert_path` | Extra root certificates (PEM) for a private CA |
| `object_storage.access_key_id`, `secret_access_key`, `session_token` | S3 static keys, from `/etc/secrets/object_storage/` |
| `object_storage.service_account_key` | GCS service account key (JSON), from `/etc/secrets/object_storage/` |

## ClickHouse setup

Only the `clickhouse-setup` mode reads these keys. It connects to `graph.url` as the
administrator and creates the graph database, the Orbit users, their roles, and their grants. A second
run sets the passwords again, so it also rotates them.

| Key | Effect |
| --- | --- |
| `clickhouse_setup.admin_username` | ClickHouse user that runs the statements |
| `clickhouse_setup.admin_password` | From `/etc/secrets/clickhouse_setup/admin_password` |
| `clickhouse_setup.writer_password` | Password of `gkg_writer` |
| `clickhouse_setup.reader_password` | Password of `gkg_reader` |
| `clickhouse_setup.siphon_reader_password` | Password of `gkg_siphon_reader` |

With `graph.replicated: true`, the mode first checks two things. Users must be in a replicated
user directory, and the graph database must use the `Replicated` engine. If a check fails, the
mode stops before it changes anything.

## Health check

| Key | Effect |
| --- | --- |
| `health_check.bind_address` | Listen address of the `health-check` mode |
| `health_check.targets` | Namespaces, deployments and stateful sets to check |
