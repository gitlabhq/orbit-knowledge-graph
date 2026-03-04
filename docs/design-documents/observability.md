# Observability

## Summary

This document covers how we monitor the Knowledge Graph services (Indexer and Web Service): metrics, logs, tracing, health checks, and what operators need to do for self-managed deployments.

## How Observability Works Today (GitLab.com)

On GitLab.com, the Knowledge Graph services use the existing observability stack:

- **Metrics**: [Grafana](https://grafana.com/) and [Grafana Mimir](https://grafana.com/oss/mimir/)
- **Logs**: [Elasticsearch](https://www.elastic.co/elasticsearch/) and [Logstash](https://www.elastic.co/logstash/)

### SLIs, SLOs, and Metrics

Alerting is based on SLOs and SLIs:

- **Availability SLO**: We will adopt the Dedicated reference of ≥99.5% monthly SLO for the GKG API plane (excluding planned maintenance).
- **Availability SLIs**: We will use error-rate and Apdex SLIs on request latency.

Each service exposes a Prometheus `/metrics` endpoint. We use LabKit for instrumentation where possible.

**Reliability Signals:**

- **Ingest Lag**: The delay between data being written to the Postgres WAL, processed by Siphon, sent through NATS, and ingested.
- **Consumer Health**: Monitoring NATS JetStream delivery/ack rates, dead-letter counts, and stream retention headroom.

**KG Web Server (Indexer and Web Service):**

- HTTP error and success rate by HTTP method and path
- HTTP latency (p50, p95, p99) by HTTP method and path
- gRPC error and success rate by RPC method
- gRPC latency (p50, p95, p99) by RPC method
- gRPC bidi stream duration for `ExecuteQuery` RPCs
- Redaction exchange latency (time spent waiting for Rails authorization responses)

**KG Indexer Service:**

The indexer emits metrics under four OpenTelemetry meters: `etl_engine` for the core engine, `indexer_dispatch` for the dispatch scheduling loop, `indexer_sdlc` for the SDLC module, and `indexer_code` for the code indexing module. All duration histograms use OTel-recommended buckets (5 ms to 10 s).

*Engine metrics (`etl_engine`):*

| Metric | Type | Unit | Labels | Description |
|---|---|---|---|---|
| `etl.messages.processed` | Counter | count | `topic`, `outcome` (ack/nack) | Total messages processed |
| `etl.message.duration` | Histogram | s | `topic` | End-to-end time per message through dispatch |
| `etl.handler.duration` | Histogram | s | `handler` | Time inside each handler's `handle()` call |
| `etl.handler.errors` | Counter | count | `handler`, `error_kind` | Handler errors at the engine dispatch level |
| `etl.permit.wait.duration` | Histogram | s | `permit_kind` (global/group), `group` | Time waiting for a worker pool permit |
| `etl.permits.active` | UpDownCounter | count | `permit_kind` | Worker permits currently held (global or per concurrency group) |
| `etl.nats.fetch.duration` | Histogram | s | `outcome` (success/error) | Time to fetch a batch from NATS |
| `etl.destination.write.duration` | Histogram | s | `table` | Time to write a batch to ClickHouse |
| `etl.destination.rows.written` | Counter | count | `table` | Total rows written to ClickHouse |
| `etl.destination.bytes.written` | Counter | bytes | `table` | Total bytes written to ClickHouse |
| `etl.destination.write.errors` | Counter | count | `table` | Total failed writes to ClickHouse |

*Dispatch metrics (`indexer_dispatch`):*

| Metric | Type | Unit | Labels | Description |
|---|---|---|---|---|
| `indexer.dispatch.runs` | Counter | count | `dispatcher`, `outcome` (success/error) | Total dispatch runs by dispatcher |
| `indexer.dispatch.duration` | Histogram | s | `dispatcher` | End-to-end duration of a dispatch cycle |
| `indexer.dispatch.requests.published` | Counter | count | `dispatcher` | Namespace/global requests successfully published |
| `indexer.dispatch.requests.skipped` | Counter | count | `dispatcher` | Requests skipped due to lock contention |
| `indexer.dispatch.query.duration` | Histogram | s | | Duration of the enabled-namespaces ClickHouse query |
| `indexer.dispatch.errors` | Counter | count | `dispatcher`, `stage` (lock/publish/query) | Dispatch errors by stage |

*SDLC module metrics (`indexer_sdlc`):*

| Metric | Type | Unit | Labels | Description |
|---|---|---|---|---|
| `indexer.sdlc.pipeline.duration` | Histogram | s | `entity` | End-to-end duration of an entity or edge pipeline run |
| `indexer.sdlc.pipeline.rows.processed` | Counter | count | `entity` | Total rows extracted and written |
| `indexer.sdlc.pipeline.edges.processed` | Counter | count | `entity` | Total edges written |
| `indexer.sdlc.pipeline.batches.processed` | Counter | count | `entity` | Total Arrow batches processed |
| `indexer.sdlc.pipeline.errors` | Counter | count | `entity`, `error_kind` | SDLC pipeline failures |
| `indexer.sdlc.handler.duration` | Histogram | s | `handler` | Duration of a full handler invocation |
| `indexer.sdlc.datalake.query.duration` | Histogram | s | `entity` | Duration of ClickHouse datalake extraction queries |
| `indexer.sdlc.transform.duration` | Histogram | s | `entity` | Duration of DataFusion SQL transform per batch |
| `indexer.sdlc.watermark.lag` | Gauge | s | `entity` | Seconds between the current watermark and wall clock (data freshness) |

*Code module metrics (`indexer_code`):*

| Metric | Type | Unit | Labels | Description |
|---|---|---|---|---|
| `indexer.code.events.processed` | Counter | count | `outcome` (indexed, skipped_branch, skipped_watermark, skipped_lock, skipped_project_not_found, error) | Total push events processed by the code handler |
| `indexer.code.handler.duration` | Histogram | s | | End-to-end duration of processing a single push event |
| `indexer.code.repository.fetch.duration` | Histogram | s | | Duration of fetching and extracting a repository from Gitaly |
| `indexer.code.indexing.duration` | Histogram | s | | Duration of code-graph parsing and analysis |
| `indexer.code.write.duration` | Histogram | s | | Duration of writing all graph tables to ClickHouse |
| `indexer.code.files.processed` | Counter | count | `outcome` (parsed, skipped, errored) | Total files seen by the code-graph indexer |
| `indexer.code.nodes.indexed` | Counter | count | `kind` (directory, file, definition, imported_symbol, edge) | Total graph nodes and edges indexed |
| `indexer.code.errors` | Counter | count | `stage` (decode, repository_fetch, repository_extract, indexing, arrow_conversion, write, watermark) | Code indexing errors by pipeline stage |

**KG Web Service:**

- **Query Health**: p50/p95 latency by tool (`find_nodes`, `traverse`, `explore`, `aggregate`), memory spikes, and rows/bytes read per query.
- MCP tools latency (p50, p95, p99), usage and success rate

*Query pipeline metrics (`query_pipeline`):*

The query pipeline instruments end-to-end query execution from security check through formatted output. All histograms and counters carry a `query_type` label (for example, `find_nodes`, `traverse`, `explore`, `aggregate`).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `qp.queries_total` | Counter | `query_type`, `status` (ok / error code) | Total queries processed through the pipeline |
| `qp.pipeline_duration_ms` | Histogram | `query_type`, `status` | End-to-end pipeline duration from security check to formatted output |
| `qp.compile_duration_ms` | Histogram | `query_type` | Time spent compiling a query from JSON to parameterized SQL |
| `qp.execute_duration_ms` | Histogram | `query_type` | Time spent executing the compiled query against ClickHouse |
| `qp.authorization_duration_ms` | Histogram | `query_type` | Time spent on authorization exchange with Rails |
| `qp.hydration_duration_ms` | Histogram | `query_type` | Time spent hydrating neighbor properties from ClickHouse |
| `qp.result_set_size` | Histogram | `query_type` | Number of rows returned after formatting |
| `qp.node_count` | Histogram | `query_type` | Number of Arrow record batches returned from ClickHouse |
| `qp.redacted_count` | Histogram | `query_type` | Number of rows redacted per query |
| `qp.error.security_rejected` | Counter | `reason` (security) | Pipeline rejected due to invalid or missing security context |
| `qp.error.execution_failed` | Counter | `reason` (execution) | ClickHouse query execution failed |
| `qp.error.authorization_failed` | Counter | `reason` (authorization) | Authorization exchange with Rails failed |

*Query engine metrics (`query_engine`):*

The query engine fires counters during compilation to track security-relevant rejections. Each counter uses a `reason` label for low-cardinality breakdown. Counters marked "server layer" are exported for the gRPC/HTTP layer to increment.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `qe.threat.validation_failed` | Counter | `reason` (parse/schema/reference/pagination) | Query rejected by structural validation |
| `qe.threat.allowlist_rejected` | Counter | `reason` (ontology/ontology_internal) | Entity, column, or relationship not in the ontology allowlist |
| `qe.threat.auth_filter_missing` | Counter | `reason` (security) | Security context invalid or absent (server layer) |
| `qe.threat.timeout` | Counter | `reason` | Query compilation or execution exceeded deadline (server layer) |
| `qe.threat.rate_limited` | Counter | `reason` | Caller throttled before compilation (server layer) |
| `qe.threat.depth_exceeded` | Counter | `reason` (depth) | Traversal depth or hop count exceeded the hard cap |
| `qe.internal.pipeline_invariant_violated` | Counter | `reason` (lowering/codegen) | Lowering or codegen hit a state upstream validation should have prevented |

**Shared Infrastructure Metrics:**

- Disk and Memory usage per container
- Network traffic between services

Prometheus scrapes these metrics into Grafana Mimir. We also maintain dashboards for the ClickHouse layer (queries, merges, background tasks).

### Alert Rules

Alert rules are defined as a `PrometheusRule` CRD in `helm-dev/observability/templates/gkg-alert-rules.yaml`, automatically discovered by the Prometheus Operator. Thresholds are configurable via Helm values (`alerting.rules.*`). Alerts are disabled by default and enabled per environment (currently active in sandbox).

Metrics flow through OTel SDK → OTLP → Alloy → Prometheus remote\_write. The OTel-to-Prometheus conversion replaces dots with underscores and appends `_total` for counters (e.g., `qe.threat.auth_filter_missing` → `qe_threat_auth_filter_missing_total`).

**Security alerts** (any non-zero count is anomalous):

| Alert | Metric | Default Threshold | Severity | `for` | Fires when |
|---|---|---|---|---|---|
| `GKGAuthFilterMissing` | `qe_threat_auth_filter_missing_total` | > 0 in 5m | critical | 1m | A query was processed without a valid security context, meaning authorization filtering was bypassed |
| `GKGPipelineInvariantViolated` | `qe_internal_pipeline_invariant_violated_total` | > 0 in 5m | critical | 1m | The query compiler reached a state that upstream validation should have prevented — may produce incorrect SQL |
| `GKGSecurityRejected` | `qp_error_security_rejected_total` | > 0 in 5m | warning | 5m | Pipeline rejected a request due to invalid or missing security context |

**Query health alerts** (sustained error rates or latency degradation):

| Alert | Metric | Default Threshold | Severity | `for` | Fires when |
|---|---|---|---|---|---|
| `GKGQueryingErrorRateHigh` | `qp_queries_total_total{status!="ok"}` / `qp_queries_total_total` | > 5% | warning | 5m | Aggregate error rate across all failure modes exceeds threshold — the availability SLI |
| `GKGQueryTimeoutRateHigh` | `qe_threat_timeout_total` / `qp_queries_total_total` | > 5% | warning | 5m | More than 5% of queries time out, indicating ClickHouse saturation or pathological queries |
| `GKGValidationFailedBurst` | `qe_threat_validation_failed_total` | > 10/min | warning | 5m | Sustained burst of structural validation failures (broken client or probing) |
| `GKGAllowlistRejectedBurst` | `qe_threat_allowlist_rejected_total` | > 5/min | warning | 5m | Sustained ontology violations (schema drift or enumeration attempt) |
| `GKGExecutionFailureRate` | `qp_error_execution_failed_total` | > 1/min | warning | 5m | ClickHouse query execution is failing |
| `GKGAuthorizationFailureRate` | `qp_error_authorization_failed_total` | > 1/min | warning | 5m | Rails authorization exchange is failing |
| `GKGPipelineLatencyP95High` | `qp_pipeline_duration_ms` (histogram) | > 5000ms | warning | 10m | p95 end-to-end pipeline latency exceeds threshold |

**Capacity alerts** (traffic and limit pressure):

| Alert | Metric | Default Threshold | Severity | `for` | Fires when |
|---|---|---|---|---|---|
| `GKGRateLimitedHigh` | `qe_threat_rate_limited_total` | > 10/min | warning | 5m | High rate of throttled callers — may need capacity scaling |

### Logging

All logs are structured JSON, shipped to Logstash and Elasticsearch. Every log entry includes a correlation ID so you can trace a request across services.

## Logging Structure and Format

All log output is JSON. Each entry has standard fields plus context-specific data.

**Standard Fields:**

| Field            | Type   | Description                               |
| ---------------- | ------ | ----------------------------------------- |
| `timestamp`      | String | ISO 8601 formatted timestamp (UTC)        |
| `level`          | String | Log level (e.g., `info`, `warn`, `error`) |
| `service`        | String | Name of the service (e.g., `gkg-indexer`) |
| `correlation_id` | String | A unique ID for tracing a request         |
| `message`        | String | The log message                           |

**Example Log Entry:**

```json
{
  "timestamp": "2025-10-10T12:00:00.000Z",
  "level": "info",
  "service": "gkg-indexer",
  "correlation_id": "req-xyz-123",
  "message": "Indexing started for project"
}
```

### Tracing

Services are instrumented with OpenTelemetry for distributed tracing. A single request can be followed across GKG and other GitLab services.

### Health Checks

Services expose `/health` for liveness and readiness probes. Traffic is only routed to healthy instances.

## Self-Managed Instances

For self-managed deployments, we expose a stable integration surface so operators can integrate our metrics, logs, and tracing into their existing observability stacks.

Interface contracts (what we provide):

- **Metrics**: Each service exposes a Prometheus-compatible `/metrics` endpoint for service-level KPIs; we also expose gauges for graph database disk usage where applicable. CPU and host/container resource utilization are expected to be collected via standard exporters alongside our service metrics.
- **Logs**: All services emit structured JSON to `stdout`/`stderr` using the schema defined in [Logging Structure and Format](#logging-structure-and-format) (including `correlation_id`).
- **Tracing**: Services are instrumented with OpenTelemetry, allowing operators to configure an OTLP exporter (gRPC/HTTP) to a customer-managed collector or backend.
- **Health**: Liveness and readiness `/health` endpoints for orchestration and local SLOs.

Operator responsibilities:

- Scrape `/metrics` with your Prometheus (or compatible) and manage storage, alerting, and retention.
- Collect node/container resource metrics (CPU, memory, disk I/O, and usage) via standard exporters (e.g., cAdvisor, kube-state-metrics, node_exporter) and correlate with service metrics.
- Collect and ship JSON logs (e.g., Fluentd/Vector/Filebeat) to your aggregator (e.g., Elasticsearch/Loki/Splunk) and manage parsing/retention.
- Provide and operate an OpenTelemetry collector or tracing backend if traces are required.
- Secure endpoints and govern egress in accordance with your environment.

## Deployment to Omnibus-adjacent Kubernetes Environment

In Kubernetes, most of this is automatic:

- **Metrics**: Prometheus Operator discovers and scrapes `/metrics`. Cluster exporters (cAdvisor, kube-state-metrics, node_exporter) handle CPU, memory, and disk.
- **Logging**: Container logs go to `stdout`/`stderr` and get collected by the cluster's logging agent (Fluentd, Vector).
- **Health Checks**: Kubernetes uses liveness and readiness probes to restart unhealthy pods and manage traffic during rollouts.

## Ownership, On-call & Escalation

Who owns what, and who gets paged.

### Service/Component Ownership

- **Siphon & NATS (development/bug fixes)**: Analytics stage (primarily Platform Insights), with collaboration from the Knowledge Graph team.
- **GKG Service (Indexer + API/MCP)**: Knowledge Graph team.

### On-call

- **Tier 1**: Production Engineering SRE (existing on-call rotation).
- **Tier 2**: Analytics / Platform Insights.
- **Knowledge Graph Services**: Dedicated on-call rotation (TBD). During initial launch the KG team will actively monitor the service.

### Long-term Stewardship

Future ownership will be evaluated, for example, NATS may move under the Durability team, while Siphon is likely to remain with Data Engineering (Analytics).

## Runbooks

Initial runbook procedures for GitLab.com. These will grow as we learn more in production.

### Siphon

- **Monitoring**: Regularly verify replication slots and monitor producer throughput and lag.
- **Snapshots**: Be aware that snapshots can temporarily inflate JetStream storage. Plan for sufficient headroom per table, and use work-queue/limits retention settings during bulk loads.

### NATS JetStream

- **Stream Policies**: Enforce `LimitsPolicy` (size, age, and message caps) on streams.
- **Alerting**: Configure alerts to trigger at 70%, 85%, and 95% of usage limits.

### Database (ClickHouse)

- **Ingestion**: Monitor background merge operations, as this is where data deduplication occurs.
- **ETL and Graph Ingestion**: Establish a clear set of metrics for these processes.
- **Workload Isolation**: Run GKG queries on a separate Warehouse to isolate them from ingestion workloads. Pin agent reads to read-only compute nodes.
- **Query Safety**:
  - **Limits**: Set per-user quotas for `max_memory_usage`, `max_rows_to_read`, `max_bytes_to_read`, and timeouts on the GKG role.
  - **Join & Scan Budgets**: Enforce linting rules in the query planner to block unbounded joins, text-search filters in aggregates, or multi-hop traversals (>3) unless pre-materialized.
