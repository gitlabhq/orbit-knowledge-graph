---
title: "Knowledge Graph: Observability"
description: How the Knowledge Graph services will be monitored and observed.
---

{{< engineering/design-document-header >}}

## Summary

This document details the observability strategy for Knowledge Graph services (Indexer and Web Service). It outlines the current architecture, plans for self-managed instances, deployment considerations and logging standards. The goal is to ensure robust and actionable monitoring across all deployment environments.

## How Observability Works Today (GitLab.com)

On GitLab.com, the Knowledge Graph services will integrate with our existing observability stack. Our stack is composed of the following technologies:

- **Metrics and Visualization**: [Grafana](https://grafana.com/) and [Grafana Mimir](https://grafana.com/oss/mimir/)
- **Log Aggregation and Analysis**: [Elasticsearch](https://www.elastic.co/elasticsearch/) and [Logstash](https://www.elastic.co/logstash/)

These services provide a comprehensive view of the services' health and performance, which is built on the following pillars:

### SLIs, SLOs, and Metrics

To ensure the reliability and performance of the Knowledge Graph services, we will establish clear Service Level Objectives (SLOs) and Service Level Indicators (SLIs). Our alerting strategy will be based on these.

- **Availability SLO**: We will adopt the Dedicated reference of ≥99.5% monthly SLO for the GKG API plane (excluding planned maintenance).
- **Availability SLIs**: We will use error-rate and Apdex SLIs on request latency.

The Knowledge Graph will expose a Prometheus `/metrics` endpoint, which allows for the collection of detailed time-series data about the services' operations. These metrics serve as our primary reliability signals. We will use LabKit where possible for instrumentation.

**Reliability Signals:**

- **Ingest Lag**: The delay between data being written to the Postgres WAL, processed by Siphon, sent through NATS, and ingested.
- **Consumer Health**: Monitoring NATS JetStream delivery/ack rates, dead-letter counts, and stream retention headroom.

**KG HTTP Server (Indexer and Web Service):**

- HTTP error and success rate by HTTP method and path
- HTTP latency (p50, p95, p99) by HTTP method and path

**KG Indexer Service:**

- Indexing success rate and duration (p50, p95, p99)
- Code indexing per-phase (parsing, analysis, writing) durations
- Ongoing indexing operations
- Language-specific parsing metrics

**KG Web Service:**

- **Query Health**: p50/p95 latency by tool (`find_nodes`, `traverse`, `explore`, `aggregate`), memory spikes, and rows/bytes read per query.
- MCP tools latency (p50, p95, p99), usage and success rate

**Shared Infrastructure Metrics:**

- Disk and Memory usage per container
- Network traffic between services

These metrics are collected by our central Prometheus infrastructure, stored in Grafana Mimir, and are used to populate Grafana dashboards. We will also maintain specific dashboards for our database layer (e.g., ClickHouse queries, merges, background tasks).

### Logging

We use structured logging, with all logs formatted as JSON. This allows for efficient parsing, searching, and analysis in our log aggregation platform powered by Logstash and Elasticsearch. Each log entry is to include a correlation ID, which is fundamental to our observability strategy as it enables tracing of a request across multiple services.

## Logging Structure and Format

A consistent and structured logging format is essential for effective observability. All log output from the Knowledge Graph services will be in JSON format. Each log entry will contain a set of standard fields, along with a payload of context-specific information.

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

We will support distributed tracing with an OpenTelemetry integration. This allows us to follow a single request as it passes through the various components of the Knowledge Graph services and other GitLab services. This is useful for debugging complex interactions and identifying performance bottlenecks.

### Health Checks

The services expose a `/health` endpoint, which will be used by our infrastructure to manage the service lifecycle, ensuring that traffic is only routed to healthy instances.

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

In a Kubernetes environment, observability is highly automated:

- **Metrics**: The Prometheus Operator will automatically discover and scrape the metrics endpoints of the Knowledge Graph services. Cluster exporters (e.g., cAdvisor, kube-state-metrics, node_exporter) should provide CPU, memory, and disk utilization metrics.
- **Logging**: Logs written to `stdout` and `stderr` by the service containers will be automatically collected by the cluster's logging agent (e.g., Fluentd, Vector) and shipped to a central log aggregator.
- **Health Checks**: Kubernetes will use the liveness and readiness probes to manage pod health, automatically restarting unhealthy pods and managing traffic during deployments.

## Ownership, On-call & Escalation

Clear ownership and on-call responsibilities are crucial for maintaining service health.

### Service/Component Ownership

- **Siphon & NATS (development/bug fixes)**: Analytics stage (primarily Platform Insights), with collaboration from the Knowledge Graph team.
- **GKG Service (Indexer + API/MCP)**: Knowledge Graph team.

### On-call

- **Tier 1**: Production Engineering SRE (existing on-call rotation).
- **Tier 2**: Analytics / Platform Insights.
- **Knowledge Graph Services**: A dedicated on-call plan for the Knowledge Graph team will be established. During the initial launch phase, the Knowledge Graph team can help provide active monitoring of the service.

### Long-term Stewardship

Future ownership will be evaluated, for example, NATS may move under the Durability team, while Siphon is likely to remain with Data Engineering (Analytics).

## Runbooks

This section outlines initial runbook procedures for operating the Knowledge Graph on GitLab.com. These will be expanded and adapted for self-managed instances based on our operational experience.

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
