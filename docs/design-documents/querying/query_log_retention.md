# Query-log retention

Every ClickHouse query a graph request issues is tagged with the request
correlation ID: a per-stage `query_id` and a `log_comment` of the form
`gkg;<kind>;correlation_id=<id>`. ClickHouse records both in `system.query_log`,
so a request can be traced to the queries it ran.

`system.query_log` is node-local on ClickHouse Cloud and is discarded when a
replica scales down or is replaced. Debugging, pricing, and audit lookups that
arrive after node churn find nothing. This subsystem spills the fields we need
into a durable table so the correlation data survives.

## Mechanism

An insert-trigger materialized view copies finished GKG queries out of
`system.query_log` into a durable table as they are logged:

- `query_log_retention` — an unversioned auxiliary table (`MergeTree`, 30-day
  `TTL` on `event_time`) holding one row per finished GKG query. Each row
  stores:

  | Column | Description |
  |--------|-------------|
  | `event_time` | When ClickHouse finished executing the query. |
  | `query_id` | The sanitized-or-ULID query identifier set by the server. |
  | `log_comment` | Attribution string, e.g. `gkg;correlation_id=<id>`. |
  | `query_duration_ms` | Wall-clock execution time in milliseconds. |
  | `read_rows` | Number of rows read from storage. |
  | `read_bytes` | Bytes read from storage. |
  | `result_rows` | Number of rows in the result set. |
  | `result_bytes` | Bytes in the result set. |
  | `memory_usage` | Peak memory consumed by the query. |
  | `exception_code` | ClickHouse error code, 0 on success. |
  | `exception` | Error message text, empty on success. |
- `query_log_retention_mv` — an unversioned materialized view that reads
  `system.query_log` directly and writes into the table, filtered to
  `type = 'QueryFinish' AND log_comment LIKE 'gkg%'`.

Both are unversioned: created once at boot (`CREATE ... IF NOT EXISTS`), never
schema-version-prefixed, and excluded from dead-version GC, so they outlive
schema migrations. They live in the ontology under `settings.auxiliary_tables`
and `settings.materialized_views` (`versioned: false`), are emitted into
`config/graph_persistent.sql`, and are fingerprinted in the auxiliary snapshot
(a body change is `mise schema:snapshot`, not a version bump). See
[`schema_management.md`](../schema_management.md).

## Notes and limitations

- The MV captures only rows written after it exists; it does not backfill
  `system.query_log` rows that predate boot.
- Retention is best-effort: it depends on `system.query_log` being written on the
  node that ran the query. Cross-replica gaps are not reconciled.
- The correlation ID in `log_comment` is the raw (possibly forwarded) request ID;
  `query_id` carries the sanitized-or-ULID form. Join on `log_comment` for the
  request-level view, on `query_id` for a single query.
