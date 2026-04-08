//! Schema version tracking and mismatch detection.
//!
//! Embeds a schema version in the GKG binary and persists it in ClickHouse.
//! A periodic background task detects mismatches and, when no namespaces are
//! enabled, triggers a drop-and-recreate reset of all GKG-owned tables.

use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge};
use thiserror::Error;
use tracing::{info, warn};

use crate::locking::LockService;

/// Manually bumped whenever `config/graph.sql` changes.
/// Any MR that modifies `graph.sql` must also bump this constant.
pub const SCHEMA_VERSION: u64 = 1;

/// Lock key used to coordinate concurrent reset attempts across pods.
const SCHEMA_RESET_LOCK_KEY: &str = "schema_reset";

/// TTL for the schema reset NATS KV lock.
const SCHEMA_RESET_LOCK_TTL: Duration = Duration::from_secs(120);

/// The embedded DDL for all GKG-owned tables.
pub(crate) const GRAPH_SCHEMA_SQL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph.sql"));

const CREATE_VERSION_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS gkg_schema_version (
    version UInt64,
    applied_at DateTime64(6, 'UTC') DEFAULT now64(6),
    _version DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (version)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1";

const READ_VERSION: &str = "\
SELECT version FROM gkg_schema_version FINAL ORDER BY applied_at DESC LIMIT 1";

const WRITE_VERSION: &str = "\
INSERT INTO gkg_schema_version (version) VALUES ({version:UInt64})";

const COUNT_ENABLED_NAMESPACES: &str = "\
SELECT count() AS cnt
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false";

#[derive(Debug, Error)]
pub enum SchemaVersionError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
}

/// Outcome of a single schema version check cycle.
#[derive(Debug, PartialEq)]
pub enum CheckOutcome {
    /// Schema is current — nothing to do.
    Current,
    /// Mismatch detected but namespaces are still enabled.
    MismatchWaiting {
        persisted: Option<u64>,
        enabled_count: u64,
    },
    /// Mismatch with zero enabled namespaces — reset should be triggered.
    ResetReady,
}

/// Reads the persisted schema version from ClickHouse.
/// Returns `None` on a fresh install (no rows).
pub async fn read_persisted_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u64>, SchemaVersionError> {
    let batches = graph.query_arrow(READ_VERSION).await?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch
            .column_by_name("version")
            .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing version column".into()))?;
        let col = col
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                SchemaVersionError::UnexpectedResult("version column is not UInt64".into())
            })?;
        return Ok(Some(col.value(0)));
    }

    Ok(None)
}

/// Writes the current schema version to ClickHouse.
pub async fn write_schema_version(
    graph: &ArrowClickHouseClient,
    version: u64,
) -> Result<(), SchemaVersionError> {
    graph
        .query(WRITE_VERSION)
        .param("version", version)
        .execute()
        .await?;
    Ok(())
}

/// Creates the `gkg_schema_version` control table if it doesn't exist.
pub async fn ensure_version_table(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    graph.execute(CREATE_VERSION_TABLE).await?;
    Ok(())
}

/// Counts enabled namespaces in the datalake.
pub async fn count_enabled_namespaces(
    datalake: &ArrowClickHouseClient,
) -> Result<u64, SchemaVersionError> {
    let batches = datalake.query_arrow(COUNT_ENABLED_NAMESPACES).await?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch
            .column_by_name("cnt")
            .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing cnt column".into()))?;
        let col = col
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                SchemaVersionError::UnexpectedResult("cnt column is not UInt64".into())
            })?;
        return Ok(col.value(0));
    }

    Ok(0)
}

/// Performs one check cycle: compares the persisted version to the embedded
/// constant and returns the appropriate outcome.
pub async fn check_version(
    graph: &ArrowClickHouseClient,
    datalake: &ArrowClickHouseClient,
    mismatch_gauge: &Gauge<u64>,
) -> Result<CheckOutcome, SchemaVersionError> {
    let persisted = read_persisted_version(graph).await?;

    if persisted == Some(SCHEMA_VERSION) {
        mismatch_gauge.record(0, &[]);
        return Ok(CheckOutcome::Current);
    }

    mismatch_gauge.record(1, &[]);

    let enabled_count = count_enabled_namespaces(datalake).await?;

    if enabled_count > 0 {
        return Ok(CheckOutcome::MismatchWaiting {
            persisted,
            enabled_count,
        });
    }

    info!(
        persisted = ?persisted,
        embedded = SCHEMA_VERSION,
        "schema version mismatch with zero enabled namespaces — ready for reset"
    );
    Ok(CheckOutcome::ResetReady)
}

/// Extracts GKG-owned table names from the embedded `graph.sql` DDL.
///
/// Parses `CREATE TABLE IF NOT EXISTS <name>` statements and returns the
/// table names. `gkg_schema_version` is excluded — it must never be dropped.
pub fn parse_gkg_table_names(schema_sql: &str) -> Vec<String> {
    let mut tables = Vec::new();
    for line in schema_sql.lines() {
        let trimmed = line.trim();
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("CREATE TABLE IF NOT EXISTS ") {
            let rest = &trimmed["CREATE TABLE IF NOT EXISTS ".len()..];
            let name = rest
                .split(|c: char| c.is_whitespace() || c == '(')
                .next()
                .unwrap_or("")
                .trim_matches('`')
                .trim();
            if !name.is_empty() && name != "gkg_schema_version" {
                tables.push(name.to_string());
            }
        }
    }
    tables
}

/// Drop-and-recreate outcome.
#[derive(Debug, PartialEq)]
pub enum ResetOutcome {
    /// Reset completed successfully; new schema version was recorded.
    Success,
    /// Another pod holds the reset lock; skipped this cycle.
    LockNotAcquired,
}

/// Drops all GKG-owned tables and recreates them from `graph.sql`.
///
/// Steps:
/// 1. Parse table names from the embedded DDL
/// 2. Drop each table with `DROP TABLE IF EXISTS … SYNC`
/// 3. Re-execute every statement in `graph.sql`
/// 4. Write the new schema version
///
/// `gkg_schema_version` is never dropped.
pub async fn schema_reset(
    graph: &ArrowClickHouseClient,
    new_version: u64,
) -> Result<(), SchemaVersionError> {
    let tables = parse_gkg_table_names(GRAPH_SCHEMA_SQL);

    warn!(
        table_count = tables.len(),
        new_version, "schema reset: dropping GKG-owned tables"
    );

    for table in &tables {
        info!(table, "dropping table");
        graph
            .execute(&format!("DROP TABLE IF EXISTS `{table}` SYNC"))
            .await?;
    }

    info!("schema reset: recreating tables from graph.sql");
    for statement in GRAPH_SCHEMA_SQL.split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        graph.execute(statement).await?;
    }

    write_schema_version(graph, new_version).await?;
    info!(version = new_version, "schema version recorded after reset");

    Ok(())
}

/// Attempts to acquire the NATS lock and run [`schema_reset`].
///
/// Returns [`ResetOutcome::LockNotAcquired`] when another pod already holds
/// the lock, allowing the caller to skip this cycle gracefully.
pub async fn try_schema_reset(
    graph: &ArrowClickHouseClient,
    lock_service: &dyn LockService,
    new_version: u64,
    reset_counter: &Counter<u64>,
) -> Result<ResetOutcome, SchemaVersionError> {
    match lock_service
        .try_acquire(SCHEMA_RESET_LOCK_KEY, SCHEMA_RESET_LOCK_TTL)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            info!("schema reset lock held by another pod — skipping this cycle");
            return Ok(ResetOutcome::LockNotAcquired);
        }
        Err(e) => {
            warn!(error = %e, "failed to acquire schema reset lock — skipping reset");
            return Ok(ResetOutcome::LockNotAcquired);
        }
    }

    let result = schema_reset(graph, new_version).await;

    let _ = lock_service.release(SCHEMA_RESET_LOCK_KEY).await;

    match result {
        Ok(()) => {
            reset_counter.add(1, &[KeyValue::new("result", "success")]);
            Ok(ResetOutcome::Success)
        }
        Err(e) => {
            reset_counter.add(1, &[KeyValue::new("result", "failure")]);
            Err(e)
        }
    }
}

/// Runs the schema version check loop as a background task.
///
/// This task:
/// 1. Creates the `gkg_schema_version` table if needed
/// 2. Periodically checks for version mismatches
/// 3. When a mismatch is detected with zero enabled namespaces, acquires
///    a NATS KV lock and executes the drop-and-recreate reset
pub async fn run_check_loop(
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    lock_service: Arc<dyn LockService>,
    interval: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let meter = global::meter("gkg_indexer");
    let mismatch_gauge = meter
        .u64_gauge("gkg.schema.version.mismatch")
        .with_description("1 when a schema version mismatch is detected, 0 otherwise")
        .build();
    let check_loop_active_gauge = meter
        .u64_gauge("gkg.schema.version.check_loop_active")
        .with_description("1 while the schema version check loop is running, 0 after it exits")
        .build();
    let reset_counter = meter
        .u64_counter("gkg.schema.reset.total")
        .with_description("Total schema reset attempts, labeled by result")
        .build();

    if let Err(e) = ensure_version_table(&graph).await {
        // Silent return is intentional: ClickHouse connectivity issues surface
        // through other probes (health check, query failures). Starting the loop
        // without the version table would cause repeated noisy errors with no
        // recovery path, so we exit early and let the process restart instead.
        warn!(error = %e, "failed to create gkg_schema_version table; check loop will not start");
        return;
    }

    check_loop_active_gauge.record(1, &[]);

    info!(
        interval_secs = interval.as_secs(),
        "schema version check loop started"
    );

    loop {
        match check_version(&graph, &datalake, &mismatch_gauge).await {
            Ok(CheckOutcome::Current) => {}
            Ok(CheckOutcome::MismatchWaiting {
                persisted,
                enabled_count,
            }) => {
                warn!(
                    persisted = ?persisted,
                    embedded = SCHEMA_VERSION,
                    enabled_count,
                    "schema version mismatch — disable all namespaces to proceed with schema reset"
                );
            }
            Ok(CheckOutcome::ResetReady) => {
                warn!(
                    "schema version mismatch with zero enabled namespaces — beginning schema reset"
                );
                match try_schema_reset(
                    &graph,
                    lock_service.as_ref(),
                    SCHEMA_VERSION,
                    &reset_counter,
                )
                .await
                {
                    Ok(ResetOutcome::Success) => {
                        info!(
                            version = SCHEMA_VERSION,
                            "schema reset completed successfully"
                        );
                        mismatch_gauge.record(0, &[]);
                    }
                    Ok(ResetOutcome::LockNotAcquired) => {
                        info!("schema reset skipped — lock held by another pod");
                    }
                    Err(e) => {
                        warn!(error = %e, "schema reset failed — will retry on next cycle");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "schema version check failed");
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.cancelled() => {
                info!("schema version check loop shutting down");
                break;
            }
        }
    }

    check_loop_active_gauge.record(0, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;

    const _: () = assert!(SCHEMA_VERSION > 0, "SCHEMA_VERSION must be at least 1");

    #[test]
    fn create_table_ddl_uses_replacing_merge_tree() {
        assert!(CREATE_VERSION_TABLE.contains("ReplacingMergeTree"));
        assert!(CREATE_VERSION_TABLE.contains("IF NOT EXISTS"));
    }

    #[test]
    fn read_query_uses_final() {
        assert!(
            READ_VERSION.contains("FINAL"),
            "version query must use FINAL for ReplacingMergeTree"
        );
    }

    #[test]
    fn parse_gkg_table_names_extracts_all_tables() {
        let names = parse_gkg_table_names(GRAPH_SCHEMA_SQL);
        assert!(
            names.contains(&"checkpoint".to_string()),
            "must include checkpoint"
        );
        assert!(
            names.contains(&"namespace_deletion_schedule".to_string()),
            "must include namespace_deletion_schedule"
        );
        assert!(
            names.contains(&"gl_user".to_string()),
            "must include gl_user"
        );
        assert!(
            names.contains(&"gl_edge".to_string()),
            "must include gl_edge"
        );
        assert!(
            names.contains(&"code_indexing_checkpoint".to_string()),
            "must include code_indexing_checkpoint"
        );
    }

    #[test]
    fn parse_gkg_table_names_excludes_version_table() {
        let names = parse_gkg_table_names(GRAPH_SCHEMA_SQL);
        assert!(
            !names.contains(&"gkg_schema_version".to_string()),
            "gkg_schema_version must never appear in the drop set"
        );
    }

    #[test]
    fn parse_gkg_table_names_no_siphon_tables() {
        let names = parse_gkg_table_names(GRAPH_SCHEMA_SQL);
        let siphon_tables: Vec<_> = names.iter().filter(|n| n.starts_with("siphon_")).collect();
        assert!(
            siphon_tables.is_empty(),
            "siphon tables must not appear in the drop set: {siphon_tables:?}"
        );
    }
}
