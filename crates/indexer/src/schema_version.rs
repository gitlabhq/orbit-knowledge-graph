//! Schema version tracking and mismatch detection.
//!
//! Embeds a schema version in the GKG binary and persists it in ClickHouse.
//! A periodic background task detects mismatches and, when no namespaces are
//! enabled, triggers the downstream reset flow (Issue 2).

use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use opentelemetry::global;
use opentelemetry::metrics::Gauge;
use thiserror::Error;
use tracing::{info, warn};

/// Manually bumped whenever `config/graph.sql` changes.
/// Any MR that modifies `graph.sql` must also bump this constant.
pub const SCHEMA_VERSION: u64 = 1;

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
    MismatchWaiting { enabled_count: u64 },
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
        warn!(
            persisted = ?persisted,
            embedded = SCHEMA_VERSION,
            enabled_count,
            "schema version mismatch, but namespaces are still enabled — waiting"
        );
        return Ok(CheckOutcome::MismatchWaiting { enabled_count });
    }

    info!(
        persisted = ?persisted,
        embedded = SCHEMA_VERSION,
        "schema version mismatch with zero enabled namespaces — ready for reset"
    );
    Ok(CheckOutcome::ResetReady)
}

/// Runs the schema version check loop as a background task.
///
/// This task:
/// 1. Creates the `gkg_schema_version` table if needed
/// 2. Periodically checks for version mismatches
/// 3. When a mismatch is detected with zero enabled namespaces, logs
///    that a reset is ready (actual reset is implemented in Issue 2)
pub async fn run_check_loop(
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    interval: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let meter = global::meter("gkg_indexer");
    let mismatch_gauge = meter
        .u64_gauge("gkg.schema.version.mismatch")
        .with_description("1 when a schema version mismatch is detected, 0 otherwise")
        .build();

    if let Err(e) = ensure_version_table(&graph).await {
        warn!(error = %e, "failed to create gkg_schema_version table");
        return;
    }

    info!(
        interval_secs = interval.as_secs(),
        "schema version check loop started"
    );

    loop {
        match check_version(&graph, &datalake, &mismatch_gauge).await {
            Ok(CheckOutcome::Current) => {}
            Ok(CheckOutcome::MismatchWaiting { enabled_count }) => {
                warn!(
                    enabled_count,
                    "schema version mismatch — disable all namespaces to proceed with schema reset"
                );
            }
            Ok(CheckOutcome::ResetReady) => {
                // Issue 2 will implement the actual reset here.
                // For now, log and write the version after "reset" (which for fresh
                // installs means the schema was just applied via graph.sql).
                info!("schema reset ready — actual reset will be implemented in Issue 2");
                if let Err(e) = write_schema_version(&graph, SCHEMA_VERSION).await {
                    warn!(error = %e, "failed to write schema version after reset");
                } else {
                    info!(version = SCHEMA_VERSION, "schema version recorded");
                    mismatch_gauge.record(0, &[]);
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
}
