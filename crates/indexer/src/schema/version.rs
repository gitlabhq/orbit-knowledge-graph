//! Table prefix derivation maps a version number to the string prepended to
//! graph table names. Version 0 uses no prefix (backward compatible); version N
//! uses `vN_`.

use std::sync::LazyLock;
use std::time::Duration;

use arrow::datatypes::UInt32Type;
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::ArrowUtils;
use query_engine::compiler::ast::ddl::{ColumnDef, ColumnType, CreateTable, Engine};
use query_engine::compiler::emit_create_table;
use query_engine::compiler::emit_simple_query;
use query_engine::compiler::{Expr, Insert, Node, OrderExpr, Query, SelectExpr, TableRef};
use thiserror::Error;
use tokio::time::Instant;

use crate::engine::retry::{Backoff, RetryMode, RetryPolicy, Step, drive_until};
use tracing::{info, warn};

const VERSION_TABLE: &str = "gkg_schema_version";

/// Schema version loaded from `config/SCHEMA_VERSION`.
///
/// Bump this file whenever `config/graph.sql` or `config/ontology/` changes
/// in a way that requires a new table-set or invalidates existing stored data.
/// This includes:
/// - DDL shape changes (new columns, type changes, index additions)
/// - Edge type renames (e.g. `MERGED_BY` → `MERGED`) since `gl_edge.relationship_kind`
///   stores the string value and old rows become invisible to the new compiler
/// - ETL mapping changes (column renames, enum value changes, FK rewiring)
///
/// The ETL pipeline is fully ontology-driven (`PlanInput` is built from
/// `&Ontology`), so all data-affecting changes are ontology YAML changes and
/// the CI `schema-version-check` job catches them automatically.
pub static SCHEMA_VERSION: LazyLock<u32> = LazyLock::new(|| {
    include_str!("../../../../config/SCHEMA_VERSION")
        .trim()
        .parse()
        .expect("config/SCHEMA_VERSION must contain a valid u32")
});

fn version_table_ddl() -> CreateTable {
    CreateTable {
        name: VERSION_TABLE.into(),
        columns: vec![
            ColumnDef::new("version", ColumnType::UInt32),
            ColumnDef::new(
                "status",
                ColumnType::Enum8(vec![
                    ("active".into(), 1),
                    ("migrating".into(), 2),
                    ("retired".into(), 3),
                    ("dropped".into(), 4),
                ]),
            ),
            ColumnDef::new("created_at", ColumnType::DateTime).with_default("now()"),
        ],
        indexes: vec![],
        projections: vec![],
        engine: Engine::replacing_merge_tree_version_only("created_at"),
        order_by: vec!["version".into()],
        primary_key: None,
        settings: vec![],
    }
}

fn read_active_version_query() -> (
    String,
    std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
) {
    let query = Query {
        select: vec![SelectExpr {
            expr: Expr::col("t", "version"),
            alias: None,
        }],
        from: TableRef::scan_final(VERSION_TABLE, "t"),
        where_clause: Some(Expr::eq(Expr::col("t", "status"), Expr::string("active"))),
        order_by: vec![OrderExpr {
            expr: Expr::col("t", "created_at"),
            desc: true,
        }],
        limit: Some(1),
        ..Query::default()
    };
    emit_simple_query(&Node::Query(Box::new(query)))
        .expect("read_active_version query must be valid")
}

fn write_version_query(
    version: u32,
) -> (
    String,
    std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
) {
    let insert = Insert::new(
        VERSION_TABLE,
        vec!["version".into(), "status".into()],
        vec![vec![Expr::uint32(version), Expr::string("active")]],
    );
    emit_simple_query(&Node::Insert(Box::new(insert))).expect("write_version query must be valid")
}

fn write_migrating_version_query(
    version: u32,
) -> (
    String,
    std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
) {
    let insert = Insert::new(
        VERSION_TABLE,
        vec!["version".into(), "status".into()],
        vec![vec![Expr::uint32(version), Expr::string("migrating")]],
    );
    emit_simple_query(&Node::Insert(Box::new(insert)))
        .expect("write_migrating_version query must be valid")
}

#[derive(Debug, Error)]
pub enum SchemaVersionError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
}

/// Returns the table prefix for a given schema version.
///
/// Version 0 → `""` (no prefix, backward compatible).
/// Version N → `"vN_"`.
pub fn table_prefix(schema_version: u32) -> String {
    if schema_version == 0 {
        String::new()
    } else {
        format!("v{schema_version}_")
    }
}

pub fn prefixed_table_name(table: &str, schema_version: u32) -> String {
    format!("{}{}", table_prefix(schema_version), table)
}

/// This table is never prefixed or dropped across schema versions — it is the
/// single source of truth for which version is active.
pub async fn ensure_version_table(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    let ddl = emit_create_table(&version_table_ddl());
    graph.execute(&ddl).await?;
    Ok(())
}

/// Reads the active schema version from ClickHouse.
///
/// Returns `None` on a fresh install (no rows yet).
/// Uses `FINAL` to handle `ReplacingMergeTree` eventual consistency.
pub async fn read_active_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u32>, SchemaVersionError> {
    let (sql, params) = read_active_version_query();
    let mut query = graph.query(&sql);
    for (name, param) in &params {
        query = query.param(name, &param.value);
    }
    let batches = query.fetch_arrow().await?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        return Ok(ArrowUtils::get_column::<UInt32Type>(batch, "version", 0));
    }

    Ok(None)
}

pub async fn write_schema_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    let (sql, params) = write_version_query(version);
    let mut query = graph.query(&sql);
    for (name, param) in &params {
        query = query.param(name, &param.value);
    }
    query.execute().await?;
    Ok(())
}

/// Records a schema version as `migrating` in ClickHouse.
///
/// Used by the migration orchestrator to signal that new-prefix tables are
/// being populated. The version remains `migrating` until the Webserver
/// cutover (tracked in a subsequent issue).
pub async fn write_migrating_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    let (sql, params) = write_migrating_version_query(version);
    let mut query = graph.query(&sql);
    for (name, param) in &params {
        query = query.param(name, &param.value);
    }
    query.execute().await?;
    Ok(())
}

const READ_MIGRATING_VERSION: &str = "\
SELECT version FROM gkg_schema_version FINAL WHERE status = 'migrating' ORDER BY created_at DESC LIMIT 1";

const READ_ALL_VERSIONS: &str = "\
SELECT version, CAST(status AS String) AS status FROM gkg_schema_version FINAL ORDER BY version DESC";

const WRITE_ACTIVE_VERSION: &str = "\
INSERT INTO gkg_schema_version (version, status) VALUES ({version:UInt32}, 'active')";

const WRITE_RETIRED_VERSION: &str = "\
INSERT INTO gkg_schema_version (version, status) VALUES ({version:UInt32}, 'retired')";

const WRITE_DROPPED_VERSION: &str = "\
INSERT INTO gkg_schema_version (version, status) VALUES ({version:UInt32}, 'dropped')";

/// Reads the migrating schema version from ClickHouse.
///
/// Returns `None` if no version is currently migrating.
pub async fn read_migrating_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u32>, SchemaVersionError> {
    let batches = graph.query_arrow(READ_MIGRATING_VERSION).await?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        return Ok(ArrowUtils::get_column::<UInt32Type>(batch, "version", 0));
    }

    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionEntry {
    pub version: u32,
    pub status: String,
}

/// Reads all schema versions from ClickHouse, ordered by version descending.
pub async fn read_all_versions(
    graph: &ArrowClickHouseClient,
) -> Result<Vec<VersionEntry>, SchemaVersionError> {
    let batches = graph.query_arrow(READ_ALL_VERSIONS).await?;
    let mut entries = Vec::new();

    for batch in &batches {
        for i in 0..batch.num_rows() {
            let version = ArrowUtils::get_column::<UInt32Type>(batch, "version", i)
                .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing version".into()))?;
            let status = ArrowUtils::get_column_string(batch, "status", i)
                .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing status".into()))?;
            entries.push(VersionEntry { version, status });
        }
    }

    Ok(entries)
}

pub async fn mark_version_active(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    graph
        .query(WRITE_ACTIVE_VERSION)
        .param("version", version)
        .execute()
        .await?;
    Ok(())
}

pub async fn mark_version_retired(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    graph
        .query(WRITE_RETIRED_VERSION)
        .param("version", version)
        .execute()
        .await?;
    Ok(())
}

pub async fn mark_version_dropped(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    graph
        .query(WRITE_DROPPED_VERSION)
        .param("version", version)
        .execute()
        .await?;
    Ok(())
}

/// Ensures the `gkg_schema_version` table exists.
///
/// Called by all service modes (Indexer, Webserver, DispatchIndexing) at
/// startup so the control table is always present. Fresh install handling
/// (recording version + creating tables) is done by the migration
/// orchestrator in `schema::migration::run_if_needed`.
pub async fn init(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    ensure_version_table(graph).await?;
    Ok(())
}

const MAX_BACKOFF_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaReadiness {
    Ready,
    Pending,
    Outdated,
}

fn classify_readiness(
    active: Option<u32>,
    migrating: Option<u32>,
    embedded: u32,
) -> SchemaReadiness {
    if let Some(active_version) = active
        && active_version > embedded
    {
        return SchemaReadiness::Outdated;
    }

    if active == Some(embedded) || migrating == Some(embedded) {
        return SchemaReadiness::Ready;
    }

    SchemaReadiness::Pending
}

#[derive(Debug, Error)]
pub enum SchemaWaitError {
    #[error(
        "timed out after {seconds}s waiting for schema version {target} to become ready \
         (last seen active={active:?}, migrating={migrating:?})"
    )]
    Timeout {
        target: u32,
        seconds: u64,
        active: Option<u32>,
        migrating: Option<u32>,
    },

    #[error(
        "binary schema version {embedded} is older than the active version {active}; \
         binary is outdated and must not process"
    )]
    Outdated { embedded: u32, active: u32 },
}

/// Blocks until `target_version` is ready or the `timeout` budget is exhausted.
/// Transient read errors are retried; an outdated binary fails fast.
pub async fn wait_until_ready(
    graph: &ArrowClickHouseClient,
    target_version: u32,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), SchemaWaitError> {
    info!(
        target_version,
        timeout_secs = timeout.as_secs(),
        "waiting for schema version to become ready"
    );

    let deadline = Instant::now() + timeout;
    let policy = RetryPolicy {
        mode: RetryMode::Local,
        backoff: Backoff::Exponential {
            base: poll_interval,
            cap: MAX_BACKOFF_INTERVAL,
        },
        max_attempts: u32::MAX, // the deadline is the real bound
        dead_letter: false,
    };

    // Carried state is the last-seen (active, migrating), read back by on_deadline for the report.
    drive_until(
        &policy,
        deadline,
        (None, None),
        |_carried, _attempt| async move {
            // A failed read is treated as "unknown" (None) so the other read can still drive an
            // outdated/ready decision; both failing falls through to a retry within the budget.
            let active = read_active_version(graph).await.unwrap_or_else(|error| {
                warn!(%error, "failed to read active schema version — retrying");
                None
            });
            let migrating = read_migrating_version(graph).await.unwrap_or_else(|error| {
                warn!(%error, "failed to read migrating schema version — retrying");
                None
            });

            match classify_readiness(active, migrating, target_version) {
                SchemaReadiness::Ready => {
                    info!(target_version, "schema version is ready — proceeding");
                    Step::Done(())
                }
                SchemaReadiness::Outdated => Step::GiveUp(SchemaWaitError::Outdated {
                    embedded: target_version,
                    active: active.expect("Outdated requires a known active version"),
                }),
                SchemaReadiness::Pending => {
                    info!(
                        target_version,
                        active_version = ?active,
                        migrating_version = ?migrating,
                        "schema version not ready yet — dispatcher has not prepared it"
                    );
                    Step::Retry((active, migrating))
                }
            }
        },
        |(active, migrating)| SchemaWaitError::Timeout {
            target: target_version,
            seconds: timeout.as_secs(),
            active: *active,
            migrating: *migrating,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_is_valid_u32() {
        let _ = *SCHEMA_VERSION;
    }

    #[test]
    fn table_prefix_v0_is_empty() {
        assert_eq!(table_prefix(0), "");
    }

    #[test]
    fn table_prefix_v1() {
        assert_eq!(table_prefix(1), "v1_");
    }

    #[test]
    fn table_prefix_v2() {
        assert_eq!(table_prefix(2), "v2_");
    }

    #[test]
    fn prefixed_table_name_v0_no_change() {
        assert_eq!(prefixed_table_name("gl_user", 0), "gl_user");
    }

    #[test]
    fn prefixed_table_name_v1() {
        assert_eq!(prefixed_table_name("gl_user", 1), "v1_gl_user");
    }

    #[test]
    fn prefixed_table_name_v2() {
        assert_eq!(prefixed_table_name("gl_user", 2), "v2_gl_user");
    }

    #[test]
    fn create_table_ddl_uses_replacing_merge_tree() {
        let ddl = emit_create_table(&version_table_ddl());
        assert!(ddl.contains("ReplacingMergeTree"));
        assert!(ddl.contains("IF NOT EXISTS"));
        assert!(ddl.contains("gkg_schema_version"));
        assert!(ddl.contains("UInt32"));
        assert!(ddl.contains("Enum8("));
        assert!(ddl.contains("'active' = 1"));
        assert!(ddl.contains("DateTime"));
        assert!(ddl.contains("ORDER BY (version)"));
    }

    #[test]
    fn read_query_uses_final() {
        let (sql, _params) = read_active_version_query();
        assert!(
            sql.contains("FINAL"),
            "version query must use FINAL for ReplacingMergeTree consistency: {sql}"
        );
        assert!(sql.contains("gkg_schema_version"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 1"));
    }

    #[test]
    fn table_prefix_large_version() {
        assert_eq!(table_prefix(99), "v99_");
        assert_eq!(prefixed_table_name("checkpoint", 99), "v99_checkpoint");
    }

    #[test]
    fn migrating_query_uses_migrating_status() {
        let (sql, params) = write_migrating_version_query(1);
        assert!(
            sql.contains("gkg_schema_version"),
            "migrating query must target version table: {sql}"
        );
        assert!(!params.is_empty(), "migrating query must have parameters");
    }

    #[test]
    fn readiness_active_matches_is_ready() {
        assert_eq!(classify_readiness(Some(2), None, 2), SchemaReadiness::Ready);
    }

    #[test]
    fn readiness_migrating_matches_is_ready() {
        assert_eq!(
            classify_readiness(Some(1), Some(2), 2),
            SchemaReadiness::Ready
        );
    }

    #[test]
    fn readiness_no_version_is_pending() {
        assert_eq!(classify_readiness(None, None, 2), SchemaReadiness::Pending);
    }

    #[test]
    fn readiness_migrating_without_active_is_ready() {
        assert_eq!(classify_readiness(None, Some(2), 2), SchemaReadiness::Ready);
    }

    #[test]
    fn readiness_only_older_active_is_pending() {
        assert_eq!(
            classify_readiness(Some(1), None, 2),
            SchemaReadiness::Pending
        );
    }

    #[test]
    fn readiness_higher_active_is_outdated() {
        assert_eq!(
            classify_readiness(Some(3), None, 2),
            SchemaReadiness::Outdated
        );
    }

    #[test]
    fn readiness_outdated_takes_precedence_over_matching_migrating() {
        assert_eq!(
            classify_readiness(Some(3), Some(2), 2),
            SchemaReadiness::Outdated
        );
    }
}
