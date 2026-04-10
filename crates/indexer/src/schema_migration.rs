//! Schema migration orchestrator for zero-downtime table-prefix migrations.
//!
//! When the indexer starts and detects a mismatch between the embedded
//! `SCHEMA_VERSION` and the active version in `gkg_schema_version`, it runs
//! this migration flow before accepting any NATS messages:
//!
//! 1. **Acquire lock** — NATS KV `indexing_locks/schema_migration` (TTL-based).
//!    If another pod holds the lock, wait and skip — it is handling the migration.
//! 2. **Drain** — The engine has not started yet; no in-flight jobs exist.
//!    This phase is a no-op today but is reserved for future dual-write scenarios.
//! 3. **Create new-prefix tables** — Execute `CREATE TABLE IF NOT EXISTS vN_*`
//!    DDL from `config/graph.sql` with the new prefix applied.
//! 4. **Mark migrating** — Insert the new version with status `migrating` in
//!    `gkg_schema_version`. The Webserver cutover (issue #441) switches reads.
//! 5. **Release lock** — Allow other pods to detect the migration is done.
//!
//! After this function returns, the indexer starts normally and writes to the
//! new-prefix tables. Because all new-prefix checkpoints are empty, the
//! dispatcher's normal namespace poll cycle re-dispatches backfill work
//! automatically — no explicit trigger is needed.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use thiserror::Error;
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::{LockError, LockService};
use crate::schema_version::{
    SCHEMA_VERSION, SchemaVersionError, all_graph_tables, read_active_version, table_prefix,
    write_migrating_version, write_schema_version,
};

/// NATS KV key used to serialize schema migrations across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// TTL for the migration lock. Set high enough to cover DDL execution across all graph tables.
const MIGRATION_LOCK_TTL: Duration = Duration::from_secs(120);

/// How long to wait between polling for an active lock held by another pod.
const LOCK_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum number of lock-poll iterations before giving up.
const MAX_LOCK_WAIT_ITERATIONS: u32 = 60;

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("schema version error: {0}")]
    SchemaVersion(#[from] SchemaVersionError),

    #[error("lock error: {0}")]
    Lock(#[from] LockError),

    #[error("ClickHouse DDL error for table '{table}': {reason}")]
    Ddl { table: String, reason: String },

    #[error("migration lock held by another pod after {seconds}s; giving up")]
    LockTimeout { seconds: u64 },
}

/// Pre-built OTel instruments for schema migration observability.
///
/// Metric: `gkg_schema_migration_total` with labels `phase` and `result`.
/// - phase: `acquire_lock` | `create_tables` | `mark_migrating` | `complete`
/// - result: `success` | `failure` | `skipped`
#[derive(Clone)]
pub struct MigrationMetrics {
    pub(crate) migration_total: Counter<u64>,
}

impl MigrationMetrics {
    pub fn new() -> Self {
        let meter = global::meter("gkg_schema_migration");
        let migration_total = meter
            .u64_counter("gkg_schema_migration_total")
            .with_description(
                "Total schema migration phase executions, labelled by phase and result",
            )
            .build();
        Self { migration_total }
    }

    pub(crate) fn record(&self, phase: &'static str, result: &'static str) {
        self.migration_total.add(
            1,
            &[
                KeyValue::new("phase", phase),
                KeyValue::new("result", result),
            ],
        );
    }
}

impl Default for MigrationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Embeds `config/graph.sql` at compile time for DDL reuse during migration.
///
/// The SQL is parsed at runtime to extract `CREATE TABLE IF NOT EXISTS` statements,
/// which are then re-executed with the new schema version prefix applied.
const GRAPH_SQL: &str = include_str!("../../../config/graph.sql");

/// Parses all `CREATE TABLE IF NOT EXISTS <name> (...)` blocks from the
/// embedded `graph.sql` and returns them keyed by unprefixed table name.
///
/// Each value is the full DDL statement (suitable for direct ClickHouse execution)
/// with the original unprefixed table name still in place. The caller substitutes
/// the prefix before executing.
fn parse_create_table_statements(sql: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let upper = sql.to_uppercase();
    let marker = "CREATE TABLE IF NOT EXISTS ";
    let mut search_from = 0;

    while let Some(start) = upper[search_from..].find(marker) {
        let abs_start = search_from + start;

        // Find the table name: first non-whitespace token after the marker.
        let after_marker = abs_start + marker.len();
        let rest = &sql[after_marker..];
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(rest.len());
        let table_name = rest[..name_end].trim().to_string();

        // Find the matching closing ')' for the column list, then consume
        // everything up to and including the final ';'.
        let paren_start = sql[after_marker..].find('(').map(|p| after_marker + p);
        if let Some(paren_start) = paren_start {
            let stmt_end = find_statement_end(sql, paren_start);
            if let Some(stmt_end) = stmt_end {
                let full_stmt = sql[abs_start..=stmt_end].trim().to_string();
                results.push((table_name, full_stmt));
                search_from = stmt_end + 1;
                continue;
            }
        }

        search_from = abs_start + marker.len();
    }

    results
}

/// Finds the index of the `;` that terminates a DDL statement starting with
/// a `(` at `paren_open`. Handles nested parentheses.
fn find_statement_end(sql: &str, paren_open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let chars: Vec<char> = sql[paren_open..].chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    // Look for ';' after the closing paren.
                    let rest_start = paren_open
                        + sql[paren_open..]
                            .char_indices()
                            .nth(i)
                            .map(|(b, _)| b)
                            .unwrap_or(0);
                    if let Some(semi) = sql[rest_start..].find(';') {
                        return Some(rest_start + semi);
                    }
                    return None;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Applies the new-version prefix to a DDL statement.
///
/// Replaces `CREATE TABLE IF NOT EXISTS <name>` with
/// `CREATE TABLE IF NOT EXISTS <prefix><name>`.
fn apply_prefix_to_ddl(original_ddl: &str, table_name: &str, prefix: &str) -> String {
    let old = format!("CREATE TABLE IF NOT EXISTS {table_name}");
    let new = format!("CREATE TABLE IF NOT EXISTS {prefix}{table_name}");
    original_ddl.replacen(&old, &new, 1)
}

/// Runs the pre-engine migration check.
///
/// Called by `indexer::run()` after NATS and ClickHouse are connected but
/// **before** `Engine::run()` starts consuming messages. This ensures there
/// are never in-flight NATS messages to drain.
///
/// # Behaviour
///
/// - **No mismatch** (active version == embedded version): returns immediately.
/// - **Fresh install** (no active version): records embedded version as active
///   and returns. Table creation is handled by the operator applying `graph.sql`
///   before deployment.
/// - **Version mismatch**: acquires lock, creates new-prefix tables, marks
///   migrating, releases lock.
pub async fn run_if_needed(
    graph: &ArrowClickHouseClient,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    let active = read_active_version(graph).await?;

    match active {
        None => {
            info!(
                version = *SCHEMA_VERSION,
                "fresh install — no migration needed, recording initial schema version"
            );
            write_schema_version(graph, *SCHEMA_VERSION).await?;
            metrics.record("complete", "skipped");
            Ok(())
        }
        Some(v) if v == *SCHEMA_VERSION => {
            info!(
                version = *SCHEMA_VERSION,
                "schema version matches embedded version — no migration needed"
            );
            metrics.record("complete", "skipped");
            Ok(())
        }
        Some(active_version) => {
            info!(
                active_version,
                target_version = *SCHEMA_VERSION,
                "schema version mismatch detected — starting migration"
            );
            run_migration(graph, lock_service, ontology, metrics, active_version).await
        }
    }
}

async fn run_migration(
    graph: &ArrowClickHouseClient,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    active_version: u32,
) -> Result<(), MigrationError> {
    // Phase 1: acquire distributed lock.
    acquire_migration_lock(lock_service, metrics).await?;

    // Re-read after acquiring the lock — another pod may have completed the
    // migration while we were waiting.
    let current_active = read_active_version(graph).await?;
    if let Some(v) = current_active
        && v == *SCHEMA_VERSION
    {
        info!(
            version = *SCHEMA_VERSION,
            "migration already completed by another pod — releasing lock"
        );
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "skipped");
        return Ok(());
    }

    // Phase 2: drain.
    // The engine has not started — no in-flight messages exist. This phase is
    // a no-op today and is reserved for future dual-write migration scenarios.
    info!(
        active_version,
        target_version = *SCHEMA_VERSION,
        "drain phase: engine not yet started — no in-flight messages to drain"
    );
    metrics.record("drain", "success");

    // Phase 3: create new-prefix tables.
    let create_result = create_prefixed_tables(graph, ontology, metrics).await;
    if let Err(ref e) = create_result {
        warn!(error = %e, "failed to create new-prefix tables — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return create_result;
    }

    // Phase 4: mark migrating in gkg_schema_version.
    info!(
        version = *SCHEMA_VERSION,
        "marking schema version as migrating"
    );
    let mark_result = write_migrating_version(graph, *SCHEMA_VERSION).await;
    if let Err(e) = mark_result {
        warn!(error = %e, "failed to mark migrating version — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return Err(e.into());
    }
    metrics.record("mark_migrating", "success");

    // Phase 5: release lock.
    let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
    metrics.record("complete", "success");

    info!(
        active_version,
        target_version = *SCHEMA_VERSION,
        new_prefix = %table_prefix(*SCHEMA_VERSION),
        "migration complete — indexer will write to new-prefix tables; \
         dispatcher backfill will repopulate via normal namespace poll cycle"
    );

    Ok(())
}

async fn acquire_migration_lock(
    lock_service: &Arc<dyn LockService>,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    for attempt in 0..MAX_LOCK_WAIT_ITERATIONS {
        match lock_service
            .try_acquire(MIGRATION_LOCK_KEY, MIGRATION_LOCK_TTL)
            .await?
        {
            true => {
                info!("acquired schema migration lock");
                metrics.record("acquire_lock", "success");
                return Ok(());
            }
            false => {
                if attempt == 0 {
                    info!("schema migration lock held by another pod — waiting for it to complete");
                }
                tokio::time::sleep(LOCK_POLL_INTERVAL).await;
            }
        }
    }

    let seconds = MAX_LOCK_WAIT_ITERATIONS as u64 * LOCK_POLL_INTERVAL.as_secs();
    metrics.record("acquire_lock", "failure");
    Err(MigrationError::LockTimeout { seconds })
}

async fn create_prefixed_tables(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    let new_prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables: std::collections::HashSet<String> =
        all_graph_tables(ontology).into_iter().collect();

    let ddl_statements = parse_create_table_statements(GRAPH_SQL);

    let mut created = 0u32;
    let mut skipped = 0u32;

    for (table_name, original_ddl) in &ddl_statements {
        if !expected_tables.contains(table_name) {
            // Table in graph.sql but not in our expected set — skip silently.
            // This can happen for control tables like gkg_schema_version.
            skipped += 1;
            continue;
        }

        let prefixed_ddl = apply_prefix_to_ddl(original_ddl, table_name, &new_prefix);

        info!(
            table = %table_name,
            prefixed_table = %format!("{new_prefix}{table_name}"),
            "creating new-prefix table"
        );

        graph
            .execute(&prefixed_ddl)
            .await
            .map_err(|e| MigrationError::Ddl {
                table: format!("{new_prefix}{table_name}"),
                reason: e.to_string(),
            })?;

        created += 1;
    }

    info!(
        created,
        skipped,
        prefix = %new_prefix,
        "new-prefix tables created"
    );
    metrics.record("create_tables", "success");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table_statements_finds_all_tables() {
        let statements = parse_create_table_statements(GRAPH_SQL);
        let names: Vec<&str> = statements.iter().map(|(n, _)| n.as_str()).collect();

        // Non-ontology tables must be present.
        for table in crate::schema_version::NON_ONTOLOGY_GRAPH_TABLES {
            assert!(
                names.contains(table),
                "expected table '{table}' in graph.sql parse result; got: {names:?}"
            );
        }
    }

    #[test]
    fn parse_create_table_statements_each_starts_with_create() {
        let statements = parse_create_table_statements(GRAPH_SQL);
        assert!(
            !statements.is_empty(),
            "should parse at least one CREATE TABLE statement"
        );
        for (name, ddl) in &statements {
            assert!(
                ddl.starts_with("CREATE TABLE IF NOT EXISTS"),
                "DDL for '{name}' should start with CREATE TABLE IF NOT EXISTS: {ddl}"
            );
        }
    }

    #[test]
    fn apply_prefix_to_ddl_substitutes_table_name() {
        let original =
            "CREATE TABLE IF NOT EXISTS checkpoint (\n    key String\n) ENGINE = MergeTree();";
        let result = apply_prefix_to_ddl(original, "checkpoint", "v1_");
        assert!(result.contains("CREATE TABLE IF NOT EXISTS v1_checkpoint"));
        assert!(!result.contains("CREATE TABLE IF NOT EXISTS checkpoint ("));
    }

    #[test]
    fn apply_prefix_to_ddl_v0_prefix_is_empty() {
        let original =
            "CREATE TABLE IF NOT EXISTS gl_user (\n    id Int64\n) ENGINE = MergeTree();";
        let result = apply_prefix_to_ddl(original, "gl_user", "");
        assert_eq!(result, original, "empty prefix must not change the DDL");
    }

    #[test]
    fn migration_metrics_new_does_not_panic() {
        let _metrics = MigrationMetrics::new();
    }

    #[test]
    fn all_ddl_statements_end_with_semicolon() {
        let statements = parse_create_table_statements(GRAPH_SQL);
        for (name, ddl) in &statements {
            assert!(
                ddl.trim_end().ends_with(';'),
                "DDL for '{name}' should end with ';': ...{}",
                &ddl[ddl.len().saturating_sub(40)..]
            );
        }
    }

    #[test]
    fn lock_key_is_stable() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }
}
