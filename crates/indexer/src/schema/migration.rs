//! Schema migration orchestrator for zero-downtime table-prefix migrations.
//!
//! The dispatcher runs this flow at boot, before starting its task loops, when
//! the embedded `SCHEMA_VERSION` differs from the active version in
//! `gkg_schema_version`. Indexers do not migrate; they wait for the version to
//! become ready via [`crate::schema::version::wait_until_ready`].
//!
//! **Forward migration** (`active < SCHEMA_VERSION`):
//!
//! 1. **Acquire lock** — NATS KV `indexing_locks/schema_migration` (TTL-based),
//!    serializing migration across dispatcher replicas.
//! 2. **Create new-prefix tables** — DDL from the ontology via
//!    `generate_graph_tables_with_prefix()` with the version prefix applied.
//! 3. **Mark migrating** — Insert the new version with status `migrating` in
//!    `gkg_schema_version`, signalling indexers that the tables exist.
//! 4. **Release lock**.
//!
//! New-prefix checkpoints start empty, so the dispatcher's normal namespace
//! poll cycle re-dispatches backfill work automatically.
//!
//! **Rollback** (`active > SCHEMA_VERSION`, an older binary was deployed):
//! see [`run_rollback`] for the two cases (tables retained vs. already GC'd).

use std::sync::Arc;
use std::time::Duration;

use query_engine::compiler::{
    DictionarySource, emit_create_dictionary, emit_create_materialized_view, emit_create_table,
    generate_graph_dictionaries_with_prefix, generate_graph_materialized_views_with_prefix,
    generate_graph_tables_with_prefix,
};
use thiserror::Error;
use tracing::{info, warn};

use super::metrics::MigrationMetrics;

use crate::campaign::{CampaignState, campaign_id_for_version};
use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::{LockError, LockService};
use crate::schema::version::{
    SCHEMA_VERSION, SchemaVersionError, drop_kind_for_engine, list_version_objects,
    mark_version_active, mark_version_retired, read_active_version, read_all_versions,
    table_prefix, version_tables_complete, write_migrating_version, write_schema_version,
};

/// The generated DDL for the namespace-storage snapshot table and its
/// refreshable MV. Same relative-include pattern as
/// `schema::version::SCHEMA_VERSION`. Carries `__ACTIVE_PREFIX__` and
/// `__ACTIVE_VERSION__` placeholders resolved per-apply in [`apply_unversioned_ddl`].
const UNVERSIONED_DDL: &str = include_str!("../../../../config/graph_unversioned.sql");

/// Base (unprefixed) name of the refreshable MV in [`UNVERSIONED_DDL`]. The
/// generated file always writes it as `__ACTIVE_PREFIX__` + this name; a unit
/// test guards against the two drifting apart.
const UNVERSIONED_REFRESH_MV_BASE_NAME: &str = "namespace_storage_snapshot_refresh";

/// NATS KV key used to serialize schema migrations across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// TTL for the migration lock. Set high enough to cover DDL execution across all graph tables.
const MIGRATION_LOCK_TTL: Duration = Duration::from_secs(120);

const LOCK_POLL_INTERVAL: Duration = Duration::from_secs(5);

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

/// Runs the migration check. Must be called at boot, before the task loops
/// start. Fresh install (no active version) creates all graph tables from the
/// ontology; no manual DDL (`graph.sql`) is needed.
pub async fn run_if_needed(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
) -> Result<(), MigrationError> {
    let active = read_active_version(graph).await?;

    match active {
        None => {
            info!(
                version = *SCHEMA_VERSION,
                "fresh install — creating tables from ontology and recording initial schema version"
            );
            create_prefixed_tables(graph, source, ontology, metrics).await?;
            write_schema_version(graph, *SCHEMA_VERSION).await?;
            metrics.record("complete", "fresh_install");
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
        Some(active_version) if active_version > *SCHEMA_VERSION => {
            warn!(
                active_version,
                embedded_version = *SCHEMA_VERSION,
                "active schema version is newer than this binary — rolling back to the embedded version"
            );
            run_rollback(
                graph,
                source,
                lock_service,
                ontology,
                metrics,
                campaign,
                active_version,
            )
            .await
        }
        Some(active_version) => {
            info!(
                active_version,
                target_version = *SCHEMA_VERSION,
                "schema version mismatch detected — starting migration"
            );
            run_migration(
                graph,
                source,
                lock_service,
                ontology,
                metrics,
                campaign,
                active_version,
            )
            .await
        }
    }
}

/// Applies the generated namespace-storage DDL (`config/graph_unversioned.sql`)
/// for schema version `version`, in file order. Idempotent: every statement is
/// `CREATE ... IF NOT EXISTS` or `DROP ... IF EXISTS` followed by `CREATE`.
///
/// `version` must be the active schema version: the refreshable MV reads that
/// version's physical tables (`v<version>_gl_edge`, …), so applying it against
/// tables that do not yet exist would leave the nightly refresh erroring.
pub async fn apply_unversioned_ddl(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    let ddl = resolve_active_placeholders(UNVERSIONED_DDL, version);
    for statement in split_sql_statements(&ddl) {
        graph.execute(statement).await?;
    }
    Ok(())
}

/// Drops a specific schema version's refreshable MV. Called on the outgoing
/// active version after a promotion so it stops refreshing against the tables
/// GC is about to drop.
pub async fn drop_unversioned_refresh_mv(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    let name = format!(
        "{}{UNVERSIONED_REFRESH_MV_BASE_NAME}",
        table_prefix(version)
    );
    graph
        .execute(&format!("DROP VIEW IF EXISTS {name}"))
        .await?;
    Ok(())
}

/// Substitutes the two apply-time placeholders in the generated DDL:
/// `__ACTIVE_PREFIX__` with the version's table prefix (empty at version 0,
/// else `v<N>_`) and `__ACTIVE_VERSION__` with the version number.
fn resolve_active_placeholders(ddl: &str, version: u32) -> String {
    ddl.replace("__ACTIVE_PREFIX__", &table_prefix(version))
        .replace("__ACTIVE_VERSION__", &version.to_string())
}

/// Splits a SQL script into individual statements on top-level `;`, ignoring
/// semicolons inside string literals, inside parentheses, and inside `--` line
/// comments. Each returned statement has its leading comment and blank lines
/// stripped so it starts at the SQL keyword.
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut in_string = false;
    let mut in_line_comment = false;
    let mut depth = 0i32;
    let mut start = 0;
    let bytes = sql.as_bytes();

    for (i, c) in sql.char_indices() {
        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        match c {
            '\'' if i == 0 || bytes[i - 1] != b'\\' => in_string = !in_string,
            '-' if !in_string && bytes.get(i + 1) == Some(&b'-') => in_line_comment = true,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            ';' if !in_string && depth == 0 => {
                let statement = strip_leading_comment_lines(sql[start..i].trim());
                if !statement.is_empty() {
                    statements.push(statement);
                }
                start = i + 1;
            }
            _ => {}
        }
    }

    let tail = strip_leading_comment_lines(sql[start..].trim());
    if !tail.is_empty() {
        statements.push(tail);
    }
    statements
}

fn strip_leading_comment_lines(statement: &str) -> &str {
    let mut offset = 0;
    for line in statement.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            offset += line.len() + 1;
        } else {
            break;
        }
    }
    statement[offset.min(statement.len())..].trim()
}

/// Rolls back to the embedded `SCHEMA_VERSION` after an older binary is deployed over a newer
/// active version; re-activate vs. rebuild is decided by table-set completeness, not lag-prone status.
async fn run_rollback(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), MigrationError> {
    acquire_migration_lock(lock_service, metrics).await?;

    let current_active = read_active_version(graph).await?;
    if current_active == Some(*SCHEMA_VERSION) {
        info!(
            version = *SCHEMA_VERSION,
            "rollback already completed by another pod — releasing lock"
        );
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "skipped");
        return Ok(());
    }

    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected = embedded_object_names(ontology, &prefix);

    let tables_complete = match version_tables_complete(graph, *SCHEMA_VERSION, &expected).await {
        Ok(complete) => complete,
        Err(e) => {
            warn!(error = %e, "failed to check embedded version's table-set completeness — releasing lock");
            let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
            return Err(e.into());
        }
    };

    if tables_complete {
        info!(
            active_version,
            target_version = *SCHEMA_VERSION,
            "embedded version's table set is complete — rolling back via direct re-activation"
        );

        let reactivate_result = reactivate_version(graph, *SCHEMA_VERSION).await;
        if let Err(e) = reactivate_result {
            warn!(error = %e, "failed to re-activate embedded version — releasing lock");
            let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
            return Err(e.into());
        }

        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "rollback_reactivated");

        info!(
            version = *SCHEMA_VERSION,
            "rollback complete — resuming on existing tables"
        );
        return Ok(());
    }

    info!(
        active_version,
        target_version = *SCHEMA_VERSION,
        "embedded version's table set is incomplete — rolling back via rebuild"
    );

    // Creation is IF NOT EXISTS: a surviving checkpoint would make the backfill skip indexed projects.
    if let Err(e) = drop_stale_version_objects(graph, *SCHEMA_VERSION).await {
        warn!(error = %e, "failed to clear stale embedded-version objects before rebuild — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return Err(e);
    }

    run_migration_locked(
        graph,
        source,
        lock_service,
        ontology,
        metrics,
        campaign,
        active_version,
    )
    .await
}

/// Every object name `create_prefixed_tables` creates for `prefix` — tables, dictionaries,
/// and materialized views all land in `system.tables`, so all three count for completeness.
fn embedded_object_names(ontology: &ontology::Ontology, prefix: &str) -> Vec<String> {
    let mut names: Vec<String> = generate_graph_tables_with_prefix(ontology, prefix)
        .into_iter()
        .map(|t| t.name)
        .collect();
    names.extend(
        generate_graph_dictionaries_with_prefix(ontology, prefix)
            .into_iter()
            .map(|d| d.name),
    );
    names.extend(
        generate_graph_materialized_views_with_prefix(ontology, prefix)
            .into_iter()
            .map(|v| v.name),
    );
    names
}

/// Drops every surviving `v<version>_*` object; views and dictionaries select from tables, so they go first.
async fn drop_stale_version_objects(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), MigrationError> {
    let mut objects = list_version_objects(graph, version).await?;
    objects.sort_by_key(|o| match drop_kind_for_engine(&o.engine) {
        "VIEW" => 0,
        "DICTIONARY" => 1,
        _ => 2,
    });

    for object in &objects {
        let kind = drop_kind_for_engine(&object.engine);
        info!(object = %object.name, kind, "dropping stale object before rollback rebuild");
        graph
            .execute(&format!("DROP {kind} IF EXISTS {}", object.name))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: object.name.clone(),
                reason: e.to_string(),
            })?;
    }

    Ok(())
}

async fn reactivate_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    // Activate first: a crash between the two writes leaves a duplicate active row, never zero.
    mark_version_active(graph, version).await?;
    let versions = read_all_versions(graph).await?;
    for entry in &versions {
        if entry.status == "active" && entry.version != version {
            mark_version_retired(graph, entry.version).await?;
        }
    }
    Ok(())
}

async fn run_migration(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), MigrationError> {
    acquire_migration_lock(lock_service, metrics).await?;

    let current_active = read_active_version(graph).await?;
    if current_active == Some(*SCHEMA_VERSION) {
        info!(
            version = *SCHEMA_VERSION,
            "migration already completed by another pod — releasing lock"
        );
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "skipped");
        return Ok(());
    }

    run_migration_locked(
        graph,
        source,
        lock_service,
        ontology,
        metrics,
        campaign,
        active_version,
    )
    .await
}

/// Caller must hold the migration lock; released on every exit path.
async fn run_migration_locked(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), MigrationError> {
    // Drain is a no-op: the dispatcher runs no engine, so no in-flight messages
    // exist. Reserved for future dual-write scenarios.
    metrics.record("drain", "success");

    let create_result = create_prefixed_tables(graph, source, ontology, metrics).await;
    if let Err(ref e) = create_result {
        warn!(error = %e, "failed to create new-prefix tables — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return create_result;
    }

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

    // Open the re-index campaign for this migration. The completion checker
    // clears it on promotion.
    campaign.set(campaign_id_for_version(*SCHEMA_VERSION));

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
    source: &DictionarySource<'_>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    let new_prefix = table_prefix(*SCHEMA_VERSION);
    let tables = generate_graph_tables_with_prefix(ontology, &new_prefix);

    for table in &tables {
        info!(table = %table.name, "creating table");
        graph
            .execute(&emit_create_table(table))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: table.name.clone(),
                reason: e.to_string(),
            })?;
    }

    info!(count = tables.len(), prefix = %new_prefix, "new-prefix tables created");

    let dicts = generate_graph_dictionaries_with_prefix(ontology, &new_prefix);
    for dict in &dicts {
        info!(dictionary = %dict.name, source = %dict.source_table, "creating dictionary");
        graph
            .execute(&emit_create_dictionary(dict, source))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: dict.name.clone(),
                reason: e.to_string(),
            })?;
    }

    // Materialized views depend on the tables they SELECT FROM, so they
    // must be created after all tables exist.
    let views = generate_graph_materialized_views_with_prefix(ontology, &new_prefix);
    for view in &views {
        info!(view = %view.name, "creating materialized view");
        graph
            .execute(&emit_create_materialized_view(view))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: view.name.clone(),
                reason: e.to_string(),
            })?;
    }

    info!(
        tables = tables.len(),
        dictionaries = dicts.len(),
        views = views.len(),
        prefix = %new_prefix,
        "new-prefix tables, dictionaries, and materialized views created"
    );
    metrics.record("create_tables", "success");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_graph_tables_produces_ddl_for_all_ontology_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = generate_graph_tables_with_prefix(&ontology, "");
        assert!(
            !tables.is_empty(),
            "generate_graph_tables must produce at least one table"
        );
        for table in &tables {
            let ddl = emit_create_table(table);
            assert!(
                ddl.contains("CREATE TABLE IF NOT EXISTS"),
                "DDL for '{}' must start with CREATE TABLE IF NOT EXISTS: {ddl}",
                table.name
            );
        }
    }

    #[test]
    fn generate_graph_tables_includes_auxiliary_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = generate_graph_tables_with_prefix(&ontology, "");
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in [
            "checkpoint",
            "code_indexing_checkpoint",
            "namespace_deletion_schedule",
        ] {
            assert!(
                names.contains(&expected),
                "expected auxiliary table '{expected}' in generated tables; got: {names:?}"
            );
        }
    }

    #[test]
    fn prefixed_tables_have_correct_names() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let prefix = "v2_";
        let tables = generate_graph_tables_with_prefix(&ontology, prefix);
        for table in &tables {
            assert!(
                table.name.starts_with(prefix),
                "table '{}' should be prefixed with '{prefix}'",
                table.name
            );
            let ddl = emit_create_table(table);
            assert!(
                ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {}", table.name)),
                "DDL should contain prefixed table name '{}': {ddl}",
                table.name
            );
        }
    }

    #[test]
    fn migration_metrics_new_does_not_panic() {
        let _metrics = MigrationMetrics::new();
    }

    #[test]
    fn lock_key_is_stable() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }

    #[test]
    fn split_sql_statements_separates_on_top_level_semicolons() {
        let sql = "CREATE TABLE a (id Int64);\nDROP VIEW IF EXISTS b;\nCREATE VIEW b AS SELECT 1;";
        let statements = split_sql_statements(sql);
        assert_eq!(
            statements,
            vec![
                "CREATE TABLE a (id Int64)",
                "DROP VIEW IF EXISTS b",
                "CREATE VIEW b AS SELECT 1",
            ]
        );
    }

    #[test]
    fn split_sql_statements_ignores_semicolons_in_string_literals() {
        let sql = "CREATE VIEW v AS SELECT 'a;b' AS s;\nCREATE VIEW w AS SELECT 1;";
        let statements = split_sql_statements(sql);
        assert_eq!(
            statements,
            vec![
                "CREATE VIEW v AS SELECT 'a;b' AS s",
                "CREATE VIEW w AS SELECT 1",
            ]
        );
    }

    #[test]
    fn split_sql_statements_ignores_semicolons_in_line_comments() {
        let sql = "-- one; two; three\nCREATE TABLE a (id Int64);";
        let statements = split_sql_statements(sql);
        assert_eq!(statements, vec!["CREATE TABLE a (id Int64)"]);
    }

    #[test]
    fn split_sql_statements_keeps_a_trailing_statement_without_semicolon() {
        let sql = "CREATE TABLE a (id Int64);\nCREATE TABLE b (id Int64)";
        let statements = split_sql_statements(sql);
        assert_eq!(
            statements,
            vec!["CREATE TABLE a (id Int64)", "CREATE TABLE b (id Int64)"]
        );
    }

    #[test]
    fn generated_unversioned_ddl_splits_into_the_three_expected_statements() {
        let statements = split_sql_statements(UNVERSIONED_DDL);
        assert_eq!(statements.len(), 3, "expected three unversioned statements");
        assert!(statements[0].starts_with("CREATE TABLE IF NOT EXISTS namespace_storage_snapshot"));
        assert!(statements[1].starts_with(
            "DROP VIEW IF EXISTS __ACTIVE_PREFIX__namespace_storage_snapshot_refresh"
        ));
        assert!(statements[2].starts_with(
            "CREATE MATERIALIZED VIEW __ACTIVE_PREFIX__namespace_storage_snapshot_refresh"
        ));
    }

    #[test]
    fn generated_ddl_writes_the_refresh_mv_with_the_expected_base_name() {
        assert!(
            UNVERSIONED_DDL.contains(&format!(
                "__ACTIVE_PREFIX__{UNVERSIONED_REFRESH_MV_BASE_NAME}"
            )),
            "generated DDL and UNVERSIONED_REFRESH_MV_BASE_NAME have drifted"
        );
    }

    #[test]
    fn resolve_active_placeholders_prefixes_names_at_a_nonzero_version() {
        let resolved = resolve_active_placeholders(UNVERSIONED_DDL, 5);
        assert!(resolved.contains("v5_namespace_storage_snapshot_refresh"));
        assert!(resolved.contains("5 AS schema_version"));
        assert!(!resolved.contains("__ACTIVE_PREFIX__"));
        assert!(!resolved.contains("__ACTIVE_VERSION__"));
    }

    #[test]
    fn resolve_active_placeholders_drops_the_prefix_at_version_zero() {
        let resolved = resolve_active_placeholders(UNVERSIONED_DDL, 0);
        assert!(resolved.contains("CREATE MATERIALIZED VIEW namespace_storage_snapshot_refresh"));
        assert!(!resolved.contains("v0_namespace_storage_snapshot_refresh"));
        assert!(!resolved.contains("__ACTIVE_PREFIX__"));
        assert!(!resolved.contains("__ACTIVE_VERSION__"));
    }
}
