//! Additive schema reconciliation.
//!
//! After the migration orchestrator confirms the schema version matches,
//! this module diffs the ontology against live ClickHouse and emits
//! ALTER TABLE statements for non-destructive changes:
//!
//! - Indexes: add/drop/materialize
//! - Projections: add/drop/materialize
//! - Columns: add new nullable columns, change codec, change default
//! - Settings: index_granularity
//!
//! Column type changes, sort key changes, and column removal still
//! require a full schema version bump.

use std::collections::{HashMap, HashSet};

use clickhouse_client::ArrowClickHouseClient;
use ontology::{StorageColumn, StorageIndex, StorageProjection};
use tracing::{debug, info};

use super::version::{SCHEMA_VERSION, table_prefix};

#[derive(Debug, thiserror::Error)]
#[error("schema reconciliation failed: {0}")]
pub struct ReconcileError(String);

/// Reconcile all ontology tables against ClickHouse.
pub async fn reconcile(
    client: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
) -> Result<(), ReconcileError> {
    let prefix = table_prefix(*SCHEMA_VERSION);
    let mut total = 0usize;

    for node in ontology.nodes() {
        let table = format!("{prefix}{}", node.destination_table);
        total += reconcile_indexes(client, &table, &node.storage.indexes).await?;
        total += reconcile_projections(client, &table, &node.storage.projections).await?;
        total += reconcile_columns(client, &table, &node.storage.columns).await?;
    }

    for edge_table_name in ontology.edge_tables() {
        let config = ontology
            .edge_table_config(edge_table_name)
            .expect("edge_tables() only returns known keys");
        let table = format!("{prefix}{edge_table_name}");
        total += reconcile_indexes(client, &table, &config.storage.indexes).await?;
        total += reconcile_projections(client, &table, &config.storage.projections).await?;
        total += reconcile_columns(client, &table, &config.storage.columns).await?;
    }

    if total > 0 {
        info!(alterations = total, "schema reconciliation complete");
    } else {
        debug!("schema is up to date");
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Indexes
// ─────────────────────────────────────────────────────────────────────────────

async fn reconcile_indexes(
    client: &ArrowClickHouseClient,
    table: &str,
    desired: &[StorageIndex],
) -> Result<usize, ReconcileError> {
    let existing = query_names(client, "system.data_skipping_indices", table).await?;
    let desired_names: HashSet<&str> = desired.iter().map(|i| i.name.as_str()).collect();
    let mut count = 0;

    for idx in desired {
        if !existing.contains(idx.name.as_str()) {
            exec(
                client,
                &format!(
                    "ALTER TABLE {table} ADD INDEX {} {} TYPE {} GRANULARITY {}",
                    idx.name, idx.column, idx.index_type, idx.granularity
                ),
            )
            .await?;
            exec(
                client,
                &format!("ALTER TABLE {table} MATERIALIZE INDEX {}", idx.name),
            )
            .await?;
            info!(table, index = idx.name, "added index");
            count += 1;
        }
    }

    for name in &existing {
        if !desired_names.contains(name.as_str()) {
            exec(client, &format!("ALTER TABLE {table} DROP INDEX {name}")).await?;
            info!(table, index = name, "dropped index");
            count += 1;
        }
    }

    Ok(count)
}

// ─────────────────────────────────────────────────────────────────────────────
// Projections
// ─────────────────────────────────────────────────────────────────────────────

async fn reconcile_projections(
    client: &ArrowClickHouseClient,
    table: &str,
    desired: &[StorageProjection],
) -> Result<usize, ReconcileError> {
    let existing = query_names(client, "system.projections", table).await?;
    let desired_names: HashSet<&str> = desired.iter().map(|p| proj_name(p)).collect();
    let mut count = 0;

    for proj in desired {
        let name = proj_name(proj);
        if !existing.contains(name) {
            exec(
                client,
                &format!(
                    "ALTER TABLE {table} ADD PROJECTION {name} ({})",
                    proj_body(proj)
                ),
            )
            .await?;
            exec(
                client,
                &format!("ALTER TABLE {table} MATERIALIZE PROJECTION {name}"),
            )
            .await?;
            info!(table, projection = name, "added projection");
            count += 1;
        }
    }

    for name in &existing {
        if !desired_names.contains(name.as_str()) {
            exec(
                client,
                &format!("ALTER TABLE {table} DROP PROJECTION {name}"),
            )
            .await?;
            info!(table, projection = name, "dropped projection");
            count += 1;
        }
    }

    Ok(count)
}

// ─────────────────────────────────────────────────────────────────────────────
// Columns: codec, default, add new
// ─────────────────────────────────────────────────────────────────────────────

/// Column metadata from system.columns.
struct LiveColumn {
    name: String,
    ch_type: String,
    default_expression: String,
    codec_expression: String,
}

async fn reconcile_columns(
    client: &ArrowClickHouseClient,
    table: &str,
    desired: &[StorageColumn],
) -> Result<usize, ReconcileError> {
    let live = query_columns(client, table).await?;
    let live_by_name: HashMap<&str, &LiveColumn> =
        live.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut count = 0;

    for col in desired {
        match live_by_name.get(col.name.as_str()) {
            Some(live_col) => {
                // Codec drift.
                if let Some(desired_codecs) = &col.codec {
                    let want = format!("CODEC({})", desired_codecs.join(", "));
                    if !live_col.codec_expression.eq_ignore_ascii_case(&want) {
                        exec(
                            client,
                            &format!(
                                "ALTER TABLE {table} MODIFY COLUMN {} {} {want}",
                                col.name, col.ch_type
                            ),
                        )
                        .await?;
                        info!(table, column = col.name, codec = %want, "updated codec");
                        count += 1;
                    }
                }

                // Default drift.
                if let Some(desired_default) = &col.default {
                    if live_col.default_expression != *desired_default {
                        exec(
                            client,
                            &format!(
                                "ALTER TABLE {table} MODIFY COLUMN {} DEFAULT {desired_default}",
                                col.name
                            ),
                        )
                        .await?;
                        info!(table, column = col.name, "updated default");
                        count += 1;
                    }
                }
            }
            None => {
                // New column — only safe if nullable or has a default.
                if col.ch_type.contains("Nullable") || col.default.is_some() {
                    let mut parts = vec![format!(
                        "ALTER TABLE {table} ADD COLUMN {} {}",
                        col.name, col.ch_type
                    )];
                    if let Some(default) = &col.default {
                        parts.push(format!("DEFAULT {default}"));
                    }
                    if let Some(codecs) = &col.codec {
                        parts.push(format!("CODEC({})", codecs.join(", ")));
                    }
                    exec(client, &parts.join(" ")).await?;
                    info!(table, column = col.name, "added column");
                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

async fn query_columns(
    client: &ArrowClickHouseClient,
    table: &str,
) -> Result<Vec<LiveColumn>, ReconcileError> {
    use arrow::array::AsArray;

    let sql = format!(
        "SELECT name, type, default_expression, codec_expression \
         FROM system.columns WHERE table = '{table}'"
    );
    let batches = client
        .query(&sql)
        .fetch_arrow()
        .await
        .map_err(|e| ReconcileError(format!("querying system.columns for {table}: {e}")))?;

    let mut cols = Vec::new();
    for batch in &batches {
        let names = batch
            .column_by_name("name")
            .expect("name")
            .as_string::<i32>();
        let types = batch
            .column_by_name("type")
            .expect("type")
            .as_string::<i32>();
        let defaults = batch
            .column_by_name("default_expression")
            .expect("default_expression")
            .as_string::<i32>();
        let codecs = batch
            .column_by_name("codec_expression")
            .expect("codec_expression")
            .as_string::<i32>();

        for i in 0..batch.num_rows() {
            cols.push(LiveColumn {
                name: names.value(i).to_string(),
                ch_type: types.value(i).to_string(),
                default_expression: defaults.value(i).to_string(),
                codec_expression: codecs.value(i).to_string(),
            });
        }
    }
    Ok(cols)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

async fn query_names(
    client: &ArrowClickHouseClient,
    system_table: &str,
    table: &str,
) -> Result<HashSet<String>, ReconcileError> {
    use arrow::array::AsArray;

    let batches = client
        .query(&format!(
            "SELECT name FROM {system_table} WHERE table = '{table}'"
        ))
        .fetch_arrow()
        .await
        .map_err(|e| ReconcileError(format!("querying {system_table} for {table}: {e}")))?;

    let mut names = HashSet::new();
    for batch in &batches {
        let arr = batch
            .column_by_name("name")
            .expect("name")
            .as_string::<i32>();
        for i in 0..arr.len() {
            names.insert(arr.value(i).to_string());
        }
    }
    Ok(names)
}

fn proj_name(proj: &StorageProjection) -> &str {
    match proj {
        StorageProjection::Reorder { name, .. } | StorageProjection::Aggregate { name, .. } => name,
    }
}

fn proj_body(proj: &StorageProjection) -> String {
    match proj {
        StorageProjection::Reorder { order_by, .. } => {
            format!("SELECT * ORDER BY ({})", order_by.join(", "))
        }
        StorageProjection::Aggregate {
            select, group_by, ..
        } => format!(
            "SELECT {} GROUP BY {}",
            select.join(", "),
            group_by.join(", ")
        ),
    }
}

async fn exec(client: &ArrowClickHouseClient, sql: &str) -> Result<(), ReconcileError> {
    debug!(sql, "executing ALTER");
    client
        .execute(sql)
        .await
        .map_err(|e| ReconcileError(format!("{sql}: {e}")))
}
