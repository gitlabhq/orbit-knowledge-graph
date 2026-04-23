//! Additive schema reconciliation.
//!
//! After the migration orchestrator confirms the schema version matches,
//! this module diffs the ontology's declared indexes and projections against
//! what exists in ClickHouse and emits ALTER TABLE statements to close the gap.
//!
//! Only additive/drop operations on indexes and projections are supported.
//! Column or sort-key changes still require a full schema version bump.

use std::collections::HashSet;

use clickhouse_client::ArrowClickHouseClient;
use tracing::{debug, info, warn};

use super::version::{SCHEMA_VERSION, table_prefix};

/// Reconcile indexes and projections for all ontology tables against ClickHouse.
pub async fn reconcile(
    client: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
) -> Result<(), ReconcileError> {
    let prefix = table_prefix(*SCHEMA_VERSION);
    let mut total = 0usize;

    for node in ontology.nodes() {
        let table = format!("{prefix}{}", node.destination_table);
        total += reconcile_table(
            client,
            &table,
            &node.storage.indexes,
            &node.storage.projections,
        )
        .await?;
    }

    for edge_table_name in ontology.edge_tables() {
        let config = ontology
            .edge_table_config(edge_table_name)
            .expect("edge_tables() only returns known keys");
        let table = format!("{prefix}{edge_table_name}");
        total += reconcile_table(
            client,
            &table,
            &config.storage.indexes,
            &config.storage.projections,
        )
        .await?;
    }

    if total > 0 {
        info!(
            alterations = total,
            "additive schema reconciliation complete"
        );
    } else {
        debug!("indexes and projections are up to date");
    }

    Ok(())
}

async fn reconcile_table(
    client: &ArrowClickHouseClient,
    table: &str,
    desired_indexes: &[ontology::StorageIndex],
    desired_projections: &[ontology::StorageProjection],
) -> Result<usize, ReconcileError> {
    let existing_indexes = query_names(client, "system.data_skipping_indices", table).await?;
    let existing_projections = query_names(client, "system.projections", table).await?;

    let desired_idx_names: HashSet<&str> =
        desired_indexes.iter().map(|i| i.name.as_str()).collect();
    let desired_proj_names: HashSet<&str> = desired_projections
        .iter()
        .map(|p| projection_name(p))
        .collect();

    let mut count = 0;

    // Add missing indexes.
    for idx in desired_indexes {
        if !existing_indexes.contains(idx.name.as_str()) {
            let sql = format!(
                "ALTER TABLE {table} ADD INDEX {} {} TYPE {} GRANULARITY {}",
                idx.name, idx.column, idx.index_type, idx.granularity
            );
            execute(client, &sql).await?;
            execute(
                client,
                &format!("ALTER TABLE {table} MATERIALIZE INDEX {}", idx.name),
            )
            .await?;
            info!(table, index = idx.name, "added index");
            count += 1;
        }
    }

    // Drop removed indexes.
    for name in &existing_indexes {
        if !desired_idx_names.contains(name.as_str()) {
            execute(client, &format!("ALTER TABLE {table} DROP INDEX {name}")).await?;
            info!(table, index = name, "dropped index");
            count += 1;
        }
    }

    // Add missing projections.
    for proj in desired_projections {
        let name = projection_name(proj);
        if !existing_projections.contains(name) {
            let body = projection_body(proj);
            let sql = format!("ALTER TABLE {table} ADD PROJECTION {name} ({body})");
            execute(client, &sql).await?;
            execute(
                client,
                &format!("ALTER TABLE {table} MATERIALIZE PROJECTION {name}"),
            )
            .await?;
            info!(table, projection = name, "added projection");
            count += 1;
        }
    }

    // Drop removed projections.
    for name in &existing_projections {
        if !desired_proj_names.contains(name.as_str()) {
            execute(
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

async fn query_names(
    client: &ArrowClickHouseClient,
    system_table: &str,
    table: &str,
) -> Result<HashSet<String>, ReconcileError> {
    use arrow::array::AsArray;

    let sql = format!("SELECT name FROM {system_table} WHERE table = '{table}'");
    let batches = client
        .query(&sql)
        .fetch_arrow()
        .await
        .map_err(|e| ReconcileError(format!("querying {system_table} for {table}: {e}")))?;

    let mut names = HashSet::new();
    for batch in &batches {
        let col = batch.column_by_name("name").expect("name column");
        let arr = col.as_string::<i32>();
        for i in 0..arr.len() {
            names.insert(arr.value(i).to_string());
        }
    }
    Ok(names)
}

fn projection_name(proj: &ontology::StorageProjection) -> &str {
    match proj {
        ontology::StorageProjection::Reorder { name, .. } => name,
        ontology::StorageProjection::Aggregate { name, .. } => name,
    }
}

fn projection_body(proj: &ontology::StorageProjection) -> String {
    match proj {
        ontology::StorageProjection::Reorder { order_by, .. } => {
            format!("SELECT * ORDER BY ({})", order_by.join(", "))
        }
        ontology::StorageProjection::Aggregate {
            select, group_by, ..
        } => {
            format!(
                "SELECT {} GROUP BY {}",
                select.join(", "),
                group_by.join(", ")
            )
        }
    }
}

async fn execute(client: &ArrowClickHouseClient, sql: &str) -> Result<(), ReconcileError> {
    debug!(sql, "executing ALTER");
    client
        .execute(sql)
        .await
        .map_err(|e| ReconcileError(format!("{sql}: {e}")))
}

#[derive(Debug, thiserror::Error)]
#[error("schema reconciliation failed: {0}")]
pub struct ReconcileError(String);
