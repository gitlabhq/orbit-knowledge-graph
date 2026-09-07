use std::collections::HashSet;
use std::sync::LazyLock;

use arrow::datatypes::UInt64Type;
use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TopLevelSplit;

use crate::execute::{CHECKPOINT_TABLE, MigrationError};
use crate::ledger::MigrationLedger;
use crate::scope::{
    MigrationScope, find_invalidated_pipelines, widen_scope_for_shared_table_writers,
};
use crate::version::{read_active_version, table_prefix};

static FETCH_ENABLED_NAMESPACES: LazyLock<String> = LazyLock::new(|| {
    let deleted_column = ontology::siphon_deleted_column();
    format!(
        "SELECT DISTINCT root_namespace_id, traversal_path \
         FROM siphon_knowledge_graph_enabled_namespaces \
         WHERE {deleted_column} = false"
    )
});

const GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS: &str = "\
SELECT toInt64(splitByChar('.', key)[2]) AS namespace_id \
FROM {table:Identifier} FINAL \
WHERE _deleted = false \
  AND cursor_values IN ('null', '') \
  AND length(splitByChar('.', key)) = 3 \
  AND splitByChar('.', key)[1] = 'ns' \
  AND match(splitByChar('.', key)[2], '^[0-9]+$') \
  AND splitByChar('.', key)[3] IN {plans:Array(String)} \
GROUP BY namespace_id \
HAVING uniqExact(splitByChar('.', key)[3]) = {plan_count:UInt64}";

const COUNT_COMPLETE_GLOBAL_PLANS: &str = "\
SELECT count(DISTINCT splitByChar('.', key)[2]) AS plan_count \
FROM {table:Identifier} FINAL \
WHERE _deleted = false \
  AND cursor_values IN ('null', '') \
  AND length(splitByChar('.', key)) = 2 \
  AND splitByChar('.', key)[1] = 'global' \
  AND splitByChar('.', key)[2] IN {plans:Array(String)}";

#[derive(Debug)]
pub struct SdlcReindexProgress {
    pub completed_namespaces: u64,
    pub ready: bool,
}

pub async fn resolve_migration_scope(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    migrating_version: u32,
) -> Result<MigrationScope, MigrationError> {
    let active = read_active_version(graph).await?.unwrap_or(0);
    let ledger = MigrationLedger::load_embedded().map_err(MigrationError::Ledger)?;
    let requested_scope = ledger.resolve_scope_between(active, migrating_version);
    Ok(widen_scope_for_shared_table_writers(
        ontology,
        &requested_scope,
    ))
}

pub async fn fetch_enabled_top_level_namespaces(
    datalake: &ArrowClickHouseClient,
) -> Result<TopLevelSplit, MigrationError> {
    let batches = datalake
        .query(&FETCH_ENABLED_NAMESPACES)
        .fetch_arrow()
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: "siphon_knowledge_graph_enabled_namespaces".into(),
            reason: error.to_string(),
        })?;

    let ids = i64::extract_column(&batches, 0).map_err(|error| MigrationError::Ddl {
        entity_name: "siphon_knowledge_graph_enabled_namespaces".into(),
        reason: error.to_string(),
    })?;

    let paths = String::extract_column(&batches, 1).map_err(|error| MigrationError::Ddl {
        entity_name: "siphon_knowledge_graph_enabled_namespaces".into(),
        reason: error.to_string(),
    })?;

    let split = orbit_utils::traversal_path::split_top_level(
        ids,
        paths
            .into_iter()
            .map(orbit_utils::traversal_path::TraversalPath::new_unchecked)
            .collect(),
    );

    if !split.skipped.is_empty() {
        tracing::warn!(
            skipped = ?split.skipped,
            "excluding non-top-level namespace paths from completion gate"
        );
    }

    Ok(split)
}

pub async fn check_sdlc_reindex_progress(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    scope: &MigrationScope,
    version: u32,
    enabled_namespace_ids: &[i64],
) -> Result<SdlcReindexProgress, MigrationError> {
    let prefix = table_prefix(version);
    let checkpoint_table = format!("{prefix}{CHECKPOINT_TABLE}");
    let pipelines = find_invalidated_pipelines(ontology, scope);

    let completed_namespace_ids =
        namespace_ids_with_completed_plans(graph, &checkpoint_table, &pipelines.namespaced).await?;

    let completed_namespaces = enabled_namespace_ids
        .iter()
        .filter(|id| completed_namespace_ids.contains(id))
        .count() as u64;

    let namespaced_ready = pipelines.namespaced.is_empty()
        || completed_namespaces == enabled_namespace_ids.len() as u64;

    let completed_global =
        count_completed_global_plans(graph, &checkpoint_table, &pipelines.global).await?;
    let global_ready = completed_global as usize == pipelines.global.len();

    Ok(SdlcReindexProgress {
        completed_namespaces,
        ready: namespaced_ready && global_ready,
    })
}

async fn namespace_ids_with_completed_plans(
    graph: &ArrowClickHouseClient,
    checkpoint_table: &str,
    required_plan_names: &[String],
) -> Result<HashSet<i64>, MigrationError> {
    if required_plan_names.is_empty() {
        return Ok(HashSet::new());
    }
    let batches = graph
        .query(GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS)
        .param("table", checkpoint_table)
        .param("plans", required_plan_names)
        .param("plan_count", required_plan_names.len() as u64)
        .fetch_arrow()
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: checkpoint_table.to_string(),
            reason: error.to_string(),
        })?;

    Ok(i64::extract_column(&batches, 0)
        .map_err(|error| MigrationError::Ddl {
            entity_name: checkpoint_table.to_string(),
            reason: error.to_string(),
        })?
        .into_iter()
        .collect())
}

async fn count_completed_global_plans(
    graph: &ArrowClickHouseClient,
    checkpoint_table: &str,
    global_plans: &[String],
) -> Result<u64, MigrationError> {
    if global_plans.is_empty() {
        return Ok(0);
    }
    let batches = graph
        .query(COUNT_COMPLETE_GLOBAL_PLANS)
        .param("table", checkpoint_table)
        .param("plans", global_plans)
        .fetch_arrow()
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: checkpoint_table.to_string(),
            reason: error.to_string(),
        })?;

    Ok(batches
        .first()
        .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "plan_count", 0))
        .unwrap_or(0))
}
