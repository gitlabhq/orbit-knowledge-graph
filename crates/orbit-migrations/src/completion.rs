use std::collections::HashSet;
use std::sync::LazyLock;

use arrow::datatypes::UInt64Type;
use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::{TopLevelSplit, TraversalPath};

use crate::execute::{CHECKPOINT_TABLE, MigrationError};
use crate::ledger::MigrationLedger;
use crate::scope::{
    CODE_INDEXING_CHECKPOINT_TABLE, MigrationScope, find_invalidated_pipelines,
    widen_scope_for_shared_table_writers,
};
use crate::version::{
    list_version_entities, prefixed_table_name, read_active_version, table_prefix,
};

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

static COUNT_ACTIVE_CODE_PROJECTS_REINDEXED: LazyLock<String> = LazyLock::new(|| {
    let top = orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
    format!(
        "SELECT toUInt64(count()) AS expected_projects, \
                toUInt64(countIf(project_id IN (\
                    SELECT project_id FROM {{migrating_table:Identifier}} FINAL \
                    WHERE _deleted = false))) AS reindexed_projects \
         FROM (\
             SELECT DISTINCT project_id \
             FROM {{active_table:Identifier}} FINAL \
             WHERE _deleted = false \
               AND extract(traversal_path, '{top}') IN {{paths:Array(String)}})"
    )
});

// Below 100% so projects deleted, moved, or newly failing during the migration cannot block it.
const MIN_REINDEXED_CODE_PROJECTS_PERCENT: f64 = 99.0;

#[derive(Debug)]
pub struct SdlcReindexProgress {
    pub completed_namespaces: u64,
    pub ready: bool,
}

#[derive(Debug)]
pub struct CodeReindexProgress {
    pub expected_projects: u64,
    pub reindexed_projects: u64,
    pub ready: bool,
}

impl CodeReindexProgress {
    fn nothing_expected() -> Self {
        Self::from_counts(0, 0)
    }

    fn from_counts(expected_projects: u64, reindexed_projects: u64) -> Self {
        let reindexed_percent = if expected_projects == 0 {
            100.0
        } else {
            reindexed_projects as f64 * 100.0 / expected_projects as f64
        };

        Self {
            expected_projects,
            reindexed_projects,
            ready: reindexed_percent >= MIN_REINDEXED_CODE_PROJECTS_PERCENT,
        }
    }
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

pub async fn check_code_reindex_progress(
    graph: &ArrowClickHouseClient,
    version: u32,
    enabled_paths: &[TraversalPath],
) -> Result<CodeReindexProgress, MigrationError> {
    let active_version = read_active_version(graph).await?.unwrap_or(0);
    let active_table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, active_version);
    let migrating_table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, version);

    let active_table_exists = list_version_entities(graph, active_version)
        .await?
        .iter()
        .any(|entity| entity.name == active_table);
    if !active_table_exists || enabled_paths.is_empty() {
        return Ok(CodeReindexProgress::nothing_expected());
    }

    let batches = graph
        .query(&COUNT_ACTIVE_CODE_PROJECTS_REINDEXED)
        .param("active_table", &active_table)
        .param("migrating_table", &migrating_table)
        .param("paths", enabled_paths)
        .fetch_arrow()
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: migrating_table.clone(),
            reason: error.to_string(),
        })?;

    let count = |column| {
        batches
            .first()
            .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, column, 0))
            .unwrap_or(0)
    };
    Ok(CodeReindexProgress::from_counts(
        count("expected_projects"),
        count("reindexed_projects"),
    ))
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
