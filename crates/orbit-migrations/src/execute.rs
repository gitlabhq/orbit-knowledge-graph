use std::collections::HashSet;

use arrow::datatypes::UInt64Type;
use clickhouse_client::ArrowClickHouseClient;
use orbit_utils::arrow::ArrowUtils;
use thiserror::Error;

use crate::schema::{self, DictionaryCredentials, GraphSchema};
use crate::scope::{
    InvalidatedPipelines, MigrationScope, TableMigrationAction, classify_tables_for_scope,
    find_invalidated_pipelines, widen_scope_for_shared_table_writers,
};
use crate::version::{self, list_version_entities, table_prefix};

pub const CHECKPOINT_TABLE: &str = "checkpoint";

const DISPATCH_CURSOR_PREFIX: &str = "dispatch.";
const CODE_STALE_SWEEP_GATE_PREFIX: &str = "maintenance.code_stale_sweep";

const SEED_CHECKPOINT_SQL: &str = "\
INSERT INTO {new_table:Identifier} \
SELECT * FROM {old_table:Identifier} FINAL \
WHERE _deleted = false \
  AND NOT startsWith(key, {excluded_key_prefix:String}) \
  AND NOT ( \
    (splitByChar('.', key)[1] = 'ns' AND splitByChar('.', key)[3] IN {namespaced_plans:Array(String)}) \
    OR (splitByChar('.', key)[1] = 'global' AND splitByChar('.', key)[2] IN {global_plans:Array(String)}) \
  )";

struct CheckpointSeed {
    excluded_key_prefix: &'static str,
    pipelines: InvalidatedPipelines,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("schema version error: {0}")]
    SchemaVersion(#[from] version::SchemaVersionError),

    #[error("ClickHouse DDL error for '{entity_name}': {reason}")]
    Ddl { entity_name: String, reason: String },

    #[error("migration ledger error: {0}")]
    Ledger(String),
}

pub async fn create_all_versioned_tables(
    graph: &ArrowClickHouseClient,
    schema: &GraphSchema,
    credentials: &DictionaryCredentials,
    version_prefix: &str,
) -> Result<(), MigrationError> {
    for table in &schema.tables {
        tracing::info!(table = %table.name, "creating table");
        run_ddl(graph, &table.name, table.to_create_sql(version_prefix)).await?;
    }
    tracing::info!(
        count = schema.tables.len(),
        prefix = %version_prefix,
        "new-prefix tables created"
    );
    create_dictionaries_and_views(graph, schema, credentials, version_prefix).await
}

pub async fn create_tables_with_selective_cloning(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    schema: &GraphSchema,
    credentials: &DictionaryCredentials,
    requested_scope: &MigrationScope,
    active_version: u32,
    target_version: u32,
) -> Result<(), MigrationError> {
    let scope = widen_scope_for_shared_table_writers(ontology, requested_scope);
    if &scope != requested_scope {
        tracing::warn!(
            %requested_scope, %scope,
            "migration scope widened because a changed table has writers outside the requested scope"
        );
    }

    let target_prefix = table_prefix(target_version);
    if matches!(scope, MigrationScope::Full) {
        return create_all_versioned_tables(graph, schema, credentials, &target_prefix).await;
    }

    let active_prefix = table_prefix(active_version);
    let table_actions = classify_tables_for_scope(ontology, &scope);
    let active_entities = existing_entity_names(graph, active_version).await?;
    let target_entities = existing_entity_names(graph, target_version).await?;
    let seed = checkpoint_seed(ontology, &scope);

    let mut cloned = 0;
    let mut rebuilt = 0;
    let mut seeded = 0;
    for table in &schema.tables {
        let active_name = format!("{active_prefix}{}", table.name);
        let target_name = format!("{target_prefix}{}", table.name);

        if table.name == CHECKPOINT_TABLE
            && let Some(seed) = &seed
            && active_entities.contains(&active_name)
        {
            run_ddl(graph, &target_name, table.to_create_sql(&target_prefix)).await?;
            seed_checkpoint(graph, seed, &active_name, &target_name).await?;
            seeded += 1;
        } else if should_clone(&table.name, &active_name, &table_actions, &active_entities) {
            clone_from_active(graph, &active_name, &target_name, &target_entities).await?;
            cloned += 1;
        } else {
            tracing::info!(table = %target_name, "rebuilding table empty");
            run_ddl(graph, &target_name, table.to_create_sql(&target_prefix)).await?;
            rebuilt += 1;
        }
    }
    tracing::info!(
        cloned,
        rebuilt,
        seeded,
        prefix = %target_prefix,
        "clone-based migration tables prepared"
    );

    create_dictionaries_and_views(graph, schema, credentials, &target_prefix).await
}

pub async fn create_unversioned_definitions(
    graph: &ArrowClickHouseClient,
    schema: &GraphSchema,
) -> Result<(), MigrationError> {
    for definition in &schema.unversioned_definitions {
        run_ddl(graph, &definition.name, &definition.create_statement).await?;
    }
    Ok(())
}

pub async fn replace_refreshable_views(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    version: u32,
) -> Result<(), MigrationError> {
    let prefix = table_prefix(version);
    for view in ontology.refreshable_materialized_views() {
        let view_name = if view.versioned {
            format!("{prefix}{}", view.name)
        } else {
            view.name.clone()
        };
        let rendered_select =
            schema::render_refreshable_view_select(&view.select_query, ontology, version, &prefix)
                .map_err(|error| MigrationError::Ddl {
                    entity_name: view_name.clone(),
                    reason: error.to_string(),
                })?;

        run_ddl(
            graph,
            &view_name,
            format!("DROP VIEW IF EXISTS {view_name}"),
        )
        .await?;
        run_ddl(
            graph,
            &view_name,
            format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}\n\
             REFRESH {} APPEND TO {}\nAS {rendered_select}",
                view.refresh, view.append_to,
            ),
        )
        .await?;
    }
    Ok(())
}

pub async fn drop_versioned_refreshable_views(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    version: u32,
) -> Result<(), MigrationError> {
    let prefix = table_prefix(version);
    for view in ontology
        .refreshable_materialized_views()
        .iter()
        .filter(|view| view.versioned)
    {
        run_ddl(
            graph,
            &view.name,
            format!("DROP VIEW IF EXISTS {prefix}{}", view.name),
        )
        .await?;
    }
    Ok(())
}

pub async fn drop_all_version_entities(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), MigrationError> {
    let mut entities = list_version_entities(graph, version).await?;
    entities.sort_by_key(|entity| {
        match version::entity_type_for_clickhouse_engine(&entity.engine) {
            "VIEW" => 0,
            "DICTIONARY" => 1,
            _ => 2,
        }
    });
    for entity in &entities {
        let entity_type = version::entity_type_for_clickhouse_engine(&entity.engine);
        run_ddl(
            graph,
            &entity.name,
            schema::drop_entity_sql(&entity.name, entity_type),
        )
        .await?;
    }
    Ok(())
}

async fn run_ddl(
    graph: &ArrowClickHouseClient,
    entity_name: &str,
    ddl: impl AsRef<str>,
) -> Result<(), MigrationError> {
    graph
        .execute(ddl.as_ref())
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: entity_name.to_string(),
            reason: error.to_string(),
        })
}

async fn run_parameterized_query(
    entity_name: &str,
    query: clickhouse_client::ArrowQuery,
) -> Result<(), MigrationError> {
    query.execute().await.map_err(|error| MigrationError::Ddl {
        entity_name: entity_name.to_string(),
        reason: error.to_string(),
    })
}

async fn existing_entity_names(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<HashSet<String>, MigrationError> {
    Ok(list_version_entities(graph, version)
        .await?
        .into_iter()
        .map(|entity| entity.name)
        .collect())
}

async fn count_rows(
    graph: &ArrowClickHouseClient,
    table_name: &str,
) -> Result<u64, MigrationError> {
    let batches = graph
        .query("SELECT count() AS row_count FROM {table_name:Identifier}")
        .param("table_name", table_name)
        .fetch_arrow()
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: table_name.to_string(),
            reason: error.to_string(),
        })?;
    Ok(batches
        .first()
        .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "row_count", 0))
        .unwrap_or(0))
}

fn should_clone(
    base_name: &str,
    active_name: &str,
    table_actions: &std::collections::BTreeMap<String, TableMigrationAction>,
    active_entities: &HashSet<String>,
) -> bool {
    matches!(
        table_actions.get(base_name),
        Some(TableMigrationAction::CloneFromActive)
    ) && active_entities.contains(active_name)
}

async fn clone_from_active(
    graph: &ArrowClickHouseClient,
    source_name: &str,
    target_name: &str,
    existing_targets: &HashSet<String>,
) -> Result<(), MigrationError> {
    if existing_targets.contains(target_name) {
        let is_interrupted_shell =
            count_rows(graph, target_name).await? == 0 && count_rows(graph, source_name).await? > 0;
        if !is_interrupted_shell {
            return Ok(());
        }
        tracing::warn!(table = %target_name, "re-cloning empty shell left by interrupted migration");
        run_ddl(
            graph,
            target_name,
            format!("DROP TABLE IF EXISTS {target_name}"),
        )
        .await?;
    }

    tracing::info!(from = %source_name, to = %target_name, "cloning table from active version");
    run_ddl(
        graph,
        target_name,
        schema::clone_table_sql(source_name, target_name),
    )
    .await?;
    run_ddl(
        graph,
        target_name,
        schema::attach_partitions_sql(source_name, target_name),
    )
    .await
}

fn checkpoint_seed(
    ontology: &ontology::Ontology,
    scope: &MigrationScope,
) -> Option<CheckpointSeed> {
    match scope {
        MigrationScope::Sdlc(_) => Some(CheckpointSeed {
            excluded_key_prefix: DISPATCH_CURSOR_PREFIX,
            pipelines: find_invalidated_pipelines(ontology, scope),
        }),
        MigrationScope::Code => Some(CheckpointSeed {
            excluded_key_prefix: CODE_STALE_SWEEP_GATE_PREFIX,
            pipelines: InvalidatedPipelines::default(),
        }),
        MigrationScope::Full | MigrationScope::None => None,
    }
}

async fn seed_checkpoint(
    graph: &ArrowClickHouseClient,
    seed: &CheckpointSeed,
    active_name: &str,
    target_name: &str,
) -> Result<(), MigrationError> {
    tracing::info!(
        from = %active_name,
        to = %target_name,
        excluded_key_prefix = seed.excluded_key_prefix,
        "seeding checkpoint from active version"
    );
    run_parameterized_query(
        target_name,
        graph
            .query(SEED_CHECKPOINT_SQL)
            .param("new_table", target_name)
            .param("old_table", active_name)
            .param("excluded_key_prefix", seed.excluded_key_prefix)
            .param("namespaced_plans", &seed.pipelines.namespaced)
            .param("global_plans", &seed.pipelines.global),
    )
    .await
}

async fn create_dictionaries_and_views(
    graph: &ArrowClickHouseClient,
    schema: &GraphSchema,
    credentials: &DictionaryCredentials,
    version_prefix: &str,
) -> Result<(), MigrationError> {
    for dictionary in &schema.dictionaries {
        let prefixed = dictionary
            .clone()
            .with_schema_version_prefix(version_prefix);
        tracing::info!(dictionary = %prefixed.name, source = %prefixed.source_table, "creating dictionary");
        run_ddl(graph, &prefixed.name, prefixed.to_create_sql(credentials)).await?;
    }

    let all_table_names: Vec<String> = schema
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect();
    for view in schema.views.iter().filter(|view| view.versioned) {
        let prefixed = view
            .clone()
            .with_schema_version_prefix(version_prefix, &all_table_names);
        tracing::info!(view = %prefixed.name, "creating materialized view");
        run_ddl(graph, &prefixed.name, prefixed.to_create_sql()).await?;
    }
    tracing::info!(prefix = %version_prefix, "dictionaries and materialized views created");

    Ok(())
}
