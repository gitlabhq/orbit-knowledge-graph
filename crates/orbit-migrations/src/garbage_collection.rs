use std::collections::HashSet;

use arrow::datatypes::UInt32Type;
use clickhouse_client::ArrowClickHouseClient;
use orbit_utils::arrow::ArrowUtils;

use crate::execute::MigrationError;
use crate::schema::{self, GraphSchema};
use crate::version::{self, mark_version_dropped};

const LIST_DEAD_VERSION_ENTITIES: &str = "\
SELECT \
  name, engine, \
  toUInt32OrZero(extractAll(name, '^v([0-9]+)_')[1]) AS dead_version \
FROM system.tables \
WHERE database = {database:String} \
  AND match(name, '^v[0-9]+_') \
  AND toUInt32OrZero(extractAll(name, '^v([0-9]+)_')[1]) NOT IN (\
      SELECT version FROM gkg_schema_version FINAL WHERE status = 'active' \
      UNION ALL \
      SELECT version FROM (\
          SELECT version FROM gkg_schema_version FINAL \
          WHERE status = 'retired' ORDER BY version DESC LIMIT {retained_version_count:UInt32}) \
      UNION ALL \
      SELECT version FROM gkg_schema_version FINAL WHERE status = 'migrating') \
  AND (SELECT count() FROM gkg_schema_version FINAL WHERE status = 'active') > 0";

#[derive(Debug, Clone)]
pub struct DroppableEntity {
    pub version: u32,
    pub name: String,
    pub entity_type: &'static str,
}

pub struct DropResult {
    pub succeeded_versions: HashSet<u32>,
    pub failed_versions: HashSet<u32>,
    pub entity_count: usize,
}

impl DropResult {
    pub fn fully_dropped_versions(&self) -> Vec<u32> {
        self.succeeded_versions
            .iter()
            .filter(|version| !self.failed_versions.contains(version))
            .copied()
            .collect()
    }
}

pub async fn find_droppable_entities(
    graph: &ArrowClickHouseClient,
    schema: &GraphSchema,
    max_retained_versions: u32,
    preserve_pattern_strings: &[String],
) -> Result<Vec<DroppableEntity>, MigrationError> {
    let retained_count = max_retained_versions.saturating_sub(1);

    let batches = graph
        .query(LIST_DEAD_VERSION_ENTITIES)
        .param("database", graph.database())
        .param("retained_version_count", retained_count)
        .fetch_arrow()
        .await
        .map_err(|error| MigrationError::Ddl {
            entity_name: "system.tables".into(),
            reason: error.to_string(),
        })?;

    let known_names = schema.entity_base_names();
    let preserve_patterns = compile_preserve_patterns(preserve_pattern_strings);

    let mut entities = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let name = ArrowUtils::get_column_string(batch, "name", row).unwrap_or_default();
            let engine = ArrowUtils::get_column_string(batch, "engine", row).unwrap_or_default();
            let dead_version =
                ArrowUtils::get_column::<UInt32Type>(batch, "dead_version", row).unwrap_or(0);

            let base_name = name
                .strip_prefix(&format!("v{dead_version}_"))
                .unwrap_or(&name);

            if !known_names.contains(base_name)
                && matches_preserve_pattern(base_name, &preserve_patterns)
            {
                tracing::info!(version = dead_version, entity = %name, "GC: preserving (matches preserve pattern)");
                continue;
            }

            entities.push(DroppableEntity {
                version: dead_version,
                name,
                entity_type: version::entity_type_for_clickhouse_engine(&engine),
            });
        }
    }

    entities.sort_by_key(|entity| match entity.entity_type {
        "DICTIONARY" => 0,
        "VIEW" => 1,
        _ => 2,
    });

    Ok(entities)
}

pub async fn drop_entities(
    graph: &ArrowClickHouseClient,
    entities: &[DroppableEntity],
) -> DropResult {
    let mut succeeded_versions = HashSet::new();
    let mut failed_versions = HashSet::new();

    for entity in entities {
        let drop_sql = schema::drop_entity_sql(&entity.name, entity.entity_type);
        match graph.execute(&drop_sql).await {
            Ok(()) => {
                succeeded_versions.insert(entity.version);
            }
            Err(error) => {
                tracing::warn!(
                    version = entity.version,
                    entity = %entity.name,
                    %error,
                    "GC: drop failed"
                );
                failed_versions.insert(entity.version);
            }
        }
    }

    DropResult {
        succeeded_versions,
        failed_versions,
        entity_count: entities.len(),
    }
}

pub async fn mark_dropped_versions(graph: &ArrowClickHouseClient, versions: &[u32]) -> Vec<u32> {
    let mut marked = Vec::new();
    for &version in versions {
        if let Err(error) = mark_version_dropped(graph, version).await {
            tracing::warn!(version, %error, "GC: failed to mark version as dropped");
            continue;
        }
        marked.push(version);
    }
    marked
}

fn matches_preserve_pattern(name: &str, patterns: &[regex::Regex]) -> bool {
    patterns.iter().any(|pattern| pattern.is_match(name))
}

fn compile_preserve_patterns(raw_patterns: &[String]) -> Vec<regex::Regex> {
    raw_patterns
        .iter()
        .filter_map(|pattern| {
            regex::Regex::new(pattern)
                .inspect_err(|error| {
                    tracing::warn!(
                        %pattern,
                        %error,
                        "gc_preserve_patterns: invalid regex, skipping"
                    )
                })
                .ok()
        })
        .collect()
}
