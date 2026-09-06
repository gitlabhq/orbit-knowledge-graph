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

pub struct GarbageCollectionResult {
    pub dropped_versions: Vec<u32>,
    pub dropped_entity_count: usize,
}

pub async fn collect_dead_versions(
    graph: &ArrowClickHouseClient,
    schema: &GraphSchema,
    max_retained_versions: u32,
    preserve_pattern_strings: &[String],
) -> Result<GarbageCollectionResult, MigrationError> {
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

    let known_names = ontology_known_entity_names(schema);
    let preserve_patterns = compile_preserve_patterns(preserve_pattern_strings);

    let mut drops: Vec<(u32, String, &str)> = Vec::new();
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

            let entity_type = version::entity_type_for_clickhouse_engine(&engine);
            drops.push((dead_version, name, entity_type));
        }
    }

    drops.sort_by_key(|(_, _, entity_type)| match *entity_type {
        "DICTIONARY" => 0,
        "VIEW" => 1,
        _ => 2,
    });

    let mut succeeded_versions: HashSet<u32> = HashSet::new();
    let mut failed_versions: HashSet<u32> = HashSet::new();
    let dropped_entity_count = drops.len();

    for (dead_version, entity_name, entity_type) in &drops {
        let drop_sql = schema::drop_entity_sql(entity_name, entity_type);
        match graph.execute(&drop_sql).await {
            Ok(()) => {
                succeeded_versions.insert(*dead_version);
            }
            Err(error) => {
                tracing::warn!(
                    version = dead_version,
                    entity = %entity_name,
                    %error,
                    "GC: drop failed"
                );
                failed_versions.insert(*dead_version);
            }
        }
    }

    let mut dropped_versions = Vec::new();
    for version in &succeeded_versions {
        if failed_versions.contains(version) {
            continue;
        }
        if let Err(error) = mark_version_dropped(graph, *version).await {
            tracing::warn!(version, %error, "GC: failed to mark version as dropped");
            continue;
        }
        dropped_versions.push(*version);
    }

    Ok(GarbageCollectionResult {
        dropped_versions,
        dropped_entity_count,
    })
}

fn ontology_known_entity_names(schema: &GraphSchema) -> HashSet<String> {
    let mut names = HashSet::new();
    for table in &schema.tables {
        names.insert(table.name.clone());
    }
    for view in &schema.views {
        names.insert(view.name.clone());
    }
    for dictionary in &schema.dictionaries {
        names.insert(dictionary.name.clone());
    }
    names
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
