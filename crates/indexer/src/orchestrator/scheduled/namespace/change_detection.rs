use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::{PathResolution, ReindexSource};

use super::DispatchNamespace;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::scheduled::TaskError;

const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";
const ROOT_PATH_PATTERN: &str = "^[0-9]+/[0-9]+/";

#[async_trait]
pub(super) trait NamespaceChangeDetector: Send + Sync {
    async fn changed_namespaces(
        &self,
        lower: DateTime<Utc>,
        upper: DateTime<Utc>,
    ) -> Result<Vec<DispatchNamespace>, TaskError>;
}

pub(super) struct DatalakeChangeDetector {
    datalake: ArrowClickHouseClient,
    query: NamespaceChangeQuery,
}

impl DatalakeChangeDetector {
    pub(super) fn new(datalake: ArrowClickHouseClient, ontology: &ontology::Ontology) -> Self {
        Self {
            datalake,
            query: NamespaceChangeQuery::from_ontology(ontology),
        }
    }
}

#[async_trait]
impl NamespaceChangeDetector for DatalakeChangeDetector {
    async fn changed_namespaces(
        &self,
        lower: DateTime<Utc>,
        upper: DateTime<Utc>,
    ) -> Result<Vec<DispatchNamespace>, TaskError> {
        let lower = lower.format(TIMESTAMP_FORMAT).to_string();
        let upper = upper.format(TIMESTAMP_FORMAT).to_string();
        let batches = self
            .datalake
            .query(&self.query.sql)
            .param("lower", lower)
            .param("upper", upper)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;

        let namespace_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;

        Ok(namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .map(|(namespace_id, traversal_path)| DispatchNamespace {
                namespace_id,
                traversal_path,
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
struct NamespaceChangeQuery {
    sql: String,
}

impl NamespaceChangeQuery {
    fn from_ontology(ontology: &ontology::Ontology) -> Self {
        Self::new(collect_reindex_sources(ontology))
    }

    fn new(reindex_sources: impl IntoIterator<Item = ReindexSource>) -> Self {
        Self {
            sql: render_change_query(&reindex_sources.into_iter().collect()),
        }
    }
}

fn collect_reindex_sources(ontology: &ontology::Ontology) -> BTreeSet<ReindexSource> {
    let node_sources = ontology
        .nodes()
        .filter_map(|node| node.etl.as_ref())
        .flat_map(|etl| etl.reindex_on().iter().cloned());
    let derived_sources = ontology
        .derived_entities()
        .flat_map(|derived| derived.etl.reindex_on().iter().cloned());
    let edge_sources = ontology
        .edge_etl_configs()
        .flat_map(|(_, config)| config.reindex_on.iter().cloned());

    node_sources
        .chain(derived_sources)
        .chain(edge_sources)
        .chain(std::iter::once(enabled_namespace_source()))
        .collect()
}

fn enabled_namespace_source() -> ReindexSource {
    ReindexSource {
        table: ENABLED_NAMESPACE_TABLE.to_string(),
        traversal_path: PathResolution::Column("traversal_path".to_string()),
    }
}

fn render_change_query(reindex_sources: &BTreeSet<ReindexSource>) -> String {
    let changed = reindex_sources
        .iter()
        .map(render_change_branch)
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");

    let deleted_column = ontology::siphon_deleted_column();

    format!(
        r#"WITH
  enabled AS (
    SELECT DISTINCT root_namespace_id, traversal_path
    FROM {ENABLED_NAMESPACE_TABLE}
    WHERE {deleted_column} = false AND traversal_path != ''
  ),
  changed AS (
{changed}
  )
SELECT DISTINCT enabled.root_namespace_id, enabled.traversal_path
FROM changed
INNER JOIN enabled ON changed.root_path = enabled.traversal_path"#
    )
}

fn render_change_branch(source_table: &ReindexSource) -> String {
    let path = path_expression(&source_table.traversal_path);
    let watermark = ontology::siphon_watermark_column();

    format!(
        "    SELECT {root_path} AS root_path\n    FROM {table}\n    WHERE {watermark} > {{lower:String}}\n      AND {watermark} <= {{upper:String}}\n      AND match({path}, '{ROOT_PATH_PATTERN}')",
        root_path = root_path_expression(&path),
        table = source_table.table,
    )
}

fn path_expression(resolution: &PathResolution) -> String {
    match resolution {
        PathResolution::Column(column) => column.clone(),
        PathResolution::Dictionary {
            dictionary,
            key_column,
        } => format!(
            "dictGetOrDefault('{dictionary}', 'traversal_path', toUInt64({key_column}), '0/')"
        ),
    }
}

fn root_path_expression(path: &str) -> String {
    format!("concat(splitByChar('/', {path})[1], '/', splitByChar('/', {path})[2], '/')")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column_source(table: &str) -> ReindexSource {
        ReindexSource {
            table: table.to_string(),
            traversal_path: PathResolution::Column("traversal_path".to_string()),
        }
    }

    #[test]
    fn change_query_filters_enabled_namespaces() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);
        assert!(query.sql.contains(ENABLED_NAMESPACE_TABLE));
        assert!(query.sql.contains("_siphon_deleted = false"));
        assert!(query.sql.contains("traversal_path != ''"));
        assert!(query.sql.contains("INNER JOIN enabled"));
        assert!(
            query
                .sql
                .contains("SELECT DISTINCT enabled.root_namespace_id")
        );
    }

    #[test]
    fn change_query_uses_watermark_bounds() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);
        assert!(query.sql.contains("_siphon_watermark > {lower:String}"));
        assert!(query.sql.contains("_siphon_watermark <= {upper:String}"));
    }

    #[test]
    fn change_query_extracts_root_path() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);
        assert!(query.sql.contains("splitByChar('/', traversal_path)[1]"));
        assert!(query.sql.contains("splitByChar('/', traversal_path)[2]"));
        assert!(query.sql.contains(ROOT_PATH_PATTERN));
    }

    #[test]
    fn change_query_renders_dictionary_lookup() {
        let query = NamespaceChangeQuery::new([ReindexSource {
            table: "siphon_projects".to_string(),
            traversal_path: PathResolution::Dictionary {
                dictionary: "project_traversal_paths_dict".to_string(),
                key_column: "id".to_string(),
            },
        }]);
        assert!(query.sql.contains(
            "dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', toUInt64(id), '0/')"
        ));
    }

    #[test]
    fn change_query_combines_sources_with_union_all() {
        let query =
            NamespaceChangeQuery::new([column_source("work_items"), column_source("siphon_notes")]);
        assert!(query.sql.contains("UNION ALL"));
    }

    #[test]
    fn duplicate_reindex_sources_render_once() {
        let query =
            NamespaceChangeQuery::new([column_source("work_items"), column_source("work_items")]);
        assert_eq!(query.sql.matches("FROM work_items").count(), 1);
    }

    #[test]
    fn ontology_reindex_sources_include_enabled_namespaces() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let sources = collect_reindex_sources(&ontology);
        assert!(sources.contains(&enabled_namespace_source()));
    }
}
