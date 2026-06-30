use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use gkg_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
use ontology::{PathResolution, ReindexSource};

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::dispatch::NamespaceDispatchRequest;
use crate::orchestrator::scheduled::TaskError;

const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";

const CHANGE_QUERY_SQL: &str = r#"WITH
  enabled AS (
    SELECT DISTINCT root_namespace_id, traversal_path
    FROM {{enabled_namespace_table}}
    WHERE {{deleted_column}} = false AND traversal_path != ''
  ),
  changed AS (
{{branches}}
  )
SELECT DISTINCT enabled.root_namespace_id, enabled.traversal_path, changed.target
FROM changed
INNER JOIN enabled ON changed.root_path = enabled.traversal_path"#;

const CHANGE_BRANCH_SQL: &str = r#"    SELECT {{root_path}} AS root_path, '{{target}}' AS target
    FROM {{table}}
    WHERE {{watermark_column}} > {lower:String}
      AND {{watermark_column}} <= {upper:String}
      AND match({{path}}, '{{root_namespace_path_pattern}}')"#;

#[async_trait]
pub(super) trait NamespaceChangeDetector: Send + Sync {
    async fn changed_namespaces(
        &self,
        lower: DateTime<Utc>,
        upper: DateTime<Utc>,
    ) -> Result<Vec<NamespaceDispatchRequest>, TaskError>;
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
    ) -> Result<Vec<NamespaceDispatchRequest>, TaskError> {
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
        let targets = String::extract_column(&batches, 2).map_err(TaskError::new)?;

        let mut by_namespace: BTreeMap<(i64, String), Vec<String>> = BTreeMap::new();
        for ((namespace_id, traversal_path), target) in
            namespace_ids.into_iter().zip(traversal_paths).zip(targets)
        {
            by_namespace
                .entry((namespace_id, traversal_path))
                .or_default()
                .push(target);
        }

        Ok(by_namespace
            .into_iter()
            .map(
                |((namespace_id, traversal_path), targets)| NamespaceDispatchRequest {
                    namespace_id,
                    traversal_path,
                    targets,
                },
            )
            .collect())
    }
}

#[derive(Debug, Clone)]
struct NamespaceChangeQuery {
    sql: String,
}

impl NamespaceChangeQuery {
    fn from_ontology(ontology: &ontology::Ontology) -> Self {
        Self::new(ontology.reindex_sources())
    }

    fn new(reindex_sources: impl IntoIterator<Item = ReindexSource>) -> Self {
        Self {
            sql: render_change_query(&reindex_sources.into_iter().collect()),
        }
    }
}

fn render_change_query(reindex_sources: &BTreeSet<ReindexSource>) -> String {
    let branches = reindex_sources
        .iter()
        .map(render_change_branch)
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");

    CHANGE_QUERY_SQL
        .replace("{{enabled_namespace_table}}", ENABLED_NAMESPACE_TABLE)
        .replace("{{deleted_column}}", ontology::siphon_deleted_column())
        .replace("{{branches}}", &branches)
}

fn render_change_branch(source_table: &ReindexSource) -> String {
    let path = path_expression(&source_table.traversal_path);

    CHANGE_BRANCH_SQL
        .replace("{{root_path}}", &root_path_expression(&path))
        .replace("{{target}}", &source_table.target)
        .replace("{{table}}", &source_table.table)
        .replace("{{watermark_column}}", ontology::siphon_watermark_column())
        .replace("{{path}}", &path)
        .replace("{{root_namespace_path_pattern}}", TOP_LEVEL_PREFIX_REGEX)
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

#[async_trait]
pub(super) trait EnabledNamespaceReader: Send + Sync {
    async fn enabled_namespaces(&self) -> Result<Vec<NamespaceDispatchRequest>, TaskError>;
}

pub(super) struct DatalakeEnabledNamespaceReader {
    datalake: ArrowClickHouseClient,
    sql: String,
}

impl DatalakeEnabledNamespaceReader {
    pub(super) fn new(datalake: ArrowClickHouseClient) -> Self {
        Self {
            datalake,
            sql: format!(
                "SELECT root_namespace_id, traversal_path \
                 FROM {ENABLED_NAMESPACE_TABLE} \
                 WHERE {deleted} = false AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}')",
                deleted = ontology::siphon_deleted_column()
            ),
        }
    }
}

#[async_trait]
impl EnabledNamespaceReader for DatalakeEnabledNamespaceReader {
    async fn enabled_namespaces(&self) -> Result<Vec<NamespaceDispatchRequest>, TaskError> {
        let batches = self
            .datalake
            .query(&self.sql)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;

        let namespace_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;

        Ok(namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .map(|(namespace_id, traversal_path)| NamespaceDispatchRequest {
                namespace_id,
                traversal_path,
                targets: Vec::new(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column_source(table: &str) -> ReindexSource {
        ReindexSource {
            table: table.to_string(),
            target: "WorkItem".to_string(),
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
        assert!(query.sql.contains(TOP_LEVEL_PREFIX_REGEX));
    }

    #[test]
    fn change_query_renders_expected_sql_shape() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);

        assert_eq!(
            query.sql,
            r#"WITH
  enabled AS (
    SELECT DISTINCT root_namespace_id, traversal_path
    FROM siphon_knowledge_graph_enabled_namespaces
    WHERE _siphon_deleted = false AND traversal_path != ''
  ),
  changed AS (
    SELECT concat(splitByChar('/', traversal_path)[1], '/', splitByChar('/', traversal_path)[2], '/') AS root_path, 'WorkItem' AS target
    FROM work_items
    WHERE _siphon_watermark > {lower:String}
      AND _siphon_watermark <= {upper:String}
      AND match(traversal_path, '^[0-9]+/[0-9]+/')
  )
SELECT DISTINCT enabled.root_namespace_id, enabled.traversal_path, changed.target
FROM changed
INNER JOIN enabled ON changed.root_path = enabled.traversal_path"#
        );
    }

    #[test]
    fn change_query_renders_dictionary_lookup() {
        let query = NamespaceChangeQuery::new([ReindexSource {
            table: "siphon_projects".to_string(),
            target: "Project".to_string(),
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
    fn ontology_reindex_sources_cover_data_tables_not_the_enabled_table() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let sources = ontology.reindex_sources();
        let tables: BTreeSet<&str> = sources.iter().map(|s| s.table.as_str()).collect();
        assert!(tables.contains("work_items"));
        assert!(!tables.contains(ENABLED_NAMESPACE_TABLE));
    }
}
