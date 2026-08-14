use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use futures::StreamExt;
use ontology::{PathResolution, ReindexSource};
use orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
use tracing::{error, warn};

use crate::clickhouse::{ArrowClickHouseClient, ClickHouseError, TIMESTAMP_FORMAT};
use crate::orchestrator::dispatch::NamespaceDispatchRequest;
use crate::orchestrator::dispatch::enabled_namespaces::resolved_enabled_namespaces_sql;
use crate::orchestrator::scheduled::{ScheduledTaskMetrics, TaskError};

const BRANCH_FALLBACK_CONCURRENCY: usize = 4;

const CHANGE_QUERY_SQL: &str = r#"WITH
  enabled AS (
    {{enabled_namespaces}}
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
    datalake: Arc<dyn ChangeQueryClient>,
    query: NamespaceChangeQuery,
    metrics: ScheduledTaskMetrics,
}

impl DatalakeChangeDetector {
    pub(super) fn new(
        datalake: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        metrics: ScheduledTaskMetrics,
    ) -> Self {
        Self {
            datalake: Arc::new(datalake),
            query: NamespaceChangeQuery::from_ontology(ontology),
            metrics,
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

        let batches = match self
            .datalake
            .fetch_change_batches(&self.query.combined_sql, &lower, &upper)
            .await
        {
            Ok(batches) => batches,
            Err(err) => self.batches_per_branch(&lower, &upper, &err).await?,
        };

        group_by_namespace(&batches)
    }
}

impl DatalakeChangeDetector {
    async fn batches_per_branch(
        &self,
        lower: &str,
        upper: &str,
        combined_err: &ClickHouseError,
    ) -> Result<Vec<RecordBatch>, TaskError> {
        warn!(
            error = %combined_err,
            "combined change detection query failed; retrying one branch per source table"
        );

        let mut stream = futures::stream::iter(0..self.query.branches.len())
            .map(|index| async move {
                let branch = &self.query.branches[index];
                let batches = self
                    .datalake
                    .fetch_change_batches(&branch.sql, lower, upper)
                    .await;
                (branch, batches)
            })
            .buffer_unordered(BRANCH_FALLBACK_CONCURRENCY);

        let mut batches = Vec::new();
        let mut failed = 0usize;

        while let Some((branch, result)) = stream.next().await {
            match result {
                Ok(branch_batches) => batches.extend(branch_batches),
                Err(err) => {
                    failed += 1;
                    self.metrics
                        .record_error(super::TASK_NAME, "detection_branch");
                    error!(
                        table = %branch.table,
                        target = %branch.target,
                        error = %err,
                        "change detection branch failed; skipping it until the next full sweep"
                    );
                }
            }
        }

        if failed == self.query.branches.len() {
            return Err(TaskError::new(format!(
                "all {failed} change detection branches failed; combined query error: {combined_err}"
            )));
        }

        Ok(batches)
    }
}

#[async_trait]
trait ChangeQueryClient: Send + Sync {
    async fn fetch_change_batches(
        &self,
        sql: &str,
        lower: &str,
        upper: &str,
    ) -> Result<Vec<RecordBatch>, ClickHouseError>;
}

#[async_trait]
impl ChangeQueryClient for ArrowClickHouseClient {
    async fn fetch_change_batches(
        &self,
        sql: &str,
        lower: &str,
        upper: &str,
    ) -> Result<Vec<RecordBatch>, ClickHouseError> {
        self.query(sql)
            .param("lower", lower)
            .param("upper", upper)
            .fetch_arrow()
            .await
    }
}

fn group_by_namespace(batches: &[RecordBatch]) -> Result<Vec<NamespaceDispatchRequest>, TaskError> {
    let namespace_ids = i64::extract_column(batches, 0).map_err(TaskError::new)?;
    let traversal_paths = String::extract_column(batches, 1).map_err(TaskError::new)?;
    let targets = String::extract_column(batches, 2).map_err(TaskError::new)?;

    let by_namespace = namespace_ids
        .into_iter()
        .zip(traversal_paths)
        .zip(targets)
        .fold(
            BTreeMap::<(i64, String), BTreeSet<String>>::new(),
            |mut acc, ((namespace_id, traversal_path), target)| {
                acc.entry((namespace_id, traversal_path))
                    .or_default()
                    .insert(target);
                acc
            },
        );

    Ok(by_namespace
        .into_iter()
        .map(
            |((namespace_id, traversal_path), targets)| NamespaceDispatchRequest {
                namespace_id,
                traversal_path,
                targets: targets.into_iter().collect(),
            },
        )
        .collect())
}

#[derive(Debug, Clone)]
struct BranchQuery {
    table: String,
    target: String,
    sql: String,
}

#[derive(Debug, Clone)]
struct NamespaceChangeQuery {
    combined_sql: String,
    branches: Vec<BranchQuery>,
}

impl NamespaceChangeQuery {
    fn from_ontology(ontology: &ontology::Ontology) -> Self {
        Self::new(ontology.reindex_sources())
    }

    fn new(reindex_sources: impl IntoIterator<Item = ReindexSource>) -> Self {
        let sources: BTreeSet<ReindexSource> = reindex_sources.into_iter().collect();
        let branches = sources
            .iter()
            .map(|source| BranchQuery {
                table: source.table.clone(),
                target: source.target.clone(),
                sql: render_change_query(&BTreeSet::from([source.clone()])),
            })
            .collect();
        Self {
            combined_sql: render_change_query(&sources),
            branches,
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
        .replace("{{enabled_namespaces}}", resolved_enabled_namespaces_sql())
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
}

impl DatalakeEnabledNamespaceReader {
    pub(super) fn new(datalake: ArrowClickHouseClient) -> Self {
        Self { datalake }
    }
}

#[async_trait]
impl EnabledNamespaceReader for DatalakeEnabledNamespaceReader {
    async fn enabled_namespaces(&self) -> Result<Vec<NamespaceDispatchRequest>, TaskError> {
        let batches = self
            .datalake
            .query(resolved_enabled_namespaces_sql())
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::orchestrator::dispatch::enabled_namespaces::ENABLED_NAMESPACE_TABLE;

    fn column_source(table: &str) -> ReindexSource {
        ReindexSource {
            table: table.to_string(),
            target: "WorkItem".to_string(),
            traversal_path: PathResolution::Column("traversal_path".to_string()),
        }
    }

    fn change_batches(rows: &[(i64, &str, &str)]) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("root_namespace_id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("target", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        vec![batch]
    }

    fn branch_error() -> ClickHouseError {
        ClickHouseError::BadResponse {
            status: 404,
            body: "Code: 47. DB::Exception: Unknown expression or function identifier".to_string(),
        }
    }

    struct StubClient {
        combined: Result<Vec<RecordBatch>, ()>,
        branches: BTreeMap<&'static str, Result<Vec<RecordBatch>, ()>>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ChangeQueryClient for StubClient {
        async fn fetch_change_batches(
            &self,
            sql: &str,
            _lower: &str,
            _upper: &str,
        ) -> Result<Vec<RecordBatch>, ClickHouseError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let result = if sql.contains("UNION ALL") {
                &self.combined
            } else {
                self.branches
                    .iter()
                    .find(|(table, _)| sql.contains(&format!("FROM {table}")))
                    .map(|(_, result)| result)
                    .expect("branch query names a known table")
            };
            result.clone().map_err(|()| branch_error())
        }
    }

    fn detector(
        client: Arc<StubClient>,
        sources: impl IntoIterator<Item = ReindexSource>,
    ) -> DatalakeChangeDetector {
        DatalakeChangeDetector {
            datalake: client,
            query: NamespaceChangeQuery::new(sources),
            metrics: ScheduledTaskMetrics::new(),
        }
    }

    async fn detect(detector: &DatalakeChangeDetector) -> Vec<NamespaceDispatchRequest> {
        detector
            .changed_namespaces(Utc::now(), Utc::now())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn combined_success_runs_a_single_query() {
        let client = Arc::new(StubClient {
            combined: Ok(change_batches(&[(9, "1/9/", "WorkItem")])),
            branches: BTreeMap::new(),
            calls: AtomicUsize::new(0),
        });
        let detector = detector(
            client.clone(),
            [column_source("work_items"), column_source("siphon_notes")],
        );

        let namespaces = detect(&detector).await;

        assert_eq!(client.calls.load(Ordering::SeqCst), 1);
        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces[0].namespace_id, 9);
        assert_eq!(namespaces[0].targets, vec!["WorkItem"]);
    }

    #[tokio::test]
    async fn all_branches_failing_is_an_error() {
        let client = Arc::new(StubClient {
            combined: Err(()),
            branches: BTreeMap::from([("work_items", Err(())), ("siphon_notes", Err(()))]),
            calls: AtomicUsize::new(0),
        });
        let detector = detector(
            client,
            [column_source("work_items"), column_source("siphon_notes")],
        );

        let err = detector
            .changed_namespaces(Utc::now(), Utc::now())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("all 2 change detection branches"));
    }

    #[test]
    fn change_query_filters_enabled_namespaces() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);
        assert!(query.combined_sql.contains("INNER JOIN enabled"));
        assert!(
            query
                .combined_sql
                .contains("SELECT DISTINCT enabled.root_namespace_id")
        );
    }

    #[test]
    fn change_query_uses_watermark_bounds() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);
        assert!(
            query
                .combined_sql
                .contains("_siphon_watermark > {lower:String}")
        );
        assert!(
            query
                .combined_sql
                .contains("_siphon_watermark <= {upper:String}")
        );
    }

    #[test]
    fn change_query_extracts_root_path() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);
        assert!(
            query
                .combined_sql
                .contains("splitByChar('/', traversal_path)[1]")
        );
        assert!(
            query
                .combined_sql
                .contains("splitByChar('/', traversal_path)[2]")
        );
        assert!(query.combined_sql.contains(TOP_LEVEL_PREFIX_REGEX));
    }

    #[test]
    fn change_query_renders_expected_sql_shape() {
        let query = NamespaceChangeQuery::new([column_source("work_items")]);

        let expected = format!(
            r#"WITH
  enabled AS (
    {enabled}
  ),
  changed AS (
    SELECT concat(splitByChar('/', traversal_path)[1], '/', splitByChar('/', traversal_path)[2], '/') AS root_path, 'WorkItem' AS target
    FROM work_items
    WHERE _siphon_watermark > {{lower:String}}
      AND _siphon_watermark <= {{upper:String}}
      AND match(traversal_path, '^[0-9]+/[0-9]+/')
  )
SELECT DISTINCT enabled.root_namespace_id, enabled.traversal_path, changed.target
FROM changed
INNER JOIN enabled ON changed.root_path = enabled.traversal_path"#,
            enabled = resolved_enabled_namespaces_sql()
        );
        assert_eq!(query.combined_sql, expected);
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
        assert!(query.combined_sql.contains(
            "dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', toUInt64(id), '0/')"
        ));
    }

    #[test]
    fn change_query_combines_sources_with_union_all() {
        let query =
            NamespaceChangeQuery::new([column_source("work_items"), column_source("siphon_notes")]);
        assert!(query.combined_sql.contains("UNION ALL"));
    }

    #[test]
    fn each_branch_query_covers_exactly_one_source() {
        let query =
            NamespaceChangeQuery::new([column_source("work_items"), column_source("siphon_notes")]);
        assert_eq!(query.branches.len(), 2);
        for branch in &query.branches {
            assert!(!branch.sql.contains("UNION ALL"));
            assert!(branch.sql.contains(&format!("FROM {}", branch.table)));
            assert!(branch.sql.contains("INNER JOIN enabled"));
        }
    }

    #[test]
    fn duplicate_reindex_sources_render_once() {
        let query =
            NamespaceChangeQuery::new([column_source("work_items"), column_source("work_items")]);
        assert_eq!(query.combined_sql.matches("FROM work_items").count(), 1);
        assert_eq!(query.branches.len(), 1);
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
