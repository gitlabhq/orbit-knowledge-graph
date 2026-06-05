mod input;
mod lower;
mod toon;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use futures::stream::{FuturesUnordered, StreamExt};
use gkg_server_config::QueryConfig;
use gkg_utils::arrow::ArrowUtils;
use indexer::indexing_status::{IndexingProgress, IndexingStatusStore};
use ontology::{EtlScope, Ontology};
use query_engine::compiler::{ResultContext, SecurityContext, codegen};
use tonic::Status;
use tracing::{debug, info, warn};

use crate::proto::{
    GetGraphStatusResponse, GraphStatusDomain, GraphStatusItem, IndexingState, IndexingStatus,
    ProjectsStatus, ResponseFormat, StructuredGraphStatus, get_graph_status_response,
};

use self::input::GraphStatusInput;

pub struct GraphStatusService {
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
    indexing_status: Option<IndexingStatusStore>,
}

fn graph_status_query_config() -> QueryConfig {
    QueryConfig {
        use_query_cache: Some(true),
        ..QueryConfig::default()
    }
}

impl GraphStatusService {
    pub fn new(client: Arc<ArrowClickHouseClient>, ontology: Arc<Ontology>) -> Self {
        Self {
            client,
            ontology,
            indexing_status: None,
        }
    }

    pub fn with_indexing_status(mut self, store: IndexingStatusStore) -> Self {
        self.indexing_status = Some(store);
        self
    }

    pub async fn get_status(
        &self,
        traversal_path: &str,
        format: i32,
        security_context: &SecurityContext,
    ) -> Result<GetGraphStatusResponse, Status> {
        if traversal_path.is_empty() {
            return Err(Status::invalid_argument("traversal_path is required"));
        }

        info!(traversal_path, "Graph status fetching");

        let input = GraphStatusInput::from_ontology(
            &self.ontology,
            traversal_path.to_string(),
            security_context,
        )?;

        let entity_counts_future = async {
            if input.nodes.is_empty() {
                return Ok(HashMap::new());
            }
            let ast = lower::lower_entity_counts(&input);
            self.execute_count_query(&ast, "entity counts").await
        };

        let projects_future = async {
            let ast = lower::lower_projects(&input.project_tables, traversal_path);
            self.execute_projects_query(&ast).await
        };

        let indexing_future = self.fetch_indexing_status(traversal_path);

        let (entity_counts, projects, indexing) =
            tokio::join!(entity_counts_future, projects_future, indexing_future);

        let entity_counts = entity_counts.unwrap_or_else(|error| {
            warn!(traversal_path, label = "entity counts", %error, "Graph status branch failed");
            HashMap::new()
        });
        let projects = projects.unwrap_or_else(|error| {
            warn!(traversal_path, label = "projects", %error, "Graph status branch failed");
            ProjectsStatus::default()
        });
        let indexing = indexing.unwrap_or_else(|error| {
            warn!(traversal_path, label = "indexing", %error, "Graph status branch failed");
            None
        });

        info!(
            entity_count = entity_counts.len(),
            projects_indexed = projects.indexed,
            projects_total = projects.total_known,
            indexing_state = ?IndexingState::try_from(indexing.as_ref().map_or(0, |s| s.state)).ok(),
            "Graph status fetched"
        );

        let visible_nodes: HashSet<&str> = input.nodes.iter().map(|n| n.name.as_str()).collect();
        let domains = present_domain_response(&self.ontology, &entity_counts, &visible_nodes);
        let structured = StructuredGraphStatus {
            projects: Some(projects),
            domains,
            indexing,
        };

        let content = if format == ResponseFormat::Llm as i32 {
            get_graph_status_response::Content::FormattedText(toon::format_status_as_toon(
                &structured,
            ))
        } else {
            get_graph_status_response::Content::Structured(structured)
        };

        Ok(GetGraphStatusResponse {
            content: Some(content),
        })
    }

    async fn fetch_indexing_status(
        &self,
        traversal_path: &str,
    ) -> Result<Option<IndexingStatus>, Status> {
        let Some(store) = &self.indexing_status else {
            return Ok(None);
        };

        let entity_kinds = namespaced_entity_kinds(&self.ontology);

        let mut futures = FuturesUnordered::new();
        for kind in &entity_kinds {
            futures
                .push(async move { (kind.as_str(), store.get_entity(traversal_path, kind).await) });
        }

        let mut entity_progress: Vec<(String, Option<IndexingProgress>)> = Vec::new();
        let mut read_errors = 0usize;
        while let Some((kind, result)) = futures.next().await {
            match result {
                Ok(progress) => entity_progress.push((kind.to_string(), progress)),
                Err(error) => {
                    read_errors += 1;
                    warn!(%error, traversal_path, entity = kind, "failed to read entity indexing progress");
                }
            }
        }

        let legacy_progress = match store.get(traversal_path).await {
            Ok(p) => p,
            Err(error) => {
                read_errors += 1;
                warn!(%error, traversal_path, "failed to read indexing progress from NATS KV");
                None
            }
        };

        if entity_progress.is_empty() && read_errors > 0 {
            return Ok(Some(IndexingStatus {
                state: IndexingState::Unknown.into(),
                ..Default::default()
            }));
        }

        Ok(Some(aggregate_indexing_status(
            entity_progress,
            legacy_progress,
        )))
    }

    async fn execute_count_query(
        &self,
        ast: &query_engine::compiler::Node,
        label: &str,
    ) -> Result<HashMap<String, i64>, Status> {
        let batches = self.execute_query(ast, label).await?;

        let mut counts: HashMap<String, i64> = HashMap::new();
        for batch in &batches {
            let Some(labels) = ArrowUtils::get_column_by_name::<StringArray>(batch, "entity")
            else {
                continue;
            };
            let Some(values) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if labels.is_null(row) || values.is_null(row) {
                    continue;
                }
                let name = labels.value(row);
                let count = values.value(row) as i64;
                *counts.entry(name.to_string()).or_default() += count;
            }
        }

        Ok(counts)
    }

    async fn execute_projects_query(
        &self,
        ast: &query_engine::compiler::Node,
    ) -> Result<ProjectsStatus, Status> {
        let batches = self.execute_query(ast, "projects").await?;

        let mut indexed = 0i64;
        let mut total_known = 0i64;
        for batch in &batches {
            let Some(labels) = ArrowUtils::get_column_by_name::<StringArray>(batch, "metric")
            else {
                continue;
            };
            let Some(values) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if labels.is_null(row) || values.is_null(row) {
                    continue;
                }
                match labels.value(row) {
                    "indexed" => indexed += values.value(row) as i64,
                    "total_known" => total_known += values.value(row) as i64,
                    _ => {}
                }
            }
        }

        Ok(ProjectsStatus {
            indexed,
            total_known,
        })
    }

    async fn execute_query(
        &self,
        ast: &query_engine::compiler::Node,
        label: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Status> {
        let parameterized = codegen(ast, ResultContext::new(), graph_status_query_config())
            .map_err(|e| Status::internal(format!("codegen error ({label}): {e}")))?;

        debug!(sql = %parameterized.sql, label, "Graph status query compiled");

        let mut query = self.client.query(&parameterized.sql);
        for (key, param) in &parameterized.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }

        query
            .fetch_arrow()
            .await
            .map_err(|e| Status::internal(format!("ClickHouse error ({label}): {e}")))
    }
}

fn derive_indexing_state(progress: &IndexingProgress) -> IndexingState {
    match progress.last_completed_at {
        None => IndexingState::Backfilling,
        Some(completed) if progress.last_started_at > completed => IndexingState::Indexing,
        Some(_) if progress.last_error.is_some() => IndexingState::Error,
        Some(_) => IndexingState::Indexed,
    }
}

fn namespaced_entity_kinds(ontology: &Ontology) -> Vec<String> {
    ontology
        .nodes()
        .filter_map(|n| {
            let etl = n.etl.as_ref()?;
            (etl.scope() == EtlScope::Namespaced).then(|| n.name.clone())
        })
        .collect()
}

// Higher = worse, so the "worst" state wins. NotIndexed dominates a missing
// key because not-yet-started is a strictly less-known state than failing.
fn state_priority(state: IndexingState) -> u8 {
    match state {
        IndexingState::Indexed => 0,
        IndexingState::Indexing => 1,
        IndexingState::Error => 2,
        IndexingState::Backfilling => 3,
        IndexingState::NotIndexed => 4,
        IndexingState::Unknown => 5,
    }
}

fn aggregate_indexing_status(
    entity_progress: Vec<(String, Option<IndexingProgress>)>,
    legacy_progress: Option<IndexingProgress>,
) -> IndexingStatus {
    // Rollout fallback: nothing per-entity has been written yet → defer to
    // the legacy single-key format so existing pre-MR deployments keep
    // reporting state.
    let any_entity_present = entity_progress.iter().any(|(_, p)| p.is_some());
    if !any_entity_present {
        return match legacy_progress {
            None => IndexingStatus {
                state: IndexingState::NotIndexed.into(),
                ..Default::default()
            },
            Some(p) => indexing_status_from_progress(derive_indexing_state(&p), &p),
        };
    }

    let entity_entries = entity_progress.iter().map(|(_, progress)| match progress {
        None => (IndexingState::NotIndexed, None),
        Some(p) => (derive_indexing_state(p), Some(p)),
    });
    let legacy_entry = legacy_progress
        .as_ref()
        .map(|p| (derive_indexing_state(p), Some(p)));

    let (worst_state, worst_progress) = entity_entries
        .chain(legacy_entry)
        .max_by_key(|(state, _)| state_priority(*state))
        .unwrap_or((IndexingState::NotIndexed, None));

    match worst_progress {
        Some(p) => indexing_status_from_progress(worst_state, p),
        None => IndexingStatus {
            state: worst_state.into(),
            ..Default::default()
        },
    }
}

fn indexing_status_from_progress(state: IndexingState, p: &IndexingProgress) -> IndexingStatus {
    IndexingStatus {
        state: state.into(),
        last_started_at: Some(p.last_started_at.to_rfc3339()),
        last_completed_at: p.last_completed_at.map(|t| t.to_rfc3339()),
        last_duration_ms: p.last_duration_ms,
        last_error: p.last_error.clone(),
    }
}

fn present_domain_response(
    ontology: &Ontology,
    entity_counts: &HashMap<String, i64>,
    visible_nodes: &HashSet<&str>,
) -> Vec<GraphStatusDomain> {
    ontology
        .domains()
        .filter_map(|domain| {
            let items: Vec<_> = domain
                .node_names
                .iter()
                .filter(|node_name| visible_nodes.contains(node_name.as_str()))
                .map(|node_name| GraphStatusItem {
                    name: node_name.clone(),
                    count: entity_counts.get(node_name).copied().unwrap_or(0),
                })
                .collect();

            if items.is_empty() {
                return None;
            }

            Some(GraphStatusDomain {
                name: domain.name.clone(),
                items,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use clickhouse_client::ClickHouseConfigurationExt;
    use indexer::indexing_status::IndexingProgress;
    use query_engine::compiler::TraversalPath;

    fn admin_context() -> SecurityContext {
        SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 50)])
            .unwrap()
            .with_role(true, Some(50))
    }

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn all_node_names(ontology: &Ontology) -> HashSet<&str> {
        ontology.nodes().map(|n| n.name.as_str()).collect()
    }

    #[test]
    fn presents_domain_response_groups_by_domain() {
        let ontology = test_ontology();
        let visible = all_node_names(&ontology);
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Project".to_string(), 42);
        entity_counts.insert("User".to_string(), 10);

        let domains = present_domain_response(&ontology, &entity_counts, &visible);

        assert!(!domains.is_empty());

        let core_domain = domains.iter().find(|d| d.name == "core");
        assert!(core_domain.is_some(), "should have core domain");

        let core = core_domain.unwrap();
        let project_item = core.items.iter().find(|i| i.name == "Project");
        assert!(project_item.is_some());
        assert_eq!(project_item.unwrap().count, 42);

        let user_item = core.items.iter().find(|i| i.name == "User");
        assert!(user_item.is_some());
        assert_eq!(user_item.unwrap().count, 10);
    }

    #[test]
    fn presents_domain_response_missing_entity_defaults_to_zero() {
        let ontology = test_ontology();
        let visible = all_node_names(&ontology);
        let entity_counts = HashMap::new();

        let domains = present_domain_response(&ontology, &entity_counts, &visible);

        for domain in &domains {
            for item in &domain.items {
                assert_eq!(
                    item.count, 0,
                    "missing entity {} should default to 0",
                    item.name
                );
            }
        }
    }

    #[test]
    fn presents_domain_response_covers_all_domains() {
        let ontology = test_ontology();
        let visible = all_node_names(&ontology);
        let entity_counts = HashMap::new();

        let domains = present_domain_response(&ontology, &entity_counts, &visible);
        let domain_count = ontology.domains().count();

        assert_eq!(domains.len(), domain_count);
    }

    #[test]
    fn presents_domain_response_excludes_invisible_entities() {
        let ontology = test_ontology();
        let visible: HashSet<&str> = ["Project", "User", "MergeRequest"].into_iter().collect();
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Project".to_string(), 5);

        let domains = present_domain_response(&ontology, &entity_counts, &visible);

        let security = domains.iter().find(|d| d.name == "security");
        assert!(
            security.is_none(),
            "security domain should be excluded when no security nodes visible"
        );

        let core = domains.iter().find(|d| d.name == "core").unwrap();
        assert!(core.items.iter().any(|i| i.name == "Project"));
        assert!(core.items.iter().any(|i| i.name == "User"));
        assert!(
            !core.items.iter().any(|i| i.name == "Group"),
            "Group not in visible set"
        );
    }

    #[tokio::test]
    async fn empty_traversal_path_rejected() {
        let client = Arc::new(gkg_server_config::ClickHouseConfiguration::default().build_client());
        let service = GraphStatusService::new(client, test_ontology());

        let result = service
            .get_status("", ResponseFormat::Raw as i32, &admin_context())
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("traversal_path"));
    }

    #[test]
    fn derive_state_not_indexed_when_no_progress() {
        let status = IndexingStatus {
            state: IndexingState::NotIndexed.into(),
            ..Default::default()
        };
        assert_eq!(status.state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn derive_state_backfilling_when_started_but_not_completed() {
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
        };
        assert_eq!(derive_indexing_state(&progress), IndexingState::Backfilling);
    }

    #[test]
    fn derive_state_indexed_when_completed_successfully() {
        let started = Utc::now();
        let progress = IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(5)),
            last_duration_ms: Some(5000),
            last_error: None,
        };
        assert_eq!(derive_indexing_state(&progress), IndexingState::Indexed);
    }

    #[test]
    fn derive_state_indexed_when_started_equals_completed() {
        let now = Utc::now();
        let progress = IndexingProgress {
            last_started_at: now,
            last_completed_at: Some(now),
            last_duration_ms: Some(0),
            last_error: None,
        };
        assert_eq!(derive_indexing_state(&progress), IndexingState::Indexed);
    }

    #[test]
    fn derive_state_error_when_completed_with_error() {
        let started = Utc::now();
        let progress = IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(1)),
            last_duration_ms: Some(1000),
            last_error: Some("deadline exceeded".to_string()),
        };
        assert_eq!(derive_indexing_state(&progress), IndexingState::Error);
    }

    #[test]
    fn derive_state_backfilling_when_error_but_not_completed() {
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: Some("connection reset".to_string()),
        };
        assert_eq!(derive_indexing_state(&progress), IndexingState::Backfilling);
    }

    #[test]
    fn derive_state_indexing_when_started_after_completion() {
        let completed = Utc::now() - Duration::seconds(60);
        let progress = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: Some(completed),
            last_duration_ms: Some(5000),
            last_error: None,
        };
        assert_eq!(derive_indexing_state(&progress), IndexingState::Indexing);
    }

    fn completed_progress(error: Option<&str>) -> IndexingProgress {
        let started = Utc::now() - Duration::seconds(30);
        IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(5)),
            last_duration_ms: Some(5000),
            last_error: error.map(String::from),
        }
    }

    #[test]
    fn aggregate_falls_back_to_legacy_when_no_entity_keys_present() {
        let entities = vec![
            ("MergeRequest".to_string(), None),
            ("Issue".to_string(), None),
        ];
        let status = aggregate_indexing_status(entities, Some(completed_progress(None)));
        assert_eq!(status.state, IndexingState::Indexed as i32);
    }

    #[test]
    fn aggregate_not_indexed_when_no_entity_keys_and_no_legacy() {
        let entities = vec![
            ("MergeRequest".to_string(), None),
            ("Issue".to_string(), None),
        ];
        let status = aggregate_indexing_status(entities, None);
        assert_eq!(status.state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_missing_entity_key_wins_over_indexed() {
        let entities = vec![
            ("MergeRequest".to_string(), Some(completed_progress(None))),
            ("Issue".to_string(), None),
        ];
        let status = aggregate_indexing_status(entities, None);
        assert_eq!(status.state, IndexingState::NotIndexed as i32);
    }

    #[test]
    fn aggregate_error_wins_over_indexed_and_indexing() {
        let in_flight = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: Some(Utc::now() - Duration::seconds(60)),
            last_duration_ms: Some(5000),
            last_error: None,
        };
        let entities = vec![
            ("MergeRequest".to_string(), Some(completed_progress(None))),
            ("Issue".to_string(), Some(in_flight)),
            (
                "Project".to_string(),
                Some(completed_progress(Some("scan failure"))),
            ),
        ];
        let status = aggregate_indexing_status(entities, None);
        assert_eq!(status.state, IndexingState::Error as i32);
        assert_eq!(status.last_error.as_deref(), Some("scan failure"));
    }

    #[test]
    fn aggregate_legacy_folds_into_worst_state() {
        let entities = vec![("MergeRequest".to_string(), Some(completed_progress(None)))];
        let legacy = IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
        };
        let status = aggregate_indexing_status(entities, Some(legacy));
        assert_eq!(status.state, IndexingState::Backfilling as i32);
    }
}
