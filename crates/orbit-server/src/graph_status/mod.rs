mod code;
mod input;
mod sdlc;
mod toon;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use indexer::indexing_status::IndexingStatusStore;
use ontology::Ontology;
use orbit_server_config::QueryConfig;
use orbit_utils::arrow::ArrowUtils;
use query_engine::compiler::SecurityContext;
use tonic::Status;
use tracing::{debug, info, warn};

use crate::proto::{
    GetGraphStatusResponse, GraphStatusDomain, GraphStatusItem, IndexingState, IndexingStatus,
    ResponseFormat, StructuredGraphStatus, get_graph_status_response,
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

        let input = GraphStatusInput::from_ontology(&self.ontology, security_context);

        let entity_counts_future = async {
            if input.nodes.is_empty() {
                return HashMap::new();
            }
            let sql = entity_counts_sql(&input);
            execute_count_query(&self.client, &sql, traversal_path)
                .await
                .unwrap_or_else(|error| {
                    warn!(traversal_path, label = "entity counts", %error, "Graph status branch failed");
                    HashMap::new()
                })
        };
        let code_future =
            code::get_code_indexing_state(&self.client, &self.ontology, traversal_path);
        let sdlc_future = sdlc::get_sdlc_indexing_state(
            self.indexing_status.as_ref(),
            &self.ontology,
            traversal_path,
        );

        let (entity_counts, code, sdlc) =
            tokio::join!(entity_counts_future, code_future, sdlc_future);

        info!(
            entity_count = entity_counts.len(),
            projects_indexed = code.projects.indexed,
            projects_total = code.projects.total_known,
            sdlc_state = ?sdlc.aggregate.as_ref().and_then(|s| IndexingState::try_from(s.state).ok()),
            code_state = ?code.aggregate.as_ref().and_then(|s| IndexingState::try_from(s.state).ok()),
            "Graph status fetched"
        );

        let mut item_states = code.node_states;
        item_states.extend(sdlc.node_states);

        let visible_nodes: HashSet<&str> = input.nodes.iter().map(|n| n.name.as_str()).collect();
        let domains =
            present_domain_response(&self.ontology, &entity_counts, &visible_nodes, &item_states);

        let indexing = match &sdlc.aggregate {
            Some(sdlc_status) => worst_indexing_status(Some(sdlc_status), code.aggregate.as_ref()),
            None => None,
        };

        let structured = StructuredGraphStatus {
            projects: Some(code.projects),
            domains,
            indexing,
            sdlc_indexing: sdlc.aggregate,
            code_indexing: code.aggregate,
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
}

fn entity_counts_sql(input: &GraphStatusInput) -> String {
    input
        .nodes
        .iter()
        .map(|node| {
            format!(
                "SELECT '{name}' AS entity, uniqIf(d.id, d._deleted = 0) AS cnt \
                   FROM {table} AS d \
                  WHERE startsWith(d.traversal_path, {{path:String}})",
                name = node.name,
                table = node.table,
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

async fn execute_count_query(
    client: &ArrowClickHouseClient,
    sql: &str,
    traversal_path: &str,
) -> Result<HashMap<String, i64>, Status> {
    let batches = execute_query(client, sql, traversal_path, "entity counts").await?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    for batch in &batches {
        let Some(labels) = ArrowUtils::get_column_by_name::<StringArray>(batch, "entity") else {
            continue;
        };
        let Some(values) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if labels.is_null(row) || values.is_null(row) {
                continue;
            }
            *counts.entry(labels.value(row).to_string()).or_default() += values.value(row) as i64;
        }
    }

    Ok(counts)
}

pub(super) async fn execute_query(
    client: &ArrowClickHouseClient,
    sql: &str,
    traversal_path: &str,
    label: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, Status> {
    let sql = append_query_settings(sql)
        .map_err(|e| Status::internal(format!("query settings error ({label}): {e}")))?;

    debug!(sql, label, "Graph status query");

    client
        .query(&sql)
        .param("path", traversal_path)
        .fetch_arrow()
        .await
        .map_err(|e| Status::internal(format!("ClickHouse error ({label}): {e}")))
}

fn append_query_settings(sql: &str) -> Result<String, String> {
    let settings = graph_status_query_config().to_clickhouse_settings()?;
    if settings.is_empty() {
        return Ok(sql.to_string());
    }
    let clause = settings
        .iter()
        .map(|(key, value)| format!("{key} = {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("{sql} SETTINGS {clause}"))
}

pub(super) fn status_with_state(state: IndexingState) -> IndexingStatus {
    IndexingStatus {
        state: state.into(),
        ..Default::default()
    }
}

pub(super) fn unknown_status() -> IndexingStatus {
    status_with_state(IndexingState::Unknown)
}

fn worst_indexing_status(
    a: Option<&IndexingStatus>,
    b: Option<&IndexingStatus>,
) -> Option<IndexingStatus> {
    let priority = |status: &IndexingStatus| {
        state_priority(IndexingState::try_from(status.state).unwrap_or(IndexingState::Unknown))
    };
    match (a, b) {
        (Some(a), Some(b)) if priority(b) > priority(a) => Some(b.clone()),
        (Some(a), Some(_)) => Some(a.clone()),
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.clone()),
        (None, None) => None,
    }
}

// Higher = worse, so the "worst" state wins. NotIndexed dominates a missing
// key because not-yet-started is a strictly less-known state than failing.
pub(super) fn state_priority(state: IndexingState) -> u8 {
    match state {
        IndexingState::Indexed => 0,
        IndexingState::Indexing => 1,
        IndexingState::Error => 2,
        IndexingState::Backfilling => 3,
        IndexingState::NotIndexed => 4,
        IndexingState::Unknown => 5,
    }
}

fn present_domain_response(
    ontology: &Ontology,
    entity_counts: &HashMap<String, i64>,
    visible_nodes: &HashSet<&str>,
    item_states: &HashMap<String, IndexingState>,
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
                    state: item_states.get(node_name).map(|state| *state as i32),
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
    use super::input::NodeTable;
    use super::*;
    use clickhouse_client::ClickHouseConfigurationExt;
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

        let domains = present_domain_response(&ontology, &entity_counts, &visible, &HashMap::new());

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

        let domains = present_domain_response(&ontology, &entity_counts, &visible, &HashMap::new());

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

        let domains = present_domain_response(&ontology, &entity_counts, &visible, &HashMap::new());
        let domain_count = ontology.domains().count();

        assert_eq!(domains.len(), domain_count);
    }

    #[test]
    fn presents_domain_response_excludes_invisible_entities() {
        let ontology = test_ontology();
        let visible: HashSet<&str> = ["Project", "User", "MergeRequest"].into_iter().collect();
        let mut entity_counts = HashMap::new();
        entity_counts.insert("Project".to_string(), 5);

        let domains = present_domain_response(&ontology, &entity_counts, &visible, &HashMap::new());

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
        let client =
            Arc::new(orbit_server_config::ClickHouseConfiguration::default().build_client());
        let service = GraphStatusService::new(client, test_ontology());

        let result = service
            .get_status("", ResponseFormat::Raw as i32, &admin_context())
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("traversal_path"));
    }

    fn dated_status(state: IndexingState) -> IndexingStatus {
        IndexingStatus {
            state: state.into(),
            last_started_at: Some("2020-01-01T00:00:00Z".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn worst_indexing_status_picks_worse_surface() {
        let sdlc = dated_status(IndexingState::Indexed);
        let code = status_with_state(IndexingState::NotIndexed);

        let worst = worst_indexing_status(Some(&sdlc), Some(&code)).unwrap();

        assert_eq!(worst.state, IndexingState::NotIndexed as i32);
        assert!(worst.last_started_at.is_none());
    }

    #[test]
    fn worst_indexing_status_keeps_timestamps_of_winning_surface() {
        let sdlc = dated_status(IndexingState::Error);
        let code = status_with_state(IndexingState::Indexed);

        let worst = worst_indexing_status(Some(&sdlc), Some(&code)).unwrap();

        assert_eq!(worst.state, IndexingState::Error as i32);
        assert!(worst.last_started_at.is_some());

        let tie = dated_status(IndexingState::Indexed);
        let worst =
            worst_indexing_status(Some(&tie), Some(&status_with_state(IndexingState::Indexed)))
                .unwrap();
        assert!(worst.last_started_at.is_some());
    }

    #[test]
    fn worst_indexing_status_folds_absent_surfaces() {
        let status = dated_status(IndexingState::Indexed);

        assert_eq!(
            worst_indexing_status(Some(&status), None).unwrap().state,
            IndexingState::Indexed as i32
        );
        assert_eq!(
            worst_indexing_status(None, Some(&status)).unwrap().state,
            IndexingState::Indexed as i32
        );
        assert!(worst_indexing_status(None, None).is_none());
    }

    #[test]
    fn code_and_sdlc_cover_disjoint_nodes() {
        let ontology = test_ontology();

        let code_nodes = code::resolve_node_states(&ontology, Some(IndexingState::Indexed));
        let pipeline_states: HashMap<String, IndexingState> =
            sdlc::namespaced_pipeline_names(&ontology)
                .into_iter()
                .map(|name| (name, IndexingState::Indexed))
                .collect();
        let sdlc_nodes = sdlc::resolve_node_states(&ontology, &pipeline_states);

        for node_name in code_nodes.keys() {
            assert!(
                !sdlc_nodes.contains_key(node_name),
                "node {node_name} is claimed by both the code and SDLC surfaces"
            );
        }
    }

    fn counts_input() -> GraphStatusInput {
        GraphStatusInput {
            nodes: vec![
                NodeTable {
                    name: "Project".to_string(),
                    table: "v1_gl_project".to_string(),
                },
                NodeTable {
                    name: "Group".to_string(),
                    table: "v1_gl_group".to_string(),
                },
                NodeTable {
                    name: "MergeRequest".to_string(),
                    table: "v1_gl_merge_request".to_string(),
                },
                NodeTable {
                    name: "Definition".to_string(),
                    table: "v1_gl_definition".to_string(),
                },
            ],
        }
    }

    #[test]
    fn entity_counts_produces_union_all() {
        let sql = entity_counts_sql(&counts_input());

        assert!(sql.contains("UNION ALL"), "SQL: {sql}");
        assert!(sql.contains("v1_gl_project"), "SQL: {sql}");
        assert!(sql.contains("v1_gl_group"), "SQL: {sql}");
        assert!(sql.contains("v1_gl_merge_request"), "SQL: {sql}");
        assert!(sql.contains("v1_gl_definition"), "SQL: {sql}");
    }

    #[test]
    fn entity_counts_binds_traversal_path_per_subquery() {
        let input = counts_input();
        let sql = entity_counts_sql(&input);

        assert_eq!(
            sql.matches("startsWith").count(),
            input.nodes.len(),
            "each subquery filters on the bound traversal_path. SQL: {sql}"
        );
        assert!(sql.contains("{path:String}"), "SQL: {sql}");
    }

    #[test]
    fn entity_counts_deduplicates_by_id() {
        let sql = entity_counts_sql(&counts_input());

        assert!(!sql.contains("argMax("), "SQL: {sql}");
        assert!(!sql.contains("GROUP BY"), "SQL: {sql}");
    }

    #[test]
    fn entity_counts_exclude_deleted_uniformly_without_final() {
        let input = counts_input();
        let sql = entity_counts_sql(&input);

        assert_eq!(
            sql.matches("uniqIf(d.id, d._deleted = 0)").count(),
            input.nodes.len(),
            "every node counts live ids the same way. SQL: {sql}"
        );
        assert!(!sql.contains("FINAL"), "SQL: {sql}");
    }
}
