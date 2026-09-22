mod code;
mod phase;
mod toon;

use std::collections::HashSet;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use ontology::{DomainInfo, Ontology};
use orbit_server_config::QueryConfig;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};
use tonic::Status;
use tracing::{debug, info};

use crate::active_schema::SchemaSnapshot;
use crate::proto::{
    GetGraphStatusResponse, GraphStatusDomain, GraphStatusItem, IndexingPhase, IndexingProgress,
    IndexingState, IndexingStatus, ResponseFormat, StructuredGraphStatus,
    get_graph_status_response,
};

use self::code::CodeIndexingState;
use self::phase::FirstSync;

pub struct GraphStatusService {
    client: Arc<ArrowClickHouseClient>,
}

impl GraphStatusService {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn get_status(
        &self,
        schema: &SchemaSnapshot,
        traversal_path: &TraversalPath,
        format: i32,
        security_context: &SecurityContext,
    ) -> Result<GetGraphStatusResponse, Status> {
        if traversal_path.is_empty() {
            return Err(Status::invalid_argument("traversal_path is required"));
        }

        info!(%traversal_path, "Graph status fetching");

        let ontology = &schema.ontology;
        let visible_nodes = visible_node_names(ontology, security_context);
        let (code, first_sync) = tokio::join!(
            code::get_code_indexing_state(&self.client, ontology, traversal_path),
            phase::read_first_sync(&self.client, schema, traversal_path),
        );

        info!(
            phase = ?first_sync.phase,
            projects_indexed = code.projects.indexed,
            projects_total = code.projects.total_known,
            "Graph status fetched"
        );

        let indexing = Some(status_with_state(phase::indexing_state_for(
            first_sync.phase,
        )));
        let structured = StructuredGraphStatus {
            domains: visible_domains(ontology, &visible_nodes, &first_sync, &code),
            projects: Some(code.projects),
            indexing: indexing.clone(),
            sdlc_indexing: indexing,
            code_indexing: code.state.map(status_with_state),
            progress: Some(IndexingProgress {
                phase: first_sync.phase.into(),
            }),
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

fn visible_node_names(ontology: &Ontology, security_context: &SecurityContext) -> HashSet<String> {
    ontology
        .nodes()
        .filter(|node| node.has_traversal_path)
        .filter(|node| {
            let min_role = node
                .redaction
                .as_ref()
                .map(|r| r.required_role.as_access_level())
                .unwrap_or(DEFAULT_PATH_ACCESS_LEVEL);
            !security_context.paths_at_least(min_role).is_empty()
        })
        .map(|node| node.name.clone())
        .collect()
}

fn visible_domains(
    ontology: &Ontology,
    visible_nodes: &HashSet<String>,
    first_sync: &FirstSync,
    code: &CodeIndexingState,
) -> Vec<GraphStatusDomain> {
    ontology
        .domains()
        .filter_map(|domain| {
            let items = visible_items(ontology, domain, visible_nodes, first_sync, code);
            if items.is_empty() {
                return None;
            }

            Some(GraphStatusDomain {
                name: domain.name.clone(),
                items,
                phase: phase_from_sdlc_plans_and_code_coverage(ontology, domain, first_sync, code)
                    .into(),
            })
        })
        .collect()
}

fn visible_items(
    ontology: &Ontology,
    domain: &DomainInfo,
    visible_nodes: &HashSet<String>,
    first_sync: &FirstSync,
    code: &CodeIndexingState,
) -> Vec<GraphStatusItem> {
    domain
        .node_names
        .iter()
        .filter(|name| visible_nodes.contains(*name))
        .filter_map(|name| ontology.get_node(name))
        .map(|node| {
            let state = first_sync
                .node_state(node)
                .or_else(|| node.pipelines.is_empty().then_some(code.state).flatten());
            GraphStatusItem {
                name: node.name.clone(),
                count: None,
                state: state.map(|state| state as i32),
            }
        })
        .collect()
}

fn phase_from_sdlc_plans_and_code_coverage(
    ontology: &Ontology,
    domain: &DomainInfo,
    first_sync: &FirstSync,
    code: &CodeIndexingState,
) -> IndexingPhase {
    let has_code_nodes = domain
        .node_names
        .iter()
        .filter_map(|name| ontology.get_node(name))
        .any(|node| node.pipelines.is_empty());

    let sdlc_phase = first_sync.phase_of_plans_feeding(ontology, domain);
    let code_phase = has_code_nodes.then(|| phase_from_code_coverage(code));
    match (sdlc_phase, code_phase) {
        (Some(sdlc), Some(code)) if sdlc == code => sdlc,
        (Some(IndexingPhase::Unknown), Some(_)) | (Some(_), Some(IndexingPhase::Unknown)) => {
            IndexingPhase::Unknown
        }
        (Some(_), Some(_)) => IndexingPhase::Syncing,
        (Some(phase), None) | (None, Some(phase)) => phase,
        (None, None) => IndexingPhase::Unknown,
    }
}

fn phase_from_code_coverage(code: &CodeIndexingState) -> IndexingPhase {
    match code.state {
        Some(IndexingState::Indexed) => IndexingPhase::Ready,
        Some(IndexingState::Backfilling) => IndexingPhase::Syncing,
        Some(IndexingState::NotIndexed) => IndexingPhase::NotStarted,
        _ => IndexingPhase::Unknown,
    }
}

async fn execute_query(
    client: &ArrowClickHouseClient,
    sql: &str,
    params: &[(&str, &str)],
    label: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, Status> {
    let sql = append_query_settings(sql)
        .map_err(|e| Status::internal(format!("query settings error ({label}): {e}")))?;

    debug!(sql, label, "Graph status query");

    let mut query = client.query(&sql);
    for (name, value) in params {
        query = query.param(name, value);
    }
    query
        .fetch_arrow()
        .await
        .map_err(|e| Status::internal(format!("ClickHouse error ({label}): {e}")))
}

fn append_query_settings(sql: &str) -> Result<String, String> {
    let settings = QueryConfig {
        use_query_cache: Some(true),
        ..orbit_server_config::query::default_config()
    }
    .to_clickhouse_settings()?;
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

fn status_with_state(state: IndexingState) -> IndexingStatus {
    IndexingStatus {
        state: state.into(),
        ..Default::default()
    }
}
