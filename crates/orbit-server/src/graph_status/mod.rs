mod response;
mod toon;

use std::slice;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use orbit_log_fields::info;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::SecurityContext;
use tonic::Status;

use crate::active_schema::SchemaSnapshot;
use crate::indexing_status::IndexingStatusService;
use crate::item_counts::ItemCountService;
use crate::proto::{GetGraphStatusResponse, ResponseFormat, get_graph_status_response};

pub struct GraphStatusService {
    indexing_status: IndexingStatusService,
    item_counts: ItemCountService,
}

impl GraphStatusService {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self {
            indexing_status: IndexingStatusService::new(Arc::clone(&client)),
            item_counts: ItemCountService::new(client),
        }
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

        info!(
            safe(TRAVERSAL_PATH) = traversal_path.as_str(),
            "Graph status fetching"
        );

        let scopes = slice::from_ref(traversal_path);
        let (statuses, counts) = tokio::join!(
            self.indexing_status.read_scope_statuses(schema, scopes),
            self.item_counts
                .count_items(&schema.ontology, security_context, scopes),
        );
        let status = statuses
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("indexing status returned no scope"))?;
        let counts = counts.into_iter().next().unwrap_or_default();

        info!(
            safe(PHASE) = <&str>::from(status.phase),
            safe(ENTITY_COUNT) = counts.len(),
            safe(INDEXED_PROJECT_COUNT) = status.projects.indexed,
            safe(PROJECT_COUNT) = status.projects.total_known,
            "Graph status fetched"
        );

        let structured = response::build_structured_status(&status, &counts);
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
