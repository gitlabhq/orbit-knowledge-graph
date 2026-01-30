use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use labkit_rs::correlation::grpc::{
    context_from_request, with_correlation, with_correlation_stream,
};
use labkit_rs::metrics::grpc::GrpcMetrics;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, instrument};

use crate::auth::JwtValidator;
use crate::proto::{
    ExecuteToolMessage, ListToolsRequest, ListToolsResponse, ToolDefinition as ProtoToolDefinition,
};
use crate::tools::ToolRegistry;

use super::auth::extract_claims;

const SERVICE_NAME: &str = "gkg.v1.KnowledgeGraphService";

static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);

pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
}

impl KnowledgeGraphServiceImpl {
    pub fn new(validator: Arc<JwtValidator>) -> Self {
        Self { validator }
    }
}

type ExecuteToolStream =
    Pin<Box<dyn futures::Stream<Item = Result<ExecuteToolMessage, Status>> + Send>>;

#[tonic::async_trait]
impl crate::proto::knowledge_graph_service_server::KnowledgeGraphService
    for KnowledgeGraphServiceImpl
{
    #[instrument(skip(self, request), fields(user_id))]
    async fn list_tools(
        &self,
        request: Request<ListToolsRequest>,
    ) -> Result<Response<ListToolsResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        METRICS
            .record(SERVICE_NAME, "ListTools", || {
                with_correlation(&request, async {
                    info!("Listing tools for user");

                    let tools = ToolRegistry::get_all_tools()
                        .into_iter()
                        .map(|t| ProtoToolDefinition {
                            name: t.name,
                            description: t.description,
                            parameters_json_schema: t.parameters.to_string(),
                        })
                        .collect();

                    Ok(Response::new(ListToolsResponse { tools }))
                })
            })
            .await
    }

    type ExecuteToolStream = ExecuteToolStream;

    #[instrument(skip(self, request), fields(user_id))]
    async fn execute_tool(
        &self,
        request: Request<Streaming<ExecuteToolMessage>>,
    ) -> Result<Response<Self::ExecuteToolStream>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        let context = context_from_request(&request);

        info!("ExecuteTool called - streaming not yet implemented");

        let (_tx, rx) = tokio::sync::mpsc::channel(4);
        let stream = ReceiverStream::new(rx);
        let metered_stream = METRICS.record_stream(SERVICE_NAME, "ExecuteTool", stream);

        Ok(Response::new(Box::pin(with_correlation_stream(
            context,
            metered_stream,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    #[test]
    fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let _service = KnowledgeGraphServiceImpl::new(validator);
    }
}
