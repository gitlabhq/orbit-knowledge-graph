mod helpers;
pub(crate) mod metrics;
mod service;
mod stages;

// Pure pipeline framework
pub use querying_pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};

// Shared stages, formatters, and inter-stage types
pub use querying_shared_stages::{
    AuthorizationOutput, CompilationStage, ExecutionOutput, ExtractionOutput, ExtractionStage,
    FormattingStage, GoonFormatter, GraphEdge, GraphFormatter, GraphNode, GraphResponse,
    HydrationOutput, PipelineOutput, RedactionOutput, RedactionStage, ResultFormatter,
    column_value_to_json, row_to_json,
};

pub use helpers::{QueryRequest, receive_query_request, send_query_error};
pub use service::QueryPipelineService;
pub use stages::{
    AuthorizationChannel, AuthorizationStage, ClickHouseExecutor, HydrationStage, SecurityStage,
};
