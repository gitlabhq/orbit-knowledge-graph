mod helpers;
pub(crate) mod metrics;
mod service;
mod stages;

// Re-export from querying-pipeline crate for backward compatibility
pub use querying_pipeline::{
    AuthorizationOutput, ExecutionOutput, ExtractionOutput, HydrationOutput, PipelineOutput,
    PipelineRequest, QueryPipelineContext, RedactionOutput,
};
pub use querying_pipeline::{
    Authorizer, CompilationStage, ExtractionStage, FormattingStage, Hydrator, NoOpAuthorizer,
    NoOpHydrator, NoOpObserver, PipelineObserver, QueryExecutor, RedactionStage,
};
pub use querying_pipeline::{
    GoonFormatter, GraphEdge, GraphFormatter, GraphNode, GraphResponse, PipelineError,
    ResultFormatter, column_value_to_json, row_to_json,
};

pub use helpers::{QueryRequest, receive_query_request, send_query_error};
pub use service::QueryPipelineService;
pub use stages::HydrationStage;
