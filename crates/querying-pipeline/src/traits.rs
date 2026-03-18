use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::error::PipelineError;
use crate::observer::PipelineObserver;
use crate::types::{
    AuthorizationOutput, ExtractionOutput, HydrationOutput, QueryPipelineContext, RedactionOutput,
};

/// Executes a compiled query against a backend (ClickHouse, DuckDB, etc.)
/// and returns Arrow record batches.
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute(
        &self,
        ctx: &QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Vec<RecordBatch>, PipelineError>;
}

/// Performs authorization checks on extracted query results.
/// The server implements this via gRPC bidirectional streaming to Rails.
/// Local/CLI usage can use the default no-op implementation.
///
/// Takes `&mut self` because server implementations need mutable access
/// to the gRPC stream for the bidirectional exchange.
#[async_trait]
pub trait Authorizer: Send {
    async fn authorize(
        &mut self,
        input: ExtractionOutput,
        obs: &mut dyn PipelineObserver,
    ) -> Result<AuthorizationOutput, PipelineError>;
}

/// Hydrates query results with additional properties fetched from the backend.
/// The server implements this via ClickHouse follow-up queries.
/// Local/CLI usage can use the default no-op implementation.
#[async_trait]
pub trait Hydrator: Send + Sync {
    async fn hydrate(
        &self,
        input: RedactionOutput,
        ctx: &QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<HydrationOutput, PipelineError>;
}

/// No-op authorizer that marks all rows as authorized.
pub struct NoOpAuthorizer;

#[async_trait]
impl Authorizer for NoOpAuthorizer {
    async fn authorize(
        &mut self,
        input: ExtractionOutput,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<AuthorizationOutput, PipelineError> {
        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations: vec![],
        })
    }
}

/// No-op hydrator that passes results through without fetching properties.
pub struct NoOpHydrator;

#[async_trait]
impl Hydrator for NoOpHydrator {
    async fn hydrate(
        &self,
        input: RedactionOutput,
        ctx: &QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<HydrationOutput, PipelineError> {
        let result_context = ctx.compiled()?.base.result_context.clone();
        Ok(HydrationOutput {
            query_result: input.query_result,
            result_context,
            redacted_count: input.redacted_count,
        })
    }
}
