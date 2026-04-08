//! Local DuckDB query pipeline.
//!
//! ```text
//! LocalCompilation → DuckDbExecution → Extraction → LocalOutput
//! ```
//!
//! No security, authorization, redaction, or hydration.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use ontology::Ontology;

use duckdb_client::DuckDbClient;
use query_engine::compiler::compile_local;
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use query_engine::shared::{
    ExecutionOutput, ExtractionOutput, ExtractionStage, PaginationMeta, PipelineOutput,
};

/// Execute a query against the local DuckDB graph.
#[allow(dead_code)]
pub fn run(
    query_json: &str,
    ontology: Arc<Ontology>,
    db_path: &std::path::Path,
) -> Result<PipelineOutput> {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(PathBuf::from(db_path));

    let mut ctx = QueryPipelineContext {
        query_json: query_json.to_string(),
        compiled: None,
        ontology,
        security_context: None,
        server_extensions,
        phases: TypeMap::default(),
    };

    let mut obs = NoOpObserver;

    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            PipelineRunner::start(&mut ctx, &mut obs)
                .then(&LocalCompilation)
                .await?
                .then(&DuckDbExecutor)
                .await?
                .then(&ExtractionStage)
                .await?
                .then(&LocalOutput)
                .await?
                .finish()
                .ok_or_else(|| PipelineError::custom("pipeline did not produce output"))
        })
    })
    .map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
}

// ── Stages ───────────────────────────────────────────────────────────────────

/// Compile JSON DSL to DuckDB SQL (no security context).
struct LocalCompilation;

impl PipelineStage for LocalCompilation {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> std::result::Result<(), PipelineError> {
        let t = Instant::now();

        let compiled = compile_local(&ctx.query_json, &ctx.ontology)
            .map_err(|e| PipelineError::Compile {
                client_safe: e.is_client_safe(),
                message: e.to_string(),
            })
            .inspect_err(|e| obs.record_error(e))?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}

/// Execute compiled SQL against a local DuckDB database.
/// Opens a connection from the path stored in server_extensions.
struct DuckDbExecutor;

impl PipelineStage for DuckDbExecutor {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> std::result::Result<ExecutionOutput, PipelineError> {
        let db_path = ctx
            .server_extensions
            .get::<PathBuf>()
            .ok_or_else(|| PipelineError::Execution("DuckDB path not found".into()))?
            .clone();

        let compiled = ctx.compiled()?;
        let rendered_sql = compiled.base.render();
        let result_context = compiled.base.result_context.clone();

        let t = Instant::now();
        let client =
            DuckDbClient::open(&db_path).map_err(|e| PipelineError::Execution(e.to_string()))?;
        let batches = client
            .query_arrow(&rendered_sql)
            .map_err(|e| PipelineError::Execution(e.to_string()))?;
        obs.executed(t.elapsed(), batches.len());

        Ok(ExecutionOutput {
            batches,
            result_context,
        })
    }
}

/// Build PipelineOutput from extraction results. No authorization,
/// redaction, or hydration -- all rows are trusted.
struct LocalOutput;

impl PipelineStage for LocalOutput {
    type Input = ExtractionOutput;
    type Output = PipelineOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> std::result::Result<PipelineOutput, PipelineError> {
        let input = ctx.phases.get::<ExtractionOutput>().ok_or_else(|| {
            PipelineError::Execution("ExtractionOutput not found in phases".into())
        })?;

        let compiled = ctx.compiled()?;
        let mut query_result = input.query_result.clone();

        let pagination = compiled.input.cursor.map(|cursor| {
            let total_rows = query_result.authorized_count();
            let has_more = query_result.apply_cursor(cursor.offset, cursor.page_size);
            PaginationMeta {
                has_more,
                total_rows,
            }
        });

        Ok(PipelineOutput {
            row_count: query_result.authorized_count(),
            redacted_count: 0,
            query_type: compiled.query_type.to_string(),
            raw_query_strings: vec![],
            compiled: Arc::clone(compiled),
            query_result,
            result_context: input.query_result.ctx().clone(),
            execution_log: vec![],
            pagination,
        })
    }
}
