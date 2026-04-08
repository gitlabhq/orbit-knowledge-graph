//! Local DuckDB query pipeline.
//!
//! ```text
//! LocalCompilation → DuckDbExecution → Extraction → LocalHydration → LocalOutput
//! ```
//!
//! No security, authorization, redaction, or hydration (hydration is a
//! no-op pass-through until a local resolver is implemented).

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
    ExecutionOutput, ExtractionOutput, ExtractionStage, HydrationOutput, PaginationMeta,
    PipelineOutput,
};

/// Execute a query against the local DuckDB graph.
///
/// Builds a single-threaded tokio runtime internally if one isn't
/// running, or uses `block_in_place` if called from an existing runtime.
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

    let run_pipeline = async {
        PipelineRunner::start(&mut ctx, &mut obs)
            .then(&LocalCompilation)
            .await?
            .then(&DuckDbExecutor)
            .await?
            .then(&ExtractionStage)
            .await?
            .then(&LocalHydration)
            .await?
            .then(&LocalOutput)
            .await?
            .finish()
            .ok_or_else(|| PipelineError::custom("pipeline did not produce output"))
    };

    // Support both "called from tokio context" and "called standalone".
    let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(run_pipeline))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build runtime: {e}"))?
            .block_on(run_pipeline)
    };

    result.map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
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
/// DuckDB operations are synchronous -- the async boundary is handled
/// by the caller via `block_in_place`.
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
        let client = DuckDbClient::open(&db_path).map_err(|e| {
            let err = PipelineError::Execution(e.to_string());
            obs.record_error(&err);
            err
        })?;
        let batches = client.query_arrow(&rendered_sql).map_err(|e| {
            let err = PipelineError::Execution(e.to_string());
            obs.record_error(&err);
            err
        })?;
        obs.executed(t.elapsed(), batches.len());

        Ok(ExecutionOutput {
            batches,
            result_context,
        })
    }
}

/// No-op hydration stage. Passes through the extraction output as a
/// `HydrationOutput` without fetching any properties. A future local
/// resolver will populate node properties here.
struct LocalHydration;

impl PipelineStage for LocalHydration {
    type Input = ExtractionOutput;
    type Output = HydrationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> std::result::Result<HydrationOutput, PipelineError> {
        let input = ctx.phases.get::<ExtractionOutput>().ok_or_else(|| {
            PipelineError::Execution("ExtractionOutput not found in phases".into())
        })?;

        Ok(HydrationOutput {
            query_result: input.query_result.clone(),
            result_context: input.query_result.ctx().clone(),
            redacted_count: 0,
            hydration_queries: vec![],
        })
    }
}

/// Build PipelineOutput from hydration results.
/// All rows are trusted -- no authorization or redaction.
struct LocalOutput;

impl PipelineStage for LocalOutput {
    type Input = HydrationOutput;
    type Output = PipelineOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> std::result::Result<PipelineOutput, PipelineError> {
        let input = ctx.phases.get::<HydrationOutput>().ok_or_else(|| {
            PipelineError::Execution("HydrationOutput not found in phases".into())
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
            result_context: input.result_context.clone(),
            execution_log: vec![],
            pagination,
        })
    }
}
