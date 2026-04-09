//! Local DuckDB query pipeline.
//!
//! ```text
//! LocalCompilation -> DuckDbExecution -> Extraction -> LocalHydration -> LocalOutput
//! ```
//!
//! No security, authorization, or redaction. Hydration fetches node
//! properties from the same DuckDB database. Virtual columns (e.g. file
//! `content`) are resolved from the local filesystem via
//! [`LocalContentService`](crate::content::LocalContentService).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use ontology::Ontology;

use duckdb_client::DuckDbClient;
use query_engine::compiler::{HydrationPlan, compile_local, compile_local_input};
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use query_engine::shared::content::{
    ColumnResolverRegistry, EntityVirtualColumns, ResolverContext, resolve_virtual_columns,
};
use query_engine::shared::hydration as hydration_helpers;
use query_engine::shared::{
    DebugQuery, ExecutionOutput, ExtractionOutput, ExtractionStage, HydrationOutput,
    PaginationMeta, PipelineOutput,
};

use crate::content;

/// Execute a query against the local DuckDB graph.
///
/// `repo_roots` are the filesystem paths to indexed repositories, used
/// to resolve virtual columns (file content) from disk.
#[allow(dead_code)]
pub fn run(
    query_json: &str,
    ontology: Arc<Ontology>,
    db_path: &std::path::Path,
    repo_roots: Vec<PathBuf>,
) -> Result<PipelineOutput> {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(PathBuf::from(db_path));
    server_extensions.insert(content::local_resolver_registry(repo_roots));

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

// -- Stages -------------------------------------------------------------------

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

struct DuckDbExecutor;

impl PipelineStage for DuckDbExecutor {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> std::result::Result<ExecutionOutput, PipelineError> {
        let db_path = get_db_path(ctx)?;
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

/// Hydration stage that fetches node properties from the local DuckDB
/// and resolves virtual columns from the filesystem.
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

        let compiled = ctx.compiled()?;
        let db_path = get_db_path(ctx)?;

        let mut query_result = input.query_result.clone();
        let result_context = input.query_result.ctx().clone();
        let mut hydration_queries = Vec::new();

        let registry = ctx.server_extensions.get::<ColumnResolverRegistry>();
        let resolver_ctx = ResolverContext::default();

        match &compiled.hydration {
            HydrationPlan::None => {}
            HydrationPlan::Static(templates) => {
                let (nodes, total_ids) =
                    hydration_helpers::build_static_nodes(templates, &query_result);
                let (mut props, debug) =
                    execute_local_hydration(&db_path, &ctx.ontology, nodes, total_ids)?;
                hydration_queries.extend(debug);

                if let Some(registry) = registry {
                    let entity_virtuals: Vec<EntityVirtualColumns<'_>> = templates
                        .iter()
                        .map(|t| (t.entity_type.as_str(), t.virtual_columns.as_slice()))
                        .collect();
                    resolve_virtual_columns(registry, &resolver_ctx, &entity_virtuals, &mut props)
                        .await?;
                }

                hydration_helpers::strip_injected_columns(
                    &mut props,
                    templates
                        .iter()
                        .map(|t| (t.entity_type.as_str(), &t.injected_columns)),
                );

                hydration_helpers::merge_static_properties(&mut query_result, &props, templates);
            }
            HydrationPlan::Dynamic(entity_specs) => {
                let refs = hydration_helpers::extract_dynamic_refs(&query_result);
                let (nodes, total_ids) =
                    hydration_helpers::build_dynamic_nodes(entity_specs, &refs);
                let (mut props, debug) =
                    execute_local_hydration(&db_path, &ctx.ontology, nodes, total_ids)?;
                hydration_queries.extend(debug);

                if let Some(registry) = registry {
                    let entity_virtuals: Vec<EntityVirtualColumns<'_>> = entity_specs
                        .iter()
                        .map(|s| (s.entity_type.as_str(), s.virtual_columns.as_slice()))
                        .collect();
                    resolve_virtual_columns(registry, &resolver_ctx, &entity_virtuals, &mut props)
                        .await?;
                }

                hydration_helpers::strip_injected_columns(
                    &mut props,
                    entity_specs
                        .iter()
                        .map(|s| (s.entity_type.as_str(), &s.injected_columns)),
                );

                hydration_helpers::merge_dynamic_properties(&mut query_result, &props);
            }
        }

        Ok(HydrationOutput {
            query_result,
            result_context,
            redacted_count: 0,
            hydration_queries,
        })
    }
}

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

// -- Helpers ------------------------------------------------------------------

fn get_db_path(ctx: &QueryPipelineContext) -> std::result::Result<PathBuf, PipelineError> {
    ctx.server_extensions
        .get::<PathBuf>()
        .cloned()
        .ok_or_else(|| PipelineError::Execution("DuckDB path not found".into()))
}

/// Compile and execute a hydration query against the local DuckDB.
fn execute_local_hydration(
    db_path: &std::path::Path,
    ontology: &Ontology,
    nodes: Vec<query_engine::compiler::InputNode>,
    total_ids: usize,
) -> std::result::Result<(hydration_helpers::PropertyMap, Vec<DebugQuery>), PipelineError> {
    if nodes.is_empty() {
        return Ok((Default::default(), Vec::new()));
    }

    let input = hydration_helpers::build_hydration_input(nodes, total_ids);

    let compiled = compile_local_input(input, ontology).map_err(|e| PipelineError::Compile {
        client_safe: e.is_client_safe(),
        message: e.to_string(),
    })?;

    let rendered_sql = compiled.base.render();
    let debug = DebugQuery {
        sql: compiled.base.sql.clone(),
        rendered: rendered_sql.clone(),
    };

    let client =
        DuckDbClient::open(db_path).map_err(|e| PipelineError::Execution(e.to_string()))?;
    let batches = client
        .query_arrow(&rendered_sql)
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let props = hydration_helpers::parse_hydration_batches(&batches)?;
    Ok((props, vec![debug]))
}
