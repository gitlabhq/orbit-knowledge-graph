//! Local DuckDB query pipeline.
//!
//! ```text
//! LocalCompilation → DuckDbExecution → Extraction → LocalHydration → LocalOutput
//! ```
//!
//! No security, authorization, or redaction. Hydration fetches node
//! properties from the same DuckDB database. Virtual column resolution
//! (e.g. file content from filesystem) is not yet implemented.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use ontology::Ontology;

use duckdb_client::DuckDbClient;
use query_engine::compiler::{
    self, ColumnSelection, HydrationPlan, Input, InputNode, QueryType, compile_local,
    compile_local_input,
};
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineObserver, PipelineRunner, PipelineStage,
    QueryPipelineContext, TypeMap,
};
use query_engine::shared::{
    DebugQuery, ExecutionOutput, ExtractionOutput, ExtractionStage, HydrationOutput,
    PaginationMeta, PipelineOutput,
};
use query_engine::types::QueryResult;

const HYDRATION_NODE_ALIAS: &str = "h";

type PropertyMap = HashMap<(String, i64), HashMap<String, gkg_utils::arrow::ColumnValue>>;

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

// ── Stages ───────────────────────────────────────────────────────────────────

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

/// Hydration stage that fetches node properties from the local DuckDB.
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

        match &compiled.hydration {
            HydrationPlan::None => {}
            HydrationPlan::Static(templates) => {
                let (props, debug) = hydrate_static(&db_path, templates, &query_result)?;
                hydration_queries.extend(debug);
                merge_static_properties(&mut query_result, &props, templates);
            }
            HydrationPlan::Dynamic(entity_specs) => {
                let refs = extract_dynamic_refs(&query_result);
                let (props, debug) = hydrate_dynamic(&db_path, entity_specs, &refs)?;
                hydration_queries.extend(debug);
                merge_dynamic_properties(&mut query_result, &props);
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

// ── Hydration helpers ────────────────────────────────────────────────────────

fn get_db_path(ctx: &QueryPipelineContext) -> std::result::Result<PathBuf, PipelineError> {
    ctx.server_extensions
        .get::<PathBuf>()
        .cloned()
        .ok_or_else(|| PipelineError::Execution("DuckDB path not found".into()))
}

fn hydrate_static(
    db_path: &std::path::Path,
    templates: &[compiler::HydrationTemplate],
    query_result: &QueryResult,
) -> std::result::Result<(PropertyMap, Vec<DebugQuery>), PipelineError> {
    let mut nodes = Vec::new();
    let mut total_ids: usize = 0;

    for template in templates {
        if template.columns.is_empty() {
            continue;
        }
        let ids = collect_static_ids(query_result, template);
        if ids.is_empty() {
            continue;
        }
        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(template.entity_type.clone()),
            table: Some(template.destination_table.clone()),
            columns: Some(ColumnSelection::List(template.columns.clone())),
            node_ids: ids,
            ..InputNode::default()
        });
    }

    execute_hydration(db_path, nodes, total_ids)
}

fn hydrate_dynamic(
    db_path: &std::path::Path,
    entity_specs: &[compiler::DynamicEntityColumns],
    refs: &HashMap<String, Vec<i64>>,
) -> std::result::Result<(PropertyMap, Vec<DebugQuery>), PipelineError> {
    let mut nodes = Vec::new();
    let mut total_ids: usize = 0;

    for (entity_type, ids) in refs {
        let Some(spec) = entity_specs.iter().find(|s| s.entity_type == *entity_type) else {
            continue;
        };
        if spec.columns.is_empty() || ids.is_empty() {
            continue;
        }
        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(entity_type.clone()),
            table: Some(spec.destination_table.clone()),
            columns: Some(ColumnSelection::List(spec.columns.clone())),
            node_ids: ids.clone(),
            ..InputNode::default()
        });
    }

    execute_hydration(db_path, nodes, total_ids)
}

fn execute_hydration(
    db_path: &std::path::Path,
    nodes: Vec<InputNode>,
    total_ids: usize,
) -> std::result::Result<(PropertyMap, Vec<DebugQuery>), PipelineError> {
    if nodes.is_empty() {
        return Ok((HashMap::new(), Vec::new()));
    }

    let hydration_input = Input {
        query_type: QueryType::Hydration,
        nodes,
        limit: total_ids as u32,
        ..Input::default()
    };

    let compiled = compile_local_input(hydration_input).map_err(|e| PipelineError::Compile {
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

    let props = parse_hydration_batches(&batches)?;
    Ok((props, vec![debug]))
}

fn parse_hydration_batches(
    batches: &[arrow::record_batch::RecordBatch],
) -> std::result::Result<PropertyMap, PipelineError> {
    use gkg_utils::arrow::ArrowUtils;

    let id_col = format!("{HYDRATION_NODE_ALIAS}_id");
    let type_col = format!("{HYDRATION_NODE_ALIAS}_entity_type");
    let props_col = format!("{HYDRATION_NODE_ALIAS}_props");

    let mut map: PropertyMap = HashMap::new();

    for batch in batches {
        for row in 0..batch.num_rows() {
            let Some(id_str) = ArrowUtils::get_column_string(batch, &id_col, row) else {
                continue;
            };
            let Ok(id) = id_str.parse::<i64>() else {
                continue;
            };
            let Some(entity_type) = ArrowUtils::get_column_string(batch, &type_col, row) else {
                continue;
            };
            let Some(props_json) = ArrowUtils::get_column_string(batch, &props_col, row) else {
                continue;
            };
            let Ok(serde_json::Value::Object(obj)) =
                serde_json::from_str::<serde_json::Value>(&props_json)
            else {
                continue;
            };

            let props: HashMap<String, gkg_utils::arrow::ColumnValue> = obj
                .into_iter()
                .map(|(k, v)| (k, gkg_utils::arrow::ColumnValue::from(v)))
                .collect();
            map.insert((entity_type, id), props);
        }
    }

    Ok(map)
}

fn collect_static_ids(
    query_result: &QueryResult,
    template: &compiler::HydrationTemplate,
) -> Vec<i64> {
    let pk_col = format!("_gkg_{}_pk", template.node_alias);
    let mut ids = Vec::new();
    for row in query_result.authorized_rows() {
        if let Some(val) = row.get(&pk_col)
            && let Some(id) = val.coerce::<i64>()
            && !ids.contains(&id)
        {
            ids.push(id);
        }
    }
    ids
}

fn extract_dynamic_refs(query_result: &QueryResult) -> HashMap<String, Vec<i64>> {
    let mut refs: HashMap<String, Vec<i64>> = HashMap::new();
    for row in query_result.authorized_rows() {
        for node_ref in row.dynamic_nodes() {
            let ids = refs.entry(node_ref.entity_type.clone()).or_default();
            if !ids.contains(&node_ref.id) {
                ids.push(node_ref.id);
            }
        }
    }
    refs
}

fn merge_static_properties(
    query_result: &mut QueryResult,
    props: &PropertyMap,
    templates: &[compiler::HydrationTemplate],
) {
    for row in query_result.authorized_rows_mut() {
        for template in templates {
            let pk_col = format!("_gkg_{}_pk", template.node_alias);
            let Some(pk_val) = row.get(&pk_col) else {
                continue;
            };
            let Some(pk) = pk_val.coerce::<i64>() else {
                continue;
            };
            if let Some(entity_props) = props.get(&(template.entity_type.clone(), pk)) {
                for (col_name, value) in entity_props {
                    let prefixed = format!("{}_{col_name}", template.node_alias);
                    row.set_column(prefixed, value.clone());
                }
            }
        }
    }
}

fn merge_dynamic_properties(query_result: &mut QueryResult, props: &PropertyMap) {
    for row in query_result.authorized_rows_mut() {
        for node_ref in row.dynamic_nodes_mut() {
            let key = (node_ref.entity_type.clone(), node_ref.id);
            if let Some(entity_props) = props.get(&key) {
                for (col_name, value) in entity_props {
                    node_ref.properties.insert(col_name.clone(), value.clone());
                }
            }
        }
    }
}
