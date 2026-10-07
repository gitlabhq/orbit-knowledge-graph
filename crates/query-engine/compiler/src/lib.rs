pub mod analytics;
pub mod ast;
pub mod config;
pub mod constants;
pub mod data_model;
pub mod error;
pub mod input;
pub mod metrics;
pub mod passes;
pub mod query_graph;
pub(crate) mod schema_limits;
pub mod scope;
pub mod types;

pub use analytics::ExecMetrics;
pub use ast::ddl;
pub use ast::{Expr, Insert, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
pub use config::compile_graph;
pub use constants::{
    EDGE_ALIAS_SUFFIXES, EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX,
    EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX, HYDRATION_NODE_ALIAS, edge_kinds_column,
    internal_column_prefix, neighbor_id_column, neighbor_is_outgoing_column, neighbor_type_column,
    path_column, relationship_type_column,
};
pub use error::{QueryError, RejectionReason, Result};
pub use input::{
    ColumnSelection, DynamicColumnMode, FilterOp, Input, InputFilter, InputNode, QueryType,
    parse_input,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::{Ontology, OntologyError};
pub use passes::codegen::{
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect,
    clickhouse::emit_simple_query, codegen,
    ddl::duckdb::emit_create_table as emit_duckdb_create_table, ddl::duckdb::generate_local_ddl,
    ddl::generate_local_tables,
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext};
pub use passes::frontend::{Frontend, gql};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationKind, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    generate_hydration_plan,
};
pub use passes::normalize::build_entity_auth;
pub use passes::plan::HydrationCompileOptions;
pub use query_data_model::EntityAuthConfig;
pub use scope::ScopeProof;
pub use types::{AccessLevel, AuthorizedPath, DEFAULT_PATH_ACCESS_LEVEL, Realm, SecurityContext};

use config::CompilerCtx as _;
use metrics::CountErr;
use std::sync::Arc;

fn finish<C: config::CompilerCtx>(
    context: &mut C,
    run: impl FnOnce(&mut C) -> Result<()>,
) -> Result<CompiledQueryContext> {
    run(context)
        .and_then(|()| {
            context.take_output().ok_or_else(|| {
                QueryError::PipelineInvariant("pipeline did not produce output".into())
            })
        })
        .count_err()
}

#[must_use = "the compiled query context should be used"]
pub fn compile(
    raw: &str,
    frontend: Frontend,
    ontology: &Arc<Ontology>,
    security: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    compile_model(raw, frontend, &model, security)
}

pub fn compile_model(
    raw: &str,
    frontend: Frontend,
    model: &Arc<query_data_model::ClickHouseDataModel>,
    security: &SecurityContext,
) -> Result<CompiledQueryContext> {
    config::compile_graph(raw, frontend, model, security).count_err()
}

#[must_use = "the compiled query context should be used"]
pub fn compile_local(
    raw: &str,
    frontend: Frontend,
    ontology: &Arc<Ontology>,
) -> Result<CompiledQueryContext> {
    let mut ontology = ontology.as_ref().clone();
    ontology.remove_data_model_optimizations();
    let model = data_model::duckdb(Arc::new(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    match frontend {
        Frontend::JsonDsl => {
            let mut context = config::DuckdbJsonDslCtx::new(model);
            context.set_raw(raw.into());
            finish(&mut context, config::run_duckdb_json_dsl)
        }
        Frontend::Gql => {
            let mut context = config::DuckdbGqlCtx::new(model);
            context.set_raw(raw.into());
            finish(&mut context, config::run_duckdb_gql)
        }
    }
}

pub fn validate_normalize(raw: &str, ontology: &Arc<Ontology>) -> Result<Input> {
    let model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    let mut context = config::ValidateNormalizeCtx::new(model);
    context.set_raw(raw.into());
    config::run_validate_normalize(&mut context)
        .and_then(|()| {
            context.take_input().ok_or_else(|| {
                QueryError::PipelineInvariant("validate_normalize produced no input".into())
            })
        })
        .count_err()
}

pub fn validate_normalize_gql(raw: &str, ontology: &Arc<Ontology>) -> Result<Input> {
    let model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    let mut context = config::ValidateNormalizeGqlCtx::new(model);
    context.set_raw(raw.into());
    config::run_validate_normalize_gql(&mut context)
        .and_then(|()| {
            context.take_input().ok_or_else(|| {
                QueryError::PipelineInvariant("validate_normalize_gql produced no input".into())
            })
        })
        .count_err()
}

pub fn compile_input(
    input: Input,
    options: HydrationCompileOptions,
    ontology: &Arc<Ontology>,
    security: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    compile_input_model(input, options, &model, security)
}

pub fn compile_input_model(
    mut input: Input,
    options: HydrationCompileOptions,
    model: &Arc<query_data_model::ClickHouseDataModel>,
    security: &SecurityContext,
) -> Result<CompiledQueryContext> {
    use query_graph::{LatestRows, QueryGraph};
    passes::restrict::restrict(&mut input, model.as_ref(), security)?;
    let mut graph = QueryGraph::<_, LatestRows<'_>>::new(model.as_ref());
    let root = graph.plan_with_options(&input, options)?;
    let graph = graph.lower_operations();
    let base = passes::codegen::clickhouse::codegen_graph(
        graph,
        root,
        ResultContext::new().with_query_type(input.query_type),
        passes::settings::resolve(input.query_type.into()),
    )?;
    Ok(CompiledQueryContext {
        query_type: input.query_type,
        base,
        hydration: HydrationPlan::None,
        input,
        pagination: Default::default(),
        has_virtual_columns: false,
    })
}

#[cfg(test)]
mod compile_tests;
#[cfg(test)]
pub(crate) use compile_tests::testkit;
