//! Graph Query Compiler
//!
//! Compiles graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! raw → {JSON | GQL} frontend → Input → shared compiler passes → SQL
//! ```
//!
//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//! use compiler::{compile, Frontend, SecurityContext};
//! use ontology::{Ontology, DataType};
//!
//! let ontology = Arc::new(
//!     Ontology::new()
//!         .with_nodes(["User", "Project"])
//!         .with_edges(["MEMBER_OF"])
//!         .with_fields("User", [("username", DataType::String)]),
//! );
//!
//! let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
//!
//! let json = r#"{
//!     "query_type": "traversal",
//!     "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, Frontend::JsonDsl, &ontology, &ctx).unwrap();
//! println!("SQL: {}", result.base.sql);
//! ```

pub mod analytics;
pub mod ast;
pub mod constants;
pub mod data_model;
pub mod error;
pub mod input;
pub mod metrics;
pub(crate) mod query_limits;

pub mod scope;
pub mod types;

pub mod config;
pub mod passes;

pub use ast::ddl;
pub use ast::{Expr, Insert, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
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
pub use query_data_model::EntityAuthConfig;

pub use analytics::ExecMetrics;
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
pub use scope::ScopeProof;
pub use types::{AccessLevel, AuthorizedPath, DEFAULT_PATH_ACCESS_LEVEL, Realm, SecurityContext};

use metrics::CountErr;
use std::sync::Arc;

use config::CompilerCtx as _;

fn finish<C: config::CompilerCtx>(
    ctx: &mut C,
    run: impl FnOnce(&mut C) -> Result<()>,
) -> Result<CompiledQueryContext> {
    run(ctx)
        .and_then(|()| {
            ctx.take_output().ok_or_else(|| {
                error::QueryError::PipelineInvariant("pipeline did not produce output".into())
            })
        })
        .count_err()
}

#[must_use = "the compiled query context should be used"]
pub fn compile(
    raw: &str,
    fe: Frontend,
    ontology: &Arc<Ontology>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    compile_model(raw, fe, &data_model, ctx)
}

pub fn compile_model(
    raw: &str,
    fe: Frontend,
    data_model: &Arc<query_data_model::ClickHouseDataModel>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    match fe {
        Frontend::JsonDsl => {
            let mut ctx = config::ClickhouseJsonDslCtx::new(ctx.clone(), Arc::clone(data_model));
            ctx.set_raw(raw.to_string());
            finish(&mut ctx, config::run_clickhouse_json_dsl)
        }
        Frontend::Gql => {
            let mut ctx = config::ClickhouseGqlCtx::new(ctx.clone(), Arc::clone(data_model));
            ctx.set_raw(raw.to_string());
            finish(&mut ctx, config::run_clickhouse_gql)
        }
    }
}

/// Compile a graph query into DuckDB SQL for local execution.
///
/// Collapses edge tables to the local single-table layout before compiling.
/// No security, hydration, or cursor phases — local mode trusts all data.
#[must_use = "the compiled query context should be used"]
pub fn compile_local(
    raw: &str,
    fe: Frontend,
    ontology: &Arc<Ontology>,
) -> Result<CompiledQueryContext> {
    let mut ontology = ontology.as_ref().clone();
    ontology.remove_data_model_optimizations();
    let ontology = Arc::new(ontology);
    let data_model = data_model::duckdb(Arc::clone(&ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    match fe {
        Frontend::JsonDsl => {
            let mut c = config::DuckdbJsonDslCtx::new(Arc::clone(&data_model));
            c.set_raw(raw.to_string());
            finish(&mut c, config::run_duckdb_json_dsl)
        }
        Frontend::Gql => {
            let mut c = config::DuckdbGqlCtx::new(Arc::clone(&data_model));
            c.set_raw(raw.to_string());
            finish(&mut c, config::run_duckdb_gql)
        }
    }
}

/// Run only `validate` + `normalize`, returning the normalized [`Input`].
pub fn validate_normalize(json_input: &str, ontology: &Arc<Ontology>) -> Result<Input> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    let mut ctx = config::ValidateNormalizeCtx::new(data_model);
    ctx.set_raw(json_input.to_string());
    config::run_validate_normalize(&mut ctx)
        .and_then(|()| {
            ctx.take_input().ok_or_else(|| {
                error::QueryError::PipelineInvariant("validate_normalize produced no input".into())
            })
        })
        .count_err()
}

pub fn validate_normalize_gql(raw: &str, ontology: &Arc<Ontology>) -> Result<Input> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    let mut ctx = config::ValidateNormalizeGqlCtx::new(data_model);
    ctx.set_raw(raw.to_string());
    config::run_validate_normalize_gql(&mut ctx)
        .and_then(|()| {
            ctx.take_input().ok_or_else(|| {
                error::QueryError::PipelineInvariant(
                    "validate_normalize_gql produced no input".into(),
                )
            })
        })
        .count_err()
}

/// Compile a pre-built hydration `Input` into ClickHouse SQL.
///
/// Runs the hydration pipeline: Restrict → Plan → Lower → Enforce → Settings → Codegen.
/// Skips validation, normalization, security, check, and hydrate plan passes.
/// Codegen defaults to `HydrationPlan::None`.
pub fn compile_input(
    input: Input,
    options: HydrationCompileOptions,
    ontology: &Arc<Ontology>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    compile_input_model(input, options, &data_model, ctx)
}

pub fn compile_input_model(
    input: Input,
    options: HydrationCompileOptions,
    data_model: &Arc<query_data_model::ClickHouseDataModel>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut ctx = config::ChHydrationCtx::new(ctx.clone(), Arc::clone(data_model));
    ctx.set_input(input);
    ctx.set_hydration_options(options);
    config::run_ch_hydration(&mut ctx)
        .and_then(|()| {
            ctx.take_output().ok_or_else(|| {
                error::QueryError::PipelineInvariant("pipeline did not produce output".into())
            })
        })
        .count_err()
}

#[cfg(test)]
pub(crate) mod testkit {
    use crate::types::{AccessLevel, AuthorizedPath, SecurityContext};

    pub fn non_admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    pub fn admin_ctx() -> SecurityContext {
        SecurityContext::new_with_roles(
            1,
            vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
        )
        .unwrap()
        .with_role(true, Some(AccessLevel::Owner as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;
    use std::sync::atomic::Ordering;

    static ONTOLOGY: LazyLock<Arc<Ontology>> =
        LazyLock::new(|| Arc::new(Ontology::load_embedded().expect("ontology must load")));

    #[test]
    fn compile_rejections_increment_the_rejection_counter() {
        let reader = crate::testkit::non_admin_ctx();
        let narrow = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        for (query, frontend, context) in [
            ("not a query", Frontend::JsonDsl, &reader),
            ("not a query", Frontend::Gql, &reader),
            (
                r#"{"query_type":"traversal","nodes":[{"id":"x","entity":"NotARealEntity","columns":["id"]}],"limit":1}"#,
                Frontend::JsonDsl,
                &reader,
            ),
            (
                r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","filters":{"traversal_path":{"starts_with":"1/"}}}],"limit":1}"#,
                Frontend::JsonDsl,
                &narrow,
            ),
        ] {
            let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
            compile(query, frontend, &ONTOLOGY, context).expect_err(query);
            let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
            assert!(after > before, "count_err must run for {query}");
        }
    }

    #[test]
    fn native_conditions_need_a_column() {
        use crate::input::{BooleanExpression, InputFilter};
        let base = gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
        let mut empty_group = base.clone();
        empty_group.predicates = vec![BooleanExpression::Not(Box::new(BooleanExpression::And(
            vec![],
        )))];
        let mut empty_property = base;
        empty_property.nodes[0].filters.insert(
            String::new(),
            vec![InputFilter {
                value: Some(serde_json::json!(1)),
                ..Default::default()
            }],
        );
        for input in [empty_group, empty_property] {
            let error = compile_input(
                input,
                Default::default(),
                &ONTOLOGY,
                &crate::testkit::non_admin_ctx(),
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("every condition must compare a property"),
                "{error}"
            );
        }
    }
}
