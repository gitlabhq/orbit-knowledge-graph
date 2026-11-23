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

pub mod tp_bounds;
pub mod types;

pub mod config;
pub mod passes;

pub use ast::ddl;
pub use ast::{Expr, Insert, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
pub use constants::{
    EDGE_DST_KIND_SUFFIX, EDGE_DST_SUFFIX, EDGE_KIND_SUFFIX, EDGE_SRC_KIND_SUFFIX, EDGE_SRC_SUFFIX,
    edge_kinds_column, hydration_node_alias, internal_column_prefix, neighbor_id_column,
    neighbor_is_outgoing_column, neighbor_type_column, path_column, relationship_type_column,
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
pub use passes::frontend::{Frontend, gql};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationKind, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    generate_hydration_plan,
};
pub use passes::normalize::build_entity_auth;
pub use passes::plan::HydrationCompileOptions;
pub use passes::result_contract::{EdgeMeta, RedactionNode, ResultContext};
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

pub enum CompileSource<'a> {
    Raw(&'a str),
    Parsed(Box<gql::ParsedQuery>),
}

impl CompileSource<'_> {
    fn frontend(&self, requested: Frontend) -> Frontend {
        match self {
            Self::Raw(_) => requested,
            Self::Parsed(_) => Frontend::Gql,
        }
    }

    fn load(self, ctx: &mut impl config::CompilerCtx) {
        match self {
            Self::Raw(raw) => ctx.set_raw(raw.to_string()),
            Self::Parsed(query) => query.load(ctx),
        }
    }
}

pub trait CompileTarget: Sized {
    fn compile(
        source: CompileSource,
        fe: Frontend,
        model: &Arc<Self>,
        security: Option<&SecurityContext>,
    ) -> Result<CompiledQueryContext>;
}

impl CompileTarget for query_data_model::ClickHouseDataModel {
    fn compile(
        source: CompileSource,
        fe: Frontend,
        model: &Arc<Self>,
        security: Option<&SecurityContext>,
    ) -> Result<CompiledQueryContext> {
        let security = security
            .ok_or_else(|| QueryError::PipelineInvariant("security context is required".into()))?
            .clone();
        match source.frontend(fe) {
            Frontend::JsonDsl => {
                let mut ctx = config::ClickhouseJsonDslCtx::new(security, Arc::clone(model));
                source.load(&mut ctx);
                finish(&mut ctx, config::run_clickhouse_json_dsl)
            }
            Frontend::Gql => {
                let mut ctx = config::ClickhouseGqlCtx::new(security, Arc::clone(model));
                source.load(&mut ctx);
                finish(&mut ctx, config::run_clickhouse_gql)
            }
        }
    }
}

impl CompileTarget for query_data_model::DuckDbDataModel {
    fn compile(
        source: CompileSource,
        fe: Frontend,
        model: &Arc<Self>,
        _security: Option<&SecurityContext>,
    ) -> Result<CompiledQueryContext> {
        match source.frontend(fe) {
            Frontend::JsonDsl => {
                let mut ctx = config::DuckdbJsonDslCtx::new(Arc::clone(model));
                source.load(&mut ctx);
                finish(&mut ctx, config::run_duckdb_json_dsl)
            }
            Frontend::Gql => {
                let mut ctx = config::DuckdbGqlCtx::new(Arc::clone(model));
                source.load(&mut ctx);
                finish(&mut ctx, config::run_duckdb_gql)
            }
        }
    }
}

pub fn compile_source<'a, M: CompileTarget>(
    source: CompileSource<'a>,
    fe: Frontend,
    model: &Arc<M>,
    security: impl Into<Option<&'a SecurityContext>>,
) -> Result<CompiledQueryContext> {
    M::compile(source, fe, model, security.into())
}

pub fn compile_model<'a, M: CompileTarget>(
    raw: &'a str,
    fe: Frontend,
    model: &Arc<M>,
    security: impl Into<Option<&'a SecurityContext>>,
) -> Result<CompiledQueryContext> {
    compile_source(CompileSource::Raw(raw), fe, model, security)
}

/// Compile a graph query into DuckDB SQL for local execution.
///
/// Collapses edge tables to the local single-table layout before compiling.
/// No security or hydration phases — local mode trusts all data.
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
    compile_model(raw, fe, &data_model, None)
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
    use orbit_utils::traversal_path::TraversalPath;
    use std::sync::LazyLock;

    static ONTOLOGY: LazyLock<Arc<Ontology>> =
        LazyLock::new(|| Arc::new(Ontology::load_embedded().expect("ontology must load")));

    fn security_ctx() -> SecurityContext {
        crate::testkit::non_admin_ctx()
    }

    #[test]
    fn malformed_query_increments_compiler_rejected() {
        use std::sync::atomic::Ordering;
        for fe in [Frontend::JsonDsl, Frontend::Gql] {
            let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
            let err =
                compile("not a query", fe, &ONTOLOGY, &security_ctx()).expect_err("must reject");
            let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
            assert!(
                matches!(
                    (fe, &err),
                    (Frontend::JsonDsl, QueryError::Parse(_))
                        | (Frontend::Gql, QueryError::Validation(_))
                ),
                "unexpected error: {err:?}"
            );
            assert!(
                after > before,
                "count_err must run on parse errors (before={before}, after={after})"
            );
        }
    }

    #[test]
    fn allowlist_rejected_query_increments_compiler_rejected() {
        use std::sync::atomic::Ordering;
        let query = r#"{"query_type":"traversal","nodes":[{"id":"x","entity":"NotARealEntity","columns":["id"]}],"limit":1}"#;
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err =
            compile(query, Frontend::JsonDsl, &ONTOLOGY, &security_ctx()).expect_err("must reject");
        let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        assert!(
            !matches!(err, crate::error::QueryError::PipelineInvariant(_)),
            "compile errors from internal passes must surface as their own variant, not PipelineInvariant; got: {err:?}"
        );
        assert!(
            after > before,
            "count_err must run on allowlist rejections (before={before}, after={after})"
        );
    }

    #[test]
    fn hydration_widens_paths_to_segment_budget() {
        let deep = |count: usize| -> Vec<TraversalPath> {
            (0..count)
                .map(|i| TraversalPath::new_unchecked(format!("1/{i:0>40}/{:0>40}/", i + 10000)))
                .collect()
        };
        let node = |entity: &str, paths: Vec<TraversalPath>| InputNode {
            id: "hydrate".into(),
            entity: Some(entity.into()),
            columns: Some(ColumnSelection::List(vec!["id".into()])),
            node_ids: vec![1],
            traversal_paths: paths,
            ..InputNode::default()
        };
        let compile_hydration = |nodes: Vec<InputNode>, budget: Option<usize>| {
            let input = Input {
                query_type: QueryType::Hydration,
                nodes,
                limit: 10,
                ..Input::default()
            };
            compile_input(
                input,
                HydrationCompileOptions {
                    dynamic: true,
                    path_segment_budget: budget,
                },
                &ONTOLOGY,
                &security_ctx(),
            )
            .unwrap()
        };
        let bound_paths = |result: &CompiledQueryContext| -> Vec<TraversalPath> {
            result
                .base
                .params
                .values()
                .filter_map(|p| match &p.value {
                    serde_json::Value::Array(items) => Some(items),
                    _ => None,
                })
                .flat_map(|items| items.iter().filter_map(|v| v.as_str()))
                .map(TraversalPath::new_unchecked)
                .collect()
        };

        let exact = deep(500);
        let result = compile_hydration(
            vec![node("Note", exact.clone()), node("Project", exact.clone())],
            Some(2000),
        );
        let array_params = result
            .base
            .params
            .values()
            .filter(|p| matches!(p.value, serde_json::Value::Array(_)))
            .count();
        assert_eq!(array_params, 1, "arms share one path array param");
        let mut kept = bound_paths(&result);
        kept.sort_unstable();
        let mut expected = exact;
        expected.sort_unstable();
        assert_eq!(kept, expected, "under budget keeps exact leaf paths");

        let over = deep(900);
        let result = compile_hydration(vec![node("Note", over.clone())], Some(2000));
        let widened = bound_paths(&result);
        assert!(widened.iter().all(|w| !over.contains(w)));
        for path in &over {
            assert!(
                widened
                    .iter()
                    .any(|w| path.as_str().starts_with(w.as_str())),
                "{path} lost its ancestor prefix"
            );
        }

        let result = compile_hydration(vec![node("Note", over.clone())], None);
        assert_eq!(
            bound_paths(&result).len(),
            over.len(),
            "no budget, no widening"
        );
    }

    fn note_excerpt_chars(limit: u32) -> u32 {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            &format!(
                r#"{{
                "query_type": "traversal",
                "nodes": [{{"id": "n", "entity": "Note", "node_ids": [1], "columns": ["note"]}}],
                "limit": {limit}
            }}"#
            ),
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect("note query should compile");
        let sql = compiled.base.render();
        let (_, after) = sql
            .split_once("substringUTF8(n.note, 1, ")
            .unwrap_or_else(|| panic!("expected a text excerpt, got:\n{sql}"));
        after
            .split(')')
            .next()
            .and_then(|chars| chars.parse().ok())
            .unwrap_or_else(|| panic!("expected excerpt length, got:\n{sql}"))
    }

    #[test]
    fn text_excerpt_length_shrinks_with_page_size() {
        let single_row = note_excerpt_chars(1);
        let wide_page = note_excerpt_chars(1000);
        assert!(single_row > 1_000_000, "single row got {single_row} chars");
        assert!(
            (1000..3000).contains(&wide_page),
            "wide page got {wide_page} chars"
        );
    }

    #[test]
    fn compile_targets_select_the_pipeline_by_data_model() {
        let query = r#"{"query_type":"traversal","nodes":[{"id":"d","entity":"Definition","node_ids":[1],"columns":["name"]}]}"#;
        let clickhouse = data_model::clickhouse(Arc::clone(&ONTOLOGY)).unwrap();
        let duckdb = data_model::duckdb(Arc::clone(&ONTOLOGY)).unwrap();

        let error = compile_model(query, Frontend::JsonDsl, &clickhouse, None).unwrap_err();
        assert!(error.to_string().contains("security context is required"));
        let local = compile_model(query, Frontend::JsonDsl, &duckdb, None).unwrap();
        assert_eq!(local.base.dialect, SqlDialect::DuckDb);
        let remote = compile_model(
            query,
            Frontend::JsonDsl,
            &clickhouse,
            &testkit::non_admin_ctx(),
        )
        .unwrap();
        assert_eq!(remote.base.dialect, SqlDialect::ClickHouse);
    }
}
