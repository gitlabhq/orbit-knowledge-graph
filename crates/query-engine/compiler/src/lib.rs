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

    static ONTOLOGY: LazyLock<Arc<Ontology>> =
        LazyLock::new(|| Arc::new(Ontology::load_embedded().expect("ontology must load")));

    fn security_ctx() -> SecurityContext {
        crate::testkit::non_admin_ctx()
    }

    #[test]
    fn relationship_filter_keeps_edge_column_type() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "filters": {"target_id": 2}
            }]
        }"#;

        let compiled = compile(query, Frontend::JsonDsl, &ONTOLOGY, &security_ctx()).unwrap();
        let filter_param = compiled
            .base
            .params
            .values()
            .find(|param| param.value == serde_json::json!(2))
            .expect("target_id filter parameter");
        assert_eq!(
            filter_param.data_type,
            orbit_utils::query_types::SqlType::Int64
        );
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
    fn compile_with_prefixed_ontology_produces_prefixed_sql() {
        let prefixed = Arc::new((**ONTOLOGY).clone().with_schema_version_prefix("v1_"));

        let query = r#"{"query_type":"traversal","nodes":[{"id":"g","entity":"Group","node_ids":[1],"columns":["name"]}],"limit":1}"#;
        let compiled =
            compile(query, Frontend::JsonDsl, &prefixed, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("v1_gl_group"),
            "search SQL should use prefixed node table v1_gl_group, got: {sql}"
        );
    }

    #[test]
    fn compile_uses_supplied_ontology_for_scoped_user_table() {
        let scoped_user = (**ONTOLOGY).clone().with_path_scopable_nodes(["User"]);
        let query = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"columns":["id"]}],"limit":1}"#;

        for (ontology, expected_table) in [
            (Arc::new(scoped_user.clone()), "gl_user"),
            (
                Arc::new(scoped_user.with_schema_version_prefix("v1_")),
                "v1_gl_user",
            ),
        ] {
            let sql = compile(query, Frontend::JsonDsl, &ontology, &security_ctx())
                .expect("should compile")
                .base
                .render();
            assert!(
                sql.contains("startsWith(u.traversal_path"),
                "supplied ontology must keep the User alias scoped, got:\n{sql}"
            );
            assert!(
                sql.contains(expected_table),
                "ontology should render {expected_table}, got:\n{sql}"
            );
        }
    }

    #[test]
    fn compile_uses_each_ontologys_global_tables_and_keeps_join_filters() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "user", "entity": "User", "node_ids": [1], "filters": {"username": "alice"}},
                {"id": "project", "entity": "Project", "filters": {"name": "orbit"}}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "user",
                "to": "project"
            }],
            "limit": 1
        }"#;

        let mut sources = ontology::migrations::embedded_sources();
        let user_source = sources.get_mut("nodes/core/user.yaml").unwrap();
        *user_source = user_source.replace(
            "destination_table: gl_user",
            "destination_table: gl_renamed_user",
        );
        let archived_ontology = ontology::archive::OntologyArchive::from_sources(1, &sources)
            .expect("renamed archive should build")
            .load_ontology()
            .expect("renamed archive should load");
        let prefixed = archived_ontology.clone().with_schema_version_prefix("v1_");

        for (ontology, expected_user_table) in [
            (Arc::new(archived_ontology.clone()), "gl_renamed_user"),
            (ONTOLOGY.clone(), "gl_user"),
            (Arc::new(prefixed), "v1_gl_renamed_user"),
            (
                Arc::new((**ONTOLOGY).clone().with_schema_version_prefix("v2_")),
                "v2_gl_user",
            ),
            (Arc::new(archived_ontology), "gl_renamed_user"),
        ] {
            let sql = compile(query, Frontend::JsonDsl, &ontology, &security_ctx())
                .expect("should compile")
                .base
                .render();
            assert!(
                sql.contains(expected_user_table),
                "compiled SQL should use {expected_user_table}, got:\n{sql}"
            );
            assert!(
                !sql.contains("startsWith(user.traversal_path"),
                "global User must stay unscoped, got:\n{sql}"
            );
            assert!(
                sql.contains("startsWith(project.traversal_path"),
                "joined Project alias must remain scoped, got:\n{sql}"
            );
            assert!(
                sql.contains("startsWith(_orbit_edge_0.traversal_path"),
                "edge alias e0 must remain scoped, got:\n{sql}"
            );
        }
    }

    fn compile_error(query: &str) -> String {
        compile(query, Frontend::JsonDsl, &ONTOLOGY, &security_ctx())
            .expect_err("query must be rejected")
            .to_string()
    }

    #[test]
    fn multi_node_traversal_without_relationships_explains_the_shape() {
        let msg = compile_error(
            r#"{"query_type":"traversal","nodes":[
                {"id":"u","entity":"User","node_ids":[1],"columns":["username"]},
                {"id":"mr","entity":"MergeRequest","node_ids":[2]}
            ],"limit":5}"#,
        );
        assert!(
            msg.contains("Multi-node traversal requires nodes + relationships"),
            "{msg}"
        );
        assert!(!msg.contains("oneOf"), "{msg}");
    }

    #[test]
    fn non_numeric_id_range_endpoint_says_the_value_is_invalid() {
        let msg = compile_error(
            r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project",
                "columns":["name"],"id_range":{"start":"abc","end":5}}],"limit":5}"#,
        );
        assert!(
            msg.contains("invalid value at /nodes/0/id_range/start: Start of ID range (inclusive)"),
            "{msg}"
        );
    }

    #[test]
    fn malformed_filter_explains_the_accepted_shapes() {
        let msg = compile_error(
            r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","node_ids":[1],
                "columns":["name"],"filters":{"name":{"op":"bogus","value":1}}}],"limit":5}"#,
        );
        assert!(msg.contains("A bare value is an equality match"), "{msg}");
        assert!(
            msg.contains("invalid value at /nodes/0/filters/name: "),
            "{msg}"
        );
        assert!(!msg.contains("oneOf"), "{msg}");
    }

    #[test]
    fn neighbors_records_a_center_only_for_one_id() {
        let center = |ids: &str| {
            let query = format!(
                r#"{{"query_type": "neighbors", "nodes": [{{"id": "u", "entity": "User", "node_ids": {ids}}}],
                    "neighbors": {{"direction": "outgoing"}}, "limit": 10}}"#
            );
            let compiled = compile(&query, Frontend::JsonDsl, &ONTOLOGY, &security_ctx()).unwrap();
            let context = &compiled.base.result_context;
            (
                context
                    .neighbors_center()
                    .map(|(entity, id)| (entity.to_string(), id)),
                context.label("User").map(str::to_string),
            )
        };
        assert_eq!(
            center("[1]"),
            (Some(("User".into(), 1)), Some("username".into()))
        );
        assert_eq!(center("[1, 2]"), (None, None));
    }

    #[test]
    fn scope_anchor_aggregation_rejects_join_predicates() {
        for predicate in ["g.id = p.id", "p.id = g.id"] {
            let query =
                format!("MATCH (g:Group {{id: 100}})-[:CONTAINS]->(p:Project) WHERE {predicate}");
            let error = compile(
                &format!("{query} RETURN count(p) AS c LIMIT 20"),
                Frontend::Gql,
                &ONTOLOGY,
                &security_ctx(),
            )
            .unwrap_err();
            assert!(
                matches!(error, QueryError::Validation(ref message)
                    if message == "cross-node property comparisons are only supported in traversal queries"),
                "{error}"
            );
        }
    }

    #[test]
    fn multi_filter_range_compiles_both_predicates() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let query = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "mr", "entity": "MergeRequest",
                     "node_ids": [1],
                     "filters": {
                         "created_at": [
                             {"gte": "2026-04-01T00:00:00Z"},
                             {"lt": "2026-05-01T00:00:00Z"}
                         ]
                     },
                     "columns": ["id", "created_at"]}],
            "limit": 10
        }"#;
        let compiled =
            compile(query, Frontend::JsonDsl, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains(">=") && sql.contains("<"),
            "both range predicates must appear in SQL, got:\n{sql}"
        );
    }

    #[test]
    fn filter_on_virtual_column_compiles_without_sql_predicate() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            r#"{
        "query_type": "traversal",
        "nodes": [{"id": "f", "entity": "File",
                 "node_ids": [1],
                 "filters": {"content": {"eq": "x"}},
                 "columns": ["path", "content"]}],
        "limit": 5
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect("virtual column filter should compile");

        let sql = compiled.base.render();
        assert!(
            !sql.contains("content"),
            "virtual column 'content' must not appear in SQL, got:\n{sql}"
        );

        if let HydrationPlan::Static(templates) = &compiled.hydration {
            assert!(
                templates.iter().any(|t| !t.virtual_filters.is_empty()),
                "hydration plan should carry virtual filters"
            );
        } else {
            panic!("expected static hydration plan");
        }
    }

    #[test]
    fn filter_on_virtual_column_without_selecting_it_injects_resolution() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            r#"{
        "query_type": "traversal",
        "nodes": [{"id": "f", "entity": "File",
                 "node_ids": [1, 2],
                 "filters": {"content": {"contains": "needle"},
                             "project_id": {"eq": 1000}},
                 "columns": ["project_id", "path"]}],
        "limit": 10
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect("filter-only virtual column should compile");

        let HydrationPlan::Static(templates) = &compiled.hydration else {
            panic!(
                "filter-only virtual column must produce a static hydration plan, got {:?}",
                compiled.hydration
            );
        };
        let template = &templates[0];
        assert!(
            template
                .virtual_columns
                .iter()
                .any(|vc| vc.column_name == "content"),
            "content must be injected for resolution: {template:?}"
        );
        assert_eq!(template.filter_injected_columns, vec!["content"]);
        assert!(
            template
                .virtual_filters
                .iter()
                .any(|(col, _)| col == "content"),
            "content filter must be carried: {template:?}"
        );
    }

    #[test]
    fn filter_on_selected_virtual_column_is_not_marked_injected() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            r#"{
        "query_type": "traversal",
        "nodes": [{"id": "f", "entity": "File",
                 "node_ids": [1],
                 "filters": {"content": {"contains": "needle"}},
                 "columns": ["path", "content"]}],
        "limit": 5
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect("should compile");

        let HydrationPlan::Static(templates) = &compiled.hydration else {
            panic!("expected static hydration plan");
        };
        assert!(templates[0].filter_injected_columns.is_empty());
        assert_eq!(
            templates[0]
                .virtual_columns
                .iter()
                .filter(|vc| vc.column_name == "content")
                .count(),
            1
        );
    }

    #[test]
    fn filter_on_virtual_column_rejected_without_node_ids() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"{
        "query_type": "traversal",
        "nodes": [{"id": "f", "entity": "File",
                 "filters": {"content": {"contains": "needle"},
                             "project_id": {"eq": 1000}},
                 "columns": ["path"]}],
        "limit": 10
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect_err("virtual filter without node_ids should be rejected");

        assert!(err.is_client_safe());
        let msg = err.to_string();
        assert!(
            msg.contains("node_ids") && msg.contains("content"),
            "error should name node_ids and the column: {msg}"
        );
    }

    #[test]
    fn filter_on_virtual_column_rejected_for_aggregation() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"{
        "query_type": "aggregation",
        "nodes": [{"id": "f", "entity": "File",
                 "filters": {"content": {"contains": "needle"}}}],
        "aggregations": [{"count": "f", "as": "total"}],
        "group_by": ["f.language"],
        "limit": 5
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect_err("virtual filter on aggregation should be rejected");

        assert!(err.is_client_safe());
        let msg = err.to_string();
        assert!(
            msg.contains("aggregation") && msg.contains("content"),
            "error should name the query type and column: {msg}"
        );
    }

    #[test]
    fn filter_on_virtual_column_rejected_for_neighbors() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"{
        "query_type": "neighbors",
        "nodes": [{"id": "f", "entity": "File", "node_ids": [1],
                 "filters": {"content": {"contains": "needle"}}}],
        "neighbors": {"direction": "outgoing"},
        "limit": 5
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect_err("virtual filter on neighbors should be rejected");

        assert!(err.is_client_safe());
        let msg = err.to_string();
        assert!(
            msg.contains("neighbors") && msg.contains("content"),
            "error should name the query type and column: {msg}"
        );
    }

    #[test]
    fn filter_on_virtual_column_rejects_unsupported_op() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"{
        "query_type": "traversal",
        "nodes": [{"id": "f", "entity": "File",
                 "node_ids": [1],
                 "filters": {"content": {"gt": "x"}}}],
        "limit": 5
                }"#,
            Frontend::JsonDsl,
            &ontology,
            &security_ctx(),
        )
        .expect_err("unsupported op on virtual column should be rejected");

        assert!(
            err.is_client_safe(),
            "virtual column op rejection should be client-safe"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("content"),
            "error should name the column: {msg}"
        );
        assert!(msg.contains("gt"), "error should name the operator: {msg}");
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
}
