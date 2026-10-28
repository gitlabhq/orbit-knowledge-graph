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

    fn compile_sql(query: &str) -> String {
        compile(query, Frontend::JsonDsl, &ONTOLOGY, &security_ctx())
            .expect("should compile")
            .base
            .render()
    }

    fn compile_gql_sql(query: &str) -> String {
        compile(query, Frontend::Gql, &ONTOLOGY, &security_ctx())
            .expect("should compile")
            .base
            .render()
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
    fn unfiltered_edge_only_count_emits_bare_count_for_projection_routing() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "parent", "entity": "Pipeline", "node_ids": [1]},
                {"id": "child", "entity": "Pipeline"}
            ],
            "relationships": [{"type": "CHILD_OF", "from": "parent", "to": "child"}],
            "group_by": ["parent"],
            "aggregations": [{
                "count": "child",
                "as": "children"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("countIf("),
            "single-hop edge count should use countIf on the LIMIT BY path, \
             got:\n{sql}"
        );
        assert!(
            sql.contains("LIMIT 1 BY"),
            "single-hop edge aggregation should use LIMIT BY dedup, got:\n{sql}"
        );
        assert!(
            !sql.contains("COUNT(_orbit_edge_0.source_id)")
                && !sql.contains("count(_orbit_edge_0.source_id)"),
            "must not emit COUNT(source_id) -- forces unnecessary column read, \
             got:\n{sql}"
        );
    }

    /// A single-hop edge scan cannot fan a node out, so it stays a plain scan;
    /// deduplicating the hot single-edge path would cost an unnecessary aggregation.
    #[test]
    fn single_edge_scan_stays_plain() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [490855697]},
                {"id": "label", "entity": "Label", "filters": {"title": "group::source code"}}
            ],
            "relationships": [
                {"type": "HAS_LABEL", "from": "mr", "to": "label"}
            ],
            "aggregations": [{"count": "mr", "as": "n"}],
            "limit": 1
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("argMax("),
            "single-hop edge scan must not dedup, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_code_filtered_endpoints_prune_by_traversal_path() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Definition", "filters": {"name": {"eq": "compile"}}},
                {"id": "end", "entity": "Definition", "filters": {"name": {"eq": "run_query"}}}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["CALLS"]},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_path_scope_traversal_paths"),
            "code path finding should compute candidate traversal paths, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.traversal_path = e2.traversal_path"),
            "code edge self-joins should stay within one traversal_path, got:\n{sql}"
        );
        assert!(
            sql.contains("f.traversal_path = b.traversal_path"),
            "frontier intersection should stay within one traversal_path, got:\n{sql}"
        );
        assert!(
            sql.contains("forward") && sql.contains("FROM _nf_start"),
            "forward CTE should seed from _nf_start, got:\n{sql}"
        );
        assert!(
            sql.contains(
                "traversal_path IN (SELECT traversal_path FROM _path_scope_traversal_paths)"
            ),
            "edge scans should use traversal_path scope, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.source_kind = 'Definition'"),
            "forward edge scans should constrain source_kind = Definition, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_cross_level_endpoints_union_traversal_scope() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "from", "entity": "Group",   "node_ids": [9970]},
                {"id": "to",   "entity": "Project", "node_ids": [278964]}
            ],
            "path": {"type": "shortest", "from": "from", "to": "to", "max_depth": 3,
                     "rel_types": ["CONTAINS"]}
        }"#;
        let sql = compile_sql(query);
        assert!(
            sql.contains("_path_scope_traversal_paths"),
            "cross-level path finding should still compute a scope, got:\n{sql}"
        );
        assert!(
            sql.contains("UNION ALL SELECT _path_scope_end.traversal_path"),
            "scope must UNION both endpoints' traversal paths, got:\n{sql}"
        );
        assert!(
            !sql.contains("_path_scope_start.traversal_path = _path_scope_end.traversal_path"),
            "scope must not intersect endpoints on traversal_path equality, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_without_skip_orders_only_by_depth() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [100]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("toString(paths._orbit_path)")
                && !sql.contains("toString(paths._orbit_edge_kinds)"),
            "path array tie-break sorting should only be emitted with SKIP, got:\n{sql}"
        );
    }

    #[test]
    fn wildcard_traversal_infers_relationship_kinds_from_endpoint_entities() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "*", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("relationship_kind") && sql.contains("'AUTHORED'"),
            "wildcard traversal should infer concrete User to MergeRequest relationship kinds, got:\n{sql}"
        );
        assert!(
            !sql.contains("gl_code_edge"),
            "inferred SDLC relationship kinds should avoid scanning code edge table, got:\n{sql}"
        );
    }

    #[test]
    fn wildcard_neighbors_infers_relationship_kinds_from_center_entity() {
        let query = r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "neighbors": {"direction": "outgoing"},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("relationship_kind") && sql.contains("'AUTHORED'"),
            "wildcard neighbors should infer concrete outgoing User relationship kinds, got:\n{sql}"
        );
        assert!(
            !sql.contains("gl_code_edge"),
            "inferred User relationship kinds should avoid scanning code edge table, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_clamps_settings_to_safety_ceiling() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [100]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("max_execution_time = 15"),
            "pathfinding max_execution_time must not exceed 15, got:\n{sql}"
        );
        assert!(
            sql.contains("max_memory_usage = 16106127360"),
            "pathfinding max_memory_usage must not exceed 15 GiB, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_uncovered_by_query_relationships_filters_node_table() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"eq": "opened"}}},
                {"id": "cl", "entity": "Label", "filters": {"title": {"eq": "Community contribution"}}}
            ],
            "relationships": [
                {"type": "HAS_LABEL", "from": "mr", "to": "cl"}
            ],
            "aggregations": [{"count": "mr", "as": "c"}]
        }"#;
        let sql = compile_sql(query);
        assert!(
            !sql.contains("source_tags, 'state:opened'"),
            "uncovered state filter must not be pushed onto edge tags, got:\n{sql}"
        );
        assert!(
            sql.contains("gl_merge_request") && sql.contains("state = 'opened'"),
            "uncovered state filter must be enforced on the MR node table, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_not_pushed_onto_edges_written_without_tags() {
        for (entity, rel, target) in [
            ("WorkItem", "MENTIONS", "WorkItem"),
            ("MergeRequest", "HAS_NOTE", "Note"),
        ] {
            let query = format!(
                r#"{{
                    "query_type": "aggregation",
                    "nodes": [
                        {{"id": "a", "entity": "{entity}", "filters": {{"state": {{"eq": "opened"}}}}}},
                        {{"id": "b", "entity": "{target}"}}
                    ],
                    "relationships": [{{"type": "{rel}", "from": "a", "to": "b"}}],
                    "aggregations": [{{"count": "a", "as": "n"}}]
                }}"#
            );
            let sql = compile_sql(&query);
            assert!(
                !sql.contains("source_tags, 'state:opened'"),
                "{rel} edge rows carry no {entity} tags, filter must not be pushed, got:\n{sql}"
            );
            assert!(
                sql.contains("state = 'opened'"),
                "{rel}: state filter must be enforced on the node table, got:\n{sql}"
            );
        }
    }

    #[test]
    fn multi_hop_union_projects_denorm_tag_columns() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "a", "entity": "WorkItem", "filters": {"state": {"eq": "opened"}}, "columns": ["iid"]},
                {"id": "b", "entity": "WorkItem", "columns": ["iid"]}
            ],
            "relationships": [{"type": "RELATED_TO", "from": "a", "to": "b", "hops": [1, 2]}],
            "limit": 50
        }"#;
        let sql = compile_sql(query);
        assert!(
            sql.contains("AS source_tags"),
            "multi-hop union must project source_tags, got:\n{sql}"
        );
        assert!(
            sql.contains("AS target_tags"),
            "multi-hop union must project target_tags, got:\n{sql}"
        );
        assert!(
            sql.contains("source_tags, 'state:opened'"),
            "anchor state filter must push onto the union's projected source_tags, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_skips_rewrite_when_node_ids_present() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "node_ids": [1, 2, 3],
                 "filters": {"state": {"eq": "merged"}}}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_orbit_edge_0.target_id IN [1, 2, 3]"),
            "node_ids must be pushed to edge target_id filter, got:\n{sql}"
        );
        assert!(
            sql.contains("has(_orbit_edge_0.target_tags, 'state:merged')"),
            "denorm tag filter is applied alongside node_ids, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_aggregation_count_with_filter_uses_edge_column() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"eq": "merged"}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "group_by": ["u"],
            "aggregations": [{
                "count": "mr",
                "as": "n"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("has(_orbit_edge_0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        assert!(
            !sql.contains("_nf_mr"),
            "lowerer should not emit _nf_mr when filter is fully denormalized, got:\n{sql}"
        );
    }

    #[test]
    fn count_with_property_on_skip_target_drops_column_arg() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project"},
                {"id": "wi", "entity": "WorkItem", "filters": {"state": "closed"}},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "wi", "to": "p"},
                {"type": "CLOSED", "from": "u", "to": "wi"}
            ],
            "group_by": ["p"],
            "aggregations": [{
                "count": "u.id",
                "as": "closers_count"
            }],
            "limit": 20
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("COUNT(u.id)") && !sql.contains("countIf(u.id"),
            "must not count u.id when User is not hydrated, got:\n{sql}"
        );
        assert!(
            sql.contains("COUNT()") || sql.contains("countIf("),
            "Count over unhydrated target must collapse to bare COUNT()/countIf(cond), got:\n{sql}"
        );
    }

    #[test]
    fn denorm_preserves_role_gated_node_table_for_security() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "v", "entity": "Vulnerability", "filters": {
                    "state": {"eq": "detected"}
                }},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "proj"}],
            "group_by": ["proj"],
            "aggregations": [{
                "count": "v",
                "as": "n"
            }],
            "limit": 10
        }"#;

        let sql = compile(
            query,
            Frontend::JsonDsl,
            &ONTOLOGY,
            &crate::testkit::admin_ctx(),
        )
        .expect("should compile")
        .base
        .render();

        assert!(
            sql.contains("gl_vulnerability"),
            "role-gated entity gl_vulnerability must NOT be pruned from FROM, got:\n{sql}"
        );
    }

    #[test]
    fn aggregation_group_by_property_emits_scalar_group_key() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [1]}
            ],
            "group_by": ["p.visibility_level"],
            "aggregations": [{
                "count": "p",
                "as": "project_count"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("p.visibility_level AS p_visibility_level"),
            "property group key must be selected as a scalar column, got:\n{sql}"
        );
        assert!(
            sql.contains("GROUP BY p.visibility_level"),
            "property group key must drive GROUP BY, got:\n{sql}"
        );
        assert!(
            !sql.contains("_orbit_p_id"),
            "property grouping should not emit node-group columns, got:\n{sql}"
        );
    }

    #[test]
    fn node_table_reads_use_final_for_latest_rows() {
        let queries = [
            (
                "traversal",
                r#"{
                    "query_type": "traversal",
                    "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}}],
                    "limit": 10
                }"#,
            ),
            (
                "aggregation",
                r#"{
                    "query_type": "aggregation",
                    "nodes": [
                        {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                        {"id": "p", "entity": "Project"}
                    ],
                    "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
                    "group_by": ["p"],
                    "aggregations": [{"count": "mr", "as": "merged_mrs"}],
                    "limit": 10
                }"#,
            ),
            (
                "path_finding",
                r#"{
                    "query_type": "path_finding",
                    "nodes": [
                        {"id": "start", "entity": "User", "filters": {"username": "root"}},
                        {"id": "end", "entity": "Project", "node_ids": [100]}
                    ],
                    "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2,
                             "rel_types": ["MEMBER_OF", "CONTAINS"]},
                    "limit": 10
                }"#,
            ),
            (
                "neighbors",
                r#"{
                    "query_type": "neighbors",
                    "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"title": {"contains": "fix"}}}],
                    "neighbors": {"direction": "both"},
                    "limit": 10
                }"#,
            ),
        ];

        for (name, query) in queries {
            let compiled = compile(query, Frontend::JsonDsl, &ONTOLOGY, &security_ctx())
                .unwrap_or_else(|err| panic!("{name} should compile: {err}"));
            let sql = compiled.base.render();
            assert!(
                sql.contains(" FINAL"),
                "{name} should use FINAL for node-table dedup, got:\n{sql}"
            );
            if name == "traversal" || name == "path_finding" || name == "neighbors" {
                assert!(
                    sql.contains(" FINAL"),
                    "{name} must still use FINAL for its primary node scan, got:\n{sql}"
                );
            }
        }
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
    fn local_string_filters_fold_case_with_lower_on_both_sides() {
        for (operator, function) in [
            ("contains", "contains"),
            ("starts_with", "starts_with"),
            ("ends_with", "ends_with"),
        ] {
            let query = format!(
                r#"{{
                    "query_type": "traversal",
                    "nodes": [{{"id": "f", "entity": "File", "filters": {{"path": {{"{operator}": "ALI"}}}}}}],
                    "limit": 10
                }}"#
            );

            let sql = compile_local(&query, Frontend::JsonDsl, &ONTOLOGY)
                .expect("should compile")
                .base
                .render();

            let folded = format!("{function}(lower(f.path), lower('ALI'))");
            assert!(sql.contains(&folded), "expected {folded}, got:\n{sql}");
        }
    }

    #[test]
    fn single_ref_cte_is_not_materialized() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("MATERIALIZED"),
            "single-ref CTE must not use MATERIALIZED, got:\n{sql}"
        );
        assert!(
            !sql.contains("enable_materialized_cte"),
            "SETTINGS must not include enable_materialized_cte for non-materialized queries, got:\n{sql}"
        );
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

    #[test]
    fn final_settings_follow_final_scans() {
        let sql = compile_sql(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"state":"active"}}],"limit":10}"#,
        );
        assert!(sql.contains(" FINAL"), "{sql}");
        for setting in [
            "optimize_move_to_prewhere_if_final = 1",
            "use_skip_indexes_if_final = 1",
            "use_skip_indexes_if_final_exact_mode = 1",
        ] {
            assert!(sql.contains(setting), "{sql}");
        }
        assert!(
            !sql.contains("use_index_for_in_with_subqueries_max_values"),
            "no IN (subquery) in this query, got:\n{sql}"
        );
    }

    #[test]
    fn in_subquery_index_cap_follows_in_select() {
        let sql = compile_sql(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "filters": {"username": "alice"}},
                    {"id": "mr", "entity": "MergeRequest"}
                ],
                "relationships": [{"from": "u", "to": "mr", "type": "AUTHORED"}],
                "aggregations": [{"count": "mr", "as": "c"}],
                "limit": 20
            }"#,
        );
        assert!(sql.contains(" IN (SELECT"), "{sql}");
        assert!(
            sql.contains("use_index_for_in_with_subqueries_max_values = 1000000"),
            "{sql}"
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
    fn gql_where_id_equality_compiles_like_pattern_id() {
        assert_eq!(
            compile_gql_sql(
                "MATCH (w:WorkItem)-[:HAS_NOTE]->(n:Note) WHERE w.id = 7 RETURN n.note"
            ),
            compile_gql_sql("MATCH (w:WorkItem {id: 7})-[:HAS_NOTE]->(n:Note) RETURN n.note"),
        );
    }

    #[test]
    fn json_id_filter_compiles_like_node_ids() {
        let neighbors = |center: &str| {
            compile_sql(&format!(
                r#"{{"query_type": "neighbors", "nodes": [{{"id": "w", "entity": "WorkItem", {center}}}],
                    "neighbors": {{"direction": "both"}}, "limit": 30}}"#
            ))
        };
        assert_eq!(
            neighbors(r#""filters": {"id": 7}"#),
            neighbors(r#""node_ids": [7]"#)
        );
    }

    #[test]
    fn multi_id_neighbors_center_keeps_one_fused_scan() {
        let sql = compile_gql_sql(
            "MATCH (g:Group)-[r:MEMBER_OF|CONTAINS]-(m) WHERE g.id IN [1, 2] RETURN g, r, m LIMIT 30",
        );

        assert!(sql.contains("_orbit_fused"), "{sql}");
    }

    #[test]
    fn pinned_neighbors_center_scans_each_arm_by_its_own_id() {
        let sql = compile_gql_sql(
            "MATCH (w:WorkItem)-[r:RELATED_TO|HAS_LABEL]-(m) WHERE w.id = 7 RETURN w, r, m LIMIT 30",
        );

        assert!(!sql.contains("_orbit_fused"), "{sql}");
        assert!(sql.contains("(e.source_id = 7)"), "{sql}");
        assert!(sql.contains("(e.target_id = 7)"), "{sql}");
    }
    #[test]
    fn text_indexed_equality_also_matches_the_lowercase_index_expression() {
        let sql = compile_gql_sql("MATCH (d:Definition) WHERE d.name = 'compile' RETURN d.name");

        assert!(sql.contains("(d.name = 'compile')"), "{sql}");
        assert!(sql.contains("(lower(d.name) = lower('compile'))"), "{sql}");
    }

    #[test]
    fn equality_without_text_index_or_with_a_sort_key_filter_stays_plain() {
        for query in [
            "MATCH (d:Definition) WHERE d.kind = 'Function' RETURN d.name",
            "MATCH (d:Definition) WHERE d.name <> 'compile' RETURN d.name",
            "MATCH (d:Definition) WHERE d.project_id = 9 AND d.name = 'compile' RETURN d.name",
        ] {
            let sql = compile_gql_sql(query);
            assert!(!sql.contains("lower(d."), "{query}:\n{sql}");
        }
    }
}
