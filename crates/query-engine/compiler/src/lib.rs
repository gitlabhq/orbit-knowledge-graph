//! Graph Query Compiler
//!
//! Compiles JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! JSON → Validate → Normalize → Restrict → Plan → Lower → Enforce → Security → Check → Codegen → SQL
//! ```
//!
//! # Example
//!
//! ```rust
//! use compiler::{compile, SecurityContext};
//! use ontology::{Ontology, DataType};
//!
//! let ontology = Ontology::new()
//!     .with_nodes(["User", "Project"])
//!     .with_edges(["MEMBER_OF"])
//!     .with_fields("User", [("username", DataType::String)]);
//!
//! let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
//!
//! let json = r#"{
//!     "query_type": "traversal",
//!     "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, &ontology, &ctx).unwrap();
//! println!("SQL: {}", result.base.sql);
//! ```

pub mod analytics;
pub mod ast;
pub mod constants;
pub mod error;
pub mod input;
pub mod metrics;
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
pub use error::{QueryError, Result};
pub use input::{
    ColumnSelection, DynamicColumnMode, EntityAuthConfig, FilterOp, Input, InputFilter, InputNode,
    QueryType, parse_input,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::{Ontology, OntologyError};

pub use analytics::{ExecMetrics, QueryInfo};
pub use passes::codegen::{
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect,
    clickhouse::emit_simple_query, codegen, ddl::clickhouse::emit_create_table,
    ddl::duckdb::emit_create_table as emit_duckdb_create_table, ddl::generate_graph_tables,
    ddl::generate_graph_tables_with_prefix, ddl::generate_local_tables,
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    generate_hydration_plan,
};
pub use passes::normalize::{build_entity_auth, normalize};
pub use types::{AccessLevel, DEFAULT_PATH_ACCESS_LEVEL, Realm, SecurityContext, TraversalPath};

use metrics::CountErr;
use std::sync::Arc;

use config::CompilerCtx as _;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Compile a JSON query into a [`CompiledQueryContext`].
///
/// The context contains the parameterized SQL, bind parameters, result context
/// for redaction, hydration plan, and the validated input.
///
/// Runs the ClickHouse compilation pipeline. Edge-chain-first lowering
/// produces flat edge-chain JOINs with inline dedup.
///
/// ```text
/// JSON → Validate → Normalize → Restrict → Lower → Enforce → Security → Check → HydratePlan → Settings → Codegen
/// ```
#[must_use = "the compiled query context should be used"]
pub fn compile(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut ctx = config::ClickhouseCtx::new(Arc::new(ontology.clone()), ctx.clone());
    ctx.set_json(json_input.to_string());
    config::run_clickhouse(&mut ctx)
        .and_then(|()| {
            ctx.take_output().ok_or_else(|| {
                error::QueryError::PipelineInvariant("pipeline did not produce output".into())
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
    ontology: &Arc<Ontology>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut ctx = config::ChHydrationCtx::new(Arc::clone(ontology), ctx.clone());
    ctx.set_input(input);
    config::run_ch_hydration(&mut ctx)
        .and_then(|()| {
            ctx.take_output().ok_or_else(|| {
                error::QueryError::PipelineInvariant("pipeline did not produce output".into())
            })
        })
        .count_err()
}

// Pipeline presets are in `pipelines.rs`.
// Tests are in `tests/compiler_tests.rs` and `tests/ontology_tests.rs`.

/// Shared test helpers available to all test modules in this crate.
#[cfg(test)]
pub(crate) mod testkit {
    use crate::types::{AccessLevel, SecurityContext, TraversalPath};

    pub fn non_admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    pub fn admin_ctx() -> SecurityContext {
        SecurityContext::new_with_roles(
            1,
            vec![TraversalPath::new("1/", AccessLevel::Owner as u32)],
        )
        .unwrap()
        .with_role(true, Some(AccessLevel::Owner as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static ONTOLOGY: LazyLock<Ontology> =
        LazyLock::new(|| Ontology::load_embedded().expect("ontology must load"));

    fn security_ctx() -> SecurityContext {
        crate::testkit::non_admin_ctx()
    }

    /// Compile a query JSON string against the embedded ontology and return
    /// the rendered ClickHouse SQL.
    fn compile_sql(query: &str) -> String {
        compile(query, &ONTOLOGY, &security_ctx())
            .expect("should compile")
            .base
            .render()
    }

    /// Regression: pipeline-execution errors must reach `count_err`. Before
    /// this fix the body was `pipeline.execute(...)?.into_output().count_err()`,
    /// where the `?` propagated `QueryError` past `count_err`, so the counter
    /// was never incremented. Asserting the test-only `COUNT_ERR_HITS` side
    /// channel ensures the regression cannot return undetected; asserting on
    /// the error variant alone would have passed against the buggy code.
    #[test]
    fn malformed_query_increments_compiler_rejected() {
        use std::sync::atomic::Ordering;
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile("not json", &ONTOLOGY, &security_ctx()).expect_err("must reject");
        let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        assert!(
            matches!(err, crate::error::QueryError::Parse(_)),
            "expected Parse, got: {err:?}"
        );
        assert!(
            after > before,
            "count_err must run on parse errors (before={before}, after={after})"
        );
    }

    #[test]
    fn allowlist_rejected_query_increments_compiler_rejected() {
        use std::sync::atomic::Ordering;
        let query = r#"{"query_type":"traversal","node":{"id":"x","entity":"NotARealEntity","columns":["id"]},"limit":1}"#;
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile(query, &ONTOLOGY, &security_ctx()).expect_err("must reject");
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
    fn traversal_path_scope_rejection_is_observable() {
        use std::sync::atomic::Ordering;
        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/"}}},
            "limit": 1
        }"#;
        let ctx =
            SecurityContext::new(1, vec!["1/100/".to_string()]).expect("valid scoped context");
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile(query, &ONTOLOGY, &ctx).expect_err("must reject");
        let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);

        assert!(
            matches!(err, crate::error::QueryError::Authorization(_)),
            "expected Authorization, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "traversal_path scope rejection should be client safe: {err:?}"
        );
        assert_eq!(crate::metrics::failure_reason(&err), "authorization");
        assert!(
            after > before,
            "count_err must run on traversal_path authorization rejections (before={before}, after={after})"
        );
    }

    #[test]
    fn traversal_path_shape_rejection_is_validate_pass_error() {
        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "starts_with", "value": 1}}},
            "limit": 1
        }"#;
        let err = compile(query, &ONTOLOGY, &security_ctx()).expect_err("must reject");
        let msg = err.to_string();

        assert!(
            matches!(err, crate::error::QueryError::Validation(_)),
            "expected Validation, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "traversal_path shape rejection should be client safe: {err:?}"
        );
        assert!(
            msg.contains("schema violation"),
            "error should come from ValidatePass schema validation, got: {msg}"
        );
        assert!(
            !msg.contains("RestrictPass"),
            "client-facing shape errors should come from validation, got: {msg}"
        );
    }

    #[test]
    fn compile_with_prefixed_ontology_produces_prefixed_sql() {
        let prefixed = ONTOLOGY.clone().with_schema_version_prefix("v1_");

        let query = r#"{"query_type":"traversal","node":{"id":"g","entity":"Group","node_ids":[1],"columns":["name"]},"limit":1}"#;
        let compiled = compile(query, &prefixed, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("v1_gl_group"),
            "search SQL should use prefixed node table v1_gl_group, got: {sql}"
        );
    }

    #[test]
    fn compile_with_prefixed_ontology_prefixes_edge_table() {
        let prefixed = ONTOLOGY.clone().with_schema_version_prefix("v1_");

        let query = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"columns":["username"]},{"id":"mr","entity":"MergeRequest","columns":["title"]}],"relationships":[{"type":"AUTHORED","from":"u","to":"mr"}],"limit":1}"#;
        let compiled = compile(query, &prefixed, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // FK elision replaces the edge table scan with a direct FK join
        // (mr.author_id → u.id). Verify that node tables are prefixed and
        // the FK join condition is used instead of the edge table.
        assert!(
            sql.contains("v1_gl_merge_request") && sql.contains("v1_gl_user"),
            "traversal SQL should use prefixed node tables, got: {sql}"
        );
        assert!(
            sql.contains("mr.author_id"),
            "FK elision should join via mr.author_id, got: {sql}"
        );
    }

    /// Aggregation with a relationship and a property-less `count(target)`
    /// must resolve correctly without ClickHouse `Database does not exist`
    /// errors. The lowerer uses FK-shortcut joins for IN_PROJECT,
    /// joining MR and Project via `mr.project_id` instead of an edge scan.
    #[test]
    fn aggregation_with_relationship_emits_no_bare_node_ref() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "p", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{"function": "count", "target": "mr", "alias": "total_mrs"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // MR table is joined via FK shortcut (mr.project_id = p.id).
        assert!(
            sql.contains("mr.project_id"),
            "FK-shortcut join must reference mr.project_id, got:\n{sql}"
        );
        // Project node_ids constraint must survive.
        assert!(
            sql.contains("278964"),
            "Project node_ids filter must survive, got:\n{sql}"
        );
    }

    /// Org-wide `count(target) GROUP BY group` with no filters and no pinned
    /// IDs must emit bare `COUNT()`. A column argument like
    /// `COUNT(e0.source_id)` forces ClickHouse to read and null-check that
    /// column, which is unnecessary when counting rows.
    #[test]
    fn unfiltered_edge_only_count_emits_bare_count_for_projection_routing() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [1]},
                {"id": "f", "entity": "File"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "f", "to": "p"}],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "f",
                "alias": "files"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("COUNT()") || sql.contains("count()"),
            "unfiltered edge-only count must emit bare COUNT() for projection \
             routing, got:\n{sql}"
        );
        assert!(
            !sql.contains("COUNT(e0.source_id)") && !sql.contains("count(e0.source_id)"),
            "must not emit COUNT(source_id) -- forces unnecessary column read, \
             got:\n{sql}"
        );
    }

    /// When the target node has filters, the count must still be bounded
    /// by those filters. The lowerer uses FK-shortcut joins for
    /// IN_PROJECT, so the MR table is joined directly and the state
    /// filter appears as `mr.state = 'opened'` in the WHERE clause.
    #[test]
    fn filtered_edge_only_count_keeps_column_arg_for_count_if() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "opened"}
                }}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "alias": "open_mrs"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // FK-shortcut join means bare COUNT() bounded by WHERE clause.
        assert!(
            sql.contains("COUNT()"),
            "count must be bare COUNT() with WHERE bounding rows, got:\n{sql}"
        );
        // State filter applied on the MR dedup subquery.
        assert!(
            sql.contains("state = 'opened'"),
            "state filter must reach the SQL on the MR subquery, got:\n{sql}"
        );
    }

    /// Traversal with `id_range` (no `node_ids` or `filters`) must produce
    /// range conditions that reach the SQL. FK elision pushes range
    /// conditions onto the User node table subquery.
    #[test]
    fn traversal_id_range_produces_range_conditions_in_sql() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // FK elision replaces edge scans: range conditions are pushed to
        // the User node table subquery instead of the edge WHERE.
        assert!(
            sql.contains("u.id >= 1"),
            "range lower bound must reach the User subquery WHERE, got:\n{sql}"
        );
        assert!(
            sql.contains("u.id <= 100"),
            "range upper bound must reach the User subquery WHERE, got:\n{sql}"
        );
    }

    /// Path-finding with a filtered endpoint (no `node_ids`) must produce
    /// a `_nf_*` CTE that resolves the filter into IDs for frontier seeding.
    #[test]
    fn path_finding_filtered_endpoint_produces_anchor_cte() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "filters": {"username": {"op": "eq", "value": "root"}}},
                {"id": "end", "entity": "Project", "node_ids": [100]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_nf_start"),
            "filtered endpoint should generate _nf_start CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("username = 'root'") || sql.contains("username = {"),
            "CTE should contain username filter, got:\n{sql}"
        );
    }

    /// Path-finding with id_range on an endpoint must produce a `_nf_*` CTE
    /// with range conditions.
    #[test]
    fn path_finding_id_range_endpoint_produces_anchor_cte() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "id_range": {"start": 100, "end": 200}}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_nf_end"),
            "id_range endpoint should generate _nf_end CTE, got:\n{sql}"
        );
        assert!(
            sql.contains(">= 100"),
            "CTE should contain range lower bound, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_code_filtered_endpoints_prune_by_traversal_path() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Definition", "filters": {"name": {"op": "eq", "value": "compile"}}},
                {"id": "end", "entity": "Definition", "filters": {"name": {"op": "eq", "value": "run_query"}}}
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
        // lowerer uses `forward` CTE seeded from _nf_start.
        assert!(
            sql.contains("forward") && sql.contains("FROM _nf_start"),
            "forward CTE should seed from _nf_start, got:\n{sql}"
        );
        // Traversal-path scope applied to edge scans inside the forward CTE.
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
    fn path_finding_without_cursor_orders_only_by_depth() {
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
            !sql.contains("toString(paths._gkg_path)")
                && !sql.contains("toString(paths._gkg_edge_kinds)"),
            "path array tie-break sorting should only be emitted for cursor pagination, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_with_cursor_keeps_path_tie_break_order() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [100]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "cursor": {"offset": 0, "page_size": 10},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("toString(paths._gkg_path)")
                && sql.contains("toString(paths._gkg_edge_kinds)"),
            "cursor pagination should keep deterministic path tie-break sorting, got:\n{sql}"
        );
    }

    /// Wildcard path finding passes `*` through as the relationship_kind
    /// on all hops. The lowerer scans all edge tables (UNION ALL) to
    /// cover all relationship types.
    #[test]
    fn wildcard_path_finding_filters_only_endpoint_hops_by_relationship_kind() {
        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [100]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["*"]},
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // Wildcard path finding should scan all edge tables.
        assert!(
            sql.contains("gl_ci_edge") && sql.contains("gl_code_edge") && sql.contains("gl_edge"),
            "wildcard path finding should UNION ALL across all edge tables, got:\n{sql}"
        );
        // Endpoint entity kinds are constrained.
        assert!(
            sql.contains("e1.source_kind = 'User'"),
            "forward start must constrain source_kind = User, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.target_kind = 'Project'"),
            "backward end must constrain target_kind = Project, got:\n{sql}"
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
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing"},
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
    fn cursor_neighbors_both_orders_by_projected_columns() {
        let query = r#"{
            "query_type": "neighbors",
            "node": {"id": "mr", "entity": "MergeRequest", "node_ids": [1, 2, 3]},
            "neighbors": {"node": "mr", "direction": "both"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 20}
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("ORDER BY _gkg_mr_id ASC, _gkg_neighbor_id ASC"),
            "cursor neighbors over UNION should order by projected aliases, got:\n{sql}"
        );
        assert!(
            !sql.contains("ORDER BY e.source_id"),
            "cursor neighbors over UNION must not order by the inner edge alias, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_user_paths_do_not_join_on_traversal_path() {
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
            !sql.contains("_path_scope_traversal_paths"),
            "path finding without traversal_path endpoints should not compute traversal_path candidates, got:\n{sql}"
        );
        assert!(
            !sql.contains("f.traversal_path = b.traversal_path"),
            "User paths must not require traversal_path on frontier rows, got:\n{sql}"
        );
    }

    /// Multi-hop traversal must correctly resolve entity relationships.
    /// FK elision replaces edge table scans with direct FK column joins
    /// (e.g. `mr.project_id`, `mr.author_id`), which implicitly constrain
    /// entity kinds through the typed FK targets. Synthetic edge columns
    /// in the SELECT list carry the kind metadata for result formatting.
    #[test]
    fn multi_hop_traversal_constrains_kind_on_every_edge() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "mr", "to": "p"},
                {"type": "AUTHORED", "from": "u", "to": "mr"}
            ],
            "limit": 5
        }"#;

        let sql = compile_sql(query);

        // FK elision replaces edge scans with direct FK column joins.
        // IN_PROJECT → mr.project_id, AUTHORED → mr.author_id.
        // Kind constraints are implicit through the FK join targets.
        assert!(
            sql.contains("p.id = mr.project_id") || sql.contains("mr.project_id"),
            "IN_PROJECT must be resolved via FK join on project_id, got:\n{sql}"
        );
        assert!(
            sql.contains("u.id = mr.author_id") || sql.contains("mr.author_id"),
            "AUTHORED must be resolved via FK join on author_id, got:\n{sql}"
        );
        // Synthetic edge columns in the SELECT list carry the kind info.
        assert!(
            sql.contains("'MergeRequest' AS e0_src_type"),
            "e0 source type must be MergeRequest, got:\n{sql}"
        );
        assert!(
            sql.contains("'User' AS e1_src_type"),
            "e1 source type must be User, got:\n{sql}"
        );
    }

    /// Aggregation `count(MR) GROUP BY Project` with a User node + AUTHORED
    /// rel that the DSL forces for structural connectivity but never
    /// references in the aggregation must drop the `gl_user` table join,
    /// the `_cascade_u` CTE, and any User-aliased WHERE conjuncts. The
    /// `gl_edge` join for AUTHORED stays — it preserves the "MR has an
    /// author" semi-join semantics. See findings G1 in the dual-cliff MR.
    #[test]
    fn aggregation_prunes_unreferenced_node_table_join() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "merged"}
                }},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "mr", "to": "p"},
                {"type": "AUTHORED", "from": "u", "to": "mr"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "alias": "merged_mrs"
            }],
            "limit": 5
        }"#;

        let sql = compile_sql(query);

        // gl_user and any u-alias CTE should be gone.
        assert!(
            !sql.contains("gl_user AS u") && !sql.contains("FROM gl_user"),
            "gl_user join must be pruned for aggregation that never \
             references the User alias, got:\n{sql}"
        );
        assert!(
            !sql.contains("_nf_u"),
            "User-aliased CTEs must be dropped, got:\n{sql}"
        );
        // Direct FINAL scans do not need to project unused FK columns just to
        // feed a dedup subquery; the pruned User node should leave no user
        // table or CTE artifacts behind.
        assert!(
            !sql.contains("author_id"),
            "unused AUTHORED FK column should not be projected, got:\n{sql}"
        );
        // Project + MR work products survive.
        assert!(
            sql.contains("gl_project AS p") || sql.contains("FROM gl_project"),
            "gl_project must remain in FROM, got:\n{sql}"
        );
    }

    /// Neither `_target_mr_ids` nor `_cascade_mr` CTEs should appear.
    /// FK elision replaces edge scans with direct FK column joins
    /// (e.g. `mr.author_id`, `mr.project_id`) when FK columns exist.
    #[test]
    fn aggregation_skips_redundant_target_ids_cte_when_cascade_present() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [116]},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "IN_PROJECT", "from": "mr", "to": "p"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "alias": "user_mrs"
            }],
            "limit": 5
        }"#;

        let sql = compile_sql(query);

        // FK elision replaces edge-chain JOINs with direct FK joins.
        // AUTHORED → mr.author_id, IN_PROJECT → mr.project_id.
        assert!(
            sql.contains("mr.author_id = 116") || sql.contains("author_id = 116"),
            "User node_ids filter must be pushed to FK column, got:\n{sql}"
        );
        assert!(
            sql.contains("mr.project_id") || sql.contains("p.id = mr.project_id"),
            "IN_PROJECT FK join must use project_id, got:\n{sql}"
        );
    }

    /// Multi-hop traversal with a pinned source node must generate UNION ALL
    /// arms for each depth. The lowerer uses inline edge JOINs within
    /// each arm instead of frontier CTEs.
    #[test]
    fn multi_hop_traversal_generates_hop_frontier_ctes() {
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
                "min_hops": 1,
                "max_hops": 3
            }],
            "limit": 25
        }"#;

        let sql = compile_sql(query);

        // lowerer uses UNION ALL arms for variable-length hops.
        assert!(
            sql.contains("UNION ALL"),
            "variable-length traversal must use UNION ALL arms, got:\n{sql}"
        );
        // Pinned source node_ids must be pushed into the arms.
        assert!(
            sql.contains("e0.source_id = 1"),
            "pinned User node_ids must reach the outer WHERE, got:\n{sql}"
        );
        // Depth-2 and depth-3 arms must use edge JOINs.
        assert!(
            sql.contains("e1.target_id = e2.source_id"),
            "depth-2 arm must chain edges via JOIN, got:\n{sql}"
        );
    }

    /// Multi-hop traversal with a pinned to-side node. The lowerer
    /// uses UNION ALL arms with inline edge JOINs instead of frontier CTEs.
    /// The pinned `node_ids` filter is pushed into the outer WHERE.
    #[test]
    fn multi_hop_traversal_skips_frontiers_without_selectivity() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 1,
                "max_hops": 2
            }],
            "limit": 25
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("UNION ALL"),
            "variable-length traversal should use UNION ALL arms, got:\n{sql}"
        );
        // Pinned to-side node_ids filter must be pushed to outer WHERE.
        assert!(
            sql.contains("e0.target_id = 1"),
            "pinned to-side node_ids must reach the outer WHERE, got:\n{sql}"
        );
    }

    /// Multi-hop aggregation with a pinned root must generate a UNION ALL
    /// subquery with depth-1 and depth-2 arms for the variable-length
    /// CONTAINS relationship. The lowerer uses inline UNION ALL instead
    /// of cascade CTEs.
    #[test]
    fn multi_hop_aggregation_generates_cascade_cte() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [278964]},
                {"id": "f", "entity": "File"}
            ],
            "relationships": [{
                "type": "CONTAINS",
                "from": "p",
                "to": "f",
                "min_hops": 1,
                "max_hops": 2
            }],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{"function": "count", "target": "f"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // lowerer emits UNION ALL arms for variable-length hops.
        assert!(
            sql.contains("UNION ALL"),
            "multi-hop aggregation should use UNION ALL for variable-length hops, got:\n{sql}"
        );
        assert!(
            sql.contains("startsWith"),
            "edge scans should have traversal_path security filters, got:\n{sql}"
        );
        assert!(
            sql.contains("278964"),
            "pinned Project id must appear in the SQL, got:\n{sql}"
        );
    }

    /// Intermediate nodes (referenced by 2+ relationships) must keep
    /// connectivity even when absent from the aggregation target/group_by.
    /// Variable-length CONTAINS×{1..3} traversal: each UNION ALL arm should
    /// carry static `e1.source_kind = 'Group'` and `e<depth>.target_kind = 'Project'`
    /// literals so ClickHouse can use the kind-led PK projection
    /// (`by_rel_source_kind`/`by_rel_target_kind`) for granule pruning at every
    /// depth, instead of relying on dynamic IN-subqueries.
    #[test]
    fn variable_length_traversal_emits_per_arm_kind_literals() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [116]},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "p", "entity": "Project"},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "IN_PROJECT", "from": "mr", "to": "p"},
                {"type": "CONTAINS", "from": "g", "to": "p", "min_hops": 1, "max_hops": 3}
            ],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "n"}],
            "limit": 3
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("e1.source_kind") && sql.contains("'Group'"),
            "depth-1 arm must inject e1.source_kind = 'Group', got:\n{sql}"
        );
        assert!(
            sql.contains("e3.target_kind") && sql.contains("'Project'"),
            "depth-3 arm must inject e3.target_kind = 'Project', got:\n{sql}"
        );
        // With both endpoints kind-literal-pinned, the per-arm to-side IN
        // subquery is redundant with the outer node-table SIP and must be
        // suppressed. The redundant probe was costing 30%+ wall time.
        assert!(
            !sql.contains("e2.target_id IN") && !sql.contains("e3.target_id IN"),
            "arm-internal target_id IN subquery should be suppressed, got:\n{sql}"
        );
    }

    // ── Denormalization pass tests ──────────────────────────────────────

    /// Compile a User→MR REVIEWER traversal with the given MR filter JSON fragment.
    fn denorm_traversal_sql(mr_filter: &str) -> String {
        let query = format!(
            r#"{{
            "query_type": "traversal",
            "nodes": [
                {{"id": "u", "entity": "User", "node_ids": [1]}},
                {{"id": "mr", "entity": "MergeRequest", "filters": {{ {mr_filter} }}}}
            ],
            "relationships": [{{"type": "REVIEWER", "from": "u", "to": "mr"}}],
            "limit": 10
        }}"#
        );
        compile_sql(&query)
    }

    #[test]
    fn denorm_eq_filter_pushes_to_edge_tags() {
        let sql = denorm_traversal_sql(r#""state": {"op": "eq", "value": "merged"}"#);
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        assert!(
            !sql.contains("_nf_mr"),
            "no _nf_mr CTE when filter is fully denormalized, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_in_list_filter_uses_has_any() {
        let sql = denorm_traversal_sql(r#""state": {"op": "in", "value": ["merged", "opened"]}"#);
        assert!(
            sql.contains("hasAny(e0.target_tags"),
            "IN-list denorm filter must use hasAny, got:\n{sql}"
        );
        assert!(
            sql.contains("state:merged") && sql.contains("state:opened"),
            "both filter values must appear in tag predicate, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_in_list_single_value_uses_has() {
        let sql = denorm_traversal_sql(r#""state": {"op": "in", "value": ["merged"]}"#);
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "single-value IN-list must use has, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_partial_filters_keeps_filter_cte() {
        let sql = denorm_traversal_sql(
            r#""state": {"op": "eq", "value": "merged"}, "source_branch": {"op": "eq", "value": "main"}"#,
        );
        assert!(
            sql.contains("_filter_mr"),
            "partial denorm must keep a filter CTE for non-denormalized filters, got:\n{sql}"
        );
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denormalized state filter must be pushed to edge tags, got:\n{sql}"
        );
        assert!(
            sql.contains("main"),
            "filter CTE must retain non-denormalized source_branch filter, got:\n{sql}"
        );
    }

    /// When node_ids are present alongside filters, the lowerer applies
    /// both the node_ids filter (e0.target_id IN [...]) and the denorm tag
    /// filter (has on target_tags) to the edge. Both filters narrow the scan.
    #[test]
    fn denorm_skips_rewrite_when_node_ids_present() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "node_ids": [1, 2, 3],
                 "filters": {"state": {"op": "eq", "value": "merged"}}}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // Node_ids filter is pushed to the edge.
        assert!(
            sql.contains("e0.target_id IN [1, 2, 3]"),
            "node_ids must be pushed to edge target_id filter, got:\n{sql}"
        );
        // Denorm tag is also applied on the edge for additional selectivity.
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
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
                    "state": {"op": "eq", "value": "merged"}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "alias": "n"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        // lowerer pushes the denorm filter to edge tags directly.
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        // No _nf_mr CTE needed — edge tag handles the filter.
        assert!(
            !sql.contains("_nf_mr"),
            "lowerer should not emit _nf_mr when filter is fully denormalized, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_preserves_role_gated_node_table_for_security() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "v", "entity": "Vulnerability", "filters": {
                    "state": {"op": "eq", "value": "detected"}
                }},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "proj"}],
            "group_by": [{"kind": "node", "node": "proj"}],
            "aggregations": [{
                "function": "count",
                "target": "v",
                "alias": "n"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

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
            "group_by": [{"kind": "property", "node": "p", "property": "visibility_level"}],
            "aggregations": [{
                "function": "count",
                "target": "p",
                "alias": "project_count"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("p.visibility_level AS visibility_level"),
            "property group key must be selected as a scalar column, got:\n{sql}"
        );
        assert!(
            sql.contains("GROUP BY p.visibility_level"),
            "property group key must drive GROUP BY, got:\n{sql}"
        );
        assert!(
            !sql.contains("_gkg_p_id"),
            "property grouping should not emit node-group columns, got:\n{sql}"
        );
    }

    #[test]
    fn aggregation_group_by_property_keeps_role_gated_target_table() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "v", "entity": "Vulnerability"},
                {"id": "p", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
            "group_by": [{"kind": "property", "node": "v", "property": "severity"}],
            "aggregations": [{
                "function": "count",
                "target": "v",
                "alias": "vulnerability_count"
            }],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("gl_vulnerability"),
            "role-gated grouped entity must remain in FROM for SecurityPass, got:\n{sql}"
        );
        assert!(
            sql.contains("v.severity AS severity") && sql.contains("GROUP BY v.severity"),
            "security property grouping must use the protected node alias, got:\n{sql}"
        );
    }

    #[test]
    fn node_table_reads_use_final_for_latest_rows() {
        let queries = [
            (
                "traversal",
                r#"{
                    "query_type": "traversal",
                    "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
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
                    "group_by": [{"kind": "node", "node": "p"}],
                    "aggregations": [{"function": "count", "target": "mr", "alias": "merged_mrs"}],
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
                    "node": {"id": "mr", "entity": "MergeRequest", "filters": {"title": {"op": "contains", "value": "fix"}}},
                    "neighbors": {"node": "mr", "direction": "both"},
                    "limit": 10
                }"#,
            ),
        ];

        for (name, query) in queries {
            let compiled = compile(query, &ONTOLOGY, &security_ctx())
                .unwrap_or_else(|err| panic!("{name} should compile: {err}"));
            let sql = compiled.base.render();
            assert!(
                sql.contains(" FINAL"),
                "{name} should use FINAL for node-table reads, got:\n{sql}"
            );
            assert!(
                !sql.contains("LIMIT 1 BY"),
                "{name} should not use manual LIMIT BY dedup, got:\n{sql}"
            );
        }
    }

    #[test]
    fn fk_star_joined_nodes_use_candidate_ctes() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "pipe", "entity": "Pipeline", "filters": {"status": "failed", "source": "push"}},
                {"id": "j", "entity": "Job", "filters": {"status": "failed"}},
                {"id": "p", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [
                {"type": "HAS_JOB", "from": "pipe", "to": "j"},
                {"type": "IN_PROJECT", "from": "pipe", "to": "p"}
            ],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains(
                "_candidate_pipe AS (SELECT DISTINCT pipe.id AS id FROM gl_pipeline AS pipe WHERE"
            ),
            "joined target should get a non-FINAL candidate CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("_candidate_j AS (SELECT DISTINCT j.id AS id FROM gl_job AS j WHERE"),
            "center should get a non-FINAL candidate CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("FROM (SELECT * FROM gl_job AS j FINAL WHERE")
                && sql.contains("AS j INNER JOIN (SELECT * FROM gl_pipeline AS pipe FINAL WHERE"),
            "outer latest-row reads should still use FINAL with joined node filtering pushed down, got:\n{sql}"
        );
        assert!(
            sql.contains("j.pipeline_id IN (SELECT id FROM _candidate_pipe)")
                && sql.contains("pipe.id IN (SELECT id FROM _candidate_pipe)"),
            "candidate CTE should narrow both center FK values and target ids, got:\n{sql}"
        );
    }

    #[test]
    fn fk_star_filter_only_relationships_do_not_emit_candidate_ctes() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "j", "entity": "Job", "filters": {"status": "failed"}},
                {"id": "p", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "p"}],
            "group_by": [{"kind": "node", "node": "j"}],
            "aggregations": [{"function": "count", "target": "j", "alias": "fail_count"}],
            "limit": 20
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("_candidate_"),
            "filter-only FK predicates should keep the direct FINAL scan, got:\n{sql}"
        );
        assert!(
            sql.contains("FROM gl_job AS j FINAL"),
            "latest-row read should still use FINAL, got:\n{sql}"
        );
    }

    #[test]
    fn fk_star_unfiltered_join_narrow_uses_candidate_scan() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "j1", "entity": "Job", "filters": {"status": "canceled"}},
                {"id": "j2", "entity": "Job"},
                {"id": "p", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [
                {"type": "AUTO_CANCELED_BY", "from": "j1", "to": "j2"},
                {"type": "IN_PROJECT", "from": "j1", "to": "p"}
            ],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains(
                "_narrow_j2 AS (SELECT DISTINCT j1.auto_canceled_by_id AS id FROM gl_job AS j1 WHERE"
            ),
            "unfiltered joined target should be narrowed by a non-FINAL candidate scan, got:\n{sql}"
        );
        assert!(
            !sql.contains(
                "_narrow_j2 AS (SELECT DISTINCT j1.auto_canceled_by_id AS id FROM gl_job AS j1 FINAL"
            ),
            "narrowing CTE should not run a second FINAL scan, got:\n{sql}"
        );
        assert!(
            !sql.contains("_candidate_j1"),
            "center candidate CTE should not be emitted when it only repeats center filters, got:\n{sql}"
        );
        assert!(
            !sql.contains("j1.id IN (SELECT id FROM _candidate_j1)"),
            "center scan should not use a same-table candidate set without target-derived predicates, got:\n{sql}"
        );
        assert!(
            sql.contains("FROM (SELECT * FROM gl_job AS j1 FINAL WHERE")
                && sql.contains("AS j1 INNER JOIN (SELECT * FROM gl_job AS j2 FINAL WHERE"),
            "outer source and joined target should still use FINAL with joined node filtering pushed down, got:\n{sql}"
        );
    }

    #[test]
    fn flat_chain_edge_narrowing_deduplicates_frontier() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "n", "entity": "Note"},
                {"id": "p", "entity": "Project"},
                {"id": "g", "entity": "Group", "filters": {"full_path": "gitlab-org"}}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "n", "to": "p"},
                {"type": "CONTAINS", "from": "g", "to": "p"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{"function": "count", "target": "n", "alias": "note_count"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains(
                "_narrow_p AS (SELECT DISTINCT e0n.target_id AS id FROM gl_edge AS e0n WHERE"
            ),
            "edge-derived narrowing frontiers should deduplicate high fan-out IDs, got:\n{sql}"
        );
        assert!(
            sql.contains("p.id IN (SELECT id FROM _narrow_p)"),
            "joined node FINAL scan should use the edge-derived frontier, got:\n{sql}"
        );
    }

    #[test]
    fn filtered_redaction_joins_push_filters_into_final_subquery() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "f", "entity": "File", "filters": {"path": {"op": "ends_with", "value": ".rb"}}},
                {"id": "d", "entity": "Definition", "filters": {"name": {"op": "starts_with", "value": "process"}}}
            ],
            "relationships": [{"type": "DEFINES", "from": "f", "to": "d"}],
            "limit": 20
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("INNER JOIN (SELECT * FROM gl_file AS f FINAL WHERE")
                && sql.contains("endsWith(f.path, '.rb')"),
            "filtered File redaction join should push filters into the FINAL subquery, got:\n{sql}"
        );
        assert!(
            sql.contains("INNER JOIN (SELECT * FROM gl_definition AS d FINAL WHERE")
                && sql.contains("startsWith(d.name, 'process')"),
            "filtered Definition redaction join should push filters into the FINAL subquery, got:\n{sql}"
        );
    }

    #[test]
    fn hydration_uses_final_for_latest_rows() {
        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "mr".into(),
                entity: Some("MergeRequest".into()),
                table: Some("gl_merge_request".into()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "state".into()])),
                node_ids: vec![1],
                has_traversal_path: true,
                traversal_paths: vec!["1/".into()],
                ..Default::default()
            }],
            limit: 10,
            ..Default::default()
        };

        let ont = Arc::new(ONTOLOGY.clone());
        let compiled =
            compile_input(input, &ont, &security_ctx()).expect("hydration input should compile");
        let sql = compiled.base.render();
        assert!(
            sql.contains(" FINAL"),
            "hydration should use FINAL for node-table reads, got:\n{sql}"
        );
        assert!(
            !sql.contains("LIMIT 1 BY"),
            "hydration should not use manual LIMIT BY dedup, got:\n{sql}"
        );
    }

    /// Single-reference CTEs must NOT be materialized — inlining lets
    /// ClickHouse push predicates through and is the default behavior.
    #[test]
    fn single_ref_cte_is_not_materialized() {
        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
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

    /// Aggregation query where the target node has a denormalized filter
    /// (state=merged). The lowerer uses edge-chain JOINs with the
    /// denorm filter pushed to `has(e0.target_tags, 'state:merged')`.
    /// No cascade or _nf_mr CTEs are needed.
    #[test]
    fn agg_denorm_eliminates_redundant_target_and_nf_ctes() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [116]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "merged"}
                }},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "IN_PROJECT", "from": "mr", "to": "p"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "alias": "user_mrs"
            }],
            "limit": 5
        }"#;

        let sql = compile_sql(query);

        // No _nf_mr CTE — denorm covers the filter.
        assert!(
            !sql.contains("_nf_mr"),
            "_nf_mr must not be emitted when state is fully denormalized, got:\n{sql}"
        );

        // FK elision replaces edge scans, so the state filter is applied
        // directly on the MR node table instead of as an edge tag.
        assert!(
            sql.contains("mr.state = 'merged'") || sql.contains("state = 'merged'"),
            "state filter must be applied on MR node table, got:\n{sql}"
        );
    }

    #[test]
    fn multi_filter_range_compiles_both_predicates() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest",
                     "node_ids": [1],
                     "filters": {
                         "created_at": [
                             {"op": "gte", "value": "2026-04-01T00:00:00Z"},
                             {"op": "lt", "value": "2026-05-01T00:00:00Z"}
                         ]
                     },
                     "columns": ["id", "created_at"]},
            "limit": 10
        }"#;
        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains(">=") && sql.contains("<"),
            "both range predicates must appear in SQL, got:\n{sql}"
        );
    }

    #[test]
    fn filter_on_virtual_column_compiles_without_sql_predicate() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let compiled = compile(
            r#"{
                "query_type": "traversal",
                "node": {"id": "f", "entity": "File",
                         "node_ids": [1],
                         "filters": {"content": {"op": "eq", "value": "x"}},
                         "columns": ["path", "content"]},
                "limit": 5
            }"#,
            &ontology,
            &security_ctx(),
        )
        .expect("virtual column filter should compile");

        let sql = compiled.base.render();
        assert!(
            !sql.contains("content"),
            "virtual column 'content' must not appear in SQL, got:\n{sql}"
        );

        // Virtual filter should be carried on the hydration plan.
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
    fn filter_on_virtual_column_rejects_unsupported_op() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let err = compile(
            r#"{
                "query_type": "traversal",
                "node": {"id": "f", "entity": "File",
                         "node_ids": [1],
                         "filters": {"content": {"op": "gt", "value": "x"}}},
                "limit": 5
            }"#,
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
}
