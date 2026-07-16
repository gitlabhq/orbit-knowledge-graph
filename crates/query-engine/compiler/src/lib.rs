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
//!     "nodes": [{"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]}],
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
pub(crate) mod schema_limits;
mod schema_templates;
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
pub use error::{QueryError, Result};
pub use input::{
    ColumnSelection, DynamicColumnMode, EntityAuthConfig, FilterOp, Input, InputFilter, InputNode,
    QueryType, parse_input,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::{Ontology, OntologyError};

pub use analytics::ExecMetrics;
pub use passes::codegen::{
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect,
    clickhouse::emit_simple_query,
    codegen,
    ddl::clickhouse::emit_create_materialized_view,
    ddl::clickhouse::emit_create_refreshable_materialized_view,
    ddl::clickhouse::{DictionarySource, emit_create_dictionary, emit_create_table},
    ddl::duckdb::emit_create_table as emit_duckdb_create_table,
    ddl::duckdb::generate_local_ddl,
    ddl::generate_graph_dictionaries,
    ddl::generate_graph_dictionaries_with_prefix,
    ddl::generate_graph_materialized_views,
    ddl::generate_graph_materialized_views_with_prefix,
    ddl::generate_graph_tables,
    ddl::generate_graph_tables_with_prefix,
    ddl::generate_local_tables,
    ddl::generate_refreshable_materialized_views,
    ddl::generate_unversioned_graph_tables,
    ddl::{auxiliary_schema_fingerprints, ddl_fingerprints},
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    generate_hydration_plan,
};
pub use passes::normalize::{build_entity_auth, normalize};
pub use scope::{PathResolutionKey, PathScopeId, scope_edges, scope_keys};
pub use types::{
    AccessLevel, DEFAULT_PATH_ACCESS_LEVEL, Realm, SecurityContext, TraversalPath,
    is_valid_traversal_path,
};

use metrics::CountErr;
use std::sync::Arc;

use config::CompilerCtx as _;

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

/// Run only `validate` + `normalize`, returning the normalized [`Input`].
///
/// Lets the querying pipeline's path-resolution stage read normalized scope
/// keys before the full pipeline runs, then resolve and attach the tight
/// traversal_path prefix as [`SecurityContext`] scope metadata.
pub fn validate_normalize(json_input: &str, ontology: &Ontology) -> Result<Input> {
    let mut ctx = config::ValidateNormalizeCtx::new(Arc::new(ontology.clone()));
    ctx.set_json(json_input.to_string());
    config::run_validate_normalize(&mut ctx)
        .and_then(|()| {
            ctx.take_input().ok_or_else(|| {
                error::QueryError::PipelineInvariant("validate_normalize produced no input".into())
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

    fn compile_sql(query: &str) -> String {
        compile(query, &ONTOLOGY, &security_ctx())
            .expect("should compile")
            .base
            .render()
    }

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
        let query = r#"{"query_type":"traversal","nodes":[{"id":"x","entity":"NotARealEntity","columns":["id"]}],"limit":1}"#;
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
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/"}}}],
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
            "nodes": [{"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "starts_with", "value": 1}}}],
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

        let query = r#"{"query_type":"traversal","nodes":[{"id":"g","entity":"Group","node_ids":[1],"columns":["name"]}],"limit":1}"#;
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

        assert!(
            sql.contains("v1_gl_merge_request") && sql.contains("v1_gl_user"),
            "traversal SQL should use prefixed node tables, got: {sql}"
        );
        assert!(
            sql.contains("mr.author_id"),
            "FK elision should join via mr.author_id, got: {sql}"
        );
    }

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

        assert!(
            sql.contains("mr.project_id"),
            "FK-shortcut join must reference mr.project_id, got:\n{sql}"
        );
        assert!(
            sql.contains("278964"),
            "Project node_ids filter must survive, got:\n{sql}"
        );
    }

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
            sql.contains("countIf("),
            "single-hop edge count should use countIf on the LIMIT BY path, \
             got:\n{sql}"
        );
        assert!(
            sql.contains("LIMIT 1 BY"),
            "single-hop edge aggregation should use LIMIT BY dedup, got:\n{sql}"
        );
        assert!(
            !sql.contains("COUNT(e0.source_id)") && !sql.contains("count(e0.source_id)"),
            "must not emit COUNT(source_id) -- forces unnecessary column read, \
             got:\n{sql}"
        );
    }

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

        assert!(
            sql.contains("COUNT()"),
            "count must be bare COUNT() with WHERE bounding rows, got:\n{sql}"
        );
        assert!(
            sql.contains("state = 'opened'"),
            "state filter must reach the SQL on the MR subquery, got:\n{sql}"
        );
    }

    #[test]
    fn dedup_edge_scan_pushes_filter_cte_in_subquery_into_inner_where() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [490855697]},
                {"id": "label", "entity": "Label", "filters": {"title": "group::source code"}},
                {"id": "project", "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "gitlab-org/gitlab"}}}
            ],
            "relationships": [
                {"type": "HAS_LABEL", "from": "mr", "to": "label"},
                {"type": "IN_PROJECT", "from": "mr", "to": "project"}
            ],
            "aggregations": [{"function": "count", "target": "mr", "alias": "n"}],
            "limit": 1
        }"#;

        let sql = compile_sql(query);
        let (e0_inner, rest) = sql
            .split_once(") AS e0 INNER JOIN")
            .expect("e0 dedup subquery is closed before the join");
        let (e1_inner, post_join) = rest
            .split_once(") AS e1 ON")
            .expect("e1 dedup subquery is closed before ON");
        let outer = post_join
            .split_once(" WHERE ")
            .map(|(_, tail)| tail)
            .unwrap_or("");

        for clause in [
            "e0.source_id = 490855697",
            "e0.target_id IN (SELECT id FROM _filter_label)",
        ] {
            assert!(
                e0_inner.contains(clause),
                "expected `{clause}` inside e0 dedup inner WHERE, got:\n{e0_inner}"
            );
        }
        for clause in [
            "e1.source_id = 490855697",
            "e1.target_id IN (SELECT id FROM _filter_project)",
        ] {
            assert!(
                e1_inner.contains(clause),
                "expected `{clause}` inside e1 dedup inner WHERE, got:\n{e1_inner}"
            );
        }
        for kind in [
            "e0.relationship_kind = 'HAS_LABEL'",
            "e0.source_kind = 'MergeRequest'",
            "e0.target_kind = 'Label'",
            "e1.relationship_kind = 'IN_PROJECT'",
            "e1.target_kind = 'Project'",
        ] {
            assert!(
                outer.contains(kind),
                "kind predicate `{kind}` must stay in outer WHERE so CH's PredicateRewriteVisitor handles it, got:\n{outer}"
            );
            assert!(
                !e0_inner.contains(kind) && !e1_inner.contains(kind),
                "kind predicate `{kind}` must not be duplicated into dedup inner WHERE"
            );
        }
    }

    /// Regression for #801: self-joining the edge table without deduping each
    /// scan multiplies un-merged ReplacingMergeTree versions and inflates
    /// `count(mr)` (observed 7, 49, 245 for an MR that should return 1).
    #[test]
    fn multi_edge_self_join_dedups_edge_versions() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [490855697]},
                {"id": "label", "entity": "Label", "filters": {"title": "group::source code"}},
                {"id": "project", "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "gitlab-org/gitlab"}}}
            ],
            "relationships": [
                {"type": "HAS_LABEL", "from": "mr", "to": "label"},
                {"type": "IN_PROJECT", "from": "mr", "to": "project"}
            ],
            "aggregations": [{"function": "count", "target": "mr", "alias": "n"}],
            "limit": 1
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("FROM gl_edge AS e0 FINAL") && sql.contains("FROM gl_edge AS e1 FINAL"),
            "multi-edge self-join must dedup each edge scan via FINAL, got:\n{sql}"
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
            "aggregations": [{"function": "count", "target": "mr", "alias": "n"}],
            "limit": 1
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("argMax"),
            "single-hop edge scan must not dedup, got:\n{sql}"
        );
    }

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

        assert!(
            sql.contains("u.id >= 1"),
            "range lower bound must reach the User subquery WHERE, got:\n{sql}"
        );
        assert!(
            sql.contains("u.id <= 100"),
            "range upper bound must reach the User subquery WHERE, got:\n{sql}"
        );
    }

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
            "cursor": {"page_size": 10}
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("toString(paths._gkg_path)")
                && sql.contains("toString(paths._gkg_edge_kinds)"),
            "cursor pagination should keep deterministic path tie-break sorting, got:\n{sql}"
        );
    }

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

        assert!(
            sql.contains("gl_ci_edge") && sql.contains("gl_code_edge") && sql.contains("gl_edge"),
            "wildcard path finding should UNION ALL across all edge tables, got:\n{sql}"
        );
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
    fn cursor_neighbors_both_orders_by_projected_columns() {
        let query = r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [1, 2, 3]}],
            "neighbors": {"direction": "both"},
            "cursor": {"page_size": 20}
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

    #[test]
    fn path_finding_clamps_settings_to_safety_floor() {
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
            "pathfinding must clamp max_execution_time to 15, got:\n{sql}"
        );
        assert!(
            sql.contains("max_memory_usage = 16106127360"),
            "pathfinding must clamp max_memory_usage to 15 GiB, got:\n{sql}"
        );
    }

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

        assert!(
            sql.contains("p.id = mr.project_id") || sql.contains("mr.project_id"),
            "IN_PROJECT must be resolved via FK join on project_id, got:\n{sql}"
        );
        assert!(
            sql.contains("u.id = mr.author_id") || sql.contains("mr.author_id"),
            "AUTHORED must be resolved via FK join on author_id, got:\n{sql}"
        );
        assert!(
            sql.contains("'MergeRequest' AS e0_src_type"),
            "e0 source type must be MergeRequest, got:\n{sql}"
        );
        assert!(
            sql.contains("'User' AS e1_src_type"),
            "e1 source type must be User, got:\n{sql}"
        );
    }

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

        assert!(
            !sql.contains("gl_user AS u") && !sql.contains("FROM gl_user"),
            "gl_user join must be pruned for aggregation that never \
             references the User alias, got:\n{sql}"
        );
        assert!(
            !sql.contains("_nf_u"),
            "User-aliased CTEs must be dropped, got:\n{sql}"
        );
        assert!(
            !sql.contains("author_id"),
            "unused AUTHORED FK column should not be projected, got:\n{sql}"
        );
        assert!(
            sql.contains("gl_project AS p") || sql.contains("FROM gl_project"),
            "gl_project must remain in FROM, got:\n{sql}"
        );
    }

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

        assert!(
            sql.contains("mr.author_id = 116") || sql.contains("author_id = 116"),
            "User node_ids filter must be pushed to FK column, got:\n{sql}"
        );
        assert!(
            sql.contains("mr.project_id") || sql.contains("p.id = mr.project_id"),
            "IN_PROJECT FK join must use project_id, got:\n{sql}"
        );
    }

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

        assert!(
            sql.contains("UNION ALL"),
            "variable-length traversal must use UNION ALL arms, got:\n{sql}"
        );
        assert!(
            sql.contains("e0.source_id = 1"),
            "pinned User node_ids must reach the outer WHERE, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.target_id = e2.source_id"),
            "depth-2 arm must chain edges via JOIN, got:\n{sql}"
        );
    }

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
        assert!(
            sql.contains("e0.target_id = 1"),
            "pinned to-side node_ids must reach the outer WHERE, got:\n{sql}"
        );
    }

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

    /// Each UNION ALL arm must carry static kind literals so ClickHouse can use
    /// the kind-led PK projection (`by_rel_source_kind`/`by_rel_target_kind`) for
    /// granule pruning at every depth, instead of dynamic IN-subqueries.
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
    fn denorm_partial_filters_joins_for_non_denorm() {
        let sql = denorm_traversal_sql(
            r#""state": {"op": "eq", "value": "merged"}, "source_branch": {"op": "eq", "value": "main"}"#,
        );
        assert!(
            sql.contains("INNER JOIN"),
            "partial denorm must JOIN node table for non-denormalized filters, got:\n{sql}"
        );
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denormalized state filter must be pushed to edge tags, got:\n{sql}"
        );
        assert!(
            sql.contains("source_branch") && sql.contains("main"),
            "JOIN must retain non-denormalized source_branch filter, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_boolean_filter_renders_value_token() {
        let sql = denorm_traversal_sql(r#""draft": {"op": "eq", "value": true}"#);
        assert!(
            sql.contains("has(e0.target_tags, 'draft:true')"),
            "boolean denorm filter must render its value token, got:\n{sql}"
        );
        assert!(
            !sql.contains("'draft:'"),
            "boolean denorm filter must not emit an empty-value token, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_pushed_onto_carrying_edge_not_first_hop() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "opened"}}},
                {"id": "cl", "entity": "Label", "filters": {"title": {"op": "eq", "value": "Community contribution"}}},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "HAS_LABEL", "from": "mr", "to": "cl"},
                {"type": "IN_PROJECT", "from": "mr", "to": "p"}
            ],
            "aggregations": [{"function": "count", "target": "mr", "alias": "c"}]
        }"#;
        let sql = compile_sql(query);
        assert!(
            sql.contains("has(e1.source_tags, 'state:opened')"),
            "state must be pushed onto IN_PROJECT (e1), which carries it, got:\n{sql}"
        );
        assert!(
            !sql.contains("has(e0.source_tags, 'state:opened')"),
            "state must NOT be pushed onto HAS_LABEL (e0), whose tags are empty, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_uncovered_by_query_relationships_filters_node_table() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "opened"}}},
                {"id": "cl", "entity": "Label", "filters": {"title": {"op": "eq", "value": "Community contribution"}}}
            ],
            "relationships": [
                {"type": "HAS_LABEL", "from": "mr", "to": "cl"}
            ],
            "aggregations": [{"function": "count", "target": "mr", "alias": "c"}]
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

        assert!(
            sql.contains("e0.target_id IN [1, 2, 3]"),
            "node_ids must be pushed to edge target_id filter, got:\n{sql}"
        );
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

        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
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
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "u",
                "property": "id",
                "alias": "closers_count"
            }],
            "limit": 20
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("u.id") && !sql.contains("(u.id"),
            "must not reference u.id when User is not hydrated and absent from FROM, got:\n{sql}"
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
                    "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"title": {"op": "contains", "value": "fix"}}}],
                    "neighbors": {"direction": "both"},
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
            sql.contains("_candidate_pipe AS (SELECT pipe.id AS id FROM gl_pipeline AS pipe WHERE"),
            "joined target should get a candidate CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("_candidate_j AS (SELECT j.id AS id FROM gl_job AS j WHERE"),
            "center should get a candidate CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("FROM (SELECT * FROM gl_job AS j")
                && sql.contains("AS j INNER JOIN (SELECT * FROM gl_pipeline AS pipe"),
            "outer latest-row reads should use dedup (FINAL or LIMIT BY), got:\n{sql}"
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
                {"id": "p1", "entity": "Pipeline", "filters": {"status": "canceled"}},
                {"id": "p2", "entity": "Pipeline"},
                {"id": "proj", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [
                {"type": "AUTO_CANCELED_BY", "from": "p1", "to": "p2"},
                {"type": "IN_PROJECT", "from": "p1", "to": "proj"}
            ],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains(
                "_narrow_p2 AS (SELECT p1.auto_canceled_by_id AS id FROM gl_pipeline AS p1 WHERE"
            ),
            "unfiltered joined target should be narrowed by a candidate scan, got:\n{sql}"
        );
        assert!(
            !sql.contains(
                "_narrow_p2 AS (SELECT p1.auto_canceled_by_id AS id FROM gl_pipeline AS p1 FINAL"
            ),
            "narrowing CTE should not run a second FINAL scan, got:\n{sql}"
        );
        assert!(
            !sql.contains("_candidate_p1"),
            "center candidate CTE should not be emitted when it only repeats center filters, got:\n{sql}"
        );
        assert!(
            !sql.contains("p1.id IN (SELECT id FROM _candidate_p1)"),
            "center scan should not use a same-table candidate set without target-derived predicates, got:\n{sql}"
        );
        assert!(
            sql.contains("FROM (SELECT * FROM gl_pipeline AS p1")
                && sql.contains("AS p1 INNER JOIN (SELECT * FROM gl_pipeline AS p2"),
            "outer source and joined target should use dedup (FINAL or LIMIT BY), got:\n{sql}"
        );
    }

    #[test]
    fn fk_center_group_by_aggregation_drops_redundant_narrow_scan() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "j", "entity": "Job", "filters": {"status": "failed"}},
                {"id": "proj", "entity": "Project"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "proj"}],
            "group_by": [{"kind": "node", "node": "proj"}],
            "aggregations": [{"function": "count", "target": "j", "alias": "failed_jobs"}],
            "limit": 200
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("_narrow_proj"),
            "FK-center group-by aggregation must not re-scan the center for narrowing, got:\n{sql}"
        );
        assert_eq!(
            sql.matches("FROM gl_job").count(),
            1,
            "gl_job must be scanned exactly once, got:\n{sql}"
        );
        assert!(
            sql.contains("proj.id = j.project_id"),
            "Project hydration must still join on the center FK, got:\n{sql}"
        );
    }

    #[test]
    fn fk_center_traversal_keeps_narrow_scan() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "p1", "entity": "Pipeline", "filters": {"status": "canceled"}},
                {"id": "p2", "entity": "Pipeline"},
                {"id": "proj", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [
                {"type": "AUTO_CANCELED_BY", "from": "p1", "to": "p2"},
                {"type": "IN_PROJECT", "from": "p1", "to": "proj"}
            ],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_narrow_p2"),
            "traversal FK-center join must keep its narrowing CTE, got:\n{sql}"
        );
    }

    #[test]
    fn fk_chain_aggregation_joins_nodes_without_edge_scans() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed", "source": "merge_request_event"}},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "d", "entity": "MergeRequestDiff"},
                {"id": "f", "entity": "MergeRequestDiffFile"}
            ],
            "relationships": [
                {"type": "TRIGGERED", "from": "mr", "to": "pl"},
                {"type": "HAS_LATEST_DIFF", "from": "mr", "to": "d"},
                {"type": "HAS_FILE", "from": "d", "to": "f"}
            ],
            "aggregations": [{"function": "count", "target": "f", "alias": "appearances"}],
            "group_by": [{"kind": "property", "node": "f", "property": "old_path", "alias": "file_path"}],
            "limit": 60
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("gl_edge") && !sql.contains("gl_ci_edge"),
            "FK-chain aggregation must join node tables, not scan edge tables, got:\n{sql}"
        );
        for on in [
            "pl.merge_request_id = mr.id",
            "mr.latest_merge_request_diff_id = d.id",
            "f.merge_request_diff_id = d.id",
        ] {
            assert!(sql.contains(on), "expected FK join `{on}`, got:\n{sql}");
        }
        assert!(sql.contains("GROUP BY f.old_path"), "got:\n{sql}");
    }

    fn compile_sql_scoped(nodes: &str, rels: &str, group: &str, agg: &str) -> String {
        let query = format!(
            r#"{{"query_type":"aggregation","nodes":[{nodes}],"relationships":[{rels}],"group_by":[{{"kind":"node","node":"{group}"}}],"aggregations":[{{"function":"count","target":"{agg}","alias":"c"}}],"limit":20}}"#
        );
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_scope_prefixes([("g".to_string(), "1/9970/".to_string())].into());
        compile(&query, &ONTOLOGY, &ctx).unwrap().base.render()
    }

    #[test]
    fn scoped_query_pushes_down_partition_predicate() {
        let query = r#"{"query_type":"traversal","nodes":[{"id":"m","entity":"MergeRequest","columns":["id"],"filters":{"state":{"op":"eq","value":"opened"}}}],"limit":20}"#;
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_scope_prefixes([("m".to_string(), "1/9970/".to_string())].into());
        let sql = compile(query, &ONTOLOGY, &ctx).unwrap().base.render();
        assert!(
            sql.contains("startsWith(m.traversal_path"),
            "scoped query should always carry the startsWith prefix:\n{sql}"
        );
        let has_partition_pred = sql.contains(
            "m._partition_id = toString(modulo(sipHash64(toUInt64OrZero(arrayElement(splitByChar(",
        );
        assert_eq!(
            has_partition_pred,
            ONTOLOGY.partition().is_some(),
            "the _partition_id predicate should track whether the ontology partitions:\n{sql}"
        );
    }

    // SQL-shape smoke: the 4-hop diff chain elides CONTAINS to FK node-joins
    // (guards the orientation-agnostic emit_chain), and a non-FK survivor blocks
    // elision. `expect` is `|`-separated; a `!x` token asserts `x` is absent.
    // End-to-end result parity (chain/star, scoped vs unscoped) lives in the
    // data-correctness suite (`scope_implied_container_elision_*`).
    #[test]
    fn scope_implied_container_hop_elision() {
        let cases: &[(&str, &str, &str, &str, &str)] = &[
            (
                r#"{"id":"g","entity":"Group","filters":{"full_path":"gitlab-org"}},{"id":"p","entity":"Project"},{"id":"mr","entity":"MergeRequest"},{"id":"d","entity":"MergeRequestDiff"},{"id":"f","entity":"MergeRequestDiffFile"}"#,
                r#"{"type":"CONTAINS","from":"g","to":"p"},{"type":"IN_PROJECT","from":"mr","to":"p"},{"type":"HAS_LATEST_DIFF","from":"mr","to":"d"},{"type":"HAS_FILE","from":"d","to":"f"}"#,
                "p",
                "f",
                "mr.project_id = p.id|mr.latest_merge_request_diff_id = d.id|f.merge_request_diff_id = d.id|gl_project|!gl_edge|!gl_ci_edge|!gl_group",
            ),
            (
                r#"{"id":"g","entity":"Group","filters":{"full_path":"gitlab-org"}},{"id":"p","entity":"Project"},{"id":"mr","entity":"MergeRequest"},{"id":"n","entity":"Note"}"#,
                r#"{"type":"CONTAINS","from":"g","to":"p"},{"type":"IN_PROJECT","from":"mr","to":"p"},{"type":"HAS_NOTE","from":"mr","to":"n"}"#,
                "p",
                "n",
                "'CONTAINS'|'HAS_NOTE'",
            ),
        ];
        for (nodes, rels, group, agg, expect) in cases {
            let sql = compile_sql_scoped(nodes, rels, group, agg);
            for e in expect.split('|') {
                match e.strip_prefix('!') {
                    Some(absent) => assert!(!sql.contains(absent), "{absent} present:\n{sql}"),
                    None => assert!(sql.contains(e), "{e} missing:\n{sql}"),
                }
            }
        }
    }

    #[test]
    fn single_filter_only_skips_cascade_narrowing_when_in_cte_push_covers_it() {
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
            !sql.contains("_narrow_p"),
            "single FilterOnly node should not emit _narrow_p cascade CTE; the IN-CTE push on the same hop already narrows the join, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.source_id IN (SELECT id FROM _filter_g)"),
            "FilterOnly IN-CTE should land inside the dedup CTE inner WHERE, got:\n{sql}"
        );
    }

    #[test]
    fn cascade_narrowing_skipped_for_convergent_join_target() {
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "n", "entity": "Note"},
                {"id": "p", "entity": "Project"},
                {"id": "g", "entity": "Group", "filters": {"full_path": "gitlab-org"}},
                {"id": "u", "entity": "User", "filters": {"username": "stanhu"}}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "n", "to": "p"},
                {"type": "CONTAINS", "from": "g", "to": "p"},
                {"type": "AUTHORED", "from": "u", "to": "n"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [{"function": "count", "target": "n", "alias": "note_count"}],
            "limit": 10
        }"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("_narrow_p"),
            "p is the join target of two hops (IN_PROJECT and CONTAINS), so the cross-hop joins narrow it without a cascade CTE; got:\n{sql}"
        );
    }

    #[test]
    fn filtered_redaction_joins_push_filters_into_subquery() {
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
            "filtered File join should push filters into the subquery, got:\n{sql}"
        );
        assert!(
            sql.contains("INNER JOIN (SELECT * FROM gl_definition AS d FINAL WHERE")
                && sql.contains("startsWith(d.name, 'process')"),
            "filtered Definition join should push filters into the subquery, got:\n{sql}"
        );
    }

    #[test]
    fn cross_namespace_fk_chain_elides_to_node_joins() {
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "filters": {"username": "stanhu"}},
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "pipe", "entity": "Pipeline"},
                {"id": "j", "entity": "Job", "filters": {"status": "failed"}}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "HAS_HEAD_PIPELINE", "from": "mr", "to": "pipe"},
                {"type": "HAS_JOB", "from": "pipe", "to": "j"}
            ],
            "limit": 10
        }"#;
        let sql = compile_sql(query);
        assert!(
            !sql.contains("gl_edge") && !sql.contains("gl_ci_edge"),
            "FK-backed cross-namespace chain must elide to node joins, not edge scans; got:\n{sql}"
        );
        assert!(
            sql.contains("gl_user")
                && sql.contains("gl_merge_request")
                && sql.contains("gl_pipeline")
                && sql.contains("gl_job"),
            "must join the node tables; got:\n{sql}"
        );
        // authz boundary preserved: every in-namespace node scoped, global hub not.
        assert!(
            sql.contains("startsWith(mr.traversal_path")
                && sql.contains("startsWith(pipe.traversal_path")
                && sql.contains("startsWith(j.traversal_path")
                && !sql.contains("startsWith(u.traversal_path"),
            "all in-namespace nodes (mr, pipe, j) scoped, global User hub unscoped; got:\n{sql}"
        );
    }

    #[test]
    fn hydration_uses_limit_by_for_latest_rows() {
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
            !sql.contains(" FINAL"),
            "hydration should dedup via LIMIT BY, not FINAL, got:\n{sql}"
        );
        assert!(
            sql.contains("LIMIT 1 BY"),
            "hydration should dedup latest rows via LIMIT 1 BY, got:\n{sql}"
        );
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

        assert!(
            !sql.contains("_nf_mr"),
            "_nf_mr must not be emitted when state is fully denormalized, got:\n{sql}"
        );

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
            "nodes": [{"id": "mr", "entity": "MergeRequest",
                     "node_ids": [1],
                     "filters": {
                         "created_at": [
                             {"op": "gte", "value": "2026-04-01T00:00:00Z"},
                             {"op": "lt", "value": "2026-05-01T00:00:00Z"}
                         ]
                     },
                     "columns": ["id", "created_at"]}],
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
                "nodes": [{"id": "f", "entity": "File",
                         "node_ids": [1],
                         "filters": {"content": {"op": "eq", "value": "x"}},
                         "columns": ["path", "content"]}],
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
                "nodes": [{"id": "f", "entity": "File",
                         "node_ids": [1],
                         "filters": {"content": {"op": "gt", "value": "x"}}}],
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
