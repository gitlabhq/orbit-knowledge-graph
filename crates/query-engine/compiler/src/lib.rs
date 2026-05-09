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

pub mod ast;
pub mod constants;
pub mod error;
pub mod input;
pub mod metrics;
pub mod types;

// pipeline must come before pipelines — its macros.rs defines
// `define_env_capabilities!` and `define_state_capabilities!` which
// pipelines.rs invokes.
pub mod passes;
pub mod pipeline;
pub mod pipelines;

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
    ColumnSelection, DynamicColumnMode, EntityAuthConfig, Input, InputNode, QueryType, parse_input,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::{Ontology, OntologyError};
pub use pipeline::{
    CompilerPass, Pipeline, PipelineEnv, PipelineObserver, PipelineState, Seal, SealedPipeline,
};

// Re-export env, state, and capability traits.
pub use passes::{
    CheckPass, CodegenPass, DuckDbCodegenPass, EnforcePass, HydratePlanPass, LowerPass,
    NormalizePass, PlannerPass, SecurityPass, SettingsPass, ValidatePass,
};
pub use pipelines::{
    DuckDbState, HasHydrationPlan, HasInput, HasJson, HasNode, HasOntology, HasOutput,
    HasResultCtx, HasSecurityCtx, LocalEnv, QueryState, SealInput, SealJson, SealNode, SecureEnv,
};

// Re-export key types from pass modules.
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
pub use types::{AccessLevel, DEFAULT_PATH_ACCESS_LEVEL, SecurityContext, TraversalPath};

use metrics::CountErr;
use std::sync::Arc;

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
    let env = SecureEnv::new(Arc::new(ontology.clone()), ctx.clone());
    let state = QueryState::from_json(json_input);
    let pipeline = pipelines::clickhouse().seal();
    pipeline
        .execute(state, &env)
        .and_then(|s| s.into_output())
        .count_err()
}

/// Compile from a pre-built `Input`. Used for internal query types (Hydration)
/// that bypass JSON schema validation.
///
/// For hydration queries (`QueryType::Hydration`), skips security, check, and
/// hydrate plan passes but applies dedup (argMax) — codegen defaults to
/// `HydrationPlan::None`. For all other query types, runs the full secure pipeline.
///
/// The real ontology must be passed so `RestrictPass` can enforce
/// `admin_only` on pre-built hydration inputs as defense-in-depth against
/// bugs in upstream plan-building.
pub fn compile_input(
    input: Input,
    ontology: &Arc<Ontology>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let env = SecureEnv::new(Arc::clone(ontology), ctx.clone());
    let is_hydration = input.query_type == QueryType::Hydration;
    let state = QueryState::from_input(input);

    let pipeline = if is_hydration {
        pipelines::hydration()
    } else {
        pipelines::from_input()
    };

    pipeline
        .seal()
        .execute(state, &env)
        .and_then(|s| s.into_output())
        .count_err()
}

/// Compile a JSON query into DuckDB-dialect SQL for local/offline use.
///
/// Skips security, enforce, optimize, and hydration — the output has no
/// redaction metadata and `HydrationPlan::None`. Do not use this where
/// multi-tenant authorization or column redaction is required.
///
/// ```text
/// JSON → Validate → Normalize → Lower → DuckDbCodegen
/// ```
#[must_use = "the compiled query context should be used"]
pub fn compile_local(json_input: &str, ontology: &Ontology) -> Result<CompiledQueryContext> {
    let mut ont = ontology.clone();
    // Local mode uses a single DuckDB edge table. Collapse all edge routing
    // so the compiler doesn't emit references to tables that don't exist locally.
    if let Some(local_table) = ontology.local_edge_table_name() {
        ont.collapse_edge_tables(local_table);
    }
    let env = LocalEnv::local(Arc::new(ont));
    let state = DuckDbState::from_json(json_input);
    let pipeline = pipelines::duckdb().seal();
    pipeline
        .execute(state, &env)
        .and_then(|s| s.into_output())
        .count_err()
}

/// Compile a pre-built Input into DuckDB-dialect SQL for local hydration.
///
/// Uses the local hydration pipeline (Lower → Enforce → DuckDbCodegen).
/// No validation, normalization, security, or recursive hydration.
/// The ontology is needed because Lower/Enforce may consult entity
/// metadata for table resolution and column aliasing.
#[must_use = "the compiled query context should be used"]
pub fn compile_local_input(input: Input, ontology: &Ontology) -> Result<CompiledQueryContext> {
    let mut ont = ontology.clone();
    if let Some(local_table) = ontology.local_edge_table_name() {
        ont.collapse_edge_tables(local_table);
    }
    let env = LocalEnv::local(Arc::new(ont));
    let state = DuckDbState::from_input(input);
    let pipeline = pipelines::duckdb_hydration().seal();
    pipeline
        .execute(state, &env)
        .and_then(|s| s.into_output())
        .count_err()
}

// Pipeline presets are in `pipelines.rs`.
// Tests are in `tests/compiler_tests.rs` and `tests/ontology_tests.rs`.

#[cfg(test)]
mod tests {
    use super::*;

    fn security_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".to_string()]).expect("valid context")
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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile("not json", &ontology, &security_ctx()).expect_err("must reject");
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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{"query_type":"traversal","node":{"id":"x","entity":"NotARealEntity","columns":["id"]},"limit":1}"#;
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile(query, &ontology, &security_ctx()).expect_err("must reject");
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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/"}}},
            "limit": 1
        }"#;
        let ctx =
            SecurityContext::new(1, vec!["1/100/".to_string()]).expect("valid scoped context");
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile(query, &ontology, &ctx).expect_err("must reject");
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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project",
                     "filters": {"traversal_path": {"op": "starts_with", "value": 1}}},
            "limit": 1
        }"#;
        let err = compile(query, &ontology, &security_ctx()).expect_err("must reject");
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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let prefixed = ontology.with_schema_version_prefix("v1_");

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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let prefixed = ontology.with_schema_version_prefix("v1_");

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

    #[test]
    fn compile_without_prefix_uses_unprefixed_tables() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{"query_type":"traversal","node":{"id":"g","entity":"Group","node_ids":[1],"columns":["name"]},"limit":1}"#;
        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("gl_group") && !sql.contains("v1_gl_group"),
            "unprefixed search should use gl_group, got: {sql}"
        );
    }

    /// Traversal with 1 node + 0 relationships is a search shape.
    /// It should compile to the same flat table scan as query_type: "search".
    #[test]
    fn traversal_single_node_compiles_as_search() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let search_query = r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "limit": 10
        }"#;

        let traversal_query = r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "limit": 10
        }"#;

        let search_sql = compile(search_query, &ontology, &security_ctx())
            .expect("search should compile")
            .base
            .render();

        let traversal_sql = compile(traversal_query, &ontology, &security_ctx())
            .expect("single-node traversal should compile")
            .base
            .render();

        assert_eq!(
            search_sql, traversal_sql,
            "single-node traversal SQL should match search SQL"
        );
    }

    /// Aggregation with a relationship and a property-less `count(target)`
    /// must resolve correctly without ClickHouse `Database does not exist`
    /// errors. The v2 lowerer uses FK-shortcut joins for IN_PROJECT,
    /// joining MR and Project via `mr.project_id` instead of an edge scan.
    #[test]
    fn aggregation_with_relationship_emits_no_bare_node_ref() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "p", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "total_mrs"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
    /// IDs must emit bare `COUNT()` so ClickHouse can route to the
    /// `agg_counts` projection. With `COUNT(e0.source_id)`, projection
    /// routing fails and the query scans the full edge slice (1B+ rows for
    /// File → Project on production scale).
    #[test]
    fn unfiltered_edge_only_count_emits_bare_count_for_projection_routing() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [1]},
                {"id": "f", "entity": "File"}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "f", "to": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "f",
                "group_by": "p",
                "alias": "files"
            }],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("COUNT()") || sql.contains("count()"),
            "unfiltered edge-only count must emit bare COUNT() for projection \
             routing, got:\n{sql}"
        );
        assert!(
            !sql.contains("COUNT(e0.source_id)") && !sql.contains("count(e0.source_id)"),
            "must not emit COUNT(source_id) -- breaks agg_counts projection routing, \
             got:\n{sql}"
        );
    }

    /// When the target node has filters, the count must still be bounded
    /// by those filters. The v2 lowerer uses FK-shortcut joins for
    /// IN_PROJECT, so the MR table is joined directly and the state
    /// filter appears as `mr.state = 'opened'` in the WHERE clause.
    #[test]
    fn filtered_edge_only_count_keeps_column_arg_for_count_if() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "opened"}
                }}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "p",
                "alias": "open_mrs"
            }],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
    fn path_finding_filtered_endpoint_seeds_hop_frontier_from_anchor_cte() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "filters": {"username": {"op": "eq", "value": "root"}}},
                {"id": "end", "entity": "Project", "node_ids": [100]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]},
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 uses `forward` CTE seeded from _nf_start anchor.
        assert!(
            sql.contains("forward"),
            "filtered endpoint should create a forward expansion CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("_nf_start"),
            "forward expansion should seed from _nf_start anchor, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_code_filtered_endpoints_prune_by_traversal_path() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        // v2 lowerer uses `forward` CTE seeded from _nf_start.
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
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            !sql.contains("toString(paths._gkg_path)")
                && !sql.contains("toString(paths._gkg_edge_kinds)"),
            "path array tie-break sorting should only be emitted for cursor pagination, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_with_cursor_keeps_path_tie_break_order() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("toString(paths._gkg_path)")
                && sql.contains("toString(paths._gkg_edge_kinds)"),
            "cursor pagination should keep deterministic path tie-break sorting, got:\n{sql}"
        );
    }

    /// Wildcard path finding passes `*` through as the relationship_kind
    /// on all hops. The v2 lowerer scans all edge tables (UNION ALL) to
    /// cover all relationship types.
    #[test]
    fn wildcard_path_finding_filters_only_endpoint_hops_by_relationship_kind() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "*", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing"},
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
    fn wildcard_aggregation_infers_relationship_kinds_from_endpoint_entities() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "*", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mrs"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("relationship_kind") && sql.contains("'AUTHORED'"),
            "wildcard aggregation should infer concrete User to MergeRequest relationship kinds, got:\n{sql}"
        );
        assert!(
            !sql.contains("gl_code_edge"),
            "inferred aggregation relationship kinds should avoid scanning code edge table, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_user_paths_do_not_join_on_traversal_path() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "p",
                "alias": "merged_mrs"
            }],
            "limit": 5
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // gl_user and any u-alias CTE should be gone.
        assert!(
            !sql.contains("gl_user AS u") && !sql.contains("FROM gl_user"),
            "gl_user join must be pruned for aggregation that never \
             references the User alias, got:\n{sql}"
        );
        assert!(
            !sql.contains("_cascade_u") && !sql.contains("_nf_u"),
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
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "p",
                "alias": "user_mrs"
            }],
            "limit": 5
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // Neither CTE should exist — edge-chain JOINs replace them.
        assert!(
            !sql.contains("_target_mr_ids"),
            "_target_mr_ids must not be emitted, got:\n{sql}"
        );
        assert!(
            !sql.contains("_cascade_mr"),
            "v2 lowerer should not emit cascade CTEs, got:\n{sql}"
        );
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
    /// arms for each depth. The v2 lowerer uses inline edge JOINs within
    /// each arm instead of frontier CTEs.
    #[test]
    fn multi_hop_traversal_generates_hop_frontier_ctes() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer uses UNION ALL arms for variable-length hops.
        assert!(
            sql.contains("UNION ALL"),
            "variable-length traversal must use UNION ALL arms, got:\n{sql}"
        );
        // No frontier CTEs — v2 uses inline edge JOINs.
        assert!(
            !sql.contains("_thop0_1") && !sql.contains("_thop0_2"),
            "v2 lowerer should not emit frontier CTEs, got:\n{sql}"
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

    /// Multi-hop traversal with a pinned to-side node. The v2 lowerer
    /// uses UNION ALL arms with inline edge JOINs instead of frontier CTEs.
    /// The pinned `node_ids` filter is pushed into the outer WHERE.
    #[test]
    fn multi_hop_traversal_skips_frontiers_without_selectivity() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer uses UNION ALL for variable-length, no frontier CTEs.
        assert!(
            !sql.contains("_thop0_1"),
            "v2 lowerer should not emit frontier CTEs, got:\n{sql}"
        );
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
    /// CONTAINS relationship. The v2 lowerer uses inline UNION ALL instead
    /// of cascade CTEs.
    #[test]
    fn multi_hop_aggregation_generates_cascade_cte() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{"function": "count", "target": "f", "group_by": "p"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer emits UNION ALL arms for variable-length hops.
        assert!(
            sql.contains("UNION ALL"),
            "multi-hop aggregation should use UNION ALL for variable-length hops, got:\n{sql}"
        );
        // No cascade CTEs.
        assert!(
            !sql.contains("_cascade_f"),
            "v2 lowerer should not emit cascade CTEs, got:\n{sql}"
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
    /// FK elision replaces edge-chain JOINs with direct FK column joins
    /// (e.g. `mr.author_id`, `mr.project_id`) so intermediate nodes
    /// bridge relationships via their FK columns.
    #[test]
    fn aggregation_keeps_intermediate_node_table_join() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "p",
                "alias": "user_mrs"
            }],
            "limit": 5,
            "options": {"materialize_ctes": true}
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // FK elision replaces edge-chain JOINs with direct FK joins.
        // AUTHORED → mr.author_id, IN_PROJECT → mr.project_id.
        // MR bridges the two relationships via its FK columns.
        assert!(
            sql.contains("mr.author_id = 116") || sql.contains("author_id = 116"),
            "User node_ids filter must be pushed to FK column, got:\n{sql}"
        );
        assert!(
            sql.contains("p.id = mr.project_id") || sql.contains("mr.project_id"),
            "IN_PROJECT FK join must use project_id, got:\n{sql}"
        );
    }

    /// Variable-length CONTAINS×{1..3} traversal: each UNION ALL arm should
    /// carry static `e1.source_kind = 'Group'` and `e<depth>.target_kind = 'Project'`
    /// literals so ClickHouse can use the kind-led PK projection
    /// (`by_rel_source_kind`/`by_rel_target_kind`) for granule pruning at every
    /// depth, instead of relying on dynamic IN-subqueries.
    #[test]
    fn variable_length_traversal_emits_per_arm_kind_literals() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "n"}],
            "limit": 3
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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

    #[test]
    fn denorm_single_hop_keeps_nf_cte_and_injects_supplementary_tag() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "merged"}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer pushes the denorm filter to edge tags — no _nf CTE.
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        // No _nf_mr CTE needed — filter is fully on the edge.
        assert!(
            !sql.contains("_nf_mr"),
            "v2 lowerer should not emit _nf_mr CTE, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_in_list_filter_keeps_cte_for_dedup() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "in", "value": ["merged", "opened"]}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer pushes IN-list filter to edge tags via hasAny.
        assert!(
            sql.contains("hasAny(e0.target_tags"),
            "IN-list denorm filter must use hasAny on edge target_tags, got:\n{sql}"
        );
        assert!(
            sql.contains("state:merged") && sql.contains("state:opened"),
            "both filter values must appear in tag predicate, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_in_list_single_value_keeps_cte_for_dedup() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "in", "value": ["merged"]}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer pushes single-value IN-list to edge tags via has.
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "single-value IN-list denorm must use has on edge target_tags, got:\n{sql}"
        );
    }

    #[test]
    /// Partial denorm: when some filters are denormalized and some are not,
    /// the CTE is kept (with only non-denormalized filters) and tag
    /// predicates are injected onto the edge WHERE for the denormalized ones.
    fn denorm_partial_filters_keeps_nf_cte() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "merged"},
                    "source_branch": {"op": "eq", "value": "main"}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer emits a _filter_mr CTE for the non-denormalized filter
        // and pushes the denormalized filter to edge tags.
        assert!(
            sql.contains("_filter_mr"),
            "partial denorm must keep a filter CTE for non-denormalized filters, got:\n{sql}"
        );
        // The denormalized state filter is pushed to edge tags.
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denormalized state filter must be pushed to edge tags, got:\n{sql}"
        );
        // CTE retains the non-denormalized source_branch filter.
        assert!(
            sql.contains("main"),
            "filter CTE must retain non-denormalized source_branch filter, got:\n{sql}"
        );
    }

    /// When node_ids are present alongside filters, the v2 lowerer applies
    /// both the node_ids filter (e0.target_id IN [...]) and the denorm tag
    /// filter (has on target_tags) to the edge. Both filters narrow the scan.
    #[test]
    fn denorm_skips_rewrite_when_node_ids_present() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "state": {"op": "eq", "value": "merged"}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "u",
                "alias": "n"
            }],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer pushes the denorm filter to edge tags directly.
        assert!(
            sql.contains("has(e0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        // No _nf_mr CTE needed — edge tag handles the filter.
        assert!(
            !sql.contains("_nf_mr"),
            "v2 lowerer should not emit _nf_mr when filter is fully denormalized, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_preserves_role_gated_node_table_for_security() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "v", "entity": "Vulnerability", "filters": {
                    "state": {"op": "eq", "value": "detected"}
                }},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "proj"}],
            "aggregations": [{
                "function": "count",
                "target": "v",
                "group_by": "proj",
                "alias": "n"
            }],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("gl_vulnerability"),
            "role-gated entity gl_vulnerability must NOT be pruned from FROM, got:\n{sql}"
        );
    }

    #[test]
    fn skip_dedup_is_ignored_and_keeps_latest_row_filtering() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest", "filters": {
                    "title": {"op": "contains", "value": "fix"}
                }}
            ],
            "relationships": [{"type": "REVIEWER", "from": "u", "to": "mr"}],
            "limit": 10,
            "options": {"skip_dedup": true}
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains(" FINAL"),
            "skip_dedup is ignored; node reads should still use FINAL, got:\n{sql}"
        );
        assert!(
            !sql.contains("LIMIT 1 BY"),
            "latest-row reads should not use LIMIT 1 BY, got:\n{sql}"
        );
        assert!(
            sql.contains("_deleted"),
            "latest-row reads should still filter by _deleted, got:\n{sql}"
        );
    }

    #[test]
    fn node_table_reads_use_final_for_latest_rows() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
                    "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "merged_mrs"}],
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
            let compiled = compile(query, &ontology, &security_ctx())
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
        let ontology = Ontology::load_embedded().expect("ontology must load");
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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
            sql.contains("FROM gl_job AS j FINAL INNER JOIN gl_pipeline AS pipe FINAL"),
            "outer latest-row reads should still use FINAL, got:\n{sql}"
        );
        assert!(
            sql.contains("j.pipeline_id IN (SELECT id FROM _candidate_pipe)")
                && sql.contains("pipe.id IN (SELECT id FROM _candidate_pipe)"),
            "candidate CTE should narrow both center FK values and target ids, got:\n{sql}"
        );
    }

    #[test]
    fn fk_star_filter_only_relationships_do_not_emit_candidate_ctes() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "j", "entity": "Job", "filters": {"status": "failed"}},
                {"id": "p", "entity": "Project", "node_ids": [278964]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "p"}],
            "aggregations": [{"function": "count", "target": "j", "group_by": "j", "alias": "fail_count"}],
            "limit": 20
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
        let ontology = Ontology::load_embedded().expect("ontology must load");
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

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("_narrow_j2 AS (SELECT DISTINCT j1.auto_canceled_by_id AS id FROM gl_job AS j1 WHERE"),
            "unfiltered joined target should be narrowed by a non-FINAL candidate scan, got:\n{sql}"
        );
        assert!(
            !sql.contains("_narrow_j2 AS (SELECT DISTINCT j1.auto_canceled_by_id AS id FROM gl_job AS j1 FINAL"),
            "narrowing CTE should not run a second FINAL scan, got:\n{sql}"
        );
        assert!(
            sql.contains("FROM gl_job AS j1 FINAL INNER JOIN gl_job AS j2 FINAL"),
            "outer source and joined target should still use FINAL, got:\n{sql}"
        );
    }

    #[test]
    fn hydration_uses_final_for_latest_rows() {
        use std::sync::Arc;

        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
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

        let compiled = compile_input(input, &ontology, &security_ctx())
            .expect("hydration input should compile");
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

    /// FK elision replaces cascade CTEs and edge-chain JOINs with
    /// direct FK column joins. The `materialize_ctes` option has
    /// no effect when there are no multi-referenced CTEs to materialize.
    #[test]
    fn multi_ref_cte_emits_materialized_keyword_and_setting() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "p",
                "alias": "user_mrs"
            }],
            "limit": 5,
            "options": {"materialize_ctes": true}
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // v2 lowerer uses edge-chain JOINs — no cascade CTEs.
        assert!(
            !sql.contains("_cascade_mr"),
            "v2 lowerer should not emit cascade CTEs, got:\n{sql}"
        );
        // FK elision replaces edge-chain JOINs with direct FK column joins.
        assert!(
            sql.contains("mr.author_id = 116") || sql.contains("author_id = 116"),
            "User node_ids filter must be pushed to FK column, got:\n{sql}"
        );
        assert!(
            sql.contains("p.id = mr.project_id") || sql.contains("mr.project_id"),
            "IN_PROJECT FK join must use project_id, got:\n{sql}"
        );
    }

    /// Single-reference CTEs must NOT be materialized — inlining lets
    /// ClickHouse push predicates through and is the default behavior.
    #[test]
    fn single_ref_cte_is_not_materialized() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

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
    /// (state=merged). The v2 lowerer uses edge-chain JOINs with the
    /// denorm filter pushed to `has(e0.target_tags, 'state:merged')`.
    /// No cascade or _nf_mr CTEs are needed.
    #[test]
    fn agg_denorm_eliminates_redundant_target_and_nf_ctes() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

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
            "aggregations": [{
                "function": "count",
                "target": "mr",
                "group_by": "p",
                "alias": "user_mrs"
            }],
            "limit": 5
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // No cascade CTEs in v2 lowerer — edge-chain JOINs replace them.
        assert!(
            !sql.contains("_cascade_mr"),
            "v2 lowerer should not emit cascade CTEs, got:\n{sql}"
        );

        // No _target_mr_ids or _nf_mr CTEs — denorm covers the filter.
        assert!(
            !sql.contains("_target_mr_ids"),
            "_target_mr_ids must not be emitted, got:\n{sql}"
        );
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
}
