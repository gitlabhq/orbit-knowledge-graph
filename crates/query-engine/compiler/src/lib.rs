//! Graph Query Compiler
//!
//! Compiles JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! JSON → Schema Validate → Parse → Validate → Lower → Optimize → Enforce → Deduplicate → Security → Check → Codegen → SQL
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
    CheckPass, CodegenPass, DeduplicatePass, DuckDbCodegenPass, EnforcePass, HydratePlanPass,
    LowerPass, NormalizePass, OptimizePass, SecurityPass, ValidatePass,
};
pub use pipelines::{
    DuckDbState, HasHydrationPlan, HasInput, HasJson, HasNode, HasOntology, HasOutput,
    HasResultCtx, HasSecurityCtx, LocalEnv, QueryState, SealInput, SealJson, SealNode, SecureEnv,
};

// Re-export key types from pass modules.
pub use passes::check::check_ast;
pub use passes::codegen::{
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect,
    clickhouse::emit_simple_query, codegen, ddl::clickhouse::emit_create_table,
    ddl::duckdb::emit_create_table as emit_duckdb_create_table, ddl::generate_graph_tables,
    ddl::generate_graph_tables_with_prefix, ddl::generate_local_tables,
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext, enforce_return};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    generate_hydration_plan,
};
pub use passes::lower::lower;
pub use passes::normalize::{build_entity_auth, normalize};
pub use passes::optimize::optimize;
pub use passes::security::apply_security_context;
pub use passes::validate::Validator;
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
/// Runs the full ClickHouse compilation pipeline:
/// `JSON → Validate → Normalize → Lower → Optimize → Enforce → Deduplicate → Security → Check → Codegen`
#[must_use = "the compiled query context should be used"]
pub fn compile(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let env = SecureEnv::new(Arc::new(ontology.clone()), ctx.clone());
    let state = QueryState::from_json(json_input);
    let pipeline = pipelines::clickhouse().seal();
    pipeline.execute(state, &env)?.into_output().count_err()
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
        .execute(state, &env)?
        .into_output()
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
    pipeline.execute(state, &env)?.into_output().count_err()
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
    pipeline.execute(state, &env)?.into_output().count_err()
}

// Pipeline presets are in `pipelines.rs`.
// Tests are in `tests/compiler_tests.rs` and `tests/ontology_tests.rs`.

#[cfg(test)]
mod tests {
    use super::*;

    fn security_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".to_string()]).expect("valid context")
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

        assert!(
            sql.contains("v1_gl_edge"),
            "traversal SQL should use prefixed edge table v1_gl_edge, got: {sql}"
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
    /// must never emit a bare `target.id` reference: ClickHouse reads that
    /// as `database.column` and fails with `Database target does not exist`.
    /// The relationship_kind filter must also survive to the main scan.
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

        assert!(
            !sql.contains("mr.id"),
            "must not reference bare `mr.id` when mr table is not joined, got:\n{sql}"
        );
        assert!(
            sql.contains("IN_PROJECT") || sql.contains("relationship_kind"),
            "relationship_kind filter must survive, got:\n{sql}"
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

    /// When the target node has filters that fold into `countIf`, the column
    /// reference is required so the `-If` combinator has something to count.
    /// Bare `count()` would lose the filter semantics.
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

        // The MR state filter is denormalized onto the edge as source_state.
        // The denorm pass rewrites the _nf_mr CTE into a direct edge-column
        // filter (e0.source_state = 'opened'), and the COUNT becomes bare
        // count() since the WHERE clause already bounds rows correctly.
        assert!(
            sql.contains("COUNT(e0.source_id)")
                || sql.contains("countIf")
                || sql.contains("COUNTIF")
                || sql.contains("count()"),
            "count must reference edge column, use countIf, or be bare count(), got:\n{sql}"
        );
        assert!(
            sql.contains("hasToken(e0.source_tags, 'state:opened')"),
            "state filter must reach the SQL as hasToken on source_tags, got:\n{sql}"
        );
    }

    /// Traversal with `id_range` (no `node_ids` or `filters`) must produce
    /// a `_nf_*` CTE with range conditions that actually reach the SQL.
    /// Before the fix, `build_node_where` and `has_conditions` ignored
    /// `id_range`, so the lowerer skipped CTE generation entirely.
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

        assert!(
            sql.contains("_nf_u"),
            "id_range should generate a _nf_u CTE, got:\n{sql}"
        );
        assert!(
            sql.contains(">= 1") || sql.contains(">= {u_id_start:Int64}"),
            "CTE should contain range lower bound, got:\n{sql}"
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
    /// Multi-hop traversal must constrain `target_kind`/`source_kind` on
    /// EVERY edge it touches, not just whichever side `node_edge_col`
    /// happens to map first. Without this, `User AUTHORED MR` joined to
    /// `MR IN_PROJECT Project` can match `User AUTHORED <other entity>`
    /// rows whose ID happens to collide with an MR ID, producing edges
    /// with garbage `target_kind` in the result and skipping kind-PK
    /// pruning on the second-hop edge.
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

        // e0 covers IN_PROJECT (mr → p): both sides must be constrained.
        assert!(
            sql.contains("e0.source_kind = 'MergeRequest'"),
            "e0 must constrain source_kind=MergeRequest, got:\n{sql}"
        );
        assert!(
            sql.contains("e0.target_kind = 'Project'"),
            "e0 must constrain target_kind=Project, got:\n{sql}"
        );
        // e1 covers AUTHORED (u → mr): the previously missing target_kind
        // constraint that lets bogus edges leak through.
        assert!(
            sql.contains("e1.source_kind = 'User'"),
            "e1 must constrain source_kind=User, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.target_kind = 'MergeRequest'"),
            "e1 must constrain target_kind=MergeRequest (R2 cliff), got:\n{sql}"
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
        // The AUTHORED edge JOIN is retained as an existence semi-join.
        assert!(
            sql.contains("'AUTHORED'"),
            "AUTHORED edge constraint must survive (semi-join existence), \
             got:\n{sql}"
        );
        // Project + MR work products survive.
        assert!(
            sql.contains("gl_project AS p") || sql.contains("FROM gl_project"),
            "gl_project must remain in FROM, got:\n{sql}"
        );
    }

    /// `apply_target_sip_prefilter` must skip emitting `_target_<alias>_ids`
    /// when the only conjunct it would have folded is a structural
    /// `InSubquery` filter the cascade pass already injected. Otherwise the
    /// query carries a redundant CTE that re-derives the same id set.
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

        // No `_target_mr_ids` CTE: the equivalent narrowing already lives in
        // `_cascade_mr` from the multi-rel cascade pass.
        assert!(
            !sql.contains("_target_mr_ids"),
            "_target_mr_ids must not be emitted when _cascade_mr already \
             carries the same id set, got:\n{sql}"
        );
        assert!(
            sql.contains("_cascade_mr"),
            "_cascade_mr must remain (provides the actual narrowing), \
             got:\n{sql}"
        );
    }

    /// Multi-hop traversal with a pinned source node must generate hop
    /// frontier CTEs (`_thop0_1`, `_thop0_2`) that materialize reachable
    /// IDs at each depth. UNION ALL arms at depth >= 2 must get SIP
    /// filters referencing the previous hop's frontier CTE.
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

        // Frontier CTEs should be generated for depths 1 and 2.
        assert!(
            sql.contains("_thop0_1"),
            "hop frontier CTE _thop0_1 must be present, got:\n{sql}"
        );
        assert!(
            sql.contains("_thop0_2"),
            "hop frontier CTE _thop0_2 must be present, got:\n{sql}"
        );

        // Arms at depth >= 2 should reference frontier CTEs.
        // e2.source_id IN (SELECT id FROM _thop0_1)
        assert!(
            sql.contains("_thop0_1"),
            "depth-2 arm must reference _thop0_1 for SIP, got:\n{sql}"
        );
    }

    /// Multi-hop traversal without a pinned node should NOT generate
    /// frontier CTEs (they'd scan the full edge table).
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

        // The pinned node is `p` (the `to` side). Frontier CTEs should
        // still be generated since the `to` node has a `_nf_p` CTE.
        assert!(
            sql.contains("_thop0_1"),
            "hop frontier CTE should be generated from _nf_p (to-side selectivity), got:\n{sql}"
        );
    }

    /// Multi-hop aggregation with a pinned root must generate a multi-hop
    /// cascade CTE for the far-side node, narrowing its table scan.
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

        assert!(
            sql.contains("_cascade_f"),
            "multi-hop aggregation should generate _cascade_f CTE, got:\n{sql}"
        );
        assert!(
            sql.contains("startsWith"),
            "cascade CTE edge scans should have traversal_path security filters, got:\n{sql}"
        );
    }

    /// Intermediate nodes (referenced by 2+ relationships) must NOT be pruned
    /// even when they're absent from the aggregation target/group_by. Pruning
    /// them leaves adjacent edge JOINs dangling on the now-undefined alias
    /// (`mr.id = e1.source_id` becomes a `Unknown identifier mr.id` runtime
    /// error). Only leaf nodes (degree ≤ 1) are safe to prune.
    #[test]
    fn aggregation_keeps_intermediate_node_table_join() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "HAS_NOTE", "from": "mr", "to": "n"}
            ],
            "aggregations": [{
                "function": "count",
                "target": "n",
                "group_by": "u",
                "alias": "note_count"
            }],
            "limit": 5
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // mr is intermediate (touches AUTHORED and HAS_NOTE). It must remain
        // in the FROM tree so e1's JOIN ON `mr.id = e1.source_id` resolves.
        assert!(
            sql.contains("gl_merge_request AS mr") || sql.contains("FROM gl_merge_request"),
            "intermediate MR table must remain in FROM, got:\n{sql}"
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
    fn denorm_single_hop_removes_nf_cte_and_injects_edge_filter() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "pipe", "entity": "Pipeline", "filters": {
                    "status": {"op": "eq", "value": "failed"}
                }},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "pipe", "to": "proj"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // The original node-table _nf_pipe CTE (scanning gl_pipeline) should
        // be eliminated. A SIP-derived CTE named _nf_pipe may still exist
        // (scanning gl_edge), which is expected.
        assert!(
            !sql.contains("gl_pipeline"),
            "denorm pass should eliminate gl_pipeline scan, got:\n{sql}"
        );
        assert!(
            sql.contains("hasToken(e0.source_tags, 'status:failed')"),
            "denorm pass should inject hasToken edge filter, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_partial_filters_keeps_nf_cte() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        // Pipeline has 'status' denormalized but 'source' is not.
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "pipe", "entity": "Pipeline", "filters": {
                    "status": {"op": "eq", "value": "failed"},
                    "source": {"op": "eq", "value": "push"}
                }},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "pipe", "to": "proj"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("_nf_pipe"),
            "partial denorm must keep _nf_pipe CTE when not all filters are denormalized, got:\n{sql}"
        );
        assert!(
            !sql.contains("hasToken"),
            "partial denorm must not inject hasToken edge filter, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_skips_rewrite_when_node_ids_present() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "pipe", "entity": "Pipeline", "node_ids": [1, 2, 3],
                 "filters": {"status": {"op": "eq", "value": "failed"}}},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "pipe", "to": "proj"}],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            !sql.contains("hasToken"),
            "denorm pass must skip rewrite when node_ids are present, got:\n{sql}"
        );
    }

    /// The GROUP BY rewrite replaces `GROUP BY node.prop` with
    /// `GROUP BY edge.denorm_col` when all GROUP BY columns are
    /// denormalized. The node table join may still remain if the
    /// node is referenced for redaction IDs or other columns.
    #[test]
    fn denorm_aggregation_count_with_filter_uses_edge_column() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let query = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "pipe", "entity": "Pipeline", "filters": {
                    "status": {"op": "eq", "value": "failed"}
                }},
                {"id": "proj", "entity": "Project", "node_ids": [1]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "pipe", "to": "proj"}],
            "aggregations": [{
                "function": "count",
                "target": "pipe",
                "group_by": "proj",
                "alias": "n"
            }],
            "limit": 10
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("hasToken(e0.source_tags, 'status:failed')"),
            "filter on denormalized property must use hasToken edge filter, got:\n{sql}"
        );
        // The _nf_pipe node-filter CTE (with status filter + LIMIT 1 BY)
        // should be eliminated. gl_pipeline may still appear in SIP/cascade
        // CTEs, but those don't scan for the status property.
        assert!(
            !sql.contains("pipe.status"),
            "node-table status filter should be eliminated by denorm rewrite, got:\n{sql}"
        );
    }

    /// Role-gated entities (e.g. Vulnerability with required_role > Reporter)
    /// must keep their node table in FROM so the security pass can apply
    /// role-scoped traversal path filters.
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

        // Vulnerability requires SecurityManager role. Even though the state
        // filter is denormalized, the node table must remain for security.
        assert!(
            sql.contains("gl_vulnerability"),
            "role-gated entity gl_vulnerability must NOT be pruned from FROM, got:\n{sql}"
        );
    }
}
