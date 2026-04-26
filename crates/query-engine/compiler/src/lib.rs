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
//!     "query_type": "search",
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
pub use types::{AccessLevel, SecurityContext};

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

        let query = r#"{"query_type":"search","node":{"id":"g","entity":"Group","node_ids":[1],"columns":["name"]},"limit":1}"#;
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

        let query = r#"{"query_type":"search","node":{"id":"g","entity":"Group","node_ids":[1],"columns":["name"]},"limit":1}"#;
        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("gl_group") && !sql.contains("v1_gl_group"),
            "unprefixed search should use gl_group, got: {sql}"
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
            "aggregations": [{"function": "count", "target": "mr", "alias": "total_mrs"}],
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

        // Filter is captured via SIP CTE (`_nf_mr` filtered by state='opened',
        // edge `source_id IN _nf_mr`). The COUNT must reference the edge column
        // so that filtered edges are what's counted; emitting bare `count()`
        // here would not be wrong (the IN-filter still bounds rows) but would
        // suppress the projection-routing decision based on filter presence.
        assert!(
            sql.contains("COUNT(e0.source_id)")
                || sql.contains("countIf")
                || sql.contains("COUNTIF"),
            "filtered count must reference edge column or use countIf, got:\n{sql}"
        );
        assert!(
            sql.contains("state = 'opened'"),
            "state filter must reach the SQL (in _nf_* or WHERE), got:\n{sql}"
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

    /// F1: path_finding without `rel_types` should drop edge tables that
    /// carry no edges touching either anchor entity. `User → Vulnerability`
    /// has no code-graph component, so `gl_code_edge` (which only stores
    /// File/Definition/ImportedSymbol/Branch edges) must not appear in any
    /// hop arm.
    #[test]
    fn path_finding_prunes_edge_tables_unreachable_from_anchors() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1]},
                {"id": "v", "entity": "Vulnerability", "node_ids": [2]}
            ],
            "path": {"type": "any", "from": "u", "to": "v", "max_depth": 3}
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            !sql.contains("gl_code_edge"),
            "gl_code_edge must not appear when neither anchor entity is in \
             its edge variants, got:\n{sql}"
        );
        assert!(
            sql.contains("gl_edge"),
            "gl_edge must remain (carries User and Vulnerability edges), \
             got:\n{sql}"
        );
    }

    /// F2: pinned-root SIP should be injected into each multi-hop UNION ALL
    /// arm, not only at the outer subquery level. Without per-arm injection
    /// the depth-1 edge scan reads the full authorized scope before the
    /// outer `IN _nf_<root>` filter fires.
    #[test]
    fn multihop_traversal_pushes_pinned_root_sip_into_each_arm() {
        let ontology = Ontology::load_embedded().expect("ontology must load");

        let query = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [116]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF", "from": "u", "to": "p",
                "min_hops": 1, "max_hops": 3
            }],
            "limit": 5
        }"#;

        let compiled = compile(query, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        // The arm-level injection rewrites the depth-1 edge scan to include
        // `e1.source_id IN _nf_u`. Without F2 the only matching reference
        // would be `hop_e0.start_id IN _nf_u` at the outer level.
        assert!(
            sql.contains("e1.source_id IN (SELECT id FROM _nf_u)"),
            "depth-1 arm must reference _nf_u via e1.source_id, got:\n{sql}"
        );
    }
}
