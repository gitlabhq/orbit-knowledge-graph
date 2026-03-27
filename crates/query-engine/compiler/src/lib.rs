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
//!     "node": {"id": "u", "entity": "User", "columns": ["username"]},
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

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
pub use constants::{
    EDGE_ALIAS_SUFFIXES, EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_KINDS_COLUMN, EDGE_SRC_SUFFIX,
    EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX, GKG_COLUMN_PREFIX, HYDRATION_NODE_ALIAS,
    NEIGHBOR_ID_COLUMN, NEIGHBOR_IS_OUTGOING_COLUMN, NEIGHBOR_TYPE_COLUMN, PATH_COLUMN,
    RELATIONSHIP_TYPE_COLUMN,
};
pub use error::{QueryError, Result};
pub use input::{
    ColumnSelection, DynamicColumnMode, EntityAuthConfig, Input, InputNode, QueryType, parse_input,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::constants::EDGE_TABLE;
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
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect, codegen,
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
pub use types::SecurityContext;

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
/// hydrate plan passes — codegen defaults to `HydrationPlan::None`.
/// For all other query types, runs the full secure pipeline.
pub fn compile_input(input: Input, ctx: &SecurityContext) -> Result<CompiledQueryContext> {
    let env = SecureEnv::new(Arc::new(Ontology::new()), ctx.clone());
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
    let env = LocalEnv::new(Arc::new(ontology.clone()));
    let state = DuckDbState::from_json(json_input);
    let pipeline = pipelines::duckdb().seal();
    pipeline.execute(state, &env)?.into_output().count_err()
}

// Pipeline presets are in `pipelines.rs`.
// Tests are in `tests/compiler_tests.rs` and `tests/ontology_tests.rs`.
