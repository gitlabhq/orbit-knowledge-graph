//! Graph Query Compiler
//!
//! Compiles JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! JSON → Schema Validate → Parse → Validate → Lower → Optimize → Enforce → Security → Check → Codegen → SQL
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
pub use pipeline::{CompilerPass, Pipeline, PipelineEnv, PipelineState, SealedPipeline};

// Re-export env, state, and capability traits.
pub use passes::{
    CheckPass, CodegenPass, EnforcePass, HydrationCodegenPass, LowerPass, NormalizePass,
    OptimizePass, SecurityPass, ValidatePass,
};
pub use pipelines::{
    DuckDbState, HasInput, HasJson, HasNode, HasOntology, HasOutput, HasResultCtx, HasSecurityCtx,
    LocalEnv, QueryState, SecureEnv,
};

// Re-export key types from pass modules.
pub use passes::check::check_ast;
pub use passes::codegen::{
    CompiledQueryContext, HydrationPlan, HydrationTemplate, ParamValue, ParameterizedQuery, codegen,
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext, enforce_return};
pub use passes::hydrate::generate_hydration_plan;
pub use passes::lower::lower;
pub use passes::normalize::{build_entity_auth, normalize};
pub use passes::optimize::optimize;
pub use passes::security::apply_security_context;
pub use passes::validate::Validator;
pub use types::SecurityContext;

use metrics::CountErr;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Validate and normalize a JSON query string into a typed `Input`.
pub(crate) fn validated_input(json_input: &str, ontology: &Ontology) -> Result<Input> {
    let v = Validator::new(ontology);
    let value = v.check_json(json_input).count_err()?;
    v.check_ontology(&value).count_err()?;
    let input: Input = serde_json::from_value(value).count_err()?;
    v.check_references(&input).count_err()?;
    normalize(input, ontology).count_err()
}

/// Compile a JSON query into a [`CompiledQueryContext`].
///
/// The context contains the parameterized SQL, bind parameters, result context
/// for redaction, hydration plan, and the validated input.
#[must_use = "the compiled query context should be used"]
pub fn compile(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let input = validated_input(json_input, ontology).count_err()?;
    compile_input(input, ctx)
}

/// Compile from a pre-built `Input`. Used for internal query types (Hydration)
/// that bypass JSON schema validation.
pub fn compile_input(mut input: Input, ctx: &SecurityContext) -> Result<CompiledQueryContext> {
    let mut node = lower(&mut input).count_err()?;
    optimize(&mut node, &mut input, ctx);
    let result_context = enforce_return(&mut node, &input)?;
    if input.query_type != QueryType::Hydration {
        apply_security_context(&mut node, ctx).count_err()?;
        check_ast(&node, ctx).count_err()?;
    }
    let base = codegen(&node, result_context).count_err()?;

    let hydration = generate_hydration_plan(&input);
    let query_type = input.query_type;

    Ok(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
    })
}

// Pipeline presets are in `pipelines.rs`.
// Tests are in `tests/compiler_tests.rs` and `tests/ontology_tests.rs`.
