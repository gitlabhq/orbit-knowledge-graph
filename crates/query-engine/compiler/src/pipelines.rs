//! Pipeline definitions: environments, state, capability traits, and presets.

use std::sync::Arc;

use compiler_pipeline_macros::{PipelineEnv, PipelineState};
use ontology::Ontology;

use gkg_config::QueryConfig;

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::*;
use crate::pipeline::{Pipeline, PipelineEnv, PipelineState};
use crate::types::SecurityContext;

// ═════════════════════════════════════════════════════════════════════════════
// Env capability traits
// ═════════════════════════════════════════════════════════════════════════════

crate::define_env_capabilities! {
    pub ontology: Arc<Ontology>,
    pub security_ctx: SecurityContext,
}

// ═════════════════════════════════════════════════════════════════════════════
// State capability traits
// ═════════════════════════════════════════════════════════════════════════════

crate::define_state_capabilities! {
    pub json: String,
    pub input: Input,
    pub node: Node,
    pub result_ctx: ResultContext,
    pub hydration_plan: HydrationPlan,
    pub query_config: QueryConfig,
    pub output: CompiledQueryContext,
}

// ═════════════════════════════════════════════════════════════════════════════
// Environments
// ═════════════════════════════════════════════════════════════════════════════

#[derive(PipelineEnv)]
pub struct SecureEnv {
    ontology: Arc<Ontology>,
    security_ctx: SecurityContext,
}

#[derive(PipelineEnv)]
pub struct LocalEnv {
    ontology: Arc<Ontology>,
}

// ═════════════════════════════════════════════════════════════════════════════
// State
// ═════════════════════════════════════════════════════════════════════════════

#[derive(PipelineState)]
pub struct QueryState {
    pub json: Option<String>,
    pub input: Option<Input>,
    pub node: Option<Node>,
    pub result_ctx: Option<ResultContext>,
    pub hydration_plan: Option<HydrationPlan>,
    pub query_config: Option<gkg_config::QueryConfig>,
    pub output: Option<CompiledQueryContext>,
}

impl QueryState {
    /// Extract the compiled output, consuming the state.
    pub fn into_output(self) -> Result<CompiledQueryContext> {
        self.output
            .ok_or_else(|| QueryError::PipelineInvariant("pipeline did not produce output".into()))
    }
}

#[derive(PipelineState)]
pub struct DuckDbState {
    pub json: Option<String>,
    pub input: Option<Input>,
    pub node: Option<Node>,
    pub output: Option<CompiledQueryContext>,
}

impl DuckDbState {
    pub fn into_output(self) -> Result<CompiledQueryContext> {
        self.output
            .ok_or_else(|| QueryError::PipelineInvariant("pipeline did not produce output".into()))
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Pipeline presets
// ═════════════════════════════════════════════════════════════════════════════

/// Standard ClickHouse compilation pipeline.
///
/// ```text
/// JSON → Validate → Normalize → Lower → Optimize → Enforce → Deduplicate → Security → Check → HydratePlan → Codegen
/// ```
///
/// Deduplicate runs before Security so that Security's subquery recursion
/// injects `startsWith(traversal_path, ...)` directly into inner queries
/// where the `gl_*` Scans live.
pub fn clickhouse() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(ValidatePass)
        .seal(SealJson)
        .pass(NormalizePass)
        .pass(LowerPass)
        .pass(OptimizePass)
        .pass(EnforcePass)
        .pass(DeduplicatePass)
        .pass(SecurityPass)
        .pass(CheckPass)
        .pass(HydratePlanPass)
        .pass(SettingsPass)
        .pass(CodegenPass)
        .build()
}

/// Compile from a pre-built [`Input`]. Runs full security and check passes.
///
/// Used by tests and the `compile_input()` public API for non-hydration queries.
///
/// ```text
/// Input → Lower → Optimize → Enforce → Deduplicate → Security → Check → HydratePlan → Codegen
/// ```
pub fn from_input() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(LowerPass)
        .pass(OptimizePass)
        .pass(EnforcePass)
        .pass(DeduplicatePass)
        .pass(SecurityPass)
        .pass(CheckPass)
        .pass(HydratePlanPass)
        .pass(SettingsPass)
        .pass(CodegenPass)
        .build()
}

/// Hydration compilation — skips security, check, and hydration plan generation.
///
/// No `HydratePlanPass` means `CodegenPass` defaults to `HydrationPlan::None`,
/// preventing recursive hydration.
///
/// ```text
/// Input → Lower → Optimize → Enforce → Codegen
/// ```
pub fn hydration() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(LowerPass)
        .pass(OptimizePass)
        .pass(EnforcePass)
        .pass(SettingsPass)
        .pass(CodegenPass)
        .build()
}

/// Local DuckDB compilation pipeline.
///
/// Skips security, check, enforce, and optimize — those are ClickHouse/multi-tenant
/// concerns. Emits DuckDB-dialect SQL via [`DuckDbCodegenPass`].
///
/// ```text
/// JSON → Validate → Normalize → Lower → DuckDbCodegen
/// ```
pub fn duckdb() -> Pipeline<LocalEnv, DuckDbState> {
    Pipeline::builder()
        .pass(ValidatePass)
        .seal(SealJson)
        .pass(NormalizePass)
        .pass(LowerPass)
        .pass(DuckDbCodegenPass)
        .build()
}
