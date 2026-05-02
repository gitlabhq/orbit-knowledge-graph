//! Pipeline definitions: environments, state, capability traits, and presets.

use std::sync::Arc;

use compiler_pipeline_macros::{PipelineEnv, PipelineState};
use gkg_server_config::QueryConfig;
use ontology::Ontology;

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
    pub query_config: QueryConfig,
    pub hydration_plan: HydrationPlan,
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
    /// Local/CLI execution has no multi-tenant security boundary; running
    /// as admin lets `HydratePlanPass` emit unrestricted column specs.
    security_ctx: SecurityContext,
}

impl LocalEnv {
    /// Convenience constructor that fixes `security_ctx` to an admin context,
    /// so `admin_only` ontology fields remain accessible in local tooling.
    #[must_use]
    pub fn local(ontology: Arc<Ontology>) -> Self {
        let security_ctx = SecurityContext::new(0, vec![])
            .expect("empty traversal paths are always valid")
            .with_role(true, None);
        Self::new(ontology, security_ctx)
    }
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
    pub query_config: Option<QueryConfig>,
    pub hydration_plan: Option<HydrationPlan>,
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
    pub result_ctx: Option<ResultContext>,
    pub hydration_plan: Option<HydrationPlan>,
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
/// JSON → Validate → Normalize → Restrict → Lower → Optimize → Enforce → Deduplicate → Security → Check → HydratePlan → Settings → Codegen
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
        .pass(RestrictPass)
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

/// Skeleton-first v2 pipeline. Replaces Lower+Optimize+Deduplicate with
/// a single LowerV2 pass that produces flat edge-chain JOINs with inline
/// dedup. No CTEs for the common case.
///
/// ```text
/// JSON → Validate → Normalize → Restrict → LowerV2 → Enforce → Security → Check → HydratePlan → Settings → Codegen
/// ```
pub fn clickhouse_v2() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(ValidatePass)
        .seal(SealJson)
        .pass(NormalizePass)
        .pass(RestrictPass)
        .pass(LowerV2Pass)
        .pass(EnforceV2Pass)
        .pass(SecurityPass)
        .pass(CheckPass)
        .pass(HydratePlanPass)
        .pass(SettingsPass)
        .pass(CodegenPass)
        .build()
}

/// v2 pipeline from pre-built Input (for tests and profiler).
///
/// ```text
/// Input → Restrict → LowerV2 → Enforce → Security → Check → HydratePlan → Settings → Codegen
/// ```
pub fn from_input_v2() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(RestrictPass)
        .pass(LowerV2Pass)
        .pass(EnforceV2Pass)
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
/// Input → Restrict → Lower → Optimize → Enforce → Deduplicate → Security → Check → HydratePlan → Settings → Codegen
/// ```
pub fn from_input() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(RestrictPass)
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
/// preventing recursive hydration. `DeduplicatePass` applies argMax dedup
/// to each UNION ALL arm so hydration reads the latest non-deleted version.
///
/// ```text
/// Input → Restrict → Lower → Optimize → Enforce → Deduplicate → Settings → Codegen
/// ```
pub fn hydration() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(RestrictPass)
        .pass(LowerPass)
        .pass(OptimizePass)
        .pass(EnforcePass)
        .pass(DeduplicatePass)
        .pass(SettingsPass)
        .pass(CodegenPass)
        .build()
}

/// Local DuckDB hydration compilation pipeline.
///
/// Compiles a programmatically-built hydration Input into DuckDB SQL.
/// No validate/normalize (input is pre-built), no security, no
/// hydrate-plan (prevents recursive hydration).
///
/// ```text
/// Input → Lower → Enforce → DuckDbCodegen
/// ```
pub fn duckdb_hydration() -> Pipeline<LocalEnv, DuckDbState> {
    Pipeline::builder()
        .pass(LowerPass)
        .pass(EnforcePass)
        .pass(DuckDbCodegenPass)
        .build()
}

/// Local DuckDB compilation pipeline.
///
/// Skips security, check, deduplicate, and optimize — those are
/// ClickHouse/multi-tenant concerns. Enforce builds the ResultContext
/// (node/edge column metadata) needed by formatters. HydratePlan
/// generates a hydration plan for the local execution pipeline.
/// Emits DuckDB-dialect SQL via [`DuckDbCodegenPass`].
///
/// ```text
/// JSON → Validate → Normalize → Lower → Enforce → HydratePlan → DuckDbCodegen
/// ```
pub fn duckdb() -> Pipeline<LocalEnv, DuckDbState> {
    Pipeline::builder()
        .pass(ValidatePass)
        .seal(SealJson)
        .pass(NormalizePass)
        .pass(LowerPass)
        .pass(EnforcePass)
        .pass(HydratePlanPass)
        .pass(DuckDbCodegenPass)
        .build()
}
