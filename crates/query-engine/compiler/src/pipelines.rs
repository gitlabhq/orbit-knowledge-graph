//! Pipeline definitions: environments, state, capability traits, and presets.

use std::sync::Arc;

use compiler_pipeline_macros::{PipelineEnv, PipelineState};
use ontology::Ontology;

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
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
    pub output: Option<CompiledQueryContext>,
}

#[derive(PipelineState)]
pub struct DuckDbState {
    pub json: Option<String>,
    pub input: Option<Input>,
    pub node: Option<Node>,
    pub output: Option<CompiledQueryContext>,
}

// ═════════════════════════════════════════════════════════════════════════════
// Pipeline presets
// ═════════════════════════════════════════════════════════════════════════════

/// Standard ClickHouse compilation pipeline.
///
/// ```text
/// JSON → Validate → Normalize → Lower → Optimize → Enforce → Security → Check → Codegen
/// ```
pub fn clickhouse() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(ValidatePass)
        .seal(SealJson)
        .pass(NormalizePass)
        .pass(LowerPass)
        .pass(OptimizePass)
        .pass(EnforcePass)
        .seal(SealInput)
        .pass(SecurityPass)
        .pass(CheckPass)
        .pass(CodegenPass)
        .build()
}

/// Hydration pipeline — skips security and check passes.
///
/// ```text
/// Input → Normalize → Lower → Optimize → Enforce → Check → Codegen
/// ```
pub fn hydration() -> Pipeline<SecureEnv, QueryState> {
    Pipeline::builder()
        .pass(NormalizePass)
        .pass(LowerPass)
        .pass(OptimizePass)
        .pass(EnforcePass)
        .seal(SealInput)
        .pass(CheckPass)
        .pass(CodegenPass)
        .build()
}
