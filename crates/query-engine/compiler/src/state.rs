//! Pipeline state types and capability traits.

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::pipeline::PipelineState;
use pipeline_macros::PipelineState;

crate::define_state_capabilities! {
    pub json: String,
    pub input: Input,
    pub node: Node,
    pub result_ctx: ResultContext,
    pub output: CompiledQueryContext,
}

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
