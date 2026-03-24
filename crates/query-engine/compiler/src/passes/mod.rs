//! Compiler passes.

pub mod check;
pub mod codegen;
pub mod enforce;
pub mod hydrate;
pub mod lower;
pub mod normalize;
pub mod optimize;
pub mod security;
pub mod validate;

use crate::error::Result;
use crate::input::Input;
use crate::pipeline::{CompilerPass, PipelineEnv, PipelineState};
use crate::pipelines::{
    HasInput, HasJson, HasNode, HasOntology, HasOutput, HasResultCtx, HasSecurityCtx,
};

pub struct ValidatePass;

impl<E, S> CompilerPass<E, S> for ValidatePass
where
    E: PipelineEnv + HasOntology,
    S: PipelineState + HasJson + HasInput,
{
    const NAME: &'static str = "validate";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let json = state.json()?;
        let ontology = env.ontology();
        let v = validate::Validator::new(ontology);
        let value = v.check_json(json)?;
        v.check_ontology(&value)?;
        let input: Input = serde_json::from_value(value)?;
        v.check_references(&input)?;
        state.set_input(input);
        Ok(())
    }
}

pub struct NormalizePass;

impl<E, S> CompilerPass<E, S> for NormalizePass
where
    E: PipelineEnv + HasOntology,
    S: PipelineState + HasInput,
{
    const NAME: &'static str = "normalize";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let input = state.take_input()?;
        state.set_input(normalize::normalize(input, env.ontology())?);
        Ok(())
    }
}

pub struct LowerPass;

impl<E, S> CompilerPass<E, S> for LowerPass
where
    E: PipelineEnv,
    S: PipelineState + HasInput + HasNode,
{
    const NAME: &'static str = "lower";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let input = state.input_mut()?;
        let node = lower::lower(input)?;
        state.set_node(node);
        Ok(())
    }
}

pub struct OptimizePass;

impl<E, S> CompilerPass<E, S> for OptimizePass
where
    E: PipelineEnv + HasSecurityCtx,
    S: PipelineState + HasNode + HasInput,
{
    const NAME: &'static str = "optimize";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let security_ctx = env.security_ctx().clone();
        let mut node = state.take_node()?;
        let mut input = state.take_input()?;
        optimize::optimize(&mut node, &mut input, &security_ctx);
        state.set_node(node);
        state.set_input(input);
        Ok(())
    }
}

pub struct EnforcePass;

impl<E, S> CompilerPass<E, S> for EnforcePass
where
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput + HasResultCtx,
{
    const NAME: &'static str = "enforce";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let mut node = state.take_node()?;
        let input = state.input()?;
        let result_context = enforce::enforce_return(&mut node, input)?;
        state.set_node(node);
        state.set_result_ctx(result_context);
        Ok(())
    }
}

pub struct SecurityPass;

impl<E, S> CompilerPass<E, S> for SecurityPass
where
    E: PipelineEnv + HasSecurityCtx,
    S: PipelineState + HasNode,
{
    const NAME: &'static str = "security";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let node = state.node_mut()?;
        security::apply_security_context(node, env.security_ctx())
    }
}

pub struct CheckPass;

impl<E, S> CompilerPass<E, S> for CheckPass
where
    E: PipelineEnv + HasSecurityCtx,
    S: PipelineState + HasNode,
{
    const NAME: &'static str = "check";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let node = state.node()?;
        check::check_ast(node, env.security_ctx())
    }
}

pub struct CodegenPass;

impl<E, S> CompilerPass<E, S> for CodegenPass
where
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput + HasResultCtx + HasOutput,
{
    const NAME: &'static str = "codegen";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let result_context = state.take_result_ctx()?;
        let node = state.node()?;
        let input = state.input()?;
        let base = codegen::codegen(node, result_context)?;
        let hydration = hydrate::generate_hydration_plan(input);
        let query_type = input.query_type;
        let input = input.clone();
        state.set_output(codegen::CompiledQueryContext {
            query_type,
            base,
            hydration,
            input,
        });
        Ok(())
    }
}

pub struct HydrationCodegenPass;

impl<E, S> CompilerPass<E, S> for HydrationCodegenPass
where
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput + HasResultCtx + HasOutput,
{
    const NAME: &'static str = "hydration_codegen";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let result_context = state.take_result_ctx()?;
        let node = state.node()?;
        let input = state.input()?;
        let base = codegen::codegen(node, result_context)?;
        let query_type = input.query_type;
        let input = input.clone();
        state.set_output(codegen::CompiledQueryContext {
            query_type,
            base,
            hydration: codegen::HydrationPlan::None,
            input,
        });
        Ok(())
    }
}
