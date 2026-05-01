//! Compiler passes.

pub mod check;
pub mod codegen;
pub mod deduplicate;
pub mod enforce;
pub mod hydrate;
pub mod lower;
pub mod lower_v2;
pub mod normalize;
pub mod optimize;
pub mod restrict;
pub mod security;
pub mod settings;
pub mod validate;

use crate::ast::Node;
use crate::error::Result;
use crate::input::Input;
use crate::pipeline::{CompilerPass, PipelineEnv, PipelineState};
use crate::pipelines::{
    HasHydrationPlan, HasInput, HasJson, HasNode, HasOntology, HasOutput, HasQueryConfig,
    HasResultCtx, HasSecurityCtx,
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
        let mut input: Input = serde_json::from_value(value)?;
        v.check_references(&input)?;
        v.annotate_filter_types(&mut input);
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

pub struct RestrictPass;

impl<E, S> CompilerPass<E, S> for RestrictPass
where
    E: PipelineEnv + HasOntology + HasSecurityCtx,
    S: PipelineState + HasInput,
{
    const NAME: &'static str = "restrict";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let input = state.input_mut()?;
        restrict::restrict(input, env.ontology(), env.security_ctx())
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
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput,
{
    const NAME: &'static str = "optimize";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let mut node = state.take_node()?;
        let mut input = state.take_input()?;
        optimize::optimize(&mut node, &mut input);
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
    E: PipelineEnv + HasSecurityCtx + HasOntology,
    S: PipelineState + HasNode,
{
    const NAME: &'static str = "security";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let node = state.node_mut()?;
        security::apply_security_context(node, env.security_ctx(), env.ontology())
    }
}

pub struct DeduplicatePass;

impl<E, S> CompilerPass<E, S> for DeduplicatePass
where
    E: PipelineEnv + HasOntology,
    S: PipelineState + HasNode + HasInput,
{
    const NAME: &'static str = "deduplicate";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let input = state.input()?.clone();
        let node = state.node_mut()?;
        deduplicate::deduplicate(node, &input, env.ontology());
        Ok(())
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

pub struct HydratePlanPass;

impl<E, S> CompilerPass<E, S> for HydratePlanPass
where
    E: PipelineEnv + HasOntology + HasSecurityCtx,
    S: PipelineState + HasInput + HasHydrationPlan,
{
    const NAME: &'static str = "hydrate_plan";

    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        let input = state.input()?;
        let plan = hydrate::generate_hydration_plan(input, env.ontology(), env.security_ctx());
        state.set_hydration_plan(plan);
        Ok(())
    }
}

pub struct SettingsPass;

impl<E, S> CompilerPass<E, S> for SettingsPass
where
    E: PipelineEnv,
    S: PipelineState + HasInput + HasNode + HasQueryConfig,
{
    const NAME: &'static str = "settings";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let input = state.input()?;
        let query_type: &str = input.query_type.into();
        let has_cursor = input.cursor.is_some();
        let mut config = settings::resolve(query_type, has_cursor);

        // ClickHouse 26.2+ requires `enable_materialized_cte = 1` when any
        // CTE uses the MATERIALIZED keyword (also needs enable_analyzer = 1,
        // which is the default in 26.x).
        if let Node::Query(q) = state.node()?
            && q.ctes.iter().any(|c| c.materialized)
        {
            config.compiler_derived.enable_materialized_cte = true;
        }

        state.set_query_config(config);
        Ok(())
    }
}

pub struct CodegenPass;

impl<E, S> CompilerPass<E, S> for CodegenPass
where
    E: PipelineEnv,
    S: PipelineState
        + HasNode
        + HasInput
        + HasResultCtx
        + HasQueryConfig
        + HasHydrationPlan
        + HasOutput,
{
    const NAME: &'static str = "codegen";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let result_context = state.take_result_ctx()?;
        let query_config = state.take_query_config().unwrap_or_default();
        let hydration = state
            .take_hydration_plan()
            .unwrap_or(hydrate::HydrationPlan::None);
        let node = state.node()?;
        let input = state.input()?;
        let base = codegen::codegen(node, result_context, query_config)?;
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

pub struct DuckDbCodegenPass;

impl<E, S> CompilerPass<E, S> for DuckDbCodegenPass
where
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput + HasResultCtx + HasHydrationPlan + HasOutput,
{
    const NAME: &'static str = "duckdb_codegen";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let result_context = state.take_result_ctx()?;
        let hydration = state
            .take_hydration_plan()
            .unwrap_or(hydrate::HydrationPlan::None);
        let node = state.node()?;
        let input = state.input()?;
        let base = codegen::duckdb::codegen(node, result_context)?;
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
