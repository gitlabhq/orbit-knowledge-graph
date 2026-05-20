//! Compiler passes.

pub mod check;
pub mod codegen;
pub mod enforce;
pub mod hydrate;
pub mod lower;
pub mod normalize;
pub mod plan;
pub mod restrict;
pub mod security;
pub mod settings;
pub mod shared;
pub mod validate;

use crate::ast::Node;
use crate::error::Result;
use crate::input::Input;
use crate::pipeline::{CompilerPass, PipelineEnv, PipelineState};
use crate::pipelines::HasQueryPlan;
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

pub struct PlannerPass;

impl<E, S> CompilerPass<E, S> for PlannerPass
where
    E: PipelineEnv,
    S: PipelineState + HasInput + HasQueryPlan,
{
    const NAME: &'static str = "plan";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let input = state.input_mut()?;
        let plan = plan::plan(input)?;
        state.set_query_plan(plan);
        Ok(())
    }
}

pub struct LowerPass;

impl<E, S> CompilerPass<E, S> for LowerPass
where
    E: PipelineEnv,
    S: PipelineState + HasInput + HasQueryPlan + HasNode,
{
    const NAME: &'static str = "lower";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let plan = state.take_query_plan()?;
        let node = {
            let input = state.input()?;
            lower::emit(&plan, input)?
        };
        state.set_query_plan(plan);
        state.set_node(node);
        Ok(())
    }
}

pub struct EnforcePass;

impl<E, S> CompilerPass<E, S> for EnforcePass
where
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput + HasQueryPlan + HasResultCtx,
{
    const NAME: &'static str = "enforce";

    fn run(&self, _env: &E, state: &mut S) -> Result<()> {
        let plan = state.take_query_plan()?;
        let node_edge_col = plan.node_edge_mappings();
        state.set_query_plan(plan);
        let mut node = state.take_node()?;
        let input = state.input()?;
        let result_context = enforce::enforce_return(&mut node, input, &node_edge_col)?;
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
    S: PipelineState + HasInput + HasNode + HasQueryConfig + HasQueryPlan,
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

        // Enable DP join reordering for queries with 3+ edge hops.
        // ClickHouse's dpsize algorithm finds better join orders for
        // multi-join chains than our fixed left-to-right emit order.
        // Skipped for 1-2 hops where our selectivity reordering is
        // already optimal and dpsize can make worse choices.
        let plan = state.take_query_plan()?;
        if plan.hops.len() >= 3 {
            config.compiler_derived.join_order_algorithm = Some("dpsize".into());
        }
        state.set_query_plan(plan);

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
