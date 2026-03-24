use crate::constants::HYDRATION_NODE_ALIAS;
use crate::input::{ColumnSelection, Input, QueryType};
use crate::passes::codegen::{CompiledQueryContext, HydrationPlan, HydrationTemplate};
use crate::pipeline::{CompilerPass, PipelineEnv, PipelineState};
use crate::pipelines::{HasInput, HasNode, HasOutput, HasResultCtx};

/// Pipeline pass: codegen for hydration queries (no security/check passes).
pub struct HydrationCodegenPass;

impl<E, S> CompilerPass<E, S> for HydrationCodegenPass
where
    E: PipelineEnv,
    S: PipelineState + HasNode + HasInput + HasResultCtx + HasOutput,
{
    const NAME: &'static str = "hydration_codegen";

    fn run(&self, _env: &E, state: &mut S) -> crate::error::Result<()> {
        let result_context = state.take_result_ctx()?;
        let node = state.node()?;
        let input = state.input()?;
        let base = crate::passes::codegen::codegen(node, result_context)?;
        let query_type = input.query_type;
        let input = input.clone();
        state.set_output(CompiledQueryContext {
            query_type,
            base,
            hydration: HydrationPlan::None,
            input,
        });
        Ok(())
    }
}

/// Build the hydration context for a compiled query.
///
/// - Aggregation: no hydration (results are aggregate values, not entity rows).
/// - Search: no hydration (base query already carries node columns).
/// - Traversal (edge-centric): static hydration — entity types are known at
///   compile time, so we build one search query template per entity type.
/// - Traversal (join-based fallback): no hydration — base query already joins
///   node tables and carries their columns.
/// - PathFinding/Neighbors: dynamic hydration — entity types are discovered at
///   runtime from edge data, so the server builds search queries on the fly.
pub fn generate_hydration_plan(input: &Input) -> HydrationPlan {
    match input.query_type {
        QueryType::Aggregation | QueryType::Hydration => HydrationPlan::None,
        QueryType::PathFinding | QueryType::Neighbors => HydrationPlan::Dynamic,
        QueryType::Search => HydrationPlan::None,
        QueryType::Traversal => HydrationPlan::Static(build_static_templates(input)),
    }
}

fn build_static_templates(input: &Input) -> Vec<HydrationTemplate> {
    input
        .nodes
        .iter()
        .filter_map(|node| {
            let entity = node.entity.as_ref()?;
            let columns = match &node.columns {
                Some(ColumnSelection::List(cols)) => serde_json::json!(cols),
                Some(ColumnSelection::All) => serde_json::json!("*"),
                None => serde_json::json!(null),
            };
            let mut query = serde_json::json!({
                "query_type": "search",
                "node": {
                    "id": HYDRATION_NODE_ALIAS,
                    "entity": entity,
                },
                "limit": 1000
            });
            if !columns.is_null() {
                query["node"]["columns"] = columns;
            }
            Some(HydrationTemplate {
                entity_type: entity.clone(),
                node_alias: node.id.clone(),
                query_json: query.to_string(),
            })
        })
        .collect()
}
