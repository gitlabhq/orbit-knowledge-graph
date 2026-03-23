use crate::codegen::{CompiledQueryContext, HydrationPlan, HydrationTemplate};
use crate::constants::HYDRATION_NODE_ALIAS;
use crate::input::{ColumnSelection, Input, QueryType};
use crate::pipeline::{CompilerContext, CompilerPass, Emitted, Enforced};

/// Pipeline pass: codegen for hydration queries.
///
/// Accepts `Enforced` directly, skipping security and check passes.
/// Hydration queries are internal-only and operate on pre-authorized IDs.
pub struct HydrationCodegenPass;

impl CompilerPass for HydrationCodegenPass {
    const NAME: &'static str = "hydration_codegen";
    type In = Enforced;
    type Out = Emitted;

    fn run(&self, ctx: &mut CompilerContext<Enforced>) -> crate::error::Result<()> {
        let node = ctx.node.as_ref().expect("node must exist");
        let result_context = ctx
            .result_context
            .take()
            .expect("result_context must exist");
        let base = crate::codegen::codegen(node, result_context)?;
        let query_type = ctx.input.query_type;
        let input = ctx.input.clone();
        ctx.output = Some(CompiledQueryContext {
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
