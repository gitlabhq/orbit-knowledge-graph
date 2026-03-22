use crate::codegen::{HydrationPlan, HydrationTemplate};
use crate::input::{ColumnSelection, Input, QueryType};

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
        QueryType::Traversal => {
            let is_edge_centric = input.relationships.len() == 1
                && input.relationships.iter().all(|r| r.max_hops == 1);
            if is_edge_centric {
                HydrationPlan::Static(build_static_templates(input))
            } else {
                HydrationPlan::None
            }
        }
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
                    "id": node.id,
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
