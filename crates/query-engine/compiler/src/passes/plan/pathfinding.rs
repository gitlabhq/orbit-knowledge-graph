use std::collections::HashMap;

use crate::error::Result;
use crate::input::*;

use super::{EdgeTableConfig, NodePlan, PathFindingBody, Plan, PlanBody, Strategy, find_node};
use query_data_model::QueryDataModel;

pub fn plan_pathfinding<M>(input: &Input, model: &M) -> Result<Plan>
where
    M: QueryDataModel + ?Sized,
{
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("path config missing".into()))?;

    let start_node = find_node(input, &path.from)?;
    let end_node = find_node(input, &path.to)?;
    let start_alias = start_node.id.clone();
    let end_alias = end_node.id.clone();

    let start_np = node_plan_from(start_node, model)?;
    let end_np = node_plan_from(end_node, model)?;

    let scoped_by_tp = start_np.has_traversal_path && end_np.has_traversal_path;
    let edge = EdgeTableConfig::from_model(model, &path.rel_types);

    let endpoint_kinds = |entity: &str, source: bool| {
        let relationships = if source {
            model.graph().relationship_names(Some(entity), None)
        } else {
            model.graph().relationship_names(None, Some(entity))
        };
        crate::passes::shared::rel_kind_filter_values(&relationships)
    };
    let forward_first_hop_filter = start_node
        .entity
        .as_deref()
        .and_then(|entity| endpoint_kinds(entity, true));
    let backward_first_hop_filter = end_node
        .entity
        .as_deref()
        .and_then(|entity| endpoint_kinds(entity, false));

    let max_depth = path.max_depth;
    let forward_depth = max_depth / 2 + max_depth % 2;
    let backward_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

    let mut nodes = HashMap::new();
    nodes.insert(start_alias.clone(), start_np);
    nodes.insert(end_alias.clone(), end_np);

    Ok(Plan {
        scope_requirements: Vec::new(),
        nodes,
        hops: vec![],
        strategy: Strategy::SingleNode,
        node_edge_mappings: HashMap::new(),
        denorm_columns: HashMap::new(),
        denorm_rel_kinds: HashMap::new(),
        table_columns: HashMap::new(),
        table_sort_keys: HashMap::new(),
        body: PlanBody::PathFinding(PathFindingBody {
            start: start_alias,
            end: end_alias,
            max_depth,
            forward_depth,
            backward_depth,
            edge,
            forward_first_hop_filter,
            backward_first_hop_filter,
            scoped_by_tp,
        }),
    })
}

fn node_plan_from<M>(node: &InputNode, model: &M) -> Result<NodePlan>
where
    M: QueryDataModel + ?Sized,
{
    NodePlan::from_input(node, model, false)
        .ok_or_else(|| crate::error::QueryError::Lowering("path node entity is unknown".into()))
}
