//! PathFinding plan: bidirectional frontier expansion.

use std::collections::HashMap;

use crate::error::Result;
use crate::input::*;

use super::{
    EdgeTableConfig, HydrationStrategy, NodePlan, PathFindingBody, Plan, PlanBody, Selectivity,
    Strategy, find_node,
};

pub fn plan_pathfinding(input: &Input) -> Result<Plan> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("path config missing".into()))?;

    let start_node = find_node(input, &path.from)?;
    let end_node = find_node(input, &path.to)?;
    let start_alias = start_node.id.clone();
    let end_alias = end_node.id.clone();

    let start_np = node_plan_from(start_node);
    let end_np = node_plan_from(end_node);

    let scoped_by_tp = start_np.has_traversal_path && end_np.has_traversal_path;
    let edge = EdgeTableConfig::from_input(&input.compiler, &path.rel_types);

    let forward_first_hop_filter =
        crate::passes::shared::rel_kind_filter_values(&path.forward_first_hop_rel_types);
    let backward_first_hop_filter =
        crate::passes::shared::rel_kind_filter_values(&path.backward_first_hop_rel_types);

    let max_depth = path.max_depth;
    let forward_depth = max_depth / 2 + max_depth % 2;
    let backward_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

    let mut nodes = HashMap::new();
    nodes.insert(start_alias.clone(), start_np);
    nodes.insert(end_alias.clone(), end_np);

    Ok(Plan {
        nodes,
        hops: vec![],
        strategy: Strategy::SingleNode,
        limit: input.limit,
        order_by: None,
        cursor: input.cursor,
        node_edge_mappings: HashMap::new(),
        denorm_columns: input.compiler.denormalized_columns.clone(),
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

fn node_plan_from(node: &InputNode) -> NodePlan {
    NodePlan {
        alias: node.id.clone(),
        entity: node.entity.clone(),
        table: node.table.clone(),
        selectivity: Selectivity::from_node(node),
        hydration: HydrationStrategy::Skip,
        filters: node.filters.clone().into_iter().collect(),
        node_ids: node.node_ids.clone(),
        id_range: node.id_range.clone(),
        has_traversal_path: node.has_traversal_path,
        redaction_id_column: node.redaction_id_column.clone(),
        columns: node.columns.clone(),
        dedup_columns: vec![],
        use_narrowing: false,
        needs_elevated_filter: false,
        fk_needs_join: false,
        emit_select: true,
    }
}
