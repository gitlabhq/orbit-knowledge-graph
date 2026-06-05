//! Neighbors plan: single-hop edge scan for adjacent entities.

use std::collections::HashMap;

use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::{
    EdgeTableConfig, HydrationStrategy, NodePlan, Plan, PlanBody, Selectivity, Strategy, find_node,
};
use crate::passes::shared::has_non_denorm_filters;

pub fn plan_neighbors(input: &Input) -> Result<Plan> {
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = find_node(input, &config.node)?;
    let center_alias = center_node.id.clone();

    let center_np = NodePlan {
        alias: center_node.id.clone(),
        entity: center_node.entity.clone(),
        table: center_node.table.clone(),
        selectivity: Selectivity::from_node(center_node),
        hydration: HydrationStrategy::Skip,
        filters: center_node
            .filters
            .iter()
            .flat_map(|(k, v)| v.iter().map(move |f| (k.clone(), f.clone())))
            .collect(),
        node_ids: center_node.node_ids.clone(),
        id_range: center_node.id_range.clone(),
        has_traversal_path: center_node.has_traversal_path,
        redaction_id_column: center_node.redaction_id_column.clone(),
        columns: center_node.columns.clone(),
        dedup_columns: vec![],
        use_narrowing: false,
        needs_elevated_filter: false,
        fk_needs_join: false,
        emit_select: true,
    };

    let has_non_denorm = has_non_denorm_filters(
        center_np.entity.as_deref().unwrap_or(""),
        &center_np.filters,
        &input.compiler.denormalized_columns,
    ) || center_np.id_range.is_some();

    let edge = EdgeTableConfig::from_input(&input.compiler, &config.rel_types);

    let node_edge_mappings = HashMap::from([(
        center_alias.clone(),
        ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
    )]);

    let mut nodes = HashMap::new();
    nodes.insert(center_alias.clone(), center_np);

    Ok(Plan {
        nodes,
        hops: vec![],
        strategy: Strategy::SingleNode,
        limit: input.limit,
        order_by: input.order_by.clone(),
        cursor: input.cursor,
        node_edge_mappings,
        denorm_columns: input.compiler.denormalized_columns.clone(),
        denorm_rel_kinds: input.compiler.denorm_rel_kinds.clone(),
        table_columns: input.compiler.table_columns.clone(),
        table_sort_keys: input.compiler.table_sort_keys.clone(),
        body: PlanBody::Neighbors {
            center: center_alias,
            direction: config.direction,
            edge,
            has_non_denorm,
        },
    })
}
