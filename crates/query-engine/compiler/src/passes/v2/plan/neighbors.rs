//! Neighbors plan: single-hop edge scan for adjacent entities.

use std::collections::HashMap;

use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::super::shared::has_non_denorm_filters;
use super::{EdgeTableConfig, PlanNode, find_node};

pub struct NeighborsPlan {
    pub center: PlanNode,
    pub has_non_denorm: bool,
    pub direction: Direction,
    pub edge: EdgeTableConfig,
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub order_by: Option<InputOrderBy>,
    pub cursor: Option<InputCursor>,
    pub limit: u32,
}

pub fn plan_neighbors(input: &Input) -> Result<NeighborsPlan> {
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = find_node(input, &config.node)?;
    let center = PlanNode::from_input(center_node)?;

    let has_non_denorm = has_non_denorm_filters(
        &center.entity,
        &center.filters,
        &input.compiler.denormalized_columns,
    ) || center.id_range.is_some();

    let edge = EdgeTableConfig::from_input(&input.compiler, &config.rel_types);

    let node_edge_mappings = HashMap::from([(
        center.id.clone(),
        ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
    )]);

    Ok(NeighborsPlan {
        center,
        has_non_denorm,
        direction: config.direction,
        edge,
        denorm_columns: input.compiler.denormalized_columns.clone(),
        node_edge_mappings,
        order_by: input.order_by.clone(),
        cursor: input.cursor,
        limit: input.limit,
    })
}
