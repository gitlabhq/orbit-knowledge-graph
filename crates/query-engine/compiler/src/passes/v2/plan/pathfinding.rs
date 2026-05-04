//! PathFinding plan: bidirectional frontier expansion.

use std::collections::HashMap;

use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::{EdgeTableConfig, PlanNode, find_node};

pub struct PathFindingPlan {
    pub start: PlanNode,
    pub end: PlanNode,
    pub max_depth: u32,
    pub forward_depth: u32,
    pub backward_depth: u32,
    pub edge: EdgeTableConfig,
    pub forward_first_hop_filter: Option<Vec<String>>,
    pub backward_first_hop_filter: Option<Vec<String>>,
    pub scoped_by_tp: bool,
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    pub cursor: Option<InputCursor>,
    pub limit: u32,
}

pub fn plan_pathfinding(input: &Input) -> Result<PathFindingPlan> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("path config missing".into()))?;

    let start_node = find_node(input, &path.from)?;
    let end_node = find_node(input, &path.to)?;

    let start = PlanNode::from_input(start_node)?;
    let end = PlanNode::from_input(end_node)?;

    let scoped_by_tp = start.has_traversal_path && end.has_traversal_path;

    let edge = EdgeTableConfig::from_input(&input.compiler, &path.rel_types);

    let forward_first_hop_filter =
        super::super::shared::rel_kind_filter_values(&path.forward_first_hop_rel_types);
    let backward_first_hop_filter =
        super::super::shared::rel_kind_filter_values(&path.backward_first_hop_rel_types);

    let max_depth = path.max_depth;
    let forward_depth = max_depth / 2 + max_depth % 2;
    let backward_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

    Ok(PathFindingPlan {
        start,
        end,
        max_depth,
        forward_depth,
        backward_depth,
        edge,
        forward_first_hop_filter,
        backward_first_hop_filter,
        scoped_by_tp,
        denorm_columns: input.compiler.denormalized_columns.clone(),
        cursor: input.cursor,
        limit: input.limit,
    })
}

fn can_scope_by_tp(
    start_has_tp: bool,
    end_has_tp: bool,
    edge_tables: &[String],
    table_has_column: impl Fn(&str, &str) -> bool,
) -> bool {
    if edge_tables.is_empty()
        || edge_tables
            .iter()
            .any(|t| !table_has_column(t, TRAVERSAL_PATH_COLUMN))
    {
        return false;
    }
    start_has_tp && end_has_tp
}
