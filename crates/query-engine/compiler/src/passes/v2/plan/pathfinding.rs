//! PathFinding plan: bidirectional frontier expansion.

use std::collections::HashMap;

use ontology::constants::*;

use crate::error::{QueryError, Result};
use crate::input::*;

pub struct PathFindingPlan {
    pub start: PathEndpoint,
    pub end: PathEndpoint,
    pub max_depth: u32,
    pub forward_depth: u32,
    pub backward_depth: u32,
    pub rel_type_filter: Option<Vec<String>>,
    pub forward_first_hop_filter: Option<Vec<String>>,
    pub backward_first_hop_filter: Option<Vec<String>>,
    pub edge_tables: Vec<String>,
    pub scoped_by_tp: bool,
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    pub cursor: Option<InputCursor>,
    pub limit: u32,
}

/// One endpoint of a path-finding query (start or end).
pub struct PathEndpoint {
    pub id: String,
    pub entity: String,
    pub table: String,
    pub node_ids: Vec<i64>,
    pub filters: HashMap<String, InputFilter>,
    pub id_range: Option<InputIdRange>,
    pub has_tp: bool,
}

pub fn plan_pathfinding(input: &Input) -> Result<PathFindingPlan> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?
        .clone();

    let start_node = input
        .nodes
        .iter()
        .find(|n| n.id == path.from)
        .ok_or_else(|| QueryError::Lowering(format!("start node '{}' not found", path.from)))?;
    let end_node = input
        .nodes
        .iter()
        .find(|n| n.id == path.to)
        .ok_or_else(|| QueryError::Lowering(format!("end node '{}' not found", path.to)))?;

    let start_entity = start_node
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("start node has no entity".into()))?
        .to_string();
    let end_entity = end_node
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("end node has no entity".into()))?
        .to_string();

    let start_table = start_node
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", start_node.id)))?
        .to_string();
    let end_table = end_node
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", end_node.id)))?
        .to_string();

    let wildcard_path = path.rel_types.is_empty();
    let rel_type_filter = make_type_filter(&path.rel_types);
    let forward_first_hop_types = if wildcard_path {
        path.forward_first_hop_rel_types.clone()
    } else {
        path.rel_types.clone()
    };
    let backward_first_hop_types = if wildcard_path {
        path.backward_first_hop_rel_types.clone()
    } else {
        path.rel_types.clone()
    };
    let forward_first_hop_filter = make_type_filter(&forward_first_hop_types);
    let backward_first_hop_filter = make_type_filter(&backward_first_hop_types);

    let max_depth = path.max_depth;
    let forward_depth = max_depth.div_ceil(2);
    let backward_depth = max_depth / 2;

    let edge_tables = input.compiler.resolve_edge_tables(&path.rel_types);

    let start_has_tp = input
        .compiler
        .table_has_column(&start_table, TRAVERSAL_PATH_COLUMN);
    let end_has_tp = input
        .compiler
        .table_has_column(&end_table, TRAVERSAL_PATH_COLUMN);

    let scoped_by_tp = can_scope_by_tp(start_has_tp, end_has_tp, &edge_tables, |t, c| {
        input.compiler.table_has_column(t, c)
    });

    let denorm_columns = input.compiler.denormalized_columns.clone();

    Ok(PathFindingPlan {
        start: PathEndpoint {
            id: start_node.id.clone(),
            entity: start_entity,
            table: start_table,
            node_ids: start_node.node_ids.clone(),
            filters: start_node.filters.clone(),
            id_range: start_node.id_range.clone(),
            has_tp: start_has_tp,
        },
        end: PathEndpoint {
            id: end_node.id.clone(),
            entity: end_entity,
            table: end_table,
            node_ids: end_node.node_ids.clone(),
            filters: end_node.filters.clone(),
            id_range: end_node.id_range.clone(),
            has_tp: end_has_tp,
        },
        max_depth,
        forward_depth,
        backward_depth,
        rel_type_filter,
        forward_first_hop_filter,
        backward_first_hop_filter,
        edge_tables,
        scoped_by_tp,
        denorm_columns,
        cursor: input.cursor,
        limit: input.limit,
    })
}

fn make_type_filter(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() {
        None
    } else {
        Some(types.to_vec())
    }
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
