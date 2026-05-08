//! Hydration plan: fetch node properties for a set of IDs.

use std::collections::HashMap;

use crate::error::{QueryError, Result};
use crate::input::*;

use super::{Plan, PlanBody, Strategy};

pub struct HydrationNodePlan {
    pub alias: String,
    pub table: String,
    pub entity: String,
    pub id_property: String,
    pub node_ids: Vec<i64>,
    pub columns: Vec<String>,
    /// Traversal paths extracted from the base query, used to narrow hydration
    /// scans via `startsWith(traversal_path, tp)`.
    pub traversal_paths: Vec<String>,
    /// Filters from the base query pushed into the hydration scan to help
    /// skip indexes prune granules.
    pub filters: Vec<(String, InputFilter)>,
}

pub fn plan_hydration(input: &Input) -> Result<Plan> {
    if input.nodes.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }
    let hydration_nodes = input
        .nodes
        .iter()
        .map(|node| {
            let table = node
                .table
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
            let entity = node
                .entity
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
            let columns = match &node.columns {
                Some(ColumnSelection::List(cols)) => cols.clone(),
                _ => vec![],
            };
            Ok(HydrationNodePlan {
                alias: node.id.clone(),
                table: table.clone(),
                entity: entity.clone(),
                id_property: node.id_property.clone(),
                node_ids: node.node_ids.clone(),
                columns,
                traversal_paths: node.traversal_paths.clone(),
                filters: node
                    .filters
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Plan {
        nodes: HashMap::new(),
        hops: vec![],
        strategy: Strategy::SingleNode,
        limit: input.limit,
        order_by: None,
        cursor: None,
        node_edge_mappings: HashMap::new(),
        denorm_columns: HashMap::new(),
        body: PlanBody::Hydration(hydration_nodes),
    })
}
