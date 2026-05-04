//! Hydration plan: fetch node properties for a set of IDs.

use crate::error::{QueryError, Result};
use crate::input::*;

pub struct HydrationNodePlan {
    pub alias: String,
    pub table: String,
    pub entity: String,
    pub id_property: String,
    pub node_ids: Vec<i64>,
    pub columns: Vec<String>,
}

pub struct HydrationPlan {
    pub nodes: Vec<HydrationNodePlan>,
    pub limit: u32,
}

pub fn plan_hydration(input: &Input) -> Result<HydrationPlan> {
    if input.nodes.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }
    let nodes = input
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
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(HydrationPlan {
        nodes,
        limit: input.limit,
    })
}
