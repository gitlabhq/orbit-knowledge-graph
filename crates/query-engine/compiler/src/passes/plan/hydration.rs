use std::collections::HashMap;

use crate::error::{QueryError, Result};
use crate::input::*;
use orbit_utils::traversal_path::TraversalPath;

use super::{Plan, PlanBody, Strategy};
use query_data_model::{QueryBackendCatalog, QueryDataModel};

#[derive(Clone, Copy, Debug, Default)]
pub struct HydrationCompileOptions {
    pub dynamic: bool,
    pub path_segment_budget: Option<usize>,
}

pub struct HydrationNodePlan {
    pub alias: String,
    pub table: String,
    pub entity: String,
    pub id_property: String,
    pub node_ids: Vec<i64>,
    pub columns: Vec<String>,
    /// Traversal paths extracted from the base query, used to narrow hydration
    /// scans via `startsWith(traversal_path, tp)`.
    pub traversal_paths: Vec<TraversalPath>,
    /// Table sort key (ReplacingMergeTree ORDER BY) — the dedup identity for the
    /// `LIMIT 1 BY <sort_key>` latest-row scan that replaces `FINAL`. Required.
    pub sort_key: Vec<String>,
}

pub fn plan_hydration(
    input: &Input,
    model: &(impl QueryDataModel + ?Sized),
    options: HydrationCompileOptions,
) -> Result<Plan> {
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
                .entity
                .as_deref()
                .and_then(|entity| model.graph().entity_id(entity))
                .and_then(|entity| model.query_backend().entity_table(entity))
                .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
            let entity = node
                .entity
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
            let columns = match &node.columns {
                Some(ColumnSelection::List(cols)) => cols.clone(),
                _ => vec![],
            };
            let sort_key = model
                .query_backend()
                .table_sort_key(table)
                .filter(|sk| !sk.is_empty())
                .map(<[String]>::to_vec)
                .ok_or_else(|| {
                    QueryError::Lowering(format!("hydration table {table} has no sort key"))
                })?;
            Ok(HydrationNodePlan {
                alias: node.id.clone(),
                table: table.to_string(),
                entity: entity.clone(),
                id_property: node.id_property.clone(),
                node_ids: node.node_ids.clone(),
                columns,
                traversal_paths: node.traversal_paths.clone(),
                sort_key,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Plan {
        scope_requirements: Vec::new(),
        nodes: HashMap::new(),
        hops: vec![],
        strategy: Strategy::SingleNode,
        node_edge_mappings: HashMap::new(),
        denorm_columns: HashMap::new(),
        denorm_rel_kinds: HashMap::new(),
        table_columns: HashMap::new(),
        table_sort_keys: HashMap::new(),
        body: PlanBody::Hydration {
            nodes: hydration_nodes,
            options,
        },
    })
}
