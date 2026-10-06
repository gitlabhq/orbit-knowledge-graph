use crate::error::{QueryError, Result};
use crate::input::*;
use orbit_utils::traversal_path::{TraversalPath, prune_to_leaves};

use super::context::PlanningContext;
use super::{Hydration, Plan};
use query_data_model::QueryDataModel;

const PREFIX_SET_PATH_THRESHOLD: usize = 256;

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
    pub path_filter: Option<HydrationPathFilter>,
    pub sort_key: Vec<String>,
}

pub enum HydrationPathFilter {
    PrefixUnion(Vec<TraversalPath>),
    PrefixSet(Vec<TraversalPath>),
}

fn path_filter(
    paths: &[TraversalPath],
    options: HydrationCompileOptions,
) -> Option<HydrationPathFilter> {
    let mut leaves = prune_to_leaves(paths);
    if leaves.is_empty() {
        return None;
    }
    if let Some(budget) = options.path_segment_budget {
        while leaves
            .iter()
            .map(|path| path.segment_count())
            .sum::<usize>()
            > budget
        {
            let parents: Vec<_> = leaves.iter().map(|path| path.parent()).collect();
            if parents == leaves {
                break;
            }
            leaves = prune_to_leaves(&parents);
        }
    }
    Some(
        if options.dynamic && leaves.len() > PREFIX_SET_PATH_THRESHOLD {
            HydrationPathFilter::PrefixSet(leaves)
        } else {
            HydrationPathFilter::PrefixUnion(leaves)
        },
    )
}

pub(super) fn plan_hydration<M: QueryDataModel + ?Sized>(
    context: PlanningContext<'_, M>,
    options: HydrationCompileOptions,
) -> Result<Plan<Hydration>> {
    let input = context.input;
    let model = context.model;
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
                .and_then(|entity| model.entity_table(entity))
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
                table: table.to_string(),
                entity: entity.clone(),
                id_property: node.id_property.clone(),
                node_ids: node.node_ids.clone(),
                columns,
                path_filter: path_filter(&node.traversal_paths, options),
                sort_key: context.latest_row_key(table)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(context.finish(Hydration {
        nodes: hydration_nodes,
    }))
}
