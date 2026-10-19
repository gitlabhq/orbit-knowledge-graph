use orbit_utils::traversal_path::{TraversalPath, prune_to_leaves};
use query_data_model::OrbitQueryModel;

use super::{Hydration, Plan, sort_key};
use crate::error::{QueryError, Result};
use crate::input::*;

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

pub(super) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
    options: HydrationCompileOptions,
) -> Result<Plan<Hydration>> {
    if input.nodes.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }
    let nodes = input
        .nodes
        .iter()
        .map(|node| {
            let entity = node
                .entity
                .as_deref()
                .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
            let table = model
                .entity_table(entity)
                .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
            let columns = match &node.columns {
                Some(ColumnSelection::List(cols)) => cols.clone(),
                _ => vec![],
            };
            Ok(HydrationNodePlan {
                alias: node.id.clone(),
                table: table.to_string(),
                entity: entity.to_string(),
                id_property: node.id_property.clone(),
                node_ids: node.node_ids.clone(),
                columns,
                path_filter: path_filter(&node.traversal_paths, options),
                sort_key: sort_key(model, table)?.to_vec(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Plan::new(Hydration { nodes }))
}
