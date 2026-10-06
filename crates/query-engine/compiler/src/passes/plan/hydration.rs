use crate::error::{QueryError, Result};
use crate::input::*;
use orbit_utils::traversal_path::{TraversalPath, prune_to_leaves};

use super::context::PlanningContext;
use super::physical::{PhysicalPlan, PhysicalSource};
use super::requirements::{Column, OutputValue, Predicate, PrefixPaths, Projection, id_list};
use super::{Hydration, Plan};
use query_data_model::QueryDataModel;

const PREFIX_SET_PATH_THRESHOLD: usize = 256;

#[derive(Clone, Copy, Debug, Default)]
pub struct HydrationCompileOptions {
    pub dynamic: bool,
    pub path_segment_budget: Option<usize>,
}

fn path_filter(paths: &[TraversalPath], options: HydrationCompileOptions) -> Option<PrefixPaths> {
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
            PrefixPaths::Set(leaves)
        } else {
            PrefixPaths::Union(leaves)
        },
    )
}

pub(super) fn plan_hydration<M: QueryDataModel + ?Sized>(
    mut context: PlanningContext<'_, M>,
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
            let alias = &node.id;
            let mut predicates = Vec::new();
            if let Some(paths) = path_filter(&node.traversal_paths, options) {
                predicates.push(Predicate::PathPrefixes {
                    column: Column::new(alias, ontology::TRAVERSAL_PATH_COLUMN),
                    paths,
                });
            }
            if !node.node_ids.is_empty() {
                predicates.push(id_list(alias, &node.id_property, &node.node_ids));
            }
            let properties = columns
                .iter()
                .map(|name| Column::new(alias, name))
                .collect();
            let projected_columns = std::iter::once(node.id_property.clone())
                .chain(columns)
                .collect::<Vec<_>>();
            Ok(PhysicalPlan {
                source: PhysicalSource::current_rows(
                    &mut context.bindings,
                    model,
                    table,
                    alias,
                    &projected_columns,
                    predicates,
                )?,
                outputs: vec![
                    Projection::new(
                        OutputValue::Column(Column::new(alias, &node.id_property)),
                        format!("{alias}_{}", node.id_property),
                    ),
                    Projection::new(
                        OutputValue::Text(entity.clone()),
                        format!("{alias}_entity_type"),
                    ),
                    Projection::new(
                        OutputValue::Properties(properties),
                        format!("{alias}_props"),
                    ),
                ],
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(context.finish(Hydration {
        nodes: hydration_nodes,
    }))
}
