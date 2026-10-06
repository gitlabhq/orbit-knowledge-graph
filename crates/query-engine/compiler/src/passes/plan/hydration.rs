use crate::error::{QueryError, Result};
use crate::input::*;
use orbit_utils::traversal_path::{TraversalPath, prune_to_leaves};

use super::context::PlanningContext;
use super::physical::PhysicalPlan;
use super::requirements::{OutputValue, Predicate, PrefixPaths, id_list};
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
            let scope = context.child_scope(context.bindings.root())?;
            let scan_scope = if model.table(table).is_some_and(|table| {
                *table.row_semantics() == query_data_model::storage::RowSemantics::Current
            }) {
                scope
            } else {
                context.child_scope(scope)?
            };
            let scan = context.scan(scan_scope, table, alias, false, None)?;
            let relation = scan.relation();
            let mut predicates = Vec::new();
            if let Some(paths) = path_filter(&node.traversal_paths, options) {
                predicates.push(Predicate::PathPrefixes {
                    column: context.column(relation, ontology::TRAVERSAL_PATH_COLUMN)?,
                    paths,
                });
            }
            if !node.node_ids.is_empty() {
                predicates.push(id_list(
                    context.column(relation, &node.id_property)?,
                    &node.node_ids,
                ));
            }
            let projected_columns = std::iter::once(node.id_property.clone())
                .chain(columns.iter().cloned())
                .collect::<Vec<_>>();
            let source =
                context.current_rows(scope, scan, table, alias, &projected_columns, predicates)?;
            let relation = source.relation();
            let properties = columns
                .iter()
                .map(|name| Ok((name.clone(), context.column(relation, name)?)))
                .collect::<Result<_>>()?;
            Ok(PhysicalPlan {
                scope,
                source,
                outputs: vec![
                    context.projection(
                        scope,
                        OutputValue::Column(context.column(relation, &node.id_property)?),
                        format!("{alias}_{}", node.id_property),
                    )?,
                    context.projection(
                        scope,
                        OutputValue::Text(entity.clone()),
                        format!("{alias}_entity_type"),
                    )?,
                    context.projection(
                        scope,
                        OutputValue::Properties(properties),
                        format!("{alias}_props"),
                    )?,
                ],
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(context.finish(Hydration {
        nodes: hydration_nodes,
    }))
}
