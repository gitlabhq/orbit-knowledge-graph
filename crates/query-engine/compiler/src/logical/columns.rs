//! The properties each binding returns.

use crate::passes::codegen::RelationalModel;
use std::collections::HashMap;

use query_data_model::plan::logical::{Columns, Expr, LogicalOperation, Projection};
use query_data_model::plan::tracking::Ref;
use query_data_model::{EntityId, PropertyId};

use super::Query;

/// The first graph projection of each binding, through the group outputs that stand for a node.
pub fn selections<'p>(query: Query<'p>) -> HashMap<Ref, &'p Columns> {
    let mut nodes = HashMap::new();
    for (node, operation) in query.operations() {
        if let LogicalOperation::Aggregate { groups, .. } = operation {
            for (group, output) in groups.iter().zip(query.plan.outputs(node)) {
                if let Expr::Node(binding) = group {
                    nodes.insert(output, *binding);
                }
            }
        }
    }
    let mut selected = HashMap::new();
    for item in query.selected() {
        if let Projection::Graph { binding, columns } = item {
            let binding = nodes.get(binding).copied().unwrap_or(*binding);
            selected.entry(binding).or_insert(columns);
        }
    }
    selected
}

/// The properties a binding returns: the listed ones, else the defaults or every readable property.
pub fn returned<M>(model: &M, entity: EntityId, selection: Option<&Columns>) -> Vec<String>
where
    M: RelationalModel,
{
    let graph = model.graph();
    let properties: Vec<PropertyId> = match selection {
        Some(Columns::Listed(listed)) => listed.clone(),
        selection => {
            let defaults = model.default_properties(entity);
            let defaulted = matches!(selection, None | Some(Columns::Default));
            let properties = match defaulted && !defaults.is_empty() {
                true => defaults,
                false => &graph.entity(entity).properties,
            };
            properties
                .iter()
                .copied()
                .filter(|property| model.property_realization(*property).is_some())
                .filter(|property| !model.property_is_hidden(*property))
                .filter(|property| !model.property_is_admin_only(*property))
                .collect()
        }
    };
    properties
        .into_iter()
        .map(|property| graph.property(property).name.clone())
        .collect()
}
