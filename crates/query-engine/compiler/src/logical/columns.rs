//! The properties each binding returns.

use crate::passes::codegen::RelationalModel;
use std::collections::HashMap;

use query_data_model::plan::logical::{Columns, Expr, LogicalOperation, Projection};
use query_data_model::plan::tracking::Ref;
use query_data_model::{DataType, FieldSelectivity};
use query_data_model::{Entity, EntityId, GraphCatalog, PropertyId};

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

/// An entity's declared default properties, in property order.
pub fn defaults(entity: &Entity) -> impl Iterator<Item = PropertyId> + '_ {
    entity
        .properties
        .iter()
        .copied()
        .filter(|property| entity.defaults.contains(property))
}

/// The properties a binding returns: the listed ones, else the defaults or every readable property.
pub fn returned<M>(model: &M, entity: EntityId, selection: Option<&Columns>) -> Vec<String>
where
    M: RelationalModel,
{
    let catalog = model.catalog();
    let graph = catalog.graph();
    let properties: Vec<PropertyId> = match selection {
        Some(Columns::Listed(listed)) => listed.clone(),
        selection => {
            let entity = graph.entity(entity);
            let defaulted = matches!(selection, None | Some(Columns::Default));
            let properties: Vec<PropertyId> = match defaulted && !entity.defaults.is_empty() {
                true => defaults(entity).collect(),
                false => entity.properties.clone(),
            };
            properties
                .into_iter()
                .filter(|property| catalog.readable(*property))
                .filter(|property| !catalog.model().access().property(*property).hidden)
                .filter(|property| !catalog.admin_only(*property))
                .collect()
        }
    };
    properties
        .into_iter()
        .map(|property| graph.property(property).name.clone())
        .collect()
}

/// The property that identifies an entity's rows to redaction, when the entity is redacted.
pub fn redaction_column(model: &impl RelationalModel, entity: EntityId) -> Option<&str> {
    let catalog = model.catalog();
    let redaction = catalog.redaction(entity)?;
    Some(&catalog.graph().property(redaction.id).name)
}

/// Free text: a uuid, or a string not declared low selectivity.
pub fn free_text(graph: &GraphCatalog, property: PropertyId) -> bool {
    let property = graph.property(property);
    match property.data_type {
        DataType::Uuid => true,
        DataType::String => property.selectivity != FieldSelectivity::Low,
        _ => false,
    }
}
