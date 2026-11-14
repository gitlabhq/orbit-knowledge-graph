//! The properties each binding returns.

use crate::passes::codegen::SqlBackend;
use query_data_model::Catalog;
use std::collections::HashMap;

use query_data_model::plan::logical::{Columns, Expr, LogicalOperation, Projection};
use query_data_model::plan::tracking::Ref;
use query_data_model::{DataType, FieldSelectivity};
use query_data_model::{Entity, EntityId, PropertyId};

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
        .filter(|property| entity.default_columns.contains(property))
}

/// The properties a binding returns: the listed ones, else the defaults or every readable property.
pub fn returned<B>(
    model: &Catalog<B>,
    entity: EntityId,
    selection: Option<&Columns>,
) -> Vec<PropertyId>
where
    B: SqlBackend,
{
    let graph = model.graph();
    match selection {
        Some(Columns::Listed(listed)) => listed.clone(),
        selection => {
            let entity = graph.entity(entity);
            let defaulted = matches!(selection, None | Some(Columns::Default));
            let properties: Vec<PropertyId> = match defaulted {
                true => defaults(entity).collect(),
                false => entity.properties.clone(),
            };
            properties
                .into_iter()
                .filter(|property| model.readable(*property))
                .filter(|property| !model.model().access().property(*property).hidden)
                .filter(|property| !model.admin_only(*property))
                .collect()
        }
    }
}

/// The first property other than the id that a row check of the entity keys rows by; the
/// row carries it in the redaction id column next to the id.
pub fn redaction_column(model: &Catalog<impl SqlBackend>, entity: EntityId) -> Option<&str> {
    model.checks(entity).iter().find_map(|check| match check {
        query_data_model::catalog::Check::Rows { id, .. } => {
            let graph = model.graph();
            (graph.entity(entity).identity != [*id]).then(|| graph.property(*id).name.as_str())
        }
        query_data_model::catalog::Check::Delegated { .. } => None,
    })
}

/// The column holding the entity's identity on the catalog's connection; an entity this
/// connection does not store has none, and no SQL plan reads it.
pub fn identity_column(
    model: &Catalog<impl SqlBackend>,
    entity: EntityId,
) -> crate::error::Result<&str> {
    model.identity_column(entity).ok_or_else(|| {
        crate::error::QueryError::Lowering(format!(
            "{} has no identity column on connection {}",
            model.graph().entity(entity).name,
            model.model().connection(model.connection()).name()
        ))
    })
}

/// Free text: a uuid, or a string its store does not declare low selectivity.
pub fn free_text(model: &Catalog<impl SqlBackend>, property: PropertyId) -> bool {
    match model.graph().property(property).data_type {
        DataType::Uuid => true,
        DataType::String => model.selectivity(property) != Some(FieldSelectivity::Low),
        _ => false,
    }
}
