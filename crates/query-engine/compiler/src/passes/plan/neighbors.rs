use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::context::PlanningContext;
use super::helpers::has_non_denorm_filters;
use super::{EdgeTableConfig, Neighbors, Plan};
use query_data_model::QueryDataModel;

pub(super) fn plan_neighbors<M>(mut context: PlanningContext<'_, M>) -> Result<Plan<Neighbors>>
where
    M: QueryDataModel + ?Sized,
{
    let input = context.input;
    let model = context.model;
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("neighbors config missing".into()))?;

    let [center_node] = input.nodes.as_slice() else {
        return Err(crate::error::QueryError::Lowering(
            "neighbors query requires exactly one node".into(),
        ));
    };
    let center_alias = center_node.id.clone();
    let center_entity = center_node.entity.as_deref().ok_or_else(|| {
        crate::error::QueryError::Lowering("neighbors center has no entity".into())
    })?;
    let center_entity_id = model
        .entity(center_entity)
        .map(|entity| entity.id)
        .ok_or_else(|| {
            crate::error::QueryError::Lowering("neighbors center entity is unknown".into())
        })?;

    let center_np = context.resolve_node(center_node)?;

    context.denormalized = super::denormalized_facts(input, model);
    let has_non_denorm = has_non_denorm_filters(&center_np.filters, &context.denormalized)
        || center_np.id_range.is_some();

    let mut edge = EdgeTableConfig::from_model(model, &config.rel_types);
    {
        let relationships: Vec<&str> = if config.rel_types.is_any() {
            model
                .graph()
                .relationships()
                .map(|relationship| relationship.name.as_str())
                .collect()
        } else {
            config.rel_types.iter().map(String::as_str).collect()
        };
        let tables_for = |source: bool| -> Vec<String> {
            let mut tables: Vec<String> = relationships
                .iter()
                .filter_map(|relationship| model.relationship_route(relationship))
                .filter(|route| match source {
                    true => route.has_source(center_entity_id),
                    false => route.has_target(center_entity_id),
                })
                .map(|route| route.table.to_string())
                .collect();
            tables.sort();
            tables.dedup();
            tables
        };
        let outgoing = tables_for(true);
        let incoming = tables_for(false);
        if !outgoing.is_empty() {
            edge.outgoing_tables = outgoing;
        }
        if !incoming.is_empty() {
            edge.incoming_tables = incoming;
        }
    }

    context.node_edge_mappings.insert(
        center_alias.clone(),
        ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
    );
    let mut tables = edge.outgoing_tables.clone();
    tables.extend(edge.incoming_tables.iter().cloned());
    tables.sort();
    tables.dedup();
    let fused_table = (config.direction == Direction::Both
        && !has_non_denorm
        && center_np.uses_default_pk()
        && tables.len() == 1)
        .then(|| tables.remove(0));
    context.nodes.insert(center_alias.clone(), center_np);
    Ok(context.finish(Neighbors {
        center: center_alias,
        direction: config.direction,
        edge,
        has_non_denorm,
        fused_table,
        center_tp_lookup: center_node
            .entity
            .as_deref()
            .and_then(|entity| model.traversal_path_lookup(entity, ontology::TraversalPathKind::Id))
            .map(|(table, column)| (table.to_string(), column.to_string())),
    }))
}
