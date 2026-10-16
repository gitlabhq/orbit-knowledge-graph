use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::context::PlanningContext;
use super::{EdgeTableConfig, Neighbors, Plan};
use query_data_model::OrbitQueryModel;

pub(super) fn plan_neighbors<M>(mut context: PlanningContext<'_, M>) -> Result<Plan<Neighbors>>
where
    M: OrbitQueryModel + ?Sized,
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
    let relationships: Vec<&str> = if config.rel_types.is_empty() {
        model
            .graph()
            .relationships()
            .map(|relationship| relationship.name.as_str())
            .collect()
    } else {
        config.rel_types.iter().map(String::as_str).collect()
    };
    let touches = |relationship: &str, source: bool| {
        model
            .relationship_routes(relationship)
            .into_iter()
            .any(|route| match source {
                true => route.has_source(center_entity_id),
                false => route.has_target(center_entity_id),
            })
    };
    let relationship_ids = |source: bool| {
        relationships
            .iter()
            .filter(|relationship| touches(relationship, source))
            .filter_map(|relationship| model.graph().relationship_id(relationship))
            .collect::<Vec<_>>()
    };
    let arm_kinds = |source: bool| {
        let kinds: Vec<String> = config
            .rel_types
            .iter()
            .filter(|kind| touches(kind, source))
            .cloned()
            .collect();
        match kinds.is_empty() {
            true => config.rel_types.clone(),
            false => kinds,
        }
    };
    let outgoing_relationships = relationship_ids(true);
    let incoming_relationships = relationship_ids(false);
    let incoming_kinds: Vec<_> = relationships
        .iter()
        .filter(|relationship| touches(relationship, false))
        .copied()
        .collect();
    let incoming_scope_preserving: Vec<_> = incoming_kinds
        .iter()
        .filter(|relationship| {
            let mut found = false;
            let mut scope_preserving = true;
            for route in model.relationship_routes(relationship) {
                for variant in route
                    .variants()
                    .filter(|variant| variant.target == center_entity_id)
                {
                    found = true;
                    scope_preserving &= model
                        .variant_scope(
                            relationship,
                            &model.graph().entity(variant.source).name,
                            &model.graph().entity(variant.target).name,
                        )
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving);
                }
            }
            found && scope_preserving
        })
        .map(|relationship| relationship.to_string())
        .collect();
    let incoming_cross_scope = incoming_kinds
        .into_iter()
        .filter(|relationship| {
            !incoming_scope_preserving
                .iter()
                .any(|kind| kind == relationship)
        })
        .map(str::to_string)
        .collect();
    let covered = |filter: &super::BoundFilter,
                   direction: query_data_model::DenormalizedDirection,
                   relationships: &[query_data_model::RelationshipId]| {
        relationships.is_empty()
            || super::helpers::lowerable_denorm(filter, direction, &context.denormalized)
                .is_some_and(|facts| {
                    relationships.iter().all(|relationship| {
                        model
                            .relationship_routes(
                                model.graph().relationship(*relationship).name.as_str(),
                            )
                            .into_iter()
                            .any(|route| {
                                route.source_entities().any(|source| {
                                    route.target_entities().any(|target| {
                                        facts.variants.contains(&(*relationship, source, target))
                                    })
                                })
                            })
                    })
                })
    };
    let has_non_denorm = center_np.id_range.is_some()
        || center_np.filters.iter().any(|(_, filter)| {
            (config.direction != Direction::Incoming
                && !covered(
                    filter,
                    query_data_model::DenormalizedDirection::Source,
                    &outgoing_relationships,
                ))
                || (config.direction != Direction::Outgoing
                    && !covered(
                        filter,
                        query_data_model::DenormalizedDirection::Target,
                        &incoming_relationships,
                    ))
        });

    let mut edge = EdgeTableConfig::from_model(model, &config.rel_types);
    {
        let tables_for = |source: bool| -> Vec<String> {
            let mut tables: Vec<String> = relationships
                .iter()
                .flat_map(|relationship| model.relationship_routes(relationship))
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
        outgoing_kinds: arm_kinds(true),
        incoming_kinds: arm_kinds(false),
        incoming_scope_preserving,
        incoming_cross_scope,
    }))
}
