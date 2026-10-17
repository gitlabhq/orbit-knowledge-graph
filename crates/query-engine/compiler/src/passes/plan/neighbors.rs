use std::collections::BTreeMap;

use ontology::{EdgeVariantScope, constants::*};

use crate::error::Result;
use crate::input::*;

use super::context::PlanningContext;
use super::{EdgeTableConfig, NeighborArm, Neighbors, Plan};
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
    let outgoing_relationships = relationship_ids(true);
    let incoming_relationships = relationship_ids(false);
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
    let filter_pinned = has_non_denorm && center_np.has_traversal_path;
    let pinned = filter_pinned || (!center_np.node_ids.is_empty() && center_np.has_traversal_path);

    let anchors_center =
        |relationship: &str, variant: &query_data_model::RelationshipVariant, source: bool| {
            let scope = model.variant_scope(
                relationship,
                &model.graph().entity(variant.source).name,
                &model.graph().entity(variant.target).name,
            );
            scope.is_some_and(|scope| match source {
                true => {
                    filter_pinned
                        && (scope == EdgeVariantScope::PruneToSource
                            || scope.propagates_to_source(relationship, center_entity))
                }
                false => {
                    (pinned && scope.is_scope_preserving())
                        || (filter_pinned && scope == EdgeVariantScope::PruneToTarget)
                }
            })
        };
    let arms = |source: bool| -> Vec<NeighborArm> {
        let mut groups: BTreeMap<(String, bool), Vec<String>> = BTreeMap::new();
        for relationship in &relationships {
            for route in model.relationship_routes(relationship) {
                let mut variants = route
                    .variants()
                    .filter(|variant| match source {
                        true => variant.source == center_entity_id,
                        false => variant.target == center_entity_id,
                    })
                    .peekable();
                if variants.peek().is_none() {
                    continue;
                }
                let anchored =
                    variants.all(|variant| anchors_center(relationship, variant, source));
                groups
                    .entry((route.table.to_string(), anchored))
                    .or_default()
                    .push(relationship.to_string());
            }
        }
        groups
            .into_iter()
            .map(|((table, anchored), kinds)| NeighborArm {
                table,
                kinds,
                anchored,
            })
            .collect()
    };
    let tables_of = |arms: &[NeighborArm]| -> Vec<String> {
        let mut tables: Vec<String> = arms.iter().map(|arm| arm.table.clone()).collect();
        tables.dedup();
        tables
    };
    let fallback = |tables: &[String]| -> Vec<NeighborArm> {
        tables
            .iter()
            .map(|table| NeighborArm {
                table: table.clone(),
                kinds: config.rel_types.clone(),
                anchored: false,
            })
            .collect()
    };

    let mut edge = EdgeTableConfig::from_model(model, &config.rel_types);
    let mut outgoing = arms(true);
    let mut incoming = arms(false);
    if outgoing.is_empty() {
        outgoing = fallback(&edge.outgoing_tables);
    } else {
        edge.outgoing_tables = tables_of(&outgoing);
    }
    if incoming.is_empty() {
        incoming = fallback(&edge.incoming_tables);
    } else {
        edge.incoming_tables = tables_of(&incoming);
    }
    if config.direction == Direction::Outgoing {
        incoming.clear();
    }
    if config.direction == Direction::Incoming {
        outgoing.clear();
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
        && center_np.node_ids.len() != 1
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
        outgoing,
        incoming,
    }))
}
