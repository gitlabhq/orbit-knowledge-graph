//! Neighbors: the center node, the edge tables and relationship kinds per direction, and whether the
//! center's filters are answered by denormalized edge columns.

use ontology::constants::*;
use query_data_model::{DenormalizedDirection, OrbitQueryModel, RelationshipId};

use super::EdgeTableConfig;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::filters::{BoundFilter, lowerable_denorm};
use crate::passes::plan::{NodePlan, Plan, denormalized_facts};

pub struct Neighbors {
    pub center: String,
    pub direction: Direction,
    pub edge: EdgeTableConfig,
    pub has_non_denorm: bool,
    pub fused_table: Option<String>,
    pub outgoing_kinds: Vec<String>,
    pub incoming_kinds: Vec<String>,
    pub incoming_scope_preserving: Vec<String>,
    pub incoming_cross_scope: Vec<String>,
    pub outgoing_neighbor_paths: Vec<(String, String)>,
    pub incoming_neighbor_paths: Vec<(String, String)>,
}

pub(crate) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
) -> Result<Plan<Neighbors>> {
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;
    let [center_node] = input.nodes.as_slice() else {
        return Err(QueryError::Lowering(
            "neighbors query requires exactly one node".into(),
        ));
    };
    let center_entity = center_node
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("neighbors center has no entity".into()))?;
    let center_entity_id = model
        .entity(center_entity)
        .map(|entity| entity.id)
        .ok_or_else(|| QueryError::Lowering("neighbors center entity is unknown".into()))?;
    let center = NodePlan::resolve(center_node, model)?;
    let denormalized = denormalized_facts(input, model);

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
    let neighbor_paths = |source: bool| {
        let graph = model.graph();
        let mut pairs = Vec::new();
        for relationship in &relationships {
            for route in model.relationship_routes(relationship) {
                for variant in route.variants() {
                    let (center, neighbor) = match source {
                        true => (variant.source, variant.target),
                        false => (variant.target, variant.source),
                    };
                    if center != center_entity_id {
                        continue;
                    }
                    let source_name = &graph.entity(variant.source).name;
                    let target_name = &graph.entity(variant.target).name;
                    let owned = model
                        .variant_scope(relationship, source_name, target_name)
                        .is_some_and(|scope| match source {
                            true => scope.edge_path_is_target_path(),
                            false => scope.edge_path_is_source_path(),
                        });
                    if owned {
                        pairs.push((
                            relationship.to_string(),
                            graph.entity(neighbor).name.clone(),
                        ));
                    }
                }
            }
        }
        pairs
    };
    let outgoing_relationships = relationship_ids(true);
    let incoming_relationships = relationship_ids(false);
    let direction = match (
        config.direction,
        outgoing_relationships.is_empty(),
        incoming_relationships.is_empty(),
    ) {
        (Direction::Both, false, true) => Direction::Outgoing,
        (Direction::Both, true, false) => Direction::Incoming,
        (direction, _, _) => direction,
    };
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
    let covered = |filter: &BoundFilter,
                   direction: DenormalizedDirection,
                   relationships: &[RelationshipId]| {
        relationships.is_empty()
            || lowerable_denorm(filter, direction, &denormalized).is_some_and(|facts| {
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
    let has_non_denorm = center.id_range.is_some()
        || center.filters.iter().any(|(_, filter)| {
            (direction != Direction::Incoming
                && !covered(
                    filter,
                    DenormalizedDirection::Source,
                    &outgoing_relationships,
                ))
                || (direction != Direction::Outgoing
                    && !covered(
                        filter,
                        DenormalizedDirection::Target,
                        &incoming_relationships,
                    ))
        });

    let mut edge = EdgeTableConfig::from_model(model, &config.rel_types);
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
    let mut tables = edge.outgoing_tables.clone();
    tables.extend(edge.incoming_tables.iter().cloned());
    tables.sort();
    tables.dedup();
    let fused_table = (direction == Direction::Both
        && !has_non_denorm
        && center.node_ids.len() != 1
        && center.uses_default_pk()
        && tables.len() == 1)
        .then(|| tables.remove(0));

    let mut plan = Plan::new(Neighbors {
        center: center.alias.clone(),
        direction,
        edge,
        has_non_denorm,
        fused_table,
        outgoing_kinds: arm_kinds(true),
        incoming_kinds: arm_kinds(false),
        incoming_scope_preserving,
        incoming_cross_scope,
        outgoing_neighbor_paths: neighbor_paths(true),
        incoming_neighbor_paths: neighbor_paths(false),
    });
    plan.node_edge_mappings.insert(
        center.alias.clone(),
        ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
    );
    plan.nodes.insert(center.alias.clone(), center);
    plan.denormalized = denormalized;
    Ok(plan)
}
