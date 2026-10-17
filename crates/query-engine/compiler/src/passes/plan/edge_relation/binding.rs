use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::conditions::{node_has_uncovered_filter, node_is_relation_source};
use super::{EdgeRelation, NodePlan, NodeRelationSource};
use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::helpers::{FilterOwner, ordered_filters};
use crate::passes::plan::{DenormalizedKey, DenormalizedProperty};

pub(super) fn linearize_relations(
    relations: &mut [EdgeRelation],
    nodes: &HashMap<String, NodePlan>,
) -> Result<()> {
    if relations.is_empty() {
        return Ok(());
    }
    let first = relations
        .iter()
        .enumerate()
        .min_by_key(|(index, relation)| (Reverse(relation_selectivity(relation, nodes)), *index))
        .map(|(index, _)| index)
        .unwrap_or_default();
    relations.swap(0, first);
    let first = &relations[0];
    let mut reached = HashSet::from([first.from_node.clone(), first.to_node.clone()]);
    for index in 1..relations.len() {
        let last = &relations[index - 1];
        let connected_to_last = |candidate: usize| {
            [
                &relations[candidate].from_node,
                &relations[candidate].to_node,
            ]
            .into_iter()
            .any(|node| node == &last.from_node || node == &last.to_node)
        };
        let next = (index..relations.len())
            .filter(|candidate| {
                reached.contains(&relations[*candidate].from_node)
                    || reached.contains(&relations[*candidate].to_node)
            })
            .min_by_key(|candidate| {
                (
                    Reverse(relation_selectivity(&relations[*candidate], nodes)),
                    Reverse(connected_to_last(*candidate)),
                    *candidate,
                )
            })
            .ok_or_else(|| {
                QueryError::Validation("relationships must form a connected tree".into())
            })?;
        relations[index..=next].rotate_right(1);
        reached.insert(relations[index].from_node.clone());
        reached.insert(relations[index].to_node.clone());
    }
    Ok(())
}

fn relation_selectivity(
    relation: &EdgeRelation,
    nodes: &HashMap<String, NodePlan>,
) -> u8 {
    let endpoint = [&relation.from_node, &relation.to_node]
        .into_iter()
        .filter_map(|node| nodes.get(node))
        .map(|node| {
            if !node.node_ids.is_empty() || node.id_range.is_some() {
                2
            } else if node.filters.iter().any(|(_, filter)| filter.is_selective()) {
                1
            } else {
                0
            }
        })
        .max()
        .unwrap_or_default();
    endpoint.max(u8::from(
        relation.filters.iter().any(|(_, filter)| filter.is_selective()),
    ))
}

pub(super) fn build_nodes<M>(input: &Input, model: &M) -> HashMap<String, NodePlan>
where
    M: OrbitQueryModel + ?Sized,
{
    input
        .nodes
        .iter()
        .filter_map(|node| Some((node.id.clone(), NodePlan::from_input(node, model, true)?)))
        .collect()
}

pub(super) fn build_relations<M>(input: &Input, model: &M) -> Vec<EdgeRelation>
where
    M: OrbitQueryModel + ?Sized,
{
    let entities: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|node| Some((node.id.as_str(), node.entity.as_deref()?)))
        .collect();
    input
        .relationships
        .iter()
        .enumerate()
        .map(|(input_index, relationship)| {
            let from_entity = entities
                .get(relationship.from.as_str())
                .copied()
                .unwrap_or_default();
            let to_entity = entities
                .get(relationship.to.as_str())
                .copied()
                .unwrap_or_default();
            let endpoint_entities = match relationship.direction {
                Direction::Outgoing | Direction::Both => (from_entity, to_entity),
                Direction::Incoming => (to_entity, from_entity),
            };
            let mut edge_tables = if relationship.hops.max > 1 {
                model.relationship_tables(&relationship.types)
            } else {
                match relationship.direction {
                    Direction::Outgoing => model.relationship_tables_between(
                        &relationship.types,
                        from_entity,
                        to_entity,
                    ),
                    Direction::Incoming => model.relationship_tables_between(
                        &relationship.types,
                        to_entity,
                        from_entity,
                    ),
                    Direction::Both => {
                        let mut tables = model.relationship_tables_between(
                            &relationship.types,
                            from_entity,
                            to_entity,
                        );
                        tables.extend(model.relationship_tables_between(
                            &relationship.types,
                            to_entity,
                            from_entity,
                        ));
                        tables.sort();
                        tables.dedup();
                        tables
                    }
                }
            };
            if edge_tables.is_empty() {
                edge_tables = model.relationship_tables(&relationship.types);
            }
            if edge_tables.is_empty() {
                edge_tables.push(model.default_edge_table().to_string());
            }
            let (start, end) = relationship.direction.edge_columns();
            let endpoints: Vec<_> = [(&relationship.from, start), (&relationship.to, end)]
                .into_iter()
                .filter(|(node, _)| {
                    relationship.hops.max == 1 && node_ids_are_selective(input, node)
                })
                .map(|(_, column)| column)
                .collect();
            select_edge_layouts(model, &mut edge_tables, &endpoints);
            let source_entity = model.graph().entity_id(endpoint_entities.0);
            let target_entity = model.graph().entity_id(endpoint_entities.1);
            let node_source = if relationship.hops.max == 1
                && relationship.filters.is_empty()
                && !input.references(&PredicateTarget::Relationship(input_index))
                && relationship.direction != Direction::Both
            {
                model
                    .foreign_key(
                        &relationship.types,
                        endpoint_entities.0,
                        endpoint_entities.1,
                    )
                    .and_then(|foreign_key| {
                        let column = model.property_column(foreign_key.property)?.to_string();
                        model.property_column(foreign_key.referenced_key)?;
                        let (source_node, target_node) = match relationship.direction {
                            Direction::Outgoing | Direction::Both => {
                                (&relationship.from, &relationship.to)
                            }
                            Direction::Incoming => (&relationship.to, &relationship.from),
                        };
                        let node = match foreign_key.holder {
                            query_data_model::Endpoint::Source => source_node.clone(),
                            query_data_model::Endpoint::Target => target_node.clone(),
                        };
                        model.entity_table(input.entity_of(&node)?)?;
                        Some(NodeRelationSource { node, column })
                    })
            } else {
                None
            };
            EdgeRelation {
                input_index,
                rel_types: relationship.types.clone(),
                relationships: relationship
                    .types
                    .iter()
                    .filter_map(|kind| model.graph().relationship_id(kind))
                    .collect(),
                edge_tables: edge_tables.clone(),
                alias: format!("{}edge_{input_index}", internal_column_prefix()),
                source_entity,
                target_entity,
                from_node: relationship.from.clone(),
                to_node: relationship.to.clone(),
                direction: relationship.direction,
                min_hops: relationship.hops.min,
                max_hops: relationship.hops.max,
                filters: ordered_filters(
                    &relationship.filters,
                    FilterOwner::Tables(&edge_tables),
                    model,
                ),
                node_source,
            }
        })
        .collect()
}

fn node_ids_are_selective(input: &Input, alias: &str) -> bool {
    input
        .nodes
        .iter()
        .find(|node| node.id == alias)
        .is_some_and(|node| !node.node_ids.is_empty() || node.id_range.is_some())
}

pub(super) fn select_edge_layouts(
    model: &(impl OrbitQueryModel + ?Sized),
    tables: &mut [String],
    endpoints: &[&str],
) {
    if endpoints.is_empty() {
        return;
    }
    for table in tables {
        let selected = std::iter::once(table.as_str())
            .chain(model.equivalent_layouts(table))
            .min_by_key(|layout| {
                model.table_sort_key(layout).map_or(usize::MAX, |key| {
                    endpoints
                        .iter()
                        .filter_map(|endpoint| key.iter().position(|column| column == endpoint))
                        .min()
                        .unwrap_or(usize::MAX)
                })
            });
        if let Some(selected) = selected {
            *table = selected.to_string();
        }
    }
}

pub(super) fn assign_node_access(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    condition_nodes: &HashSet<String>,
    nodes: &mut HashMap<String, NodePlan>,
) {
    for node in nodes.values_mut() {
        let group_target = input
            .aggregation
            .group_by
            .iter()
            .any(|group| group.node() == node.alias);
        let metric_target = input.aggregation.metrics.iter().any(|metric| {
            metric.expr.node() == node.alias
                && metric.expr.property().is_some_and(|property| {
                    !matches!(metric.expr.function(), AggFunction::Count)
                        || node
                            .entity
                            .as_deref()
                            .is_some_and(|entity| model.property_is_nullable(entity, property))
                })
        });
        let ordered = input
            .order_by
            .as_ref()
            .is_some_and(|order| order.node == node.alias);
        let local_predicate = condition_nodes.contains(&node.alias)
            && !node_is_relation_source(&node.alias, relations);
        let uncovered_filter = node_has_uncovered_filter(node, relations, denormalized, model);
        let filter_only = input.query_type == QueryType::Aggregation
            && uncovered_filter
            && !group_target
            && !metric_target
            && !ordered
            && !local_predicate;
        node.joined = input.relationships.is_empty()
            || !model.supports_deferred_node_loading()
            || group_target
            || metric_target
            || ordered
            || local_predicate
            || uncovered_filter && !filter_only;
    }
}
