//! Relation planning: an `EdgeRelation` per input relationship (edge tables, layout, foreign-key
//! shortcut), ordered most selective first along a connected chain.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, Fk, NodePlan};
use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::filters::{FilterOwner, bind_filter, ordered_filters};
use crate::passes::plan::predicates::{Predicate, property_filter};

fn type_filter<M>(
    model: &M,
    holder: &str,
    alias: &str,
    filter: query_data_model::TypeFilter,
) -> Option<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let entity = model.graph().entity_id(holder)?;
    let property = model.graph().property(filter.property).name.clone();
    let bound = bind_filter(
        &property,
        InputFilter {
            op: Some(FilterOp::In),
            value: Some(serde_json::json!(filter.values)),
            ..InputFilter::default()
        },
        &FilterOwner::Entity(entity),
        model,
    );
    Some(property_filter(alias, &property, &bound))
}

impl EdgeRelation {
    pub(super) fn reversed(&self) -> Self {
        let mut relation = self.clone();
        std::mem::swap(&mut relation.from_node, &mut relation.to_node);
        relation.direction = match relation.direction {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Both => Direction::Both,
        };
        relation
    }

    pub fn single_edge_table(&self) -> Option<&str> {
        let [table] = self.edge_tables.as_slice() else {
            return None;
        };
        Some(table)
    }

    pub fn column_prefix(&self) -> String {
        if self.max_hops > 1 {
            format!("hop_e{}_", self.input_index)
        } else {
            format!("e{}_", self.input_index)
        }
    }

    pub(super) fn endpoint_column(&self, node: &str) -> Option<&'static str> {
        let (start, end) = self.direction.edge_columns();
        if self.from_node == node {
            Some(start)
        } else if self.to_node == node {
            Some(end)
        } else {
            None
        }
    }

    /// The endpoints as the edge row stores them: the node in `source_id`, then the node in `target_id`.
    pub(super) fn stored_endpoints(&self) -> (&str, &str) {
        self.direction
            .stored_order(self.from_node.as_str(), self.to_node.as_str())
    }

    pub(super) fn other_end(&self, node: &str) -> &str {
        if self.from_node == node {
            &self.to_node
        } else {
            &self.from_node
        }
    }
}

pub(super) fn build_relations<M>(
    input: &Input,
    model: &M,
    nodes: &HashMap<String, NodePlan>,
) -> Vec<EdgeRelation>
where
    M: OrbitQueryModel + ?Sized,
{
    input
        .relationships
        .iter()
        .enumerate()
        .map(|(input_index, relationship)| {
            let (source_node, target_node) = relationship
                .direction
                .stored_order(&relationship.from, &relationship.to);
            let source = input.entity_of(source_node).unwrap_or_default();
            let target = input.entity_of(target_node).unwrap_or_default();
            let mut edge_tables = if relationship.hops.max > 1 {
                model.relationship_tables(&relationship.types)
            } else {
                let mut tables =
                    model.relationship_tables_between(&relationship.types, source, target);
                if relationship.direction == Direction::Both {
                    tables.extend(model.relationship_tables_between(
                        &relationship.types,
                        target,
                        source,
                    ));
                    tables.sort();
                    tables.dedup();
                }
                tables
            };
            if edge_tables.is_empty() {
                edge_tables = model.relationship_tables(&relationship.types);
            }
            if edge_tables.is_empty() {
                edge_tables.push(model.default_edge_table().to_string());
            }
            let (start, end) = relationship.direction.edge_columns();
            let pinned: Vec<&str> = [(&relationship.from, start), (&relationship.to, end)]
                .into_iter()
                .filter(|(node, _)| {
                    relationship.hops.max == 1
                        && nodes.get(node.as_str()).is_some_and(NodePlan::is_pinned)
                })
                .map(|(_, column)| column)
                .collect();
            select_edge_layouts(model, &mut edge_tables, &pinned);
            let fk = (relationship.hops.max == 1
                && relationship.filters.is_empty()
                && !input.references(&PredicateTarget::Relationship(input_index))
                && relationship.direction != Direction::Both)
                .then(|| {
                    let foreign_key = model.foreign_key(&relationship.types, source, target)?;
                    let column = model.property_column(foreign_key.property)?.to_string();
                    model.property_column(foreign_key.referenced_key)?;
                    let node = match foreign_key.holder {
                        query_data_model::Endpoint::Source => source_node.clone(),
                        query_data_model::Endpoint::Target => target_node.clone(),
                    };
                    let holder = input.entity_of(&node)?;
                    model.entity_table(holder)?;
                    let type_filter = match foreign_key.type_filter {
                        Some(filter) => Some(type_filter(model, holder, &node, filter)?),
                        None => None,
                    };
                    Some(Fk {
                        node,
                        column,
                        type_filter,
                    })
                })
                .flatten();
            let filters = ordered_filters(
                &relationship.filters,
                FilterOwner::Tables(&edge_tables),
                model,
            );
            EdgeRelation {
                input_index,
                rel_types: relationship.types.clone(),
                relationships: relationship
                    .types
                    .iter()
                    .filter_map(|kind| model.graph().relationship_id(kind))
                    .collect(),
                alias: format!("{}edge_{input_index}", internal_column_prefix()),
                source_entity: model.graph().entity_id(source),
                target_entity: model.graph().entity_id(target),
                from_node: relationship.from.clone(),
                to_node: relationship.to.clone(),
                direction: relationship.direction,
                min_hops: relationship.hops.min,
                max_hops: relationship.hops.max,
                filters,
                edge_tables,
                fk,
            }
        })
        .collect()
}

/// Swaps each table for the equivalent layout whose sort key reaches a pinned endpoint column soonest.
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

/// Orders relations most selective first while keeping each one connected to the ones before it.
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

fn relation_selectivity(relation: &EdgeRelation, nodes: &HashMap<String, NodePlan>) -> u8 {
    let endpoint = [&relation.from_node, &relation.to_node]
        .into_iter()
        .filter_map(|node| nodes.get(node))
        .map(|node| {
            if node.is_pinned() {
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
        relation
            .filters
            .iter()
            .any(|(_, filter)| filter.is_selective()),
    ))
}
