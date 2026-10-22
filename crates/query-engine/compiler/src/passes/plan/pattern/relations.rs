//! Relation planning: an `EdgeRelation` per input relationship (edge tables, layout, foreign-key
//! shortcut), ordered most selective first along a connected chain.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, Fk, NodePlan};
use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::normalize::is_wildcard;
use crate::passes::plan::filters::{FilterOwner, ordered_filters};

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
                hop_routes(
                    model,
                    &relationship.types,
                    relationship.direction,
                    [
                        input.entity_of(&relationship.from),
                        input.entity_of(&relationship.to),
                    ],
                    relationship.hops.max,
                )
                .map(|routes| routes.tables.into_iter().collect::<Vec<_>>())
                .filter(|tables| !tables.is_empty())
                .unwrap_or_else(|| model.relationship_tables(&relationship.types))
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
                    model.entity_table(input.entity_of(&node)?)?;
                    Some(Fk { node, column })
                })
                .flatten()
                .filter(|key| {
                    prefer_foreign_key(input, model, nodes, relationship, key, &edge_tables)
                });
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

/// Keeps the foreign-key shortcut unless it would filter an unbounded holder by a column outside its sort key
/// with ids reached through another relation, while an edge table can look that endpoint up by key.
fn prefer_foreign_key<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
    nodes: &HashMap<String, NodePlan>,
    relationship: &InputRelationship,
    key: &Fk,
    edge_tables: &[String],
) -> bool {
    let other = if key.node == relationship.from {
        &relationship.to
    } else {
        &relationship.from
    };
    let other_column = if relationship
        .direction
        .stored_order(&relationship.from, &relationship.to)
        .0
        == other
    {
        ontology::constants::SOURCE_ID_COLUMN
    } else {
        ontology::constants::TARGET_ID_COLUMN
    };
    let holder_keyed = input
        .entity_of(&key.node)
        .and_then(|entity| model.entity_table(entity))
        .is_none_or(|table| model.in_sort_key(table, &key.column));
    let edge_keyed = !edge_tables.is_empty()
        && edge_tables.iter().all(|table| {
            std::iter::once(table.as_str())
                .chain(model.equivalent_layouts(table))
                .any(|layout| model.in_sort_key(layout, other_column))
        });
    let bounded = bounded_without(input, nodes, relationship);
    holder_keyed
        || model.anchor_foreign_keys().contains_key(&key.column)
        || bounded.contains(key.node.as_str())
        || !bounded.contains(other.as_str())
        || nodes.get(other.as_str()).is_some_and(NodePlan::is_pinned)
        || !edge_keyed
}

/// Nodes bounded by their own filters, or reached from one through relations other than `skip`.
fn bounded_without<'a>(
    input: &'a Input,
    nodes: &HashMap<String, NodePlan>,
    skip: &InputRelationship,
) -> HashSet<&'a str> {
    let mut bounded: HashSet<&str> = input
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .filter(|node| nodes.get(*node).is_some_and(NodePlan::is_bounded))
        .collect();
    loop {
        let before = bounded.len();
        for relationship in input
            .relationships
            .iter()
            .filter(|r| !std::ptr::eq(*r, skip))
        {
            let ends = [relationship.from.as_str(), relationship.to.as_str()];
            if ends.iter().any(|end| bounded.contains(end)) {
                bounded.extend(ends);
            }
        }
        if bounded.len() == before {
            return bounded;
        }
    }
}

/// The variants a variable-length walk can use: their edge tables, and per hop the kinds the walk can
/// stand on and still end on the target kind within the remaining hops.
#[derive(Default)]
pub(super) struct HopRoutes {
    pub tables: BTreeSet<String>,
    pub start_kinds: Vec<Vec<String>>,
}

pub(super) fn hop_routes<M: OrbitQueryModel + ?Sized>(
    model: &M,
    rel_types: &[String],
    direction: Direction,
    [from, to]: [Option<&str>; 2],
    max_hops: u32,
) -> Option<HopRoutes> {
    if direction == Direction::Both || from.is_none() && to.is_none() || is_wildcard(rel_types) {
        return None;
    }
    let graph = model.graph();
    let steps: Vec<(&str, &str, &str)> = rel_types
        .iter()
        .flat_map(|kind| model.relationship_routes(kind))
        .flat_map(|route| {
            route
                .variants()
                .map(|variant| {
                    let (source, target) = (
                        graph.entity(variant.source).name.as_str(),
                        graph.entity(variant.target).name.as_str(),
                    );
                    match direction {
                        Direction::Incoming => (target, source, route.table),
                        _ => (source, target, route.table),
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let hops = max_hops as usize;
    let mut finishing: Vec<HashSet<&str>> = vec![to.into_iter().collect()];
    for _ in 1..hops {
        let mut kinds = finishing.last().expect("seeded").clone();
        kinds.extend(
            steps
                .iter()
                .filter(|(_, next, _)| finishing.last().expect("seeded").contains(next))
                .map(|(start, _, _)| *start),
        );
        finishing.push(kinds);
    }
    let mut standing: Option<HashSet<&str>> = from.map(|kind| HashSet::from([kind]));
    let mut routes = HopRoutes::default();
    for remaining in (0..hops).rev() {
        let usable: Vec<_> = steps
            .iter()
            .filter(|(start, next, _)| {
                standing.as_ref().is_none_or(|kinds| kinds.contains(start))
                    && (to.is_none() || finishing[remaining].contains(next))
            })
            .collect();
        routes
            .tables
            .extend(usable.iter().map(|(_, _, table)| table.to_string()));
        let kinds: BTreeSet<String> = usable
            .iter()
            .map(|(start, _, _)| start.to_string())
            .collect();
        routes.start_kinds.push(kinds.into_iter().collect());
        standing = Some(usable.iter().map(|(_, next, _)| *next).collect());
    }
    Some(routes)
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
