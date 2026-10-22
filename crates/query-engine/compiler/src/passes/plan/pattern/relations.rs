//! Relation planning: an `EdgeRelation` per input relationship (edge tables, layout, foreign-key
//! shortcut), ordered most selective first along a connected chain.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, Fk, NodePlan};
use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::*;
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
                .map(|routes| routes.tables)
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
                    fk_beats_edge_scan(input, model, nodes, relationship, key, &edge_tables)
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

/// The foreign-key shortcut filters the holder table by its key column. When that column is outside the
/// holder's sort key and is not a namespace anchor (the scope proof bounds those), nothing else bounds the
/// holder, and the other endpoint arrives as an id set through another relation, the filter reads the
/// holder's whole namespace; an edge table keyed on that endpoint reads only the reached rows.
fn fk_beats_edge_scan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
    nodes: &HashMap<String, NodePlan>,
    relationship: &InputRelationship,
    key: &Fk,
    edge_tables: &[String],
) -> bool {
    let Some(holder_table) = input
        .entity_of(&key.node)
        .and_then(|e| model.entity_table(e))
    else {
        return true;
    };
    let other = if key.node == relationship.from {
        &relationship.to
    } else {
        &relationship.from
    };
    let bounded = bounded_without(input, nodes, relationship);
    let (source_node, _) = relationship
        .direction
        .stored_order(&relationship.from, &relationship.to);
    let other_column = if source_node == other {
        ontology::constants::SOURCE_ID_COLUMN
    } else {
        ontology::constants::TARGET_ID_COLUMN
    };
    let edge_keyed = !edge_tables.is_empty()
        && edge_tables.iter().all(|table| {
            std::iter::once(table.as_str())
                .chain(model.equivalent_layouts(table))
                .any(|layout| model.in_sort_key(layout, other_column))
        });
    model.in_sort_key(holder_table, &key.column)
        || model.anchor_foreign_keys().contains_key(&key.column)
        || bounded.contains(key.node.as_str())
        || nodes.get(other.as_str()).is_some_and(NodePlan::is_pinned)
        || !bounded.contains(other.as_str())
        || !edge_keyed
}

/// Nodes bounded by their own filters, or reached from a bounded node through relations other than `skip`.
fn bounded_without<'a>(
    input: &'a Input,
    nodes: &HashMap<String, NodePlan>,
    skip: &InputRelationship,
) -> HashSet<&'a str> {
    let mut bounded: HashSet<&str> = input
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .filter(|node| {
            nodes.get(*node).is_some_and(|plan| {
                plan.is_pinned()
                    || plan.is_namespace_anchored()
                    || plan.filters.iter().any(|(_, filter)| filter.is_selective())
            })
        })
        .collect();
    loop {
        let before = bounded.len();
        for relationship in input
            .relationships
            .iter()
            .filter(|r| !std::ptr::eq(*r, skip))
        {
            let (from, to) = (relationship.from.as_str(), relationship.to.as_str());
            if bounded.contains(from) || bounded.contains(to) {
                bounded.extend([from, to]);
            }
        }
        if bounded.len() == before {
            return bounded;
        }
    }
}

/// The ontology variants a variable-length walk can use: the edge tables they live in, and per hop the
/// entity kinds the walk can stand on before that hop and still end on the target kind.
pub(super) struct HopRoutes {
    pub tables: Vec<String>,
    pub start_kinds: Vec<Vec<String>>,
}

/// Plans the variants of a walk from `from` to `to` (entity names; `None` is any kind) in walk order.
/// Returns `None` when the walk is unconstrained (`Both`, wildcard types, or no endpoint kinds).
pub(super) fn hop_routes<M: OrbitQueryModel + ?Sized>(
    model: &M,
    rel_types: &[String],
    direction: Direction,
    [from, to]: [Option<&str>; 2],
    max_hops: u32,
) -> Option<HopRoutes> {
    if direction == Direction::Both
        || from.is_none() && to.is_none()
        || rel_types.is_empty()
        || rel_types.iter().any(|kind| kind == "*")
    {
        return None;
    }
    let graph = model.graph();
    let mut steps: Vec<(&str, &str, &str)> = Vec::new();
    for kind in rel_types {
        for route in model.relationship_routes(kind) {
            for variant in route.variants() {
                let (source, target) = (
                    graph.entity(variant.source).name.as_str(),
                    graph.entity(variant.target).name.as_str(),
                );
                steps.push(match direction {
                    Direction::Incoming => (target, source, route.table),
                    _ => (source, target, route.table),
                });
            }
        }
    }
    let hops = usize::try_from(max_hops).ok()?;
    let mut reach_back: Vec<HashSet<&str>> = vec![to.into_iter().collect()];
    for _ in 1..hops {
        let previous = reach_back.last().expect("seeded");
        let next = steps
            .iter()
            .filter(|(_, next, _)| to.is_none() || previous.contains(next))
            .map(|(start, _, _)| *start)
            .collect();
        reach_back.push(next);
    }
    let mut standing: HashSet<&str> = from.into_iter().collect();
    let mut tables = HashSet::new();
    let mut start_kinds = Vec::with_capacity(hops);
    for depth in 1..=hops {
        let finishes = |kind: &str| {
            to.is_none()
                || reach_back[..=hops - depth]
                    .iter()
                    .any(|kinds| kinds.contains(kind))
        };
        let usable: Vec<_> = steps
            .iter()
            .filter(|(start, next, _)| {
                (from.is_none() && depth == 1 || standing.contains(start)) && finishes(next)
            })
            .collect();
        tables.extend(usable.iter().map(|(_, _, table)| table.to_string()));
        let mut kinds: Vec<String> = usable
            .iter()
            .map(|(start, _, _)| start.to_string())
            .collect();
        kinds.sort();
        kinds.dedup();
        start_kinds.push(kinds);
        standing = usable.iter().map(|(_, next, _)| *next).collect();
    }
    let mut tables: Vec<String> = tables.into_iter().collect();
    tables.sort();
    Some(HopRoutes {
        tables,
        start_kinds,
    })
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
