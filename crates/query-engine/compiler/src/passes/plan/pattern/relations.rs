//! Relation planning: an `EdgeRelation` per input relationship (edge tables, layout, foreign-key
//! shortcut), ordered most selective first along a connected chain.

use crate::passes::codegen::SqlBackend;
use query_data_model::Catalog;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};

use query_data_model::constants::{
    SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};
use query_data_model::plan::logical::{EdgeField, Expr as Logical};
use query_data_model::plan::tracking::Ref;
use query_data_model::{Direction, EntityId, RelationshipId};

use super::{EdgeRelation, Fk, NodePlan};
use crate::constants::{EDGE_PATH_NODES_SUFFIX, edge_column_prefix, edge_relation_alias};
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::logical::facts::Fact;
use crate::passes::plan::filters::{
    BoundFilter, FilterOwner, Test, bind_filter, unindex_if_key_narrowed,
};

/// The edge tables storing a variant of `relationships` from `source` to `target`; any relationship when none is named.
fn tables_between(
    model: &Catalog<impl SqlBackend>,
    relationships: &[RelationshipId],
    source: EntityId,
    target: EntityId,
) -> Vec<String> {
    let graph = model.graph();
    let any: Vec<RelationshipId>;
    let relationships = match relationships {
        [] => {
            any = graph
                .relationships()
                .map(|relationship| relationship.id)
                .collect();
            &any
        }
        named => named,
    };
    relationships
        .iter()
        .flat_map(|relationship| model.edge_routes(*relationship))
        .filter(|route| {
            route
                .variants()
                .any(|variant| variant.source == source && variant.target == target)
        })
        .map(|route| route.table.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn edge_columns(direction: Direction) -> (&'static str, &'static str) {
    match direction {
        Direction::Outgoing | Direction::Both => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN),
        Direction::Incoming => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN),
    }
}

/// Orders a relationship's from/to pair as the edge row stores it: source first, then target.
pub(super) fn stored_order<T>(direction: Direction, from: T, to: T) -> (T, T) {
    match direction {
        Direction::Outgoing | Direction::Both => (from, to),
        Direction::Incoming => (to, from),
    }
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

    /// The kinds an edge read must test; none for a wildcard.
    pub fn kinds(&self) -> &[String] {
        match self.relationships.is_empty() {
            true => &[],
            false => &self.rel_types,
        }
    }

    pub fn single_edge_table(&self) -> Option<&str> {
        let [table] = self.edge_tables.as_slice() else {
            return None;
        };
        Some(table)
    }

    pub fn column_prefix(&self) -> String {
        edge_column_prefix(self.input_index, self.max_hops > 1)
    }

    pub fn path_nodes_column(&self) -> String {
        format!("{}{EDGE_PATH_NODES_SUFFIX}", self.column_prefix())
    }

    pub(super) fn endpoint_column(&self, node: &str) -> Option<&'static str> {
        let (start, end) = edge_columns(self.direction);
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
        stored_order(
            self.direction,
            self.from_node.as_str(),
            self.to_node.as_str(),
        )
    }

    pub(super) fn other_end(&self, node: &str) -> &str {
        if self.from_node == node {
            &self.to_node
        } else {
            &self.from_node
        }
    }
}

pub(super) fn build_relations<B>(
    query: Query<'_>,
    facts: &[Fact<'_>],
    model: &Catalog<B>,
    nodes: &HashMap<String, NodePlan>,
) -> Vec<EdgeRelation>
where
    B: SqlBackend,
{
    let graph = model.graph();
    let leaves = query.leaves();
    query
        .steps()
        .into_iter()
        .enumerate()
        .map(|(input_index, step)| {
            let (from, to, direction) =
                (query.name(step.from), query.name(step.to), step.direction);
            let types: Vec<String> = match step.relationships {
                [] => vec!["*".to_string()],
                named => named
                    .iter()
                    .map(|relationship| graph.relationship(*relationship).name.clone())
                    .collect(),
            };
            let entity = |node: &str| {
                let name = nodes.get(node)?.entity.as_deref()?;
                graph.entity_id(name)
            };
            let (source_node, target_node) = stored_order(direction, from, to);
            let (source, target) = (entity(source_node), entity(target_node));
            let relationships = step.relationships;
            let between =
                |source: Option<EntityId>, target: Option<EntityId>| match (source, target) {
                    (Some(source), Some(target)) => {
                        tables_between(model, relationships, source, target)
                    }
                    _ => Vec::new(),
                };
            let max_hops = step.range.max;
            let mut edge_tables = if max_hops > 1 {
                model.edge_tables(relationships)
            } else {
                let mut tables = between(source, target);
                if direction == Direction::Both {
                    tables.extend(between(target, source));
                    tables.sort();
                    tables.dedup();
                }
                tables
            };
            if edge_tables.is_empty() {
                edge_tables = model.edge_tables(relationships);
            }
            if edge_tables.is_empty() {
                edge_tables.extend(model.default_edge_table().map(str::to_string));
            }
            let (start, end) = edge_columns(direction);
            let pinned: Vec<&str> = [(from, start), (to, end)]
                .into_iter()
                .filter(|(node, _)| {
                    max_hops == 1 && nodes.get(*node).is_some_and(NodePlan::is_pinned)
                })
                .map(|(_, column)| column)
                .collect();
            select_edge_layouts(model, &mut edge_tables, &pinned);
            let tested = leaves
                .iter()
                .any(|leaf| leaf.binding() == step.edge && !leaf.edge_kind());
            let fk = (max_hops == 1 && !tested && direction != Direction::Both)
                .then(|| {
                    let foreign_key = model.shared_foreign_key(relationships, source?, target?)?;
                    let column = model.column(foreign_key.property)?.to_string();
                    model.column(foreign_key.referenced_key)?;
                    let node = match foreign_key.holder {
                        query_data_model::Endpoint::Source => source_node,
                        query_data_model::Endpoint::Target => target_node,
                    };
                    nodes.get(node)?.table.as_ref()?;
                    Some(Fk {
                        node: node.to_string(),
                        column,
                    })
                })
                .flatten();
            let filters = edge_filters(facts, step.edge, &edge_tables, model);
            EdgeRelation {
                input_index,
                rel_types: types,
                relationships: relationships.to_vec(),
                alias: edge_relation_alias(input_index),
                source_entity: source,
                target_entity: target,
                from_node: from.to_string(),
                to_node: to.to_string(),
                direction,
                min_hops: step.range.min,
                max_hops,
                filters,
                edge_tables,
                fk,
            }
        })
        .collect()
}

/// The filters attached to an edge, bound to the columns its tables store.
fn edge_filters<B>(
    facts: &[Fact<'_>],
    edge: Ref,
    tables: &[String],
    model: &Catalog<B>,
) -> Vec<(String, BoundFilter)>
where
    B: SqlBackend,
{
    let owner = FilterOwner::Tables(tables);
    let mut filters: Vec<(String, BoundFilter)> = facts
        .iter()
        .filter(|fact| fact.attached && fact.binding() == edge)
        .filter_map(|fact| {
            let name = match fact.left {
                Logical::Edge { field, .. } => match field {
                    EdgeField::Kind => return None,
                    EdgeField::Source => SOURCE_ID_COLUMN,
                    EdgeField::SourceKind => SOURCE_KIND_COLUMN,
                    EdgeField::Target => TARGET_ID_COLUMN,
                    EdgeField::TargetKind => TARGET_KIND_COLUMN,
                }
                .to_string(),
                Logical::Attribute { attribute, .. } => {
                    model.graph().attribute(attribute).name.clone()
                }
                _ => return None,
            };
            let test = Test::of(fact.leaf, fact.complemented, |_| None)?;
            let bound = bind_filter(&name, test, &owner, model);
            Some((name, bound))
        })
        .collect();
    unindex_if_key_narrowed(&mut filters);
    filters
}

/// Swaps each table for the equivalent layout whose sort key reaches a pinned endpoint column soonest.
pub(super) fn select_edge_layouts(
    model: &Catalog<impl SqlBackend>,
    tables: &mut [String],
    endpoints: &[&str],
) {
    if endpoints.is_empty() {
        return;
    }
    for table in tables {
        let selected = std::iter::once(table.as_str())
            .chain(model.copies_of(table))
            .min_by_key(|layout| {
                let key = model.table_facts(layout).map(|facts| &facts.sort_key);
                key.map_or(usize::MAX, |key| {
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
