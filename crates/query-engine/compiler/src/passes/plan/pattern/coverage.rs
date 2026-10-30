//! What an edge row already answers about its endpoint nodes, so the node table need not be read.

use std::collections::HashSet;

use ontology::constants::*;
use query_data_model::{DenormalizedDirection, DenormalizedProperty, OrbitQueryModel};

use super::{EdgeRelation, NodePlan, Pattern};
use crate::passes::normalize::is_wildcard;
use crate::passes::plan::filters::{BoundFilter, FilterOwner, bind_filter, lowerable_denorm};
use crate::passes::plan::predicates::{Predicate, property_filter, tag_filter};

impl<M: OrbitQueryModel + ?Sized> Pattern<'_, M> {
    pub fn denorm_facts(
        &self,
        relation: &EdgeRelation,
        node: &str,
        filter: &BoundFilter,
    ) -> Option<&DenormalizedProperty> {
        if relation.fk.is_some() || is_wildcard(&relation.rel_types) {
            return None;
        }
        let direction = if relation.endpoint_column(node)? == SOURCE_ID_COLUMN {
            DenormalizedDirection::Source
        } else {
            DenormalizedDirection::Target
        };
        let (source, target) = (relation.source_entity?, relation.target_entity?);
        lowerable_denorm(filter, direction, &self.denormalized).filter(|facts| {
            relation
                .relationships
                .iter()
                .all(|relationship| facts.variants.contains(&(*relationship, source, target)))
        })
    }

    pub fn denorm_hop(
        &self,
        node: &str,
        filter: &BoundFilter,
    ) -> Option<(usize, &DenormalizedProperty)> {
        self.relations
            .iter()
            .enumerate()
            .find_map(|(position, relation)| {
                self.denorm_facts(relation, node, filter)
                    .map(|facts| (position, facts))
            })
    }

    pub fn denorm_tag(
        &self,
        relation: &EdgeRelation,
        node: &str,
        filter: &BoundFilter,
        edge_alias: &str,
    ) -> Option<Predicate> {
        let facts = self.denorm_facts(relation, node, filter)?;
        tag_filter(
            edge_alias,
            &facts.edge_column,
            &facts.tag_key,
            &filter.filter,
        )
    }

    pub fn covered_by_edge(&self, node: &str, property: &str, filter: &BoundFilter) -> bool {
        self.relations.iter().any(|relation| match &relation.fk {
            Some(key) => key.node == node,
            None => {
                self.denorm_facts(relation, node, filter).is_some()
                    || self.edge_stores(relation, node, property, filter)
            }
        })
    }

    fn edge_stores(
        &self,
        relation: &EdgeRelation,
        node: &str,
        property: &str,
        filter: &BoundFilter,
    ) -> bool {
        filter.filter.rhs_column.is_none()
            && relation.fk.is_none()
            && relation.max_hops == 1
            && relation.endpoint_column(node).is_some()
            && relation.single_edge_table().is_some_and(|table| {
                self.model
                    .table_property_columns(table)
                    .is_none_or(|columns| !columns.contains_key(property))
                    && self
                        .model
                        .table_columns(table)
                        .is_some_and(|columns| columns.contains(property))
            })
    }

    pub fn has_uncovered_filter(&self, node: &NodePlan) -> bool {
        node.filters
            .iter()
            .any(|(property, filter)| !self.covered_by_edge(&node.alias, property, filter))
    }

    pub fn has_selective_uncovered_filter(&self, node: &str) -> bool {
        self.nodes[node].filters.iter().any(|(property, filter)| {
            filter.is_selective() && !self.covered_by_edge(node, property, filter)
        })
    }

    pub fn endpoint_is_selective(&self, node: &str) -> bool {
        self.nodes[node].is_pinned() || self.has_selective_uncovered_filter(node)
    }

    pub fn uncovered_node(&self, node: &str) -> NodePlan {
        let mut plan = self.nodes[node].clone();
        plan.filters
            .retain(|(property, filter)| !self.covered_by_edge(node, property, filter));
        plan
    }

    pub fn holds_fk(&self, node: &str) -> bool {
        self.relations
            .iter()
            .any(|relation| relation.fk.as_ref().is_some_and(|key| key.node == node))
    }

    /// Endpoint filters on properties the edge table stores as columns, each property once.
    pub fn edge_column_filters(&self, relation: &EdgeRelation) -> Vec<(&str, &BoundFilter)> {
        let mut seen = HashSet::new();
        [&relation.from_node, &relation.to_node]
            .into_iter()
            .flat_map(|node| {
                self.nodes[node]
                    .filters
                    .iter()
                    .map(move |(property, filter)| (node.as_str(), property.as_str(), filter))
            })
            .filter(|(node, property, filter)| {
                self.edge_stores(relation, node, property, filter)
                    && !EDGE_RESERVED_COLUMNS.contains(property)
                    && seen.insert(*property)
            })
            .map(|(_, property, filter)| (property, filter))
            .collect()
    }

    pub fn shared_column_predicates(&self, node: &NodePlan) -> Vec<Predicate> {
        let entity = node
            .entity
            .as_deref()
            .and_then(|entity| self.model.graph().entity_id(entity));
        let columns = node
            .table
            .as_deref()
            .and_then(|table| self.model.table_columns(table));
        let (Some(entity), Some(columns)) = (entity, columns) else {
            return vec![];
        };
        let mut seen: HashSet<&str> = node
            .filters
            .iter()
            .map(|(property, _)| property.as_str())
            .collect();
        self.relations
            .iter()
            .flat_map(|relation| {
                self.edge_column_filters(relation)
                    .into_iter()
                    .map(move |(property, filter)| (relation, property, filter))
            })
            .filter(|(relation, property, filter)| {
                self.edge_stores(relation, &node.alias, property, filter)
                    && columns.contains(*property)
                    && seen.insert(*property)
            })
            .filter_map(|(_, property, filter)| {
                let owner = FilterOwner::Entity(entity);
                let bound = bind_filter(property, filter.filter.clone(), &owner, self.model);
                (!bound.is_selective()).then(|| property_filter(&node.alias, property, &bound))
            })
            .collect()
    }
}
