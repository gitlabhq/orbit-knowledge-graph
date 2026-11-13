//! What an edge row already answers about its endpoint nodes, so the node table need not be read.

use crate::passes::codegen::SqlBackend;
use std::collections::HashSet;

use query_data_model::implementations::relational::Target;
use query_data_model::plan::logical::EdgeField;
use query_data_model::{
    DenormalizedDirection, DenormalizedProperty, Endpoint, EntityId, PropertyId,
};

use super::{EdgeRelation, NodePlan, Pattern};
use crate::passes::plan::filters::{BoundFilter, FilterOwner, bind_filter, lowerable_denorm};
use crate::passes::plan::predicates::{Predicate, property_filter, tag_filter};

impl<B: SqlBackend> Pattern<'_, B> {
    pub fn denorm_facts(
        &self,
        relation: &EdgeRelation,
        node: &str,
        filter: &BoundFilter,
    ) -> Option<&DenormalizedProperty> {
        if relation.fk.is_some() || relation.relationships.is_empty() {
            return None;
        }
        let direction = if relation.endpoint_field(node)? == EdgeField::SourceId {
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
        tag_filter(edge_alias, &facts.edge_column, &facts.tag_key, filter)
    }

    pub fn covered_by_edge(&self, node: &str, filter: &BoundFilter) -> bool {
        self.relations.iter().any(|relation| match &relation.fk {
            Some(key) => key.node == node,
            None => {
                self.denorm_facts(relation, node, filter).is_some()
                    || self.copied(relation, node, filter).is_some()
            }
        })
    }

    /// The edge column that copies a node's property, when a filter on it can run on the edge.
    fn copied(&self, relation: &EdgeRelation, node: &str, filter: &BoundFilter) -> Option<&str> {
        if filter.rhs.is_some() || relation.fk.is_some() || relation.max_hops != 1 {
            return None;
        }
        let property = filter.property()?;
        let side = match relation.endpoint_field(node)? {
            EdgeField::SourceId => Endpoint::Source,
            _ => Endpoint::Target,
        };
        let table = &self.model.table_at(relation.single_edge_table()?).name;
        let graph = self.model.graph();
        graph
            .variants()
            .filter(|variant| {
                relation.relationships.is_empty()
                    || relation.relationships.contains(&variant.relationship)
            })
            .filter(|variant| self.model.edge_table(variant.id) == Some(table.as_str()))
            .find_map(|variant| self.model.copy(variant.id, side, property))
    }

    pub fn has_uncovered_filter(&self, node: &NodePlan) -> bool {
        node.filters
            .iter()
            .any(|(_, filter)| !self.covered_by_edge(&node.alias, filter))
    }

    pub fn has_selective_uncovered_filter(&self, node: &str) -> bool {
        self.nodes[node]
            .filters
            .iter()
            .any(|(_, filter)| filter.is_selective() && !self.covered_by_edge(node, filter))
    }

    pub fn endpoint_is_selective(&self, node: &str) -> bool {
        self.nodes[node].is_pinned() || self.has_selective_uncovered_filter(node)
    }

    pub fn uncovered_node(&self, node: &str) -> NodePlan {
        let mut plan = self.nodes[node].clone();
        plan.filters
            .retain(|(_, filter)| !self.covered_by_edge(node, filter));
        plan
    }

    pub fn holds_fk(&self, node: &str) -> bool {
        self.relations
            .iter()
            .any(|relation| relation.fk.as_ref().is_some_and(|key| key.node == node))
    }

    /// Endpoint filters on properties the edge copies, each with its edge column, each property
    /// once.
    pub fn edge_column_filters(
        &self,
        relation: &EdgeRelation,
    ) -> Vec<(&str, Target, &BoundFilter)> {
        let mut seen = HashSet::new();
        [&relation.from_node, &relation.to_node]
            .into_iter()
            .flat_map(|node| {
                self.nodes[node]
                    .filters
                    .iter()
                    .map(move |(target, filter)| (node.as_str(), *target, filter))
            })
            .filter_map(|(node, target, filter)| {
                let column = self.copied(relation, node, filter)?;
                seen.insert(target).then_some((column, target, filter))
            })
            .collect()
    }

    fn copied_back(
        &self,
        relation: &EdgeRelation,
        (node, entity): (&str, EntityId),
        column: &str,
    ) -> Option<PropertyId> {
        if relation.fk.is_some() || relation.max_hops != 1 {
            return None;
        }
        let side = match relation.endpoint_field(node)? {
            EdgeField::SourceId => Endpoint::Source,
            _ => Endpoint::Target,
        };
        let table = &self.model.table_at(relation.single_edge_table()?).name;
        self.model
            .graph()
            .variants()
            .filter(|variant| {
                relation.relationships.is_empty()
                    || relation.relationships.contains(&variant.relationship)
            })
            .filter(|variant| self.model.edge_table(variant.id) == Some(table.as_str()))
            .filter(|variant| match side {
                Endpoint::Source => variant.source == entity,
                Endpoint::Target => variant.target == entity,
            })
            .find_map(|variant| self.model.copied_property(variant.id, side, column))
    }

    pub fn shared_column_predicates(&self, node: &NodePlan) -> Vec<Predicate> {
        let entity = node
            .entity
            .as_deref()
            .and_then(|entity| self.model.names().entity(entity).ok());
        let table = entity.and_then(|entity| self.model.entity_table(entity));
        let (Some(entity), Some(table)) = (entity, table) else {
            return vec![];
        };
        let graph = self.model.graph();
        let mut seen: HashSet<Target> = node.filters.iter().map(|(target, _)| *target).collect();
        self.relations
            .iter()
            .flat_map(|relation| {
                self.edge_column_filters(relation)
                    .into_iter()
                    .map(move |(column, _, filter)| (relation, column, filter))
            })
            .filter_map(|(relation, column, filter)| {
                let property = self.copied_back(relation, (&node.alias, entity), column)?;
                let target = Target::Property(property);
                self.model.column_of(target, table)?;
                seen.insert(target).then_some((target, filter))
            })
            .filter_map(|(target, filter)| {
                let owner = FilterOwner::Entity(entity);
                let leaf = filter.leaf.clone();
                let bound = bind_filter(target, leaf, filter.rhs, &owner, self.model);
                if bound.is_selective() {
                    return None;
                }
                property_filter(&node.alias, &bound, graph)
            })
            .collect()
    }
}
