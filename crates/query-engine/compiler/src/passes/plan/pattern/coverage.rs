//! What an edge row already answers about its endpoint nodes, so the node table need not be read.

use std::collections::HashSet;

use ontology::constants::*;
use ontology::{DataType, EnumType};
use query_data_model::{
    DenormalizedDirection, DenormalizedKey, DenormalizedProperty, EntityId, OrbitQueryModel,
    PropertyId, RelationshipId,
};

use super::{EdgeRelation, NodePlan, Pattern};
use crate::input::InputGroupByKey;
use crate::passes::normalize::is_wildcard;
use crate::passes::plan::filters::{BoundFilter, denorm_tag_values};
use crate::passes::plan::predicates::{Column, Predicate, tag_filter};

impl<M: OrbitQueryModel + ?Sized> Pattern<'_, M> {
    pub fn denorm_facts(
        &self,
        relation: &EdgeRelation,
        node: &str,
        filter: &BoundFilter,
    ) -> Option<&DenormalizedProperty> {
        self.denorm_property(relation, node, filter.property?)
            .filter(|facts| denorm_tag_values(&facts.tag_key, &filter.filter).is_some())
    }

    fn denorm_property(
        &self,
        relation: &EdgeRelation,
        node: &str,
        property: PropertyId,
    ) -> Option<&DenormalizedProperty> {
        if relation.fk.is_some() || is_wildcard(&relation.rel_types) {
            return None;
        }
        let direction = if relation.endpoint_column(node)? == SOURCE_ID_COLUMN {
            DenormalizedDirection::Source
        } else {
            DenormalizedDirection::Target
        };
        self.denormalized
            .get(&DenormalizedKey {
                property,
                direction,
            })
            .filter(|facts| covers_relation(relation, &facts.variants))
    }

    /// The tag that the property holder writes on every row of a one-hop edge, so grouping need not read the node.
    pub fn group_tag(&self, group: &InputGroupByKey) -> Option<(Column, String)> {
        let InputGroupByKey::Property {
            node,
            property,
            truncate: None,
            ..
        } = group
        else {
            return None;
        };
        let entity = self.nodes.get(node)?.entity.as_deref()?;
        let stored = self
            .model
            .property_for_entity_id(self.model.graph().entity_id(entity)?, property)?;
        let verbatim = match stored.data_type {
            DataType::String => !self.model.property_is_free_text(entity, property),
            DataType::Enum => stored.enum_type == EnumType::String,
            _ => false,
        };
        if stored.nullable || !verbatim {
            return None;
        }
        self.relations
            .iter()
            .filter(|relation| relation.max_hops == 1)
            .find_map(|relation| {
                let facts = self
                    .denorm_property(relation, node, stored.id)
                    .filter(|facts| covers_relation(relation, &facts.holder_variants))?;
                Some((
                    Column::new(&relation.alias, &facts.edge_column),
                    facts.tag_key.clone(),
                ))
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
                    || relation.max_hops == 1
                        && relation.endpoint_column(node).is_some()
                        && self
                            .edge_columns(relation)
                            .is_some_and(|columns| columns.contains(property))
            }
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

    fn edge_columns(&self, relation: &EdgeRelation) -> Option<&HashSet<String>> {
        self.model.table_columns(relation.single_edge_table()?)
    }

    /// Endpoint filters on properties the edge table stores as columns, each property once.
    pub fn edge_column_filters(&self, relation: &EdgeRelation) -> Vec<(&str, &BoundFilter)> {
        let Some(columns) = self.edge_columns(relation) else {
            return vec![];
        };
        let mut seen = HashSet::new();
        [&relation.from_node, &relation.to_node]
            .into_iter()
            .flat_map(|node| &self.nodes[node].filters)
            .filter(|(property, _)| {
                columns.contains(property)
                    && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                    && seen.insert(property)
            })
            .map(|(property, filter)| (property.as_str(), filter))
            .collect()
    }
}

fn covers_relation(
    relation: &EdgeRelation,
    variants: &HashSet<(RelationshipId, EntityId, EntityId)>,
) -> bool {
    let (Some(source), Some(target)) = (relation.source_entity, relation.target_entity) else {
        return false;
    };
    relation
        .relationships
        .iter()
        .all(|relationship| variants.contains(&(*relationship, source, target)))
}
