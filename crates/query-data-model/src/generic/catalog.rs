use std::collections::{BTreeMap, HashMap};

use super::{AttributeId, EntityId, PropertyId, RelationshipId, RelationshipVariantId};
use crate::spec::{Attribute, Entity, Property, Relationship, RelationshipVariant};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Style {
    pub size: i32,
    pub color: String,
}

impl Default for Style {
    fn default() -> Self {
        Self {
            size: 30,
            color: "#6B7280".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Domain {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Default)]
pub struct GraphCatalog {
    pub(super) domains: BTreeMap<String, Domain>,
    pub(super) entities: Vec<Entity>,
    pub(super) properties: Vec<Property>,
    pub(super) relationships: Vec<Relationship>,
    pub(super) variants: Vec<RelationshipVariant>,
    pub(super) attributes: Vec<Attribute>,
    pub(super) entity_ids: HashMap<String, EntityId>,
    pub(super) property_ids: Vec<HashMap<String, PropertyId>>,
    pub(super) relationship_ids: HashMap<String, RelationshipId>,
    pub(super) attribute_ids: HashMap<(RelationshipId, String), AttributeId>,
    pub(super) variant_ids: HashMap<(RelationshipId, EntityId, EntityId), RelationshipVariantId>,
    pub(super) outgoing: Vec<Vec<RelationshipVariantId>>,
    pub(super) incoming: Vec<Vec<RelationshipVariantId>>,
}

impl GraphCatalog {
    /// Domains in name order.
    pub fn domains(&self) -> impl Iterator<Item = &Domain> {
        self.domains.values()
    }

    /// Entities in name order.
    pub fn entities(&self) -> impl Iterator<Item = &Entity> {
        self.entities.iter()
    }

    pub fn properties(&self) -> impl Iterator<Item = &Property> {
        self.properties.iter()
    }

    pub fn relationships(&self) -> impl Iterator<Item = &Relationship> {
        self.relationships.iter()
    }

    pub fn variants(&self) -> impl Iterator<Item = &RelationshipVariant> {
        self.variants.iter()
    }

    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attributes.iter()
    }

    pub fn entity(&self, id: EntityId) -> &Entity {
        &self.entities[id.index()]
    }

    pub fn property(&self, id: PropertyId) -> &Property {
        &self.properties[id.index()]
    }

    pub fn relationship(&self, id: RelationshipId) -> &Relationship {
        &self.relationships[id.index()]
    }

    pub fn variant(&self, id: RelationshipVariantId) -> &RelationshipVariant {
        &self.variants[id.index()]
    }

    pub fn attribute(&self, id: AttributeId) -> &Attribute {
        &self.attributes[id.index()]
    }

    pub(crate) fn attribute_id(
        &self,
        relationship: RelationshipId,
        name: &str,
    ) -> Option<AttributeId> {
        self.attribute_ids
            .get(&(relationship, name.to_string()))
            .copied()
    }

    pub(crate) fn entity_id(&self, name: &str) -> Option<EntityId> {
        self.entity_ids.get(name).copied()
    }

    /// The entity a stored kind value names: the value writers put in an edge table's endpoint
    /// kind column and the engine in a row's type column, decoded where rows come back.
    pub fn entity_kind(&self, stored: &str) -> Option<&Entity> {
        self.entity_named(stored)
    }

    pub(crate) fn entity_named(&self, name: &str) -> Option<&Entity> {
        self.entity_id(name).map(|id| self.entity(id))
    }

    pub(crate) fn property_id(&self, entity: EntityId, name: &str) -> Option<PropertyId> {
        self.property_ids.get(entity.index())?.get(name).copied()
    }

    pub(crate) fn relationship_id(&self, name: &str) -> Option<RelationshipId> {
        self.relationship_ids.get(name).copied()
    }

    pub fn variant_id(
        &self,
        relationship: RelationshipId,
        source: EntityId,
        target: EntityId,
    ) -> Option<RelationshipVariantId> {
        self.variant_ids
            .get(&(relationship, source, target))
            .copied()
    }

    pub(crate) fn variant_named(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<&RelationshipVariant> {
        let relationship = self.relationship_id(relationship)?;
        let source = self.entity_id(source)?;
        let target = self.entity_id(target)?;
        self.variant_id(relationship, source, target)
            .map(|id| self.variant(id))
    }

    pub(crate) fn relationship_names(
        &self,
        source: Option<&str>,
        target: Option<&str>,
    ) -> Vec<String> {
        let source = source.and_then(|name| self.entity_id(name));
        let target = target.and_then(|name| self.entity_id(name));
        let connected: std::collections::BTreeSet<RelationshipId> = match (source, target) {
            (Some(source), target) => self
                .hops(source, super::Direction::Outgoing)
                .filter(|hop| target.is_none_or(|target| hop.to == target))
                .map(|hop| hop.relationship)
                .collect(),
            (None, Some(target)) => self
                .hops(target, super::Direction::Incoming)
                .map(|hop| hop.relationship)
                .collect(),
            (None, None) => self
                .variants
                .iter()
                .map(|variant| variant.relationship)
                .collect(),
        };
        connected
            .into_iter()
            .map(|relationship| self.relationship(relationship).name.clone())
            .collect()
    }
}
