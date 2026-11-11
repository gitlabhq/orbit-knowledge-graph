use std::collections::{BTreeMap, HashMap};

use crate::{DataType, EnumType};

use super::{AttributeId, EntityId, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug)]
pub struct Property {
    pub id: PropertyId,
    pub entity: EntityId,
    pub name: String,
    pub description: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
    pub unique: bool,
    pub enum_values: Option<BTreeMap<i64, String>>,
    pub enum_type: EnumType,
    pub terminal_values: Vec<String>,
    pub selectivity: crate::FieldSelectivity,
    pub default: Option<crate::spec::DefaultValue>,
}

#[derive(Debug)]
pub struct Entity {
    pub id: EntityId,
    pub name: String,
    pub package: String,
    pub domain: Option<String>,
    pub description: Option<String>,
    pub label: String,
    pub style: Style,
    pub properties: Vec<PropertyId>,
    pub defaults: Vec<PropertyId>,
}

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

#[derive(Debug)]
pub struct RelationshipVariant {
    pub id: RelationshipVariantId,
    pub relationship: RelationshipId,
    pub source: EntityId,
    pub target: EntityId,
    pub cardinality: crate::Cardinality,
}

impl RelationshipVariant {
    pub fn endpoint(&self, side: crate::Endpoint) -> EntityId {
        match side {
            crate::Endpoint::Source => self.source,
            crate::Endpoint::Target => self.target,
        }
    }
}

#[derive(Debug)]
pub struct Relationship {
    pub id: RelationshipId,
    pub name: String,
    pub description: Option<String>,
    pub variants: Vec<RelationshipVariantId>,
    pub attributes: Vec<AttributeId>,
}

#[derive(Debug)]
pub struct Attribute {
    pub id: AttributeId,
    pub relationship: RelationshipId,
    pub name: String,
    pub description: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
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
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn attribute_id(&self, relationship: RelationshipId, name: &str) -> Option<AttributeId> {
        self.attribute_ids
            .get(&(relationship, name.to_string()))
            .copied()
    }

    pub fn entity_id(&self, name: &str) -> Option<EntityId> {
        self.entity_ids.get(name).copied()
    }

    pub fn entity_named(&self, name: &str) -> Option<&Entity> {
        self.entity_id(name).map(|id| self.entity(id))
    }

    pub fn property_id(&self, entity: EntityId, name: &str) -> Option<PropertyId> {
        self.property_ids.get(entity.index())?.get(name).copied()
    }

    pub fn property_named(&self, entity: &str, property: &str) -> Option<&Property> {
        let entity = self.entity_id(entity)?;
        self.property_id(entity, property)
            .map(|id| self.property(id))
    }

    pub fn relationship_id(&self, name: &str) -> Option<RelationshipId> {
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

    pub fn variant_named(
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

    pub fn relationship_names(&self, source: Option<&str>, target: Option<&str>) -> Vec<String> {
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
