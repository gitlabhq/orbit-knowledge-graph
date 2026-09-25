use std::collections::{BTreeMap, BTreeSet, HashMap};

use ontology::{DataType, EnumType, FieldSelectivity, FieldSource, Ontology, VirtualSource};

use crate::DataModelError;

use super::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug)]
pub struct Property {
    pub id: PropertyId,
    pub entity: EntityId,
    pub name: String,
    pub data_type: DataType,
    pub enum_values: Option<BTreeMap<i64, String>>,
    pub enum_type: EnumType,
    pub selectivity: FieldSelectivity,
    pub realization: PropertyRealization,
    pub filterable: bool,
    pub like_allowed: bool,
}

#[derive(Debug)]
pub enum PropertyRealization {
    Stored,
    Virtual(VirtualSource),
}

#[derive(Debug)]
pub struct Entity {
    pub id: EntityId,
    pub name: String,
    pub properties: Vec<PropertyId>,
}

#[derive(Debug)]
pub struct RelationshipVariant {
    pub id: RelationshipVariantId,
    pub relationship: RelationshipId,
    pub source: EntityId,
    pub target: EntityId,
}

#[derive(Debug)]
pub struct Relationship {
    pub id: RelationshipId,
    pub name: String,
    pub variants: Vec<RelationshipVariantId>,
    pub sources: Vec<EntityId>,
    pub targets: Vec<EntityId>,
}

#[derive(Debug)]
pub struct GraphCatalog {
    entities: Vec<Entity>,
    properties: Vec<Property>,
    relationships: Vec<Relationship>,
    variants: Vec<RelationshipVariant>,
    entity_ids: HashMap<String, EntityId>,
    property_ids: Vec<HashMap<String, PropertyId>>,
    relationship_ids: HashMap<String, RelationshipId>,
    variant_ids: HashMap<(RelationshipId, EntityId, EntityId), RelationshipVariantId>,
}

impl GraphCatalog {
    pub(crate) fn derive(ontology: &Ontology) -> Result<Self, DataModelError> {
        let mut entities = Vec::new();
        let mut properties = Vec::new();
        let mut entity_ids = HashMap::new();
        let mut property_ids = Vec::new();

        for node in ontology.nodes() {
            let entity_id = EntityId(entities.len());
            if entity_ids.insert(node.name.clone(), entity_id).is_some() {
                return Err(DataModelError::Duplicate {
                    kind: "entity",
                    name: node.name.clone(),
                });
            }

            let mut entity_properties = Vec::new();
            let mut entity_property_ids = HashMap::new();
            for field in &node.fields {
                let property_id = PropertyId(properties.len());
                if entity_property_ids
                    .insert(field.name.clone(), property_id)
                    .is_some()
                {
                    return Err(DataModelError::Duplicate {
                        kind: "property",
                        name: format!("{}.{}", node.name, field.name),
                    });
                }
                entity_properties.push(property_id);
                properties.push(Property {
                    id: property_id,
                    entity: entity_id,
                    name: field.name.clone(),
                    data_type: field.data_type,
                    enum_values: field.enum_values.clone(),
                    enum_type: field.enum_type,
                    selectivity: field.selectivity,
                    realization: match &field.source {
                        FieldSource::DatabaseColumn(_) => PropertyRealization::Stored,
                        FieldSource::Virtual(source) => {
                            PropertyRealization::Virtual(source.clone())
                        }
                    },
                    filterable: field.filterable,
                    like_allowed: field.like_allowed,
                });
            }
            if !node.fields.iter().any(|field| field.name == "id") {
                let property_id = PropertyId(properties.len());
                entity_property_ids.insert("id".to_string(), property_id);
                entity_properties.push(property_id);
                properties.push(Property {
                    id: property_id,
                    entity: entity_id,
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    enum_values: None,
                    enum_type: EnumType::default(),
                    selectivity: FieldSelectivity::High,
                    realization: PropertyRealization::Stored,
                    filterable: true,
                    like_allowed: true,
                });
            }

            entities.push(Entity {
                id: entity_id,
                name: node.name.clone(),
                properties: entity_properties,
            });
            property_ids.push(entity_property_ids);
        }

        let mut relationships = Vec::new();
        let mut variants = Vec::new();
        let mut relationship_ids = HashMap::new();
        let mut variant_ids = HashMap::new();

        for relationship_name in ontology.edge_names() {
            let relationship_id = RelationshipId(relationships.len());
            relationship_ids.insert(relationship_name.to_string(), relationship_id);
            let mut relationship_variants = Vec::new();

            for edge in ontology.get_edge(relationship_name).unwrap_or_default() {
                if edge.source_kind.is_empty() || edge.target_kind.is_empty() {
                    continue;
                }
                let source = entity_ids.get(&edge.source_kind).copied().ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "source entity",
                        name: edge.source_kind.clone(),
                    }
                })?;
                let target = entity_ids.get(&edge.target_kind).copied().ok_or_else(|| {
                    DataModelError::UnknownReference {
                        kind: "target entity",
                        name: edge.target_kind.clone(),
                    }
                })?;
                let variant_id = RelationshipVariantId(variants.len());
                if variant_ids
                    .insert((relationship_id, source, target), variant_id)
                    .is_some()
                {
                    return Err(DataModelError::Duplicate {
                        kind: "relationship variant",
                        name: format!(
                            "{}({}->{})",
                            relationship_name, edge.source_kind, edge.target_kind
                        ),
                    });
                }
                relationship_variants.push(variant_id);
                variants.push(RelationshipVariant {
                    id: variant_id,
                    relationship: relationship_id,
                    source,
                    target,
                });
            }

            let sources = relationship_variants
                .iter()
                .map(|variant| variants[variant.index()].source)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let targets = relationship_variants
                .iter()
                .map(|variant| variants[variant.index()].target)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            relationships.push(Relationship {
                id: relationship_id,
                name: relationship_name.to_string(),
                variants: relationship_variants,
                sources,
                targets,
            });
        }

        Ok(Self {
            entities,
            properties,
            relationships,
            variants,
            entity_ids,
            property_ids,
            relationship_ids,
            variant_ids,
        })
    }

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
        self.relationships()
            .filter(|relationship| {
                source.is_none_or(|source| relationship.sources.contains(&source))
                    && target.is_none_or(|target| relationship.targets.contains(&target))
            })
            .map(|relationship| relationship.name.clone())
            .collect()
    }
}
