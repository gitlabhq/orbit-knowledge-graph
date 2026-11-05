use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct VariantRoute {
    pub table: String,
    pub foreign_key: Option<ForeignKey>,
}

use crate::{EntityId, GraphCatalog, PropertyId, RelationshipId, RelationshipVariant};

#[derive(Debug, Clone)]
pub enum PropertyRealization {
    Stored { column: String },
    Virtual(ontology::VirtualSource),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Source,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForeignKey {
    pub holder: Endpoint,
    pub property: PropertyId,
    pub referenced_key: PropertyId,
}

impl ForeignKey {
    pub fn resolve(
        graph: &GraphCatalog,
        source: EntityId,
        target: EntityId,
        column: &str,
    ) -> Option<Self> {
        let (holder, property, referenced) = match graph.property_id(source, column) {
            Some(property) => (Endpoint::Source, property, target),
            None => (Endpoint::Target, graph.property_id(target, column)?, source),
        };
        Some(Self {
            holder,
            property,
            referenced_key: graph
                .property_id(referenced, ontology::constants::DEFAULT_PRIMARY_KEY)?,
        })
    }

    pub fn referenced_entity(self, source: EntityId, target: EntityId) -> EntityId {
        match self.holder {
            Endpoint::Source => target,
            Endpoint::Target => source,
        }
    }

    pub fn holder_entity(self, source: EntityId, target: EntityId) -> EntityId {
        match self.holder {
            Endpoint::Source => source,
            Endpoint::Target => target,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraversalPathLookup {
    pub table: String,
    pub property: PropertyId,
}

#[derive(Debug, Clone)]
pub struct PathColumn {
    pub name: String,
    pub entity: Option<EntityId>,
}

#[derive(Debug)]
pub struct RelationshipRoute<'a> {
    pub table: &'a str,
    pub(crate) variants: Vec<&'a RelationshipVariant>,
}

impl RelationshipRoute<'_> {
    pub fn variants(&self) -> impl Iterator<Item = &crate::RelationshipVariant> + '_ {
        self.variants.iter().copied()
    }

    pub fn source_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.source)
    }

    pub fn target_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.target)
    }

    pub fn has_source(&self, entity: EntityId) -> bool {
        self.source_entities().any(|source| source == entity)
    }

    pub fn has_target(&self, entity: EntityId) -> bool {
        self.target_entities().any(|target| target == entity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DenormalizedDirection {
    Source,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DenormalizedKey {
    pub property: PropertyId,
    pub direction: DenormalizedDirection,
}

#[derive(Debug, Clone)]
pub struct DenormalizedProperty {
    pub edge_column: String,
    pub tag_key: String,
    pub variants: HashSet<(RelationshipId, EntityId, EntityId)>,
    pub nullable: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DenormalizedCatalog {
    properties: HashMap<DenormalizedKey, DenormalizedProperty>,
}

impl DenormalizedCatalog {
    pub fn new(properties: HashMap<DenormalizedKey, DenormalizedProperty>) -> Self {
        Self { properties }
    }

    pub fn property(&self, key: DenormalizedKey) -> Option<&DenormalizedProperty> {
        self.properties.get(&key)
    }
}
