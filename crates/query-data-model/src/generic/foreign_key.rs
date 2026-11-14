use super::{EntityId, GraphCatalog, PropertyId};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
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
            referenced_key: match graph.entity(referenced).identity.as_slice() {
                [identity] => *identity,
                _ => return None,
            },
        })
    }

    pub fn referenced_entity(self, source: EntityId, target: EntityId) -> EntityId {
        match self.holder {
            Endpoint::Source => target,
            Endpoint::Target => source,
        }
    }
}
