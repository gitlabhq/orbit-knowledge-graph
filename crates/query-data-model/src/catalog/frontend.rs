use std::collections::BTreeSet;

use super::Model;
use crate::{Direction, EntityId, Hop, Property, RelationshipId};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unresolved {
    #[error("unknown entity {name}; similar: {}", .similar.join(", "))]
    Entity { name: String, similar: Vec<String> },
    #[error("unknown property {entity}.{name}; similar: {}", .similar.join(", "))]
    Property {
        entity: String,
        name: String,
        similar: Vec<String>,
    },
    #[error("unknown relationship {name}; similar: {}", .similar.join(", "))]
    Relationship { name: String, similar: Vec<String> },
    #[error("no {relationships} relationship connects {from} to {to}; these do: {}", .connecting.join(", "))]
    NotConnected {
        from: String,
        to: String,
        relationships: String,
        connecting: Vec<String>,
    },
}

impl Model {
    pub fn entity(&self, name: &str) -> Result<EntityId, Unresolved> {
        self.graph
            .entity_id(name)
            .ok_or_else(|| Unresolved::Entity {
                name: name.to_string(),
                similar: similar(
                    name,
                    self.graph.entities().map(|entity| entity.name.as_str()),
                ),
            })
    }

    pub fn property(&self, entity: EntityId, name: &str) -> Result<&Property, Unresolved> {
        let found = self.graph.property_id(entity, name);
        found.map(|id| self.graph.property(id)).ok_or_else(|| {
            let entity = self.graph.entity(entity);
            Unresolved::Property {
                entity: entity.name.clone(),
                name: name.to_string(),
                similar: similar(
                    name,
                    entity
                        .properties
                        .iter()
                        .map(|id| self.graph.property(*id).name.as_str()),
                ),
            }
        })
    }

    pub fn relationship(&self, name: &str) -> Result<RelationshipId, Unresolved> {
        self.graph
            .relationship_id(name)
            .ok_or_else(|| Unresolved::Relationship {
                name: name.to_string(),
                similar: similar(
                    name,
                    self.graph
                        .relationships()
                        .map(|relationship| relationship.name.as_str()),
                ),
            })
    }

    pub fn connect(
        &self,
        from: EntityId,
        relationships: &[RelationshipId],
        to: Option<EntityId>,
        direction: Direction,
    ) -> Result<Vec<Hop>, Unresolved> {
        let reaches = |hop: &Hop| to.is_none_or(|to| hop.to == to);
        let hops: Vec<Hop> = self
            .graph
            .hops(from, direction)
            .filter(|hop| {
                reaches(hop)
                    && (relationships.is_empty() || relationships.contains(&hop.relationship))
            })
            .collect();
        if !hops.is_empty() {
            return Ok(hops);
        }
        let connecting: BTreeSet<&str> = self
            .graph
            .hops(from, direction)
            .filter(reaches)
            .map(|hop| self.graph.relationship(hop.relationship).name.as_str())
            .collect();
        Err(Unresolved::NotConnected {
            from: self.graph.entity(from).name.clone(),
            to: to.map_or_else(
                || "any entity".to_string(),
                |to| self.graph.entity(to).name.clone(),
            ),
            relationships: relationships
                .iter()
                .map(|id| self.graph.relationship(*id).name.as_str())
                .collect::<Vec<_>>()
                .join("|"),
            connecting: connecting.into_iter().map(str::to_string).collect(),
        })
    }
}

fn similar<'a>(name: &str, candidates: impl Iterator<Item = &'a str>) -> Vec<String> {
    let wanted = name.to_lowercase();
    candidates
        .filter(|candidate| {
            let candidate = candidate.to_lowercase();
            candidate.contains(&wanted) || wanted.contains(&candidate)
        })
        .map(str::to_string)
        .collect()
}
