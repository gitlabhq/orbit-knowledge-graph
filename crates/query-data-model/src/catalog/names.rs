use std::collections::BTreeSet;

use super::Model;
use crate::generic::AttributeId;
use crate::{
    ConnectionId, Direction, EntityId, Hop, PropertyId, RelationshipId, RelationshipVariantId,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unresolved {
    #[error("unknown entity {name}; candidates: {}", .similar.join(", "))]
    Entity { name: String, similar: Vec<String> },
    #[error("unknown property {entity}.{name}; candidates: {}", .similar.join(", "))]
    Property {
        entity: String,
        name: String,
        similar: Vec<String>,
    },
    #[error("unknown relationship {name}; candidates: {}", .similar.join(", "))]
    Relationship { name: String, similar: Vec<String> },
    #[error("no {}relationship connects {from} to {to}; {}", named(.relationships), hint(.from, .to, .connecting, .reversed))]
    NotConnected {
        from: String,
        to: String,
        relationships: String,
        connecting: Vec<String>,
        reversed: Option<Box<Reversed>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reversed {
    pub relationships: String,
    pub source: String,
    pub target: String,
}

fn named(relationships: &str) -> String {
    match relationships {
        "" | "*" => String::new(),
        named => format!("{named} "),
    }
}

fn hint(from: &str, to: &str, connecting: &[String], reversed: &Option<Box<Reversed>>) -> String {
    match (connecting, reversed) {
        (_, Some(reversed)) => format!(
            "{} goes from {} to {}; reverse the arrow",
            reversed.relationships, reversed.source, reversed.target
        ),
        ([_, ..], None) => format!("these do: {}", connecting.join(", ")),
        ([], None) => format!("nothing connects {from} and {to} in this direction"),
    }
}

/// Resolves written names to handles. The query frontend resolves a query's names once here, and
/// nothing after it looks a name up; the indexer, the CLI and tests resolve theirs here too.
#[derive(Debug, Clone, Copy)]
pub struct Names<'m>(&'m Model);

impl Model {
    pub fn names(&self) -> Names<'_> {
        Names(self)
    }
}

impl Names<'_> {
    pub fn entity(&self, name: &str) -> Result<EntityId, Unresolved> {
        let model = self.0;
        model
            .graph
            .entity_id(name)
            .ok_or_else(|| Unresolved::Entity {
                name: name.to_string(),
                similar: similar(
                    name,
                    model.graph.entities().map(|entity| entity.name.as_str()),
                ),
            })
    }

    pub fn property(&self, entity: EntityId, name: &str) -> Result<PropertyId, Unresolved> {
        self.0
            .graph
            .property_id(entity, name)
            .ok_or_else(|| self.0.unknown_property(entity, name))
    }

    pub fn relationship(&self, name: &str) -> Result<RelationshipId, Unresolved> {
        let model = self.0;
        model
            .graph
            .relationship_id(name)
            .ok_or_else(|| Unresolved::Relationship {
                name: name.to_string(),
                similar: similar(
                    name,
                    model
                        .graph
                        .relationships()
                        .map(|relationship| relationship.name.as_str()),
                ),
            })
    }

    /// The variant `source -> target` of the relationship named `relationship`.
    pub fn variant(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<RelationshipVariantId> {
        self.0
            .graph
            .variant_named(relationship, source, target)
            .map(|variant| variant.id)
    }

    pub fn attribute(&self, relationship: RelationshipId, name: &str) -> Option<AttributeId> {
        self.0.graph.attribute_id(relationship, name)
    }

    pub fn connection(&self, name: &str) -> Option<ConnectionId> {
        self.0
            .connections()
            .find(|connection| connection.name() == name)
            .map(|connection| connection.id())
    }
}

impl Model {
    pub(crate) fn unknown_property(&self, entity: EntityId, name: &str) -> Unresolved {
        let entity = self.graph.entity(entity);
        let visible = entity
            .properties
            .iter()
            .filter(|id| !self.access().property(**id).hidden)
            .map(|id| self.graph.property(*id).name.as_str());
        Unresolved::Property {
            entity: entity.name.clone(),
            name: name.to_string(),
            similar: similar(name, visible),
        }
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
        let reversed = to.and_then(|to| self.reversed(from, relationships, to, direction));
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
            reversed,
        })
    }

    fn reversed(
        &self,
        from: EntityId,
        relationships: &[RelationshipId],
        to: EntityId,
        direction: Direction,
    ) -> Option<Box<Reversed>> {
        let names: BTreeSet<&str> = self
            .graph
            .hops(to, direction)
            .filter(|hop| hop.to == from)
            .filter(|hop| relationships.is_empty() || relationships.contains(&hop.relationship))
            .map(|hop| self.graph.relationship(hop.relationship).name.as_str())
            .collect();
        let (from, to) = (self.graph.entity(from), self.graph.entity(to));
        let (source, target) = match direction {
            Direction::Outgoing => (to, from),
            Direction::Incoming => (from, to),
            Direction::Both => return None,
        };
        (!names.is_empty()).then(|| {
            Box::new(Reversed {
                relationships: names.into_iter().collect::<Vec<_>>().join("|"),
                source: source.name.clone(),
                target: target.name.clone(),
            })
        })
    }
}

fn similar<'a>(name: &str, candidates: impl Iterator<Item = &'a str>) -> Vec<String> {
    let candidates: Vec<&str> = candidates.collect();
    let wanted = name.to_lowercase();
    let close: Vec<String> = candidates
        .iter()
        .filter(|candidate| {
            let candidate = candidate.to_lowercase();
            candidate.contains(&wanted) || wanted.contains(&candidate)
        })
        .map(|candidate| candidate.to_string())
        .collect();
    match close.is_empty() {
        true => candidates.into_iter().map(str::to_string).collect(),
        false => close,
    }
}
