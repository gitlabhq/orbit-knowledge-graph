use crate::plan::tracking::Declared;
use crate::{Direction, EntityId, RelationshipId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hops {
    pub min: u32,
    pub max: u32,
}

impl Hops {
    pub const ONE: Self = Self { min: 1, max: 1 };
}

#[derive(Debug, Clone)]
pub struct Step {
    pub(super) relationships: Vec<Declared<RelationshipId>>,
    pub(super) direction: Direction,
    pub(super) target: Option<Declared<EntityId>>,
    pub(super) hops: Hops,
    pub(super) node: Option<String>,
    pub(super) edge: Option<String>,
}

impl Step {
    pub fn new(direction: Direction) -> Self {
        Self {
            relationships: Vec::new(),
            direction,
            target: None,
            hops: Hops::ONE,
            node: None,
            edge: None,
        }
    }

    #[must_use]
    pub fn via(
        mut self,
        relationships: impl IntoIterator<Item = Declared<RelationshipId>>,
    ) -> Self {
        self.relationships.extend(relationships);
        self
    }

    #[must_use]
    pub fn to(mut self, target: Declared<EntityId>) -> Self {
        self.target = Some(target);
        self
    }

    #[must_use]
    pub fn hops(mut self, hops: Hops) -> Self {
        self.hops = hops;
        self
    }

    #[must_use]
    pub fn named(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }

    #[must_use]
    pub fn edge(mut self, edge: impl Into<String>) -> Self {
        self.edge = Some(edge.into());
        self
    }
}
