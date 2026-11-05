use std::collections::VecDeque;

use super::{EntityId, GraphCatalog, RelationshipId, RelationshipVariantId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hop {
    pub variant: RelationshipVariantId,
    pub relationship: RelationshipId,
    pub from: EntityId,
    pub to: EntityId,
    pub forward: bool,
}

impl GraphCatalog {
    pub fn hops(&self, entity: EntityId, direction: Direction) -> impl Iterator<Item = Hop> + '_ {
        let outgoing = matches!(direction, Direction::Outgoing | Direction::Both).then(|| {
            self.outgoing[entity.index()]
                .iter()
                .map(move |id| (id, true))
        });
        let incoming = matches!(direction, Direction::Incoming | Direction::Both).then(|| {
            self.incoming[entity.index()]
                .iter()
                .filter(move |id| {
                    direction == Direction::Incoming || self.variant(**id).source != entity
                })
                .map(move |id| (id, false))
        });
        outgoing
            .into_iter()
            .flatten()
            .chain(incoming.into_iter().flatten())
            .map(move |(id, forward)| {
                let variant = self.variant(*id);
                Hop {
                    variant: variant.id,
                    relationship: variant.relationship,
                    from: entity,
                    to: if forward {
                        variant.target
                    } else {
                        variant.source
                    },
                    forward,
                }
            })
    }

    pub fn walk(&self, start: EntityId) -> Walk<'_> {
        Walk {
            graph: self,
            start,
            direction: Direction::Outgoing,
            max_hops: 1,
            filter: Box::new(|_| true),
        }
    }
}

pub struct Walk<'g> {
    graph: &'g GraphCatalog,
    start: EntityId,
    direction: Direction,
    max_hops: usize,
    filter: Box<dyn Fn(&Hop) -> bool + 'g>,
}

impl<'g> Walk<'g> {
    #[must_use]
    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    #[must_use]
    pub fn max_hops(mut self, max_hops: usize) -> Self {
        self.max_hops = max_hops;
        self
    }

    #[must_use]
    pub fn filter(self, keep: impl Fn(&Hop) -> bool + 'g) -> Self {
        let previous = self.filter;
        Self {
            filter: Box::new(move |hop| previous(hop) && keep(hop)),
            ..self
        }
    }

    pub fn reachable(&self) -> Reach {
        let mut reach = Reach {
            parent: vec![None; self.graph.entities.len()],
            order: vec![self.start],
        };
        let mut depth = vec![usize::MAX; self.graph.entities.len()];
        depth[self.start.index()] = 0;
        let mut frontier = VecDeque::from([self.start]);
        while let Some(entity) = frontier.pop_front() {
            if depth[entity.index()] == self.max_hops {
                continue;
            }
            for hop in self.graph.hops(entity, self.direction) {
                if depth[hop.to.index()] != usize::MAX || !(self.filter)(&hop) {
                    continue;
                }
                depth[hop.to.index()] = depth[entity.index()] + 1;
                reach.parent[hop.to.index()] = Some(hop);
                reach.order.push(hop.to);
                frontier.push_back(hop.to);
            }
        }
        reach
    }

    pub fn shortest(&self, goal: impl Fn(EntityId) -> bool) -> Option<Vec<Hop>> {
        let reach = self.reachable();
        let found = reach.order.iter().skip(1).find(|entity| goal(**entity))?;
        reach.path_to(*found)
    }
}

#[derive(Debug)]
pub struct Reach {
    parent: Vec<Option<Hop>>,
    order: Vec<EntityId>,
}

impl Reach {
    pub fn path_to(&self, entity: EntityId) -> Option<Vec<Hop>> {
        let mut path = Vec::new();
        let mut current = entity;
        while let Some(hop) = self.parent[current.index()] {
            path.push(hop);
            current = hop.from;
        }
        (current == self.order[0] && (entity == current || !path.is_empty())).then(|| {
            path.reverse();
            path
        })
    }
}
