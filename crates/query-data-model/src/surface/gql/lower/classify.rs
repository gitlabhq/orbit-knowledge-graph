use std::collections::HashSet;

use super::scope::{Scope, Shape};
use crate::Direction;
use crate::plan::logical::Hops;
use crate::surface::SurfaceError;

pub(super) fn reverse(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

impl Scope<'_> {
    pub fn classify(&mut self, referenced: &HashSet<String>) -> Result<(), SurfaceError> {
        let path = matches!(self.shape, Shape::Path(_));
        if !path
            && self.nodes.len() == 2
            && self.edges.len() == 1
            && self.nodes[0].label.is_none()
            && self.nodes[1].label.is_some()
        {
            self.nodes.swap(0, 1);
            let edge = &mut self.edges[0];
            std::mem::swap(&mut edge.from, &mut edge.to);
            edge.direction = reverse(edge.direction);
        }
        let mentioned = |variable: &Option<String>| {
            variable
                .as_ref()
                .is_some_and(|variable| referenced.contains(variable))
        };
        if path {
            if self.edges.len() != 1 || self.nodes.iter().any(|node| node.label.is_none()) {
                return Err(SurfaceError::schema(
                    "a shortest path requires one bounded relationship between two labeled nodes",
                ));
            }
            let edge = &self.edges[0];
            if edge.direction != Direction::Outgoing
                || edge.hops.min != 1
                || !edge.properties.is_empty()
                || mentioned(&edge.variable)
            {
                return Err(SurfaceError::schema(
                    "path finding supports outgoing paths starting at one hop, without relationship predicates",
                ));
            }
        } else if self.nodes.len() == 2 && self.nodes[1].label.is_none() {
            let (center, far, edge) = (&self.nodes[0], &self.nodes[1], &self.edges[0]);
            if !far.properties.is_empty()
                || referenced.contains(&far.alias)
                || center.label.is_none()
                || edge.hops != Hops::ONE
                || !edge.properties.is_empty()
                || mentioned(&edge.variable)
            {
                return Err(SurfaceError::schema(
                    "neighbors requires one labeled center and an unfiltered, unlabeled endpoint at one hop",
                ));
            }
            self.shape = Shape::Neighbors(far.alias.clone());
        } else {
            if self.nodes.iter().any(|node| node.label.is_none()) {
                return Err(SurfaceError::schema(
                    "every node needs one label, except the far endpoint of a neighbors query",
                ));
            }
            for edge in &mut self.edges {
                if edge.direction == Direction::Both {
                    return Err(SurfaceError::schema(
                        "undirected relationships are only supported for neighbors; use -> or <- between labeled nodes",
                    ));
                }
                if edge.direction == Direction::Incoming {
                    std::mem::swap(&mut edge.from, &mut edge.to);
                    edge.direction = Direction::Outgoing;
                }
            }
        }
        Ok(())
    }
}
