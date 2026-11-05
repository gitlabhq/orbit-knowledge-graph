use std::collections::HashSet;

use super::super::ast::{MapEntry, NodePattern, Pattern, PatternElement, Range, Relationship};
use super::super::errors::invalid;
use crate::Direction;
use crate::plan::logical::Hops;
use crate::surface::SurfaceError;

const MAX_NODES: usize = 5;

pub(super) struct Node<'i> {
    pub alias: String,
    pub label: Option<String>,
    pub properties: Vec<MapEntry<'i>>,
}

pub(super) struct Edge<'i> {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub from: String,
    pub to: String,
    pub direction: Direction,
    pub hops: Hops,
    pub properties: Vec<MapEntry<'i>>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Shape {
    Match,
    Path(String),
    Neighbors(String),
}

pub(super) struct Scope<'i> {
    pub nodes: Vec<Node<'i>>,
    pub edges: Vec<Edge<'i>>,
    pub shape: Shape,
}

impl<'i> Scope<'i> {
    pub fn node(&self, alias: &str) -> Option<&Node<'i>> {
        self.nodes.iter().find(|node| node.alias == alias)
    }

    pub fn edge(&self, variable: &str) -> Option<(usize, &Edge<'i>)> {
        self.edges
            .iter()
            .enumerate()
            .find(|(_, edge)| edge.variable.as_deref() == Some(variable))
    }

    pub fn dynamic(&self, variable: &str) -> bool {
        matches!(&self.shape, Shape::Path(name) | Shape::Neighbors(name) if name == variable)
    }

    pub fn collect(pattern: Pattern<'i>) -> Result<Self, SurfaceError> {
        let (elements, path) = match pattern {
            Pattern::Elements(elements) => (elements, None),
            Pattern::Shortest { variable, element } => (vec![*element], Some(variable.value)),
        };
        let mut scope = Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            shape: path.map_or(Shape::Match, Shape::Path),
        };
        for PatternElement { head, chain } in elements {
            let mut from = scope.declare(head)?;
            for (relationship, node) in chain {
                let to = scope.declare(node)?;
                scope.link(relationship, from, to.clone())?;
                from = to;
            }
        }
        scope.connected()?;
        Ok(scope)
    }

    fn declare(&mut self, pattern: NodePattern<'i>) -> Result<String, SurfaceError> {
        let alias = pattern.variable.value;
        if self.edge(&alias).is_some() || self.shape == Shape::Path(alias.clone()) {
            return Err(invalid(
                pattern.span,
                "node, relationship, and path variables must be distinct",
            ));
        }
        if let Some(node) = self.node(&alias) {
            if pattern
                .label
                .is_some_and(|label| node.label.as_deref() != Some(&label.value))
            {
                return Err(invalid(
                    pattern.span,
                    "a repeated node must keep its original label",
                ));
            }
            if !pattern.properties.is_empty() {
                return Err(invalid(
                    pattern.span,
                    "declare a node's properties once or use WHERE predicates",
                ));
            }
            return Ok(alias);
        }
        if self.nodes.len() == MAX_NODES {
            return Err(invalid(
                pattern.span,
                &format!("a query declares at most {MAX_NODES} nodes"),
            ));
        }
        self.nodes.push(Node {
            alias: alias.clone(),
            label: pattern.label.map(|label| label.value),
            properties: pattern.properties,
        });
        Ok(alias)
    }

    fn link(
        &mut self,
        relationship: Relationship<'i>,
        from: String,
        to: String,
    ) -> Result<(), SurfaceError> {
        if relationship.types.is_empty()
            && let Some(alias) = &relationship.variable
            && type_shaped(&alias.value)
        {
            return Err(invalid(
                alias.span,
                &format!(
                    "[{0}] declares a variable, not a relationship type; write [:{0}] for the type, or use a lowercase variable",
                    alias.value
                ),
            ));
        }
        if let Some(alias) = &relationship.variable
            && (self.node(&alias.value).is_some()
                || self.shape == Shape::Path(alias.value.clone())
                || self.edge(&alias.value).is_some())
        {
            return Err(invalid(alias.span, "relationship variables must be unique"));
        }
        let hops = match relationship.range {
            Some(range) => hops(range)?,
            None => Hops::ONE,
        };
        let properties = match relationship.properties {
            Some(map) if hops.max > 1 && !map.entries.is_empty() => {
                return Err(invalid(
                    map.span,
                    "property filters on variable-length relationships are unsupported",
                ));
            }
            Some(map) => map.entries,
            None => Vec::new(),
        };
        self.edges.push(Edge {
            variable: relationship.variable.map(|alias| alias.value),
            types: relationship
                .types
                .into_iter()
                .map(|name| name.value)
                .collect(),
            from,
            to,
            direction: relationship.direction,
            hops,
            properties,
        });
        Ok(())
    }

    fn connected(&self) -> Result<(), SurfaceError> {
        let mut reached = HashSet::new();
        for edge in &self.edges {
            if edge.from == edge.to
                || (!reached.is_empty()
                    && reached.contains(&edge.from) == reached.contains(&edge.to))
            {
                return Err(SurfaceError::schema(
                    "each relationship must connect one new node to the preceding pattern; disconnected hops and cycles are unsupported",
                ));
            }
            reached.insert(&edge.from);
            reached.insert(&edge.to);
        }
        if self.nodes.len() > 1 && reached.len() != self.nodes.len() {
            return Err(SurfaceError::schema(
                "all declared nodes must belong to one connected pattern",
            ));
        }
        Ok(())
    }
}

fn hops(range: Range<'_>) -> Result<Hops, SurfaceError> {
    let min = range.start.unwrap_or(1);
    let max = if range.dots { range.end } else { range.start }.ok_or_else(|| {
        invalid(
            range.span,
            "unbounded paths are unsupported; use *1..3 or an exact *2",
        )
    })?;
    if min == 0 || min > max {
        return Err(invalid(
            range.span,
            "hop bounds must be positive and ordered",
        ));
    }
    Ok(Hops { min, max })
}

fn type_shaped(name: &str) -> bool {
    name.len() > 1
        && name.starts_with(|c: char| c.is_ascii_uppercase())
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}
