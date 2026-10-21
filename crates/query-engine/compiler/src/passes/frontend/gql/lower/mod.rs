mod predicates;
mod projections;

use std::collections::{HashMap, HashSet};

use crate::input::{
    Direction, HopRange, InputNeighbors, InputPath, InputRelationship, PathType, PredicateTarget,
    PropertyRef, QueryType,
};
use crate::query_limits::MAX_NODES_CAP;
use crate::{Input, InputNode, QueryError, Result};

use super::ast::{Name, NodePattern, Pattern, PatternElement, Query, Range, Relationship};
use super::{ParsedQuery, invalid};

pub(super) fn lower(query: Query<'_>) -> Result<ParsedQuery> {
    let mut lowering = Lowering {
        input: Input::default(),
        edges: HashMap::new(),
        variable_length: HashSet::new(),
        path: None,
        neighbor: None,
        aliases: HashMap::new(),
    };
    lowering.pattern(query.pattern)?;
    lowering.predicates(query.predicates)?;
    lowering.input.promote_id_filters()?;
    lowering.classify()?;
    lowering.project(query.projections)?;
    if let Some(sort) = query.order {
        lowering.order(sort)?;
    }
    lowering.input.skip = query.skip;
    if let Some(rows) = query.limit {
        lowering.input.limit = rows;
    }
    lowering.input.options.include_debug_sql = query.debug;
    Ok(ParsedQuery {
        input: lowering.input,
    })
}

struct Lowering {
    input: Input,
    edges: HashMap<String, usize>,
    variable_length: HashSet<String>,
    path: Option<String>,
    neighbor: Option<String>,
    aliases: HashMap<String, Option<PropertyRef>>,
}

impl Lowering {
    fn pattern(&mut self, pattern: Pattern<'_>) -> Result<()> {
        let elements = match pattern {
            Pattern::Elements(elements) => elements,
            Pattern::Shortest { variable, element } => {
                self.path = Some(variable.value);
                vec![*element]
            }
        };
        for PatternElement { head, chain } in elements {
            let mut from = self.node(head)?;
            for (relationship, node) in chain {
                let to = self.node(node)?;
                self.edge(relationship, from, to.clone())?;
                from = to;
            }
        }
        self.check_connected_pattern()
    }

    fn check_connected_pattern(&self) -> Result<()> {
        let mut reached = HashSet::new();
        for edge in &self.input.relationships {
            if edge.from == edge.to
                || (!reached.is_empty()
                    && reached.contains(&edge.from) == reached.contains(&edge.to))
            {
                return Err(QueryError::Validation(
                    "each relationship must connect one new node to the preceding pattern; disconnected hops and cycles are unsupported".into(),
                ));
            }
            reached.insert(&edge.from);
            reached.insert(&edge.to);
        }
        if self.input.nodes.len() > 1 && reached.len() != self.input.nodes.len() {
            return Err(QueryError::Validation(
                "all declared nodes must belong to one connected pattern".into(),
            ));
        }
        Ok(())
    }

    fn node(&mut self, pattern: NodePattern<'_>) -> Result<String> {
        let id = pattern.variable.value;
        if self.edges.contains_key(&id) || self.path.as_ref() == Some(&id) {
            return Err(invalid(
                pattern.span,
                "node, relationship, and path variables must be distinct",
            ));
        }
        if let Some(node) = self.input.nodes.iter().find(|node| node.id == id) {
            if pattern
                .label
                .is_some_and(|label| node.entity.as_deref() != Some(&label.value))
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
            return Ok(id);
        }
        if self.input.nodes.len() == MAX_NODES_CAP {
            return Err(invalid(
                pattern.span,
                &format!("a query declares at most {MAX_NODES_CAP} nodes"),
            ));
        }
        let mut node = InputNode {
            id: id.clone(),
            entity: pattern.label.map(|label| label.value),
            ..Default::default()
        };
        Self::map_filters(pattern.properties, &mut node.filters);
        self.input.nodes.push(node);
        Ok(id)
    }

    fn edge(&mut self, relationship: Relationship<'_>, from: String, to: String) -> Result<()> {
        let mut edge = InputRelationship {
            types: vec!["*".into()],
            from,
            to,
            direction: relationship.direction,
            hops: HopRange::default(),
            filters: HashMap::new(),
        };
        if relationship.types.is_empty()
            && let Some(alias) = &relationship.variable
            && is_type_shaped(&alias.value)
        {
            return Err(invalid(
                alias.span,
                &format!(
                    "[{0}] declares a variable, not a relationship type; write [:{0}] for the type, or use a lowercase variable",
                    alias.value
                ),
            ));
        }
        let variable = relationship
            .variable
            .as_ref()
            .map(|alias| alias.value.clone());
        if let Some(alias) = relationship.variable
            && (self.input.nodes.iter().any(|n| n.id == alias.value)
                || self.path.as_ref() == Some(&alias.value)
                || self
                    .edges
                    .insert(alias.value, self.input.relationships.len())
                    .is_some())
        {
            return Err(invalid(alias.span, "relationship variables must be unique"));
        }
        if !relationship.types.is_empty() {
            edge.types = relationship
                .types
                .into_iter()
                .map(|name| name.value)
                .collect();
        }
        if let Some(range) = relationship.range {
            edge.hops = hop_range(range)?;
        }
        if edge.hops.max > 1
            && let Some(variable) = variable
        {
            self.variable_length.insert(variable);
        }
        if let Some(map) = relationship.properties {
            if edge.hops.max > 1 && !map.entries.is_empty() {
                return Err(invalid(
                    map.span,
                    "property filters on variable-length relationships are unsupported",
                ));
            }
            Self::map_filters(map.entries, &mut edge.filters);
        }
        self.input.relationships.push(edge);
        Ok(())
    }

    fn relationship_index(&self, span: pest::Span<'_>, variable: &Name<'_>) -> Result<usize> {
        let name = &variable.value;
        if let Some(index) = self.edges.get(name) {
            return Ok(*index);
        }
        let kind = if self.neighbor.as_ref() == Some(name)
            || self.input.nodes.iter().any(|n| &n.id == name)
        {
            "a node"
        } else if self.path.as_ref() == Some(name) {
            "a path"
        } else {
            return Err(invalid(span, &format!("undefined variable {name}")));
        };
        Err(invalid(
            span,
            &format!(
                "type() takes a relationship variable, such as r in -[r]->, but {name} is {kind}"
            ),
        ))
    }

    fn one_hop(&self, span: pest::Span<'_>, variable: &Name<'_>) -> Result<()> {
        let name = &variable.value;
        if self.variable_length.contains(name) {
            return Err(invalid(
                span,
                &format!(
                    "{name} has a variable length and binds a list; type({name}) and RETURN {name} need a one-hop relationship, so list the types in the pattern instead, such as -[{name}:CLOSES|MENTIONS*1..3]->"
                ),
            ));
        }
        Ok(())
    }

    fn classify(&mut self) -> Result<()> {
        if self.path.is_none()
            && self.input.nodes.len() == 2
            && self.input.relationships.len() == 1
            && self.input.nodes[0].entity.is_none()
            && self.input.nodes[1].entity.is_some()
        {
            self.input.nodes.swap(0, 1);
            let edge = &mut self.input.relationships[0];
            std::mem::swap(&mut edge.from, &mut edge.to);
            edge.direction = match edge.direction {
                Direction::Outgoing => Direction::Incoming,
                Direction::Incoming => Direction::Outgoing,
                Direction::Both => Direction::Both,
            };
        }
        if self.path.is_some() {
            if self.input.relationships.len() != 1
                || self.input.nodes.iter().any(|n| n.entity.is_none())
            {
                return Err(QueryError::Validation(
                    "a shortest path requires one bounded relationship between two labeled nodes"
                        .into(),
                ));
            }
            let edge = self
                .input
                .relationships
                .pop()
                .expect("one path relationship");
            if edge.direction != Direction::Outgoing
                || edge.hops.min != 1
                || !edge.filters.is_empty()
                || self.input.references(&PredicateTarget::Relationship(0))
            {
                return Err(QueryError::Validation("path finding supports outgoing paths starting at one hop, without relationship predicates".into()));
            }
            self.input.query_type = QueryType::PathFinding;
            self.input.path = Some(InputPath {
                path_type: PathType::Shortest,
                from: edge.from,
                to: edge.to,
                max_depth: edge.hops.max,
                rel_types: edge.types,
            });
        } else if self.input.nodes.len() == 2 && self.input.nodes[1].entity.is_none() {
            let far = self.input.nodes.pop().expect("neighbor endpoint");
            let edge = self.input.relationships.pop().expect("neighbor edge");
            if !far.filters.is_empty()
                || !far.node_ids.is_empty()
                || far.id_range.is_some()
                || self.input.nodes[0].entity.is_none()
                || edge.hops != HopRange::default()
                || !edge.filters.is_empty()
                || self
                    .input
                    .references(&PredicateTarget::Node(far.id.clone()))
                || self.input.references(&PredicateTarget::Relationship(0))
            {
                return Err(QueryError::Validation("neighbors requires one labeled center and an unfiltered, unlabeled endpoint at one hop".into()));
            }
            self.neighbor = Some(far.id);
            self.input.query_type = QueryType::Neighbors;
            self.input.neighbors = Some(InputNeighbors {
                direction: edge.direction,
                rel_types: edge.types,
            });
        } else {
            if self.input.nodes.iter().any(|n| n.entity.is_none()) {
                return Err(QueryError::Validation(
                    "every node needs one label, except the far endpoint of a neighbors query"
                        .into(),
                ));
            }
            for edge in &mut self.input.relationships {
                if edge.direction == Direction::Both {
                    return Err(QueryError::Validation(
                        "undirected relationships are only supported for neighbors; use -> or <- between labeled nodes".into(),
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

fn hop_range(range: Range<'_>) -> Result<HopRange> {
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
    Ok(HopRange { min, max })
}

fn is_type_shaped(name: &str) -> bool {
    name.len() > 1
        && name.starts_with(|c: char| c.is_ascii_uppercase())
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}
