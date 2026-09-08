mod predicates;
mod projections;

use std::collections::HashMap;

use compiler::input::{
    Direction, HopRange, InputNeighbors, InputPath, InputRelationship, PathType, QueryType,
};
use compiler::{Input, InputNode, QueryError, Result};
use pest::iterators::Pair;

use crate::value::Bindings;
use crate::{Rule, invalid, name};

pub(super) fn lower(statement: Pair<'_, Rule>, bindings: Bindings<'_>) -> Result<Input> {
    let mut lowering = Lowering {
        input: Input::default(),
        bindings,
        edges: HashMap::new(),
        path: None,
        neighbor: None,
    };
    for clause in statement.into_inner() {
        match clause.as_rule() {
            Rule::Match => lowering.pattern(clause)?,
            Rule::Return => lowering.project(clause)?,
            Rule::Order => lowering.order(clause)?,
            Rule::Limit => {
                let value = crate::value::value(
                    clause
                        .clone()
                        .into_inner()
                        .next()
                        .expect("LIMIT has a value"),
                    &lowering.bindings,
                )?;
                lowering.input.limit = value
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| invalid(&clause, "LIMIT must be a positive integer"))?;
            }
            Rule::EOI => {}
            _ => return Err(invalid(&clause, "unsupported clause")),
        }
    }
    Ok(lowering.input)
}

struct Lowering<'a> {
    input: Input,
    bindings: Bindings<'a>,
    edges: HashMap<String, usize>,
    path: Option<String>,
    neighbor: Option<String>,
}

impl Lowering<'_> {
    fn pattern(&mut self, clause: Pair<'_, Rule>) -> Result<()> {
        let mut parts = clause.into_inner();
        let pattern = parts.next().expect("MATCH has a pattern");
        let mut pattern = pattern.into_inner().next().expect("Pattern has a child");
        if pattern.as_rule() == Rule::ShortestPattern {
            let mut shortest = pattern.into_inner();
            self.path = Some(name(shortest.next().expect("shortestPath has a variable"))?);
            pattern = shortest.next().expect("shortestPath has a pattern");
        }
        let mut nodes = pattern.into_inner();
        self.node(nodes.next().expect("pattern starts with a node"))?;
        for chain in nodes {
            let mut chain = chain.into_inner();
            let edge = chain.next().expect("chain has an edge");
            let from = self.input.nodes.last().expect("previous node").id.clone();
            self.node(chain.next().expect("chain has a node"))?;
            let to = self.input.nodes.last().expect("next node").id.clone();
            self.edge(edge, from, to)?;
        }
        for clause in parts {
            self.predicates(clause)?;
        }
        self.promote_ids()?;
        self.classify()?;
        Ok(())
    }

    fn node(&mut self, pair: Pair<'_, Rule>) -> Result<()> {
        let mut parts = pair.clone().into_inner();
        let id = name(parts.next().expect("node has a variable"))?;
        if self.input.nodes.iter().any(|n| n.id == id)
            || self.edges.contains_key(&id)
            || self.path.as_ref() == Some(&id)
        {
            return Err(invalid(
                &pair,
                "variables must be unique; repeated nodes and cycles are unsupported",
            ));
        }
        let mut node = InputNode {
            id,
            ..Default::default()
        };
        for part in parts {
            match part.as_rule() {
                Rule::NodeLabel => {
                    node.entity = Some(name(part.into_inner().next().expect("label has a name"))?)
                }
                Rule::MapLiteral => Self::map_filters(part, &self.bindings, &mut node.filters)?,
                _ => return Err(invalid(&part, "unsupported node pattern")),
            }
        }
        if let Some(filters) = node.filters.get("id")
            && let [filter] = filters.as_slice()
            && let Some(id) = filter.value.as_ref().and_then(serde_json::Value::as_i64)
        {
            node.node_ids.push(id);
            node.filters.remove("id");
        }
        self.input.nodes.push(node);
        Ok(())
    }

    fn edge(&mut self, pair: Pair<'_, Rule>, from: String, to: String) -> Result<()> {
        let shape = pair
            .into_inner()
            .next()
            .expect("relationship has a direction");
        let direction = match shape.as_rule() {
            Rule::Outgoing => Direction::Outgoing,
            Rule::Incoming => Direction::Incoming,
            Rule::Undirected => Direction::Both,
            _ => return Err(invalid(&shape, "unsupported relationship direction")),
        };
        let mut edge = InputRelationship {
            types: vec!["*".into()],
            from,
            to,
            direction,
            hops: HopRange::default(),
            filters: HashMap::new(),
            fk_column: None,
            scope_prefix: None,
            scope_preserving: false,
        };
        if let Some(detail) = shape.into_inner().next() {
            for part in detail.into_inner() {
                match part.as_rule() {
                    Rule::Variable => {
                        let alias = name(part.clone())?;
                        if self.input.nodes.iter().any(|n| n.id == alias)
                            || self.path.as_ref() == Some(&alias)
                            || self
                                .edges
                                .insert(alias, self.input.relationships.len())
                                .is_some()
                        {
                            return Err(invalid(&part, "relationship variables must be unique"));
                        }
                    }
                    Rule::RelationshipTypes => {
                        edge.types = part.into_inner().map(name).collect::<Result<_>>()?
                    }
                    Rule::RangeLiteral => edge.hops = hop_range(part)?,
                    Rule::MapLiteral => {
                        if edge.hops.max > 1 && part.clone().into_inner().next().is_some() {
                            return Err(invalid(
                                &part,
                                "property filters on variable-length relationships are unsupported",
                            ));
                        }
                        Self::map_filters(part, &self.bindings, &mut edge.filters)?;
                    }
                    _ => return Err(invalid(&part, "unsupported relationship pattern")),
                }
            }
        }
        self.input.relationships.push(edge);
        Ok(())
    }

    fn classify(&mut self) -> Result<()> {
        if self.path.is_some() {
            if self.input.relationships.len() != 1
                || self.input.nodes.iter().any(|n| n.entity.is_none())
            {
                return Err(QueryError::Validation(
                    "shortestPath requires one bounded relationship between two labeled nodes"
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
                forward_first_hop_rel_types: Vec::new(),
                backward_first_hop_rel_types: Vec::new(),
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
                if edge.direction == Direction::Incoming {
                    std::mem::swap(&mut edge.from, &mut edge.to);
                    edge.direction = Direction::Outgoing;
                }
            }
        }
        Ok(())
    }
}

fn hop_range(pair: Pair<'_, Rule>) -> Result<HopRange> {
    let mut start = None;
    let mut end = None;
    let mut dots = false;
    for bound in pair.clone().into_inner() {
        match bound.as_rule() {
            Rule::RangeStart => {
                start = Some(
                    bound
                        .as_str()
                        .parse::<u32>()
                        .map_err(|_| invalid(&bound, "invalid hop bound"))?,
                )
            }
            Rule::RangeEnd => {
                end = Some(
                    bound
                        .as_str()
                        .parse::<u32>()
                        .map_err(|_| invalid(&bound, "invalid hop bound"))?,
                )
            }
            Rule::RangeDots => dots = true,
            _ => unreachable!("range grammar produces bounds or dots"),
        }
    }
    let min = start.unwrap_or(1);
    let max = if dots { end } else { start }.ok_or_else(|| {
        invalid(
            &pair,
            "unbounded paths are unsupported; use *1..3 or an exact *2",
        )
    })?;
    if min == 0 || min > max {
        return Err(invalid(&pair, "hop bounds must be positive and ordered"));
    }
    Ok(HopRange { min, max })
}
