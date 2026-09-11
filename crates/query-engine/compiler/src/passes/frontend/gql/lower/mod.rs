mod predicates;
mod projections;

use std::collections::HashMap;

use crate::input::{
    Direction, HopRange, InputCursor, InputNeighbors, InputPath, InputRelationship, PathType,
    QueryType,
};
use crate::passes::cursor;
use crate::{Input, InputNode, QueryError, Result};

use super::ast::{Limit, NodePattern, Pattern, PatternElement, Query, Range, Relationship};
use super::{QueryParser, Rule, invalid};

pub(super) fn lower(source: &str, query: Query<'_>) -> Result<Input> {
    let mut lowering = Lowering {
        input: Input::default(),
        edges: HashMap::new(),
        path: None,
        neighbor: None,
    };
    lowering.pattern(query.pattern)?;
    for predicate in query.predicates {
        lowering.predicate(predicate)?;
    }
    lowering.promote_ids()?;
    lowering.classify()?;
    lowering.project(query.projections)?;
    if let Some(sort) = query.order {
        lowering.order(sort)?;
    }
    match query.limit {
        Some(Limit::Rows(rows)) => lowering.input.limit = rows,
        Some(Limit::Page { span, size, after }) => {
            lowering.input.cursor = Some(InputCursor {
                page_size: size,
                after,
                seek: None,
            });
            lowering.input.compiler.query_hash = statement_hash(source, span);
        }
        None => {}
    }
    lowering.input.options.include_debug_sql = query.debug;
    Ok(lowering.input)
}

fn statement_hash(source: &str, page: pest::Span<'_>) -> u64 {
    let text = format!("{}{}", &source[..page.start()], &source[page.end()..]);
    let tokens = <QueryParser as pest::Parser<Rule>>::parse(Rule::HashTokens, &text)
        .expect("HashTokens accepts every character")
        .next()
        .expect("HashTokens produces one pair")
        .into_inner()
        .filter(|pair| pair.as_rule() == Rule::HashToken)
        .map(|pair| serde_json::Value::String(pair.as_str().to_owned()))
        .collect();
    cursor::canonical_hash(&serde_json::Value::Array(tokens))
}

struct Lowering {
    input: Input,
    edges: HashMap<String, usize>,
    path: Option<String>,
    neighbor: Option<String>,
}

impl Lowering {
    fn pattern(&mut self, pattern: Pattern<'_>) -> Result<()> {
        let element = match pattern {
            Pattern::Element(element) => element,
            Pattern::Shortest { variable, element } => {
                self.path = Some(variable.value);
                element
            }
        };
        let PatternElement { head, chain } = element;
        self.node(head)?;
        for (relationship, node) in chain {
            let from = self.input.nodes.last().expect("previous node").id.clone();
            self.node(node)?;
            let to = self.input.nodes.last().expect("next node").id.clone();
            self.edge(relationship, from, to)?;
        }
        Ok(())
    }

    fn node(&mut self, pattern: NodePattern<'_>) -> Result<()> {
        let id = pattern.variable.value;
        if self.input.nodes.iter().any(|n| n.id == id)
            || self.edges.contains_key(&id)
            || self.path.as_ref() == Some(&id)
        {
            return Err(invalid(
                pattern.span,
                "variables must be unique; repeated nodes and cycles are unsupported",
            ));
        }
        let mut node = InputNode {
            id,
            entity: pattern.label.map(|label| label.value),
            ..Default::default()
        };
        Self::map_filters(pattern.properties, &mut node.filters);
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

    fn edge(&mut self, relationship: Relationship<'_>, from: String, to: String) -> Result<()> {
        let mut edge = InputRelationship {
            types: vec!["*".into()],
            from,
            to,
            direction: relationship.direction,
            hops: HopRange::default(),
            filters: HashMap::new(),
            fk_column: None,
            scope_prefix: None,
            scope_preserving: false,
        };
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

    fn classify(&mut self) -> Result<()> {
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

#[cfg(test)]
mod tests {
    fn hash(query: &str) -> u64 {
        super::super::parse(query).unwrap().compiler.query_hash
    }

    #[test]
    fn statement_hash_ignores_page_and_formatting() {
        let base = hash("MATCH (u:User) RETURN u PAGE 5");
        for query in [
            "MATCH (u:User) RETURN u PAGE 7 AFTER 'x'",
            "MATCH (u:User) RETURN u  PAGE 5",
            "\nMATCH ( u : User )\nRETURN\tu\nPAGE 7\nAFTER 'x'\n",
            "MATCH/*match*/(u:User) RETURN u/*before*/PAGE 7/*after*/",
            "MATCH (u:User) RETURN u // before\nPAGE 7 AFTER 'x' // after",
            "MATCH\u{2003}(u:User) RETURN u\u{a0}PAGE 5\u{3000}",
        ] {
            assert_eq!(base, hash(query), "{query}");
        }
        assert_eq!(
            hash("MATCH (u:User) RETURN u PAGE 5 DEBUG"),
            hash("MATCH (u:User) RETURN u\nPAGE 7 AFTER 'x'\nDEBUG\n")
        );
        assert_eq!(0, hash("MATCH (u:User) RETURN u LIMIT 5"));
    }

    #[test]
    fn statement_hash_preserves_query_changes() {
        let base = hash("MATCH (u:User) RETURN u PAGE 5");
        for query in [
            "MATCH (u:User) RETURN u.id PAGE 5",
            "MATCH (u:User {id: 1}) RETURN u PAGE 5",
            "MATCH (u:User) RETURN u PAGE 5 DEBUG",
            "MATCH (u:User) RETURN u PAGE 5;",
            "match (u:User) RETURN u PAGE 5",
        ] {
            assert_ne!(base, hash(query), "{query}");
        }
        for (left, right) in [
            ("'a b'", "'ab'"),
            ("'a  b'", "'a b'"),
            ("'a\nb'", "'a b'"),
            ("'a/*b*/c'", "'ac'"),
            ("'a//b'", "'a'"),
            ("'é PAGE 5'", "'é PAGE 7'"),
            (r"'a\' b'", r"'a\'b'"),
            (r#""a b""#, r#""ab""#),
        ] {
            let query = |value| format!("MATCH (u:User {{name: {value}}}) RETURN u PAGE 5");
            assert_ne!(hash(&query(left)), hash(&query(right)), "{left}: {right}");
        }
    }
}
