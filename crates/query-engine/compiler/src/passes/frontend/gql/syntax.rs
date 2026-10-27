use std::collections::HashSet;

use pest::Span;
use pest::error::ErrorVariant;
use pest_consume::{Error, match_nodes};
use serde_json::{Number, Value};

use super::ast::*;
use super::{QueryParser, Rule};
use crate::input::{BooleanExpression, Direction, FilterOp, OrderDirection, TruncateUnit};
use crate::query_limits::MAX_FILTER_STRING_LEN;

pub(super) type Node<'i> = pest_consume::Node<'i, Rule, ()>;
pub(super) type Result<T> = std::result::Result<T, Error<Rule>>;

#[pest_consume::parser]
impl QueryParser {
    fn EOI(_input: Node) -> Result<()> {
        Ok(())
    }

    pub(super) fn Statement(input: Node) -> Result<Statement> {
        Ok(match_nodes!(input.into_children();
            [Query(query), EOI(_)] => Statement::Query(Box::new(query)),
            [SchemaCall(call), EOI(_)] => call,
        ))
    }

    fn Query(input: Node) -> Result<Query> {
        Ok(match_nodes!(input.into_children();
            [Matches((pattern, predicates)), Return(projections), clauses..] => {
                let mut query = Query {
                    pattern, predicates, projections, order: None, skip: None, limit: None, debug: false,
                };
                for clause in clauses {
                    match clause.as_rule() {
                        Rule::Order => query.order = Some(Self::Order(clause)?),
                        Rule::Skip => query.skip = Some(Self::Skip(clause)?),
                        Rule::Limit => query.limit = Some(Self::Limit(clause)?),
                        Rule::Debug => query.debug = Self::Debug(clause)?,
                        _ => return Err(mismatch(&clause)),
                    }
                }
                query
            },
        ))
    }

    fn SchemaCall(input: Node) -> Result<Statement> {
        let node = match_nodes!(input.into_children();
            [] => None,
            [literal] => Some(string(&literal)?),
        );
        Ok(Statement::SchemaCall { node })
    }

    fn Matches(input: Node) -> Result<(Pattern, Vec<BooleanExpression<Predicate>>)> {
        let node = input.clone();
        let mut matches: Vec<_> = match_nodes!(input.into_children();
            [Match(matches)..] => matches.collect(),
        );
        if matches.len() == 1 {
            return Ok(matches.pop().expect("one MATCH clause"));
        }
        let mut elements = Vec::new();
        let mut predicates = Vec::new();
        for (pattern, clause_predicates) in matches {
            let Pattern::Elements(clause_elements) = pattern else {
                return Err(node.error("a shortest path must be the only MATCH pattern"));
            };
            elements.extend(clause_elements);
            predicates.extend(clause_predicates);
        }
        Ok((Pattern::Elements(elements), predicates))
    }

    fn Match(input: Node) -> Result<(Pattern, Vec<BooleanExpression<Predicate>>)> {
        Ok(match_nodes!(input.into_children();
            [Pattern(pattern)] => (pattern, Vec::new()),
            [Pattern(pattern), Where(predicate)] => (pattern, vec![predicate]),
        ))
    }

    fn Pattern(input: Node) -> Result<Pattern> {
        Ok(match_nodes!(input.into_children();
            [ShortestPattern(pattern)] => pattern,
            [PatternElement(elements)..] => Pattern::Elements(elements.collect()),
        ))
    }

    fn ShortestPattern(input: Node) -> Result<Pattern> {
        Ok(match_nodes!(input.into_children();
            [Variable(variable), PathSearch(_), PatternElement(element)] => Pattern::Shortest { variable, element: Box::new(element) },
            [Variable(_), LegacyShortestPath(_)] => unreachable!("LegacyShortestPath always errors"),
        ))
    }

    fn LegacyShortestPath(input: Node) -> Result<()> {
        Err(input.error("use ANY SHORTEST (a)-[:CONTAINS*1..3]->(b) for shortest paths"))
    }

    fn PathSearch(input: Node) -> Result<()> {
        let span = input.as_span();
        match_nodes!(input.into_children();
            [] => Ok(()),
            [PathCount(count)] => if count.as_u64() == Some(1) {
                Ok(())
            } else {
                Err(error_at(span, "only one shortest path is supported; use ANY SHORTEST or SHORTEST 1"))
            },
        )
    }

    fn PatternElement(input: Node) -> Result<PatternElement> {
        Ok(match_nodes!(input.into_children();
            [NodePattern(head), PatternElementChain(chain)..] => PatternElement {
                head,
                chain: chain.collect(),
            },
        ))
    }

    fn PatternElementChain(input: Node) -> Result<(Relationship, NodePattern)> {
        Ok(match_nodes!(input.into_children();
            [RelationshipPattern(relationship), NodePattern(node)] => (relationship, node),
        ))
    }

    fn NodePattern(input: Node) -> Result<NodePattern> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Variable(variable)] => NodePattern {
                span, variable, label: None, properties: Vec::new(),
            },
            [Variable(variable), NodeLabel(label)] => NodePattern {
                span, variable, label: Some(label), properties: Vec::new(),
            },
            [Variable(variable), NodeMapLiteral(map)] => NodePattern {
                span, variable, label: None, properties: map.entries,
            },
            [Variable(variable), NodeLabel(label), NodeMapLiteral(map)] => NodePattern {
                span, variable, label: Some(label), properties: map.entries,
            },
        ))
    }

    fn NodeLabel(input: Node) -> Result<Name> {
        Ok(match_nodes!(input.into_children();
            [SchemaName(name)] => name,
        ))
    }

    fn RelationshipPattern(input: Node) -> Result<Relationship> {
        Ok(match_nodes!(input.into_children();
            [relationship(relationship)] => relationship,
        ))
    }

    #[alias(relationship)]
    fn Outgoing(input: Node) -> Result<Relationship> {
        directed(input, Direction::Outgoing)
    }

    #[alias(relationship)]
    fn Incoming(input: Node) -> Result<Relationship> {
        directed(input, Direction::Incoming)
    }

    #[alias(relationship)]
    fn Undirected(input: Node) -> Result<Relationship> {
        directed(input, Direction::Both)
    }

    fn RelationshipDetail(input: Node) -> Result<Relationship> {
        let mut relationship = Relationship {
            direction: Direction::Outgoing,
            variable: None,
            types: Vec::new(),
            range: None,
            properties: None,
        };
        for child in input.into_children() {
            match child.as_rule() {
                Rule::Variable => relationship.variable = Some(Self::Variable(child)?),
                Rule::RelationshipTypes => relationship.types = Self::RelationshipTypes(child)?,
                Rule::RangeLiteral => relationship.range = Some(Self::RangeLiteral(child)?),
                Rule::RelationshipMapLiteral => {
                    relationship.properties = Some(Self::RelationshipMapLiteral(child)?)
                }
                _ => return Err(mismatch(&child)),
            }
        }
        Ok(relationship)
    }

    fn RelationshipTypes(input: Node) -> Result<Vec<Name>> {
        Ok(match_nodes!(input.into_children();
            [SchemaName(names)..] => names.collect(),
        ))
    }

    fn RangeLiteral(input: Node) -> Result<Range> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [] => Range { span, start: None, dots: false, end: None },
            [RangeStart(start)] => Range { span, start: Some(start), dots: false, end: None },
            [RangeDots(_)] => Range { span, start: None, dots: true, end: None },
            [RangeStart(start), RangeDots(_)] => Range { span, start: Some(start), dots: true, end: None },
            [RangeDots(_), RangeEnd(end)] => Range { span, start: None, dots: true, end: Some(end) },
            [RangeStart(start), RangeDots(_), RangeEnd(end)] => Range {
                span, start: Some(start), dots: true, end: Some(end),
            },
        ))
    }

    fn RangeStart(input: Node) -> Result<u32> {
        hop_bound(&input)
    }

    fn RangeEnd(input: Node) -> Result<u32> {
        hop_bound(&input)
    }

    fn RangeDots(_input: Node) -> Result<()> {
        Ok(())
    }

    fn HopCount(input: Node) -> Result<Value> {
        number(&input)
    }

    fn PathCount(input: Node) -> Result<Value> {
        number(&input)
    }

    fn Where(input: Node) -> Result<BooleanExpression<Predicate>> {
        Ok(match_nodes!(input.into_children();
            [AndExpression(predicate)] => predicate,
        ))
    }

    fn AndExpression(input: Node) -> Result<BooleanExpression<Predicate>> {
        let mut children = Vec::new();
        for child in input.into_children() {
            match child.as_rule() {
                Rule::NotExpression => children.push(Self::NotExpression(child)?),
                Rule::UnsupportedBooleanOperator => {
                    return Err(child.error(
                        "OR and XOR are not supported; combine conditions with AND and NOT, or use IN [...] to match one of several values",
                    ));
                }
                _ => return Err(mismatch(&child)),
            }
        }
        Ok(if children.len() == 1 {
            children.pop().expect("one condition")
        } else {
            BooleanExpression::And(children)
        })
    }

    fn NotExpression(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let mut children: Vec<_> = input.into_children().collect();
        let predicate = children
            .pop()
            .ok_or_else(|| error_at(span, "expected a condition"))?;
        let mut expression = Self::predicate(predicate)?;
        if children.len() + expression.depth() > super::MAX_NESTING {
            return Err(error_at(span, "expression nesting is too deep"));
        }
        for _negation in children {
            expression = BooleanExpression::Not(Box::new(expression));
        }
        Ok(expression)
    }

    #[alias(predicate)]
    fn ParenthesizedExpression(input: Node) -> Result<BooleanExpression<Predicate>> {
        Ok(match_nodes!(input.into_children();
            [AndExpression(predicate)] => predicate,
        ))
    }

    #[alias(predicate)]
    fn ComparisonExpression(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let from_temporal_literal = has_temporal_literal(&input);
        let comparison = match_nodes!(input.into_children();
            [PropertyExpression(property), operator(op)] => Comparison {
                span, property, op, value: None, from_temporal_literal, rhs_property: None,
            },
            [PropertyExpression(property), operator(op), value(value)] => Comparison {
                span, property, op, value: Some(value), from_temporal_literal, rhs_property: None,
            },
            [PropertyExpression(lhs), operator(op), PropertyExpression(rhs)] => Comparison {
                span, property: lhs, op, value: None, from_temporal_literal, rhs_property: Some(rhs),
            },
            [PropertyExpression(_), operator(_), FunctionCall(_)] => unreachable!("FunctionCall always errors"),
        );
        Ok(leaf(comparison))
    }

    #[alias(predicate)]
    fn TypePredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let (variable, op, value) = match_nodes!(input.into_children();
            [RelationshipType(variable), operator(op)] => (variable, op, None),
            [RelationshipType(variable), operator(op), value(value)] => (variable, op, Some(value)),
            [RelationshipType(variable), operator(op), PropertyExpression(_)] => (variable, op, None),
            [RelationshipType(_), operator(_), FunctionCall(_)] => unreachable!("FunctionCall always errors"),
        );
        Ok(BooleanExpression::Leaf(Predicate::RelationshipType {
            span,
            variable,
            op,
            value,
        }))
    }

    fn RelationshipType(input: Node) -> Result<Name> {
        Ok(match_nodes!(input.into_children();
            [Variable(variable)] => variable,
        ))
    }

    #[alias(predicate)]
    fn FunctionPredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let call = input.children().next().ok_or_else(|| mismatch(&input))?;
        Err(unsupported_function(&call))
    }

    fn FunctionCall(input: Node) -> Result<()> {
        Err(unsupported_function(&input))
    }

    #[alias(predicate)]
    fn TokenPredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let from_temporal_literal = has_temporal_literal(&input);
        let comparison = match_nodes!(input.into_children();
            [TokenFunction(op), PropertyExpression(property), value(value)] => Comparison {
                span, property, op, value: Some(value), from_temporal_literal, rhs_property: None,
            },
        );
        Ok(leaf(comparison))
    }

    fn TokenFunction(input: Node) -> Result<FilterOp> {
        Ok(match input.as_str().to_ascii_lowercase().as_str() {
            "token_match" => FilterOp::TokenMatch,
            "all_tokens" => FilterOp::AllTokens,
            "any_tokens" => FilterOp::AnyTokens,
            _ => return Err(mismatch(&input)),
        })
    }

    #[alias(operator)]
    fn ComparisonOperator(input: Node) -> Result<FilterOp> {
        Ok(match input.as_str() {
            "=" => FilterOp::Eq,
            "<>" | "!=" => FilterOp::Ne,
            ">" => FilterOp::Gt,
            "<" => FilterOp::Lt,
            ">=" => FilterOp::Gte,
            "<=" => FilterOp::Lte,
            _ => return Err(mismatch(&input)),
        })
    }

    #[alias(operator)]
    fn StringOperator(input: Node) -> Result<FilterOp> {
        let token = input.as_str().to_ascii_lowercase();
        Ok(if token.starts_with("starts") {
            FilterOp::StartsWith
        } else if token.starts_with("ends") {
            FilterOp::EndsWith
        } else {
            FilterOp::Contains
        })
    }

    #[alias(operator)]
    fn InOperator(_input: Node) -> Result<FilterOp> {
        Ok(FilterOp::In)
    }

    #[alias(operator)]
    fn NegatedInOperator(input: Node) -> Result<FilterOp> {
        Err(input.error("write NOT n.property IN [...] to exclude values"))
    }

    #[alias(operator)]
    fn NullOperator(input: Node) -> Result<FilterOp> {
        Ok(match_nodes!(input.into_children();
            [] => FilterOp::IsNull,
            [Not(_)] => FilterOp::IsNotNull,
        ))
    }

    fn Not(_input: Node) -> Result<()> {
        Ok(())
    }

    fn Return(input: Node) -> Result<Projections> {
        Ok(match_nodes!(input.into_children();
            [ProjectionItems(projections)] => projections,
        ))
    }

    fn ProjectionItems(input: Node) -> Result<Projections> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Star(span)] => Projections::Star(span),
            [ProjectionItem(items)..] => Projections::Items { span, items: items.collect() },
        ))
    }

    fn Star(input: Node) -> Result<Span> {
        Ok(input.as_span())
    }

    fn ProjectionItem(input: Node) -> Result<ProjectionItem> {
        Ok(match_nodes!(input.into_children();
            [ProjectionExpression(expression)] => ProjectionItem { expression, alias: None },
            [ProjectionExpression(expression), Variable(alias)] => ProjectionItem {
                expression, alias: Some(alias),
            },
        ))
    }

    fn ProjectionExpression(input: Node) -> Result<Expression> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Aggregate(expression)] => expression,
            [DateTrunc(expression)] => expression,
            [AllProperties(expression)] => expression,
            [RelationshipType(variable)] => Expression::RelationshipType { span, variable },
            [FunctionCall(_)] => unreachable!("FunctionCall always errors"),
            [NodeProjection(expression)] => expression,
            [PropertyExpression(property)] => Expression::Property(property),
            [Variable(variable)] => Expression::Variable(variable),
        ))
    }

    fn Aggregate(input: Node) -> Result<Expression> {
        let span = input.as_span();
        let (function, distinct, target) = match_nodes!(input.into_children();
            [AggregateFunction(function), PropertyExpression(property)] =>
                (function, false, Target::Property(property)),
            [AggregateFunction(function), Variable(variable)] =>
                (function, false, Target::Variable(variable)),
            [AggregateFunction(function), Distinct(_), PropertyExpression(property)] =>
                (function, true, Target::Property(property)),
            [AggregateFunction(function), Distinct(_), Variable(variable)] =>
                (function, true, Target::Variable(variable)),
        );
        if distinct && function != AggregateFunction::Count {
            return Err(error_at(span, "DISTINCT works only inside count()"));
        }
        Ok(Expression::Aggregate {
            function,
            distinct,
            target,
        })
    }

    fn Distinct(_input: Node) -> Result<()> {
        Ok(())
    }

    fn AggregateFunction(input: Node) -> Result<AggregateFunction> {
        Ok(match input.as_str().to_ascii_lowercase().as_str() {
            "count" => AggregateFunction::Count,
            "sum" => AggregateFunction::Sum,
            "avg" => AggregateFunction::Avg,
            "min" => AggregateFunction::Min,
            "max" => AggregateFunction::Max,
            _ => return Err(mismatch(&input)),
        })
    }

    fn DateTrunc(input: Node) -> Result<Expression> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [unit, PropertyExpression(property)] => {
                let unit = match string(&unit)?.as_str() {
                    "minute" => TruncateUnit::Minute,
                    "hour" => TruncateUnit::Hour,
                    "day" => TruncateUnit::Day,
                    "week" => TruncateUnit::Week,
                    "month" => TruncateUnit::Month,
                    "quarter" => TruncateUnit::Quarter,
                    "year" => TruncateUnit::Year,
                    _ => {
                        return Err(error_at(
                            span,
                            "date_trunc unit must be minute, hour, day, week, month, quarter, or year",
                        ));
                    }
                };
                Expression::DateTrunc { span, unit, property }
            },
        ))
    }

    fn AllProperties(input: Node) -> Result<Expression> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Variable(variable)] => Expression::AllProperties { span, variable },
        ))
    }

    fn NodeProjection(input: Node) -> Result<Expression> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Variable(variable), ProjectionProperty(properties)..] => Expression::Node {
                span, variable, properties: properties.collect(),
            },
        ))
    }

    fn ProjectionProperty(input: Node) -> Result<Name> {
        Ok(match_nodes!(input.into_children();
            [SchemaName(name)] => name,
        ))
    }

    fn Order(input: Node) -> Result<Sort> {
        match_nodes!(input.into_children();
            [SortItem(sort)] => Ok(sort),
            [SortItem(_), SortItem(mut extra)..] => Err(error_at(
                extra.next().expect("a second sort key").span,
                "ORDER BY accepts one sort key; keep the key that matters most and remove the others",
            )),
        )
    }

    fn SortItem(input: Node) -> Result<Sort> {
        let span = input.as_span();
        if let Some(call) = input
            .children()
            .find(|child| child.as_rule() == Rule::FunctionCall)
        {
            return Err(unsupported_sort_function(&call));
        }
        Ok(match_nodes!(input.into_children();
            [FunctionCall(_)] => unreachable!("FunctionCall always errors"),
            [FunctionCall(_), SortDirection(_)] => unreachable!("FunctionCall always errors"),
            [RelationshipType(variable)] => Sort {
                span, key: SortKey::RelationshipType(variable), direction: OrderDirection::Asc,
            },
            [PropertyExpression(property)] => Sort {
                span, key: SortKey::Property(property), direction: OrderDirection::Asc,
            },
            [Variable(variable)] => Sort {
                span, key: SortKey::Variable(variable), direction: OrderDirection::Asc,
            },
            [RelationshipType(variable), SortDirection(direction)] => Sort {
                span, key: SortKey::RelationshipType(variable), direction,
            },
            [PropertyExpression(property), SortDirection(direction)] => Sort {
                span, key: SortKey::Property(property), direction,
            },
            [Variable(variable), SortDirection(direction)] => Sort {
                span, key: SortKey::Variable(variable), direction,
            },
        ))
    }

    fn SortDirection(input: Node) -> Result<OrderDirection> {
        Ok(if input.as_str().to_ascii_lowercase().starts_with("desc") {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        })
    }

    fn Skip(input: Node) -> Result<u32> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [SkipCount(value)] => value
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| error_at(span, "SKIP must be a non-negative integer"))?,
        ))
    }

    fn Limit(input: Node) -> Result<u32> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [LimitCount(value)] => row_count(&value, span, "LIMIT")?,
        ))
    }

    fn Debug(_input: Node) -> Result<bool> {
        Ok(true)
    }

    fn PropertyExpression(input: Node) -> Result<Property> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Variable(node), SchemaName(property)] => Property { span, node, property },
        ))
    }

    fn NodeMapLiteral(input: Node) -> Result<MapLiteral> {
        map(input)
    }

    fn RelationshipMapLiteral(input: Node) -> Result<MapLiteral> {
        map(input)
    }

    fn MapEntry(input: Node) -> Result<MapEntry> {
        let from_temporal_literal = has_temporal_literal(&input);
        Ok(match_nodes!(input.into_children();
            [SchemaName(key), value(value)] => MapEntry { key, value, from_temporal_literal },
        ))
    }

    #[alias(value)]
    fn TemporalLiteral(input: Node) -> Result<Value> {
        Ok(match_nodes!(input.into_children();
            [literal] => Value::String(string(&literal)?),
        ))
    }

    #[alias(value)]
    fn BooleanLiteral(input: Node) -> Result<Value> {
        Ok(Value::Bool(input.as_str().eq_ignore_ascii_case("true")))
    }

    #[alias(value)]
    fn NumberLiteral(input: Node) -> Result<Value> {
        number(&input)
    }

    fn UnsignedInteger(input: Node) -> Result<Value> {
        number(&input)
    }

    fn SkipCount(input: Node) -> Result<Value> {
        number(&input)
    }

    fn LimitCount(input: Node) -> Result<Value> {
        number(&input)
    }

    #[alias(value)]
    fn StringLiteral(input: Node) -> Result<Value> {
        string(&input).map(Value::String)
    }

    #[alias(value)]
    fn InListLiteral(input: Node) -> Result<Value> {
        list(input)
    }

    #[alias(value)]
    fn ListLiteral(input: Node) -> Result<Value> {
        list(input)
    }

    fn Variable(input: Node) -> Result<Name> {
        name(&input)
    }

    fn SchemaName(input: Node) -> Result<Name> {
        name(&input)
    }
}

fn map(input: Node) -> Result<MapLiteral> {
    let span = input.as_span();
    let mut keys = HashSet::new();
    let mut entries = Vec::new();
    for child in input.into_children() {
        let entry = QueryParser::MapEntry(child.clone())?;
        if !keys.insert(entry.key.value.clone()) {
            return Err(child.error("duplicate property in a map"));
        }
        entries.push(entry);
    }
    Ok(MapLiteral { span, entries })
}

fn directed(input: Node, direction: Direction) -> Result<Relationship> {
    Ok(match_nodes!(<QueryParser>; input.into_children();
        [] => Relationship {
            direction,
            variable: None,
            types: Vec::new(),
            range: None,
            properties: None,
        },
        [RelationshipDetail(detail)] => Relationship { direction, ..detail },
    ))
}

fn has_temporal_literal(node: &Node) -> bool {
    node.as_pair()
        .clone()
        .into_inner()
        .flatten()
        .any(|pair| pair.as_rule() == Rule::TemporalLiteral)
}

fn leaf(comparison: Comparison<'_>) -> BooleanExpression<Predicate<'_>> {
    BooleanExpression::Leaf(Predicate::Comparison(Box::new(comparison)))
}

fn list(input: Node) -> Result<Value> {
    Ok(match_nodes!(<QueryParser>; input.into_children();
        [value(values)..] => Value::Array(values.collect()),
    ))
}

fn row_count(value: &Value, span: Span<'_>, clause: &str) -> Result<u32> {
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| error_at(span, &format!("{clause} must be a positive integer")))
}

fn error_at(span: Span<'_>, message: &str) -> Error<Rule> {
    Error::new_from_span(
        ErrorVariant::CustomError {
            message: message.to_owned(),
        },
        span,
    )
}

fn unsupported_function(call: &Node) -> Error<Rule> {
    let name = call.children().next().map_or("", |node| node.as_str());
    let hint = match name.to_ascii_lowercase().as_str() {
        "type" => {
            "type() takes one relationship variable and works in WHERE type(r) = 'CLOSES', WHERE type(r) IN ['CLOSES', 'MENTIONS'], RETURN type(r), and ORDER BY type(r)"
        }
        "count" | "sum" | "avg" | "min" | "max" => &format!(
            "{name}() takes one variable or property, as in count(mr) or sum(mr.added_lines), and belongs in RETURN with an alias: count(mr) AS n; WHERE cannot filter on an aggregate"
        ),
        "date_trunc" => {
            "date_trunc() works only in RETURN, such as RETURN date_trunc('month', m.created_at)"
        }
        "properties" => {
            "properties() works only in RETURN, with one node variable, such as RETURN properties(m)"
        }
        "token_match" | "all_tokens" | "any_tokens" => &format!(
            "{name}() works only in WHERE, with a property and a string, such as WHERE {name}(m.title, 'login')"
        ),
        _ => {
            "RETURN supports count, sum, avg, min, max, date_trunc, properties, and type; WHERE supports token_match, all_tokens, any_tokens, and type"
        }
    };
    call.error(format!("function {name}() is not supported: {hint}"))
}

fn unsupported_sort_function(call: &Node) -> Error<Rule> {
    let name = call.children().next().map_or("", |node| node.as_str());
    let text = call.as_str();
    let hint = match name.to_ascii_lowercase().as_str() {
        "count" | "sum" | "avg" | "min" | "max" => format!(
            "ORDER BY takes the alias of an aggregate, not the call: write RETURN ..., {text} AS n ORDER BY n"
        ),
        _ => {
            "ORDER BY accepts a property, a variable, an aggregate's alias, or type(r)".to_string()
        }
    };
    call.error(format!(
        "function {name}() is not supported in ORDER BY: {hint}"
    ))
}

fn mismatch(node: &Node) -> Error<Rule> {
    node.error(format!(
        "grammar produced {:?} where the syntax tree has no arm",
        node.as_rule()
    ))
}

fn hop_bound(node: &Node) -> Result<u32> {
    node.as_str()
        .parse()
        .map_err(|_| node.error("invalid hop bound"))
}

fn name<'i>(node: &Node<'i>) -> Result<Name<'i>> {
    let raw = node.as_str();
    let value = raw
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(raw)
        .to_owned();
    crate::passes::validate::validate_identifier(&value)
        .map_err(|error| node.error(error.to_string()))?;
    Ok(Name {
        span: node.as_span(),
        value,
    })
}

fn string(node: &Node) -> Result<String> {
    let raw = node.as_str();
    let mut chars = raw[1..raw.len() - 1].chars();
    let mut result = String::new();
    while let Some(c) = chars.next() {
        if c != '\\' {
            result.push(c);
            continue;
        }
        let escape = chars.next().expect("grammar validates escapes");
        match escape {
            '\\' | '\'' | '"' => result.push(escape),
            'n' | 'N' => result.push('\n'),
            'r' | 'R' => result.push('\r'),
            't' | 'T' => result.push('\t'),
            'b' | 'B' => result.push('\u{8}'),
            'f' | 'F' => result.push('\u{c}'),
            'u' | 'U' => {
                let digits = if escape == 'u' { 4 } else { 8 };
                let hex: String = chars.by_ref().take(digits).collect();
                let mut code = u32::from_str_radix(&hex, 16)
                    .map_err(|_| node.error("invalid Unicode escape"))?;
                if (0xd800..=0xdbff).contains(&code) && escape == 'u' {
                    if chars.next() != Some('\\') || chars.next() != Some('u') {
                        return Err(node.error("high surrogate requires a low surrogate"));
                    }
                    let hex: String = chars.by_ref().take(4).collect();
                    let low = u32::from_str_radix(&hex, 16)
                        .map_err(|_| node.error("invalid low surrogate"))?;
                    if !(0xdc00..=0xdfff).contains(&low) {
                        return Err(node.error("invalid low surrogate"));
                    }
                    code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
                }
                result.push(
                    char::from_u32(code).ok_or_else(|| node.error("invalid Unicode scalar"))?,
                );
            }
            _ => return Err(mismatch(node)),
        }
    }
    if result.chars().count() > MAX_FILTER_STRING_LEN {
        return Err(node.error(format!(
            "string literal must not exceed {MAX_FILTER_STRING_LEN} characters"
        )));
    }
    Ok(result)
}

fn number(node: &Node) -> Result<Value> {
    let raw = node.as_str();
    let unsigned = raw.trim_start_matches(['+', '-']);
    let radix = if unsigned.starts_with("0x") {
        Some(16)
    } else if unsigned.starts_with("0o") {
        Some(8)
    } else {
        None
    };
    let out_of_range = || node.error("integer is out of range for a signed 64-bit integer");
    if let Some(radix) = radix {
        let magnitude =
            i128::from(u64::from_str_radix(&unsigned[2..], radix).map_err(|_| out_of_range())?);
        let signed = if raw.starts_with('-') {
            -magnitude
        } else {
            magnitude
        };
        return i64::try_from(signed)
            .map(Value::from)
            .map_err(|_| out_of_range());
    }
    let raw = raw.trim_start_matches('+');
    if raw.contains(['.', 'e', 'E']) {
        let number = raw
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .ok_or_else(|| node.error("number must be finite and in range"))?;
        Ok(Value::Number(number))
    } else {
        raw.parse::<i64>()
            .map(Value::from)
            .map_err(|_| out_of_range())
    }
}
