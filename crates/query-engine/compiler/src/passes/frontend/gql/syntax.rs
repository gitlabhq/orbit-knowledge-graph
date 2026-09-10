use std::collections::HashSet;

use pest::Span;
use pest::error::ErrorVariant;
use pest_consume::{Error, match_nodes};
use serde_json::{Number, Value};

use super::ast::*;
use super::{QueryParser, Rule};
use crate::input::{Direction, FilterOp, OrderDirection, TruncateUnit};

pub(super) type Node<'i> = pest_consume::Node<'i, Rule, ()>;
pub(super) type Result<T> = std::result::Result<T, Error<Rule>>;

#[pest_consume::parser]
impl QueryParser {
    fn EOI(_input: Node) -> Result<()> {
        Ok(())
    }

    pub(super) fn Query(input: Node) -> Result<Query> {
        Ok(match_nodes!(input.into_children();
            [Match((pattern, predicates)), Return(projections), EOI(_)] => Query {
                pattern, predicates, projections, order: None, limit: None,
            },
            [Match((pattern, predicates)), Return(projections), Order(order), EOI(_)] => Query {
                pattern, predicates, projections, order: Some(order), limit: None,
            },
            [Match((pattern, predicates)), Return(projections), Limit(limit), EOI(_)] => Query {
                pattern, predicates, projections, order: None, limit: Some(limit),
            },
            [Match((pattern, predicates)), Return(projections), Order(order), Limit(limit), EOI(_)] => Query {
                pattern, predicates, projections, order: Some(order), limit: Some(limit),
            },
        ))
    }

    fn Match(input: Node) -> Result<(Pattern, Vec<Comparison>)> {
        Ok(match_nodes!(input.into_children();
            [Pattern(pattern)] => (pattern, Vec::new()),
            [Pattern(pattern), Where(predicates)] => (pattern, predicates),
        ))
    }

    fn Pattern(input: Node) -> Result<Pattern> {
        Ok(match_nodes!(input.into_children();
            [ShortestPattern(pattern)] => pattern,
            [PatternElement(element)] => Pattern::Element(element),
        ))
    }

    fn ShortestPattern(input: Node) -> Result<Pattern> {
        Ok(match_nodes!(input.into_children();
            [Variable(variable), PatternElement(element)] => Pattern::Shortest { variable, element },
        ))
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
            [Variable(variable), MapLiteral(map)] => NodePattern {
                span, variable, label: None, properties: map.entries,
            },
            [Variable(variable), NodeLabel(label), MapLiteral(map)] => NodePattern {
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
        Ok(match_nodes!(input.into_children();
            [] => detail(None, Vec::new(), None, None),
            [Variable(v)] => detail(Some(v), Vec::new(), None, None),
            [RelationshipTypes(t)] => detail(None, t, None, None),
            [RangeLiteral(r)] => detail(None, Vec::new(), Some(r), None),
            [MapLiteral(m)] => detail(None, Vec::new(), None, Some(m)),
            [Variable(v), RelationshipTypes(t)] => detail(Some(v), t, None, None),
            [Variable(v), RangeLiteral(r)] => detail(Some(v), Vec::new(), Some(r), None),
            [Variable(v), MapLiteral(m)] => detail(Some(v), Vec::new(), None, Some(m)),
            [RelationshipTypes(t), RangeLiteral(r)] => detail(None, t, Some(r), None),
            [RelationshipTypes(t), MapLiteral(m)] => detail(None, t, None, Some(m)),
            [RangeLiteral(r), MapLiteral(m)] => detail(None, Vec::new(), Some(r), Some(m)),
            [Variable(v), RelationshipTypes(t), RangeLiteral(r)] => detail(Some(v), t, Some(r), None),
            [Variable(v), RelationshipTypes(t), MapLiteral(m)] => detail(Some(v), t, None, Some(m)),
            [Variable(v), RangeLiteral(r), MapLiteral(m)] => detail(Some(v), Vec::new(), Some(r), Some(m)),
            [RelationshipTypes(t), RangeLiteral(r), MapLiteral(m)] => detail(None, t, Some(r), Some(m)),
            [Variable(v), RelationshipTypes(t), RangeLiteral(r), MapLiteral(m)] => detail(Some(v), t, Some(r), Some(m)),
        ))
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

    fn Where(input: Node) -> Result<Vec<Comparison>> {
        Ok(match_nodes!(input.into_children();
            [AndExpression(predicates)] => predicates,
        ))
    }

    fn AndExpression(input: Node) -> Result<Vec<Comparison>> {
        Ok(match_nodes!(input.into_children();
            [predicate(predicates)..] => predicates.flatten().collect(),
        ))
    }

    #[alias(predicate)]
    fn ParenthesizedExpression(input: Node) -> Result<Vec<Comparison>> {
        Ok(match_nodes!(input.into_children();
            [AndExpression(predicates)] => predicates,
        ))
    }

    #[alias(predicate)]
    fn ComparisonExpression(input: Node) -> Result<Vec<Comparison>> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [PropertyExpression(property), operator(op)] => vec![Comparison {
                span, property, op, value: None,
            }],
            [PropertyExpression(property), operator(op), value(value)] => vec![Comparison {
                span, property, op, value: Some(value),
            }],
        ))
    }

    #[alias(predicate)]
    fn TokenPredicate(input: Node) -> Result<Vec<Comparison>> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [TokenFunction(op), PropertyExpression(property), value(value)] => vec![Comparison {
                span, property, op, value: Some(value),
            }],
        ))
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
            [Aggregate(e)] => item(e, None),
            [Aggregate(e), Variable(alias)] => item(e, Some(alias)),
            [DateTrunc(e)] => item(e, None),
            [DateTrunc(e), Variable(alias)] => item(e, Some(alias)),
            [AllProperties(e)] => item(e, None),
            [AllProperties(e), Variable(alias)] => item(e, Some(alias)),
            [NodeProjection(e)] => item(e, None),
            [NodeProjection(e), Variable(alias)] => item(e, Some(alias)),
            [PropertyExpression(p)] => item(Expression::Property(p), None),
            [PropertyExpression(p), Variable(alias)] => item(Expression::Property(p), Some(alias)),
            [Variable(v)] => item(Expression::Variable(v), None),
            [Variable(v), Variable(alias)] => item(Expression::Variable(v), Some(alias)),
        ))
    }

    fn Aggregate(input: Node) -> Result<Expression> {
        Ok(match_nodes!(input.into_children();
            [AggregateFunction(function), PropertyExpression(property)] => Expression::Aggregate {
                function, target: Target::Property(property),
            },
            [AggregateFunction(function), Variable(variable)] => Expression::Aggregate {
                function, target: Target::Variable(variable),
            },
        ))
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
        Ok(match_nodes!(input.into_children();
            [SortItem(sort)] => sort,
        ))
    }

    fn SortItem(input: Node) -> Result<Sort> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [PropertyExpression(property)] => Sort {
                span, key: Target::Property(property), direction: OrderDirection::Asc,
            },
            [Variable(variable)] => Sort {
                span, key: Target::Variable(variable), direction: OrderDirection::Asc,
            },
            [PropertyExpression(property), SortDirection(direction)] => Sort {
                span, key: Target::Property(property), direction,
            },
            [Variable(variable), SortDirection(direction)] => Sort {
                span, key: Target::Variable(variable), direction,
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

    fn Limit(input: Node) -> Result<u32> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [UnsignedInteger(value)] => value
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| error_at(span, "LIMIT must be a positive integer"))?,
        ))
    }

    fn PropertyExpression(input: Node) -> Result<Property> {
        let span = input.as_span();
        Ok(match_nodes!(input.into_children();
            [Variable(node), SchemaName(property)] => Property { span, node, property },
        ))
    }

    fn MapLiteral(input: Node) -> Result<MapLiteral> {
        let span = input.as_span();
        let mut keys = HashSet::new();
        let mut entries = Vec::new();
        for child in input.into_children() {
            let entry = Self::MapEntry(child.clone())?;
            if !keys.insert(entry.key.value.clone()) {
                return Err(child.error("duplicate property in a map"));
            }
            entries.push(entry);
        }
        Ok(MapLiteral { span, entries })
    }

    fn MapEntry(input: Node) -> Result<MapEntry> {
        Ok(match_nodes!(input.into_children();
            [SchemaName(key), value(value)] => MapEntry { key, value },
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

    #[alias(value)]
    fn StringLiteral(input: Node) -> Result<Value> {
        string(&input).map(Value::String)
    }

    #[alias(value)]
    fn ListLiteral(input: Node) -> Result<Value> {
        Ok(match_nodes!(input.into_children();
            [value(values)..] => Value::Array(values.collect()),
        ))
    }

    fn Variable(input: Node) -> Result<Name> {
        name(&input)
    }

    fn SchemaName(input: Node) -> Result<Name> {
        name(&input)
    }
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

fn detail<'i>(
    variable: Option<Name<'i>>,
    types: Vec<Name<'i>>,
    range: Option<Range<'i>>,
    properties: Option<MapLiteral<'i>>,
) -> Relationship<'i> {
    Relationship {
        direction: Direction::Outgoing,
        variable,
        types,
        range,
        properties,
    }
}

fn item<'i>(expression: Expression<'i>, alias: Option<Name<'i>>) -> ProjectionItem<'i> {
    ProjectionItem { expression, alias }
}

fn error_at(span: Span<'_>, message: &str) -> Error<Rule> {
    Error::new_from_span(
        ErrorVariant::CustomError {
            message: message.to_owned(),
        },
        span,
    )
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
        .map_or_else(|| raw.to_owned(), |s| s.replace("``", "`"));
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
    if let Some(radix) = radix {
        let magnitude = u64::from_str_radix(&unsigned[2..], radix)
            .map_err(|_| node.error("integer is out of range"))?;
        if raw.starts_with('-') {
            let signed = i64::try_from(-i128::from(magnitude))
                .map_err(|_| node.error("integer is out of range"))?;
            return Ok(Value::from(signed));
        }
        return Ok(Value::from(magnitude));
    }
    let raw = raw.trim_start_matches('+');
    if raw.contains(['.', 'e', 'E']) {
        let number = raw
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .ok_or_else(|| node.error("number must be finite and in range"))?;
        Ok(Value::Number(number))
    } else if let Ok(signed) = raw.parse::<i64>() {
        Ok(Value::from(signed))
    } else {
        raw.parse::<u64>()
            .map(Value::from)
            .map_err(|_| node.error("integer is out of range"))
    }
}
