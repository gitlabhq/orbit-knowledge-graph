use std::collections::HashSet;

use pest::Span;
use pest::error::ErrorVariant;
use pest_consume::{Error, match_nodes};

use super::ast::*;
use super::{QueryParser, Rule};
use crate::plan::logical::limits::{MAX_IN_VALUES, MAX_PINS, MAX_TEXT};

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
        Ok(Statement::SchemaCall(match_nodes!(input.into_children();
            [] => None,
            [literal] => Some(string(&literal)?),
        )))
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
        Err(input.error("use ANY SHORTEST (a)-[:TYPE*1..3]->(b) for shortest paths"))
    }

    fn PathSearch(input: Node) -> Result<()> {
        let span = input.as_span();
        match_nodes!(input.into_children();
            [] => Ok(()),
            [PathCount(count)] => if count == Literal::Int(1) {
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
                span, variable, label: None, properties: map,
            },
            [Variable(variable), NodeLabel(label), NodeMapLiteral(map)] => NodePattern {
                span, variable, label: Some(label), properties: map,
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
            properties: Vec::new(),
        };
        for child in input.into_children() {
            match child.as_rule() {
                Rule::Variable => relationship.variable = Some(Self::Variable(child)?),
                Rule::RelationshipTypes => relationship.types = Self::RelationshipTypes(child)?,
                Rule::RangeLiteral => relationship.range = Some(Self::RangeLiteral(child)?),
                Rule::RelationshipMapLiteral => {
                    relationship.properties = Self::RelationshipMapLiteral(child)?
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

    fn HopCount(input: Node) -> Result<Literal> {
        number(&input)
    }

    fn PathCount(input: Node) -> Result<Literal> {
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
        let (property, op, operand) = match_nodes!(input.into_children();
            [PropertyExpression(property), operator(op)] => (property, op, None),
            [PropertyExpression(property), operator(op), value(value)] => (property, op, Some(Operand::Value(value))),
            [PropertyExpression(property), operator(op), PropertyExpression(rhs)] => (property, op, Some(Operand::Property(rhs))),
            [PropertyExpression(_), operator(_), FunctionCall(_)] => unreachable!("FunctionCall always errors"),
        );
        Ok(comparison(span, Subject::Property(property), op, operand))
    }

    #[alias(predicate)]
    fn TypePredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let (variable, op, operand) = match_nodes!(input.into_children();
            [RelationshipType(variable), operator(op)] => (variable, op, None),
            [RelationshipType(variable), operator(op), value(value)] => (variable, op, Some(Operand::Value(value))),
            [RelationshipType(variable), operator(op), PropertyExpression(rhs)] => (variable, op, Some(Operand::Property(rhs))),
            [RelationshipType(_), operator(_), FunctionCall(_)] => unreachable!("FunctionCall always errors"),
        );
        Ok(comparison(span, Subject::Type(variable), op, operand))
    }

    fn RelationshipType(input: Node) -> Result<Name> {
        Ok(match_nodes!(input.into_children();
            [Variable(variable)] => variable,
        ))
    }

    #[alias(predicate)]
    fn FunctionPredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let call = input.children().next().ok_or_else(|| mismatch(&input))?;
        Err(unsupported_function(&call, true))
    }

    fn FunctionCall(input: Node) -> Result<()> {
        Err(unsupported_function(&input, false))
    }

    #[alias(predicate)]
    fn TokenPredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let (property, op, value) = match_nodes!(input.into_children();
            [TokenFunction(op), PropertyExpression(property), value(value)] => (property, op, value),
        );
        Ok(comparison(
            span,
            Subject::Property(property),
            op,
            Some(Operand::Value(value)),
        ))
    }

    #[alias(predicate)]
    fn StructuralPredicate(input: Node) -> Result<BooleanExpression<Predicate>> {
        let span = input.as_span();
        let (property, pattern, language) = match_nodes!(input.into_children();
            [PropertyExpression(property), value(pattern), value(language)] => (property, pattern, language),
        );
        let text = |value: Value| match value {
            Value::Scalar(Literal::String(text)) => Ok(text),
            _ => Err(error_at(
                span,
                "ast() takes a pattern and a language string",
            )),
        };
        Ok(BooleanExpression::Leaf(Predicate::Structural {
            span,
            property,
            pattern: text(pattern)?,
            language: text(language)?,
        }))
    }

    fn MatchField(input: Node) -> Result<Name> {
        Ok(match_nodes!(input.into_children();
            [SchemaName(field)] => field,
        ))
    }

    fn TokenFunction(input: Node) -> Result<Operator> {
        Ok(match input.as_str().to_ascii_lowercase().as_str() {
            "token_match" => Operator::TokenMatch,
            "all_tokens" => Operator::AllTokens,
            "any_tokens" => Operator::AnyTokens,
            _ => return Err(mismatch(&input)),
        })
    }

    #[alias(operator)]
    fn ComparisonOperator(input: Node) -> Result<Operator> {
        Ok(match input.as_str() {
            "=" => Operator::Eq,
            "<>" | "!=" => Operator::Ne,
            ">" => Operator::Gt,
            "<" => Operator::Lt,
            ">=" => Operator::Gte,
            "<=" => Operator::Lte,
            _ => return Err(mismatch(&input)),
        })
    }

    #[alias(operator)]
    fn StringOperator(input: Node) -> Result<Operator> {
        let token = input.as_str().to_ascii_lowercase();
        Ok(if token.starts_with("starts") {
            Operator::StartsWith
        } else if token.starts_with("ends") {
            Operator::EndsWith
        } else {
            Operator::Contains
        })
    }

    #[alias(operator)]
    fn InOperator(_input: Node) -> Result<Operator> {
        Ok(Operator::In)
    }

    #[alias(operator)]
    fn NegatedInOperator(input: Node) -> Result<Operator> {
        Err(input.error("write NOT n.property IN [...] to exclude values"))
    }

    #[alias(operator)]
    fn NullOperator(input: Node) -> Result<Operator> {
        Ok(match_nodes!(input.into_children();
            [] => Operator::IsNull,
            [Not(_)] => Operator::IsNotNull,
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
        Ok(match_nodes!(input.into_children();
            [Star(span)] => Projections::Star(span),
            [ProjectionItem(items)..] => Projections::Items(items.collect()),
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
            [MatchField(field)] => Expression::Match(field),
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
        Ok(match_nodes!(input.into_children();
            [Variable(variable)] => Expression::AllProperties(variable),
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
            [SkipCount(value)] => row_count(&value, span, "SKIP")?,
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

    fn NodeMapLiteral(input: Node) -> Result<Vec<MapEntry>> {
        map(input)
    }

    fn RelationshipMapLiteral(input: Node) -> Result<Vec<MapEntry>> {
        map(input)
    }

    fn MapEntry(input: Node) -> Result<MapEntry> {
        Ok(match_nodes!(input.into_children();
            [SchemaName(key), value(value)] => MapEntry { key, value },
        ))
    }

    #[alias(value)]
    fn TemporalLiteral(input: Node) -> Result<Value> {
        Ok(match_nodes!(input.into_children();
            [literal] => Value::Scalar(Literal::Temporal(string(&literal)?)),
        ))
    }

    #[alias(value)]
    fn BooleanLiteral(input: Node) -> Result<Value> {
        Ok(Value::Scalar(Literal::Bool(
            input.as_str().eq_ignore_ascii_case("true"),
        )))
    }

    #[alias(value)]
    fn NumberLiteral(input: Node) -> Result<Value> {
        number(&input).map(Value::Scalar)
    }

    fn UnsignedInteger(input: Node) -> Result<Literal> {
        number(&input)
    }

    fn SkipCount(input: Node) -> Result<Literal> {
        number(&input)
    }

    fn LimitCount(input: Node) -> Result<Literal> {
        number(&input)
    }

    #[alias(value)]
    fn StringLiteral(input: Node) -> Result<Value> {
        string(&input).map(|text| Value::Scalar(Literal::String(text)))
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

fn map(input: Node) -> Result<Vec<MapEntry>> {
    let mut keys = HashSet::new();
    let mut entries = Vec::new();
    for child in input.into_children() {
        let entry = QueryParser::MapEntry(child.clone())?;
        if !keys.insert(entry.key.value.clone()) {
            return Err(child.error("duplicate property in a map"));
        }
        entries.push(entry);
    }
    Ok(entries)
}

fn directed(input: Node, direction: Direction) -> Result<Relationship> {
    Ok(match_nodes!(<QueryParser>; input.into_children();
        [] => Relationship {
            direction,
            variable: None,
            types: Vec::new(),
            range: None,
            properties: Vec::new(),
        },
        [RelationshipDetail(detail)] => Relationship { direction, ..detail },
    ))
}

fn comparison<'i>(
    span: Span<'i>,
    subject: Subject<'i>,
    op: Operator,
    operand: Option<Operand<'i>>,
) -> BooleanExpression<Predicate<'i>> {
    BooleanExpression::Leaf(Predicate::Comparison {
        span,
        subject,
        op,
        operand,
    })
}

fn list(input: Node) -> Result<Value> {
    if input
        .children()
        .any(|child| child.as_rule() == Rule::ListOverflow)
    {
        let message = match input.as_rule() {
            Rule::InListLiteral => format!(
                "IN takes at most {MAX_PINS} values on id and {MAX_IN_VALUES} on other properties"
            ),
            _ => format!("a list takes at most {MAX_IN_VALUES} values"),
        };
        return Err(error_at(input.as_span(), &message));
    }
    let span = input.as_span();
    let values: Vec<Value> = match_nodes!(<QueryParser>; input.into_children();
        [value(values)..] => values.collect(),
    );
    values
        .into_iter()
        .map(|value| match value {
            Value::Scalar(literal) => Ok(literal),
            Value::List(_) => Err(error_at(span, "a list holds values, not lists")),
        })
        .collect::<Result<_>>()
        .map(Value::List)
}

fn row_count(value: &Literal, span: Span<'_>, clause: &str) -> Result<u32> {
    match value {
        Literal::Int(count) => u32::try_from(*count).ok(),
        _ => None,
    }
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

fn unsupported_function(call: &Node, in_where: bool) -> Error<Rule> {
    let name = call.children().next().map_or("", |node| node.as_str());
    let hint = match name.to_ascii_lowercase().as_str() {
        "type" => {
            "type() takes one relationship variable and works in WHERE type(r) = 'TYPE', WHERE type(r) IN ['TYPE', 'OTHER'], RETURN type(r), and ORDER BY type(r)"
        }
        "count" | "sum" | "avg" | "min" | "max" if in_where => &format!(
            "{name}() belongs in RETURN with an alias, as in RETURN count(mr) AS n; WHERE cannot filter on an aggregate"
        ),
        "count" | "sum" | "avg" | "min" | "max" => &format!(
            "{name}() takes one variable or property, as in count(mr) or sum(mr.added_lines), and belongs in RETURN with an alias: count(mr) AS n"
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
    super::identifier(&value).map_err(|error| node.error(error))?;
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
    if result.chars().count() > MAX_TEXT {
        return Err(node.error(format!(
            "string literal must not exceed {MAX_TEXT} characters"
        )));
    }
    Ok(result)
}

fn number(node: &Node) -> Result<Literal> {
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
            .map(Literal::Int)
            .map_err(|_| out_of_range());
    }
    let raw = raw.trim_start_matches('+');
    if raw.contains(['.', 'e', 'E']) {
        raw.parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .map(Literal::Float)
            .ok_or_else(|| node.error("number must be finite and in range"))
    } else {
        raw.parse::<i64>()
            .map(Literal::Int)
            .map_err(|_| out_of_range())
    }
}
