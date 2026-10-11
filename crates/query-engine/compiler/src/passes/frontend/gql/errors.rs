use pest::Span;
use pest::error::{Error, ErrorVariant, LineColLocation};

use super::Rule;
use crate::QueryError;

const INVARIANT_PREFIXES: [&str; 3] = [
    "grammar produced",
    "Nodes didn't match any pattern",
    "pest_consume::parser",
];

pub(super) fn parse_failure(error: Error<Rule>) -> QueryError {
    let (line, column) = match error.line_col {
        LineColLocation::Pos(position) | LineColLocation::Span(position, _) => position,
    };
    QueryError::Validation(format!(
        "Orbit query syntax at line {line}, column {column}: {}\nExpected one MATCH ... RETURN statement, CALL db.schema(), or CALL db.schema('NodeName'); predicates support AND, NOT, and parentheses; patterns require named nodes and bounded paths.",
        expected_message(&error.variant)
    ))
}

fn expected_message(variant: &ErrorVariant<Rule>) -> String {
    match variant {
        ErrorVariant::ParsingError {
            positives,
            negatives,
        } => match (labels(positives), labels(negatives)) {
            (expected, unexpected) if unexpected.is_empty() => format!("expected {expected}"),
            (expected, unexpected) if expected.is_empty() => format!("unexpected {unexpected}"),
            (expected, unexpected) => format!("unexpected {unexpected}; expected {expected}"),
        },
        ErrorVariant::CustomError { message } => message.clone(),
    }
}

fn labels(rules: &[Rule]) -> String {
    let mut labels: Vec<String> = Vec::new();
    for label in rules.iter().map(rule_label) {
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    match labels.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [first, second] => format!("{first} or {second}"),
        [rest @ .., last] => format!("{}, or {last}", rest.join(", ")),
    }
}

fn rule_label(rule: &Rule) -> String {
    match rule {
        Rule::EOI => "end of query",
        Rule::Query | Rule::Matches | Rule::Match => "MATCH",
        Rule::Where => "WHERE",
        Rule::Return => "RETURN",
        Rule::Order => "ORDER BY",
        Rule::Limit => "LIMIT",
        Rule::Page => "PAGE",
        Rule::After => "AFTER",
        Rule::Debug => "DEBUG",
        Rule::SchemaCall => "CALL db.schema()",
        Rule::Pattern | Rule::PatternElement | Rule::NodePattern => {
            "a node pattern such as (n:Label)"
        }
        Rule::PatternElementChain | Rule::RelationshipPattern => {
            "a relationship such as -[:TYPE]->"
        }
        Rule::NodeLabel => "a node label",
        Rule::RelationshipTypes => "a relationship type",
        Rule::Variable => "a variable",
        Rule::SchemaName => "a label, relationship type, or property name",
        Rule::PropertyExpression => "a property such as n.name",
        Rule::ProjectionItems | Rule::ProjectionItem | Rule::ProjectionExpression => {
            "a RETURN item"
        }
        Rule::AndExpression
        | Rule::NotExpression
        | Rule::ParenthesizedExpression
        | Rule::ComparisonExpression
        | Rule::TokenPredicate => "a condition",
        Rule::Negation => "NOT",
        Rule::UnsupportedBooleanOperator => "AND",
        Rule::TokenFunction => "token_match, all_tokens, or any_tokens",
        Rule::ComparisonOperator => "a comparison operator",
        Rule::StringOperator => "STARTS WITH, ENDS WITH, or CONTAINS",
        Rule::InOperator | Rule::NegatedInOperator => "IN",
        Rule::NullOperator => "IS NULL",
        Rule::SortItem => "a sort key",
        Rule::SortDirection => "ASC or DESC",
        Rule::UnsignedInteger => "a non-negative integer",
        Rule::StringLiteral
        | Rule::NumberLiteral
        | Rule::BooleanLiteral
        | Rule::TemporalLiteral
        | Rule::FunctionName => "a value",
        Rule::ListLiteral => "a list",
        Rule::MapLiteral | Rule::MapEntry => "a property map such as {name: 'x'}",
        Rule::RangeLiteral => "a hop range such as *1..3",
        other => return format!("{other:?}"),
    }
    .to_owned()
}

pub(super) fn syntax_error(error: Error<Rule>) -> QueryError {
    let (line, column) = match error.line_col {
        LineColLocation::Pos(pos) | LineColLocation::Span(pos, _) => pos,
    };
    let message = match &error.variant {
        ErrorVariant::CustomError { message } => message.clone(),
        variant => variant.message().into_owned(),
    };
    if INVARIANT_PREFIXES.iter().any(|p| message.starts_with(p)) {
        return QueryError::PipelineInvariant(message);
    }
    QueryError::Validation(format!("line {line}, column {column}: {message}"))
}

pub(super) fn invalid(span: Span<'_>, message: &str) -> QueryError {
    let (line, column) = span.start_pos().line_col();
    QueryError::Validation(format!("line {line}, column {column}: {message}"))
}
