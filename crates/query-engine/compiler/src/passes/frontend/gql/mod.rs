mod ast;
mod lower;
mod syntax;

use crate::passes::frontend::{SchemaRequest, Statement};
use crate::{QueryError, Result};
use pest::Span;
use pest::error::{ErrorVariant, LineColLocation};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "passes/frontend/gql/query.pest"]
struct QueryParser;

const MAX_QUERY_BYTES: usize = 32 * 1024;
const MAX_NESTING: usize = 32;

const INVARIANT_PREFIXES: [&str; 3] = [
    "grammar produced",
    "Nodes didn't match any pattern",
    "pest_consume::parser",
];

pub fn parse(raw: &str) -> Result<Statement> {
    Ok(match parse_statement(raw)? {
        ast::Statement::Query(query) => Statement::Query(Box::new(lower::lower(raw, *query)?)),
        ast::Statement::SchemaCall { node } => Statement::Schema(SchemaRequest { node }),
    })
}

fn parse_statement(raw: &str) -> Result<ast::Statement<'_>> {
    check_bounds(raw)?;
    let statement = <QueryParser as pest_consume::Parser>::parse(Rule::Statement, raw)
        .map_err(|error| {
            QueryError::Validation(format!(
                "Orbit query syntax: {error}\nExpected one MATCH ... RETURN statement, CALL db.schema(), or CALL db.schema('NodeName'); only AND predicates, named nodes, and bounded paths are supported."
            ))
        })?
        .single()
        .expect("Statement produces one pair");
    QueryParser::Statement(statement).map_err(syntax_error)
}

fn check_bounds(query: &str) -> Result<()> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(QueryError::LimitExceeded(format!(
            "query must not exceed {MAX_QUERY_BYTES} bytes"
        )));
    }
    check_nesting(query)
}

fn check_nesting(query: &str) -> Result<()> {
    let tokens = <QueryParser as pest::Parser<Rule>>::parse(Rule::Nesting, query)
        .expect("Nesting accepts every character")
        .next()
        .expect("Nesting produces one pair");
    let mut depth = 0usize;
    for token in tokens.into_inner() {
        match token.as_rule() {
            Rule::OpenDelimiter => {
                depth += 1;
                if depth > MAX_NESTING {
                    return Err(invalid(token.as_span(), "expression nesting is too deep"));
                }
            }
            Rule::CloseDelimiter => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

fn syntax_error(error: pest_consume::Error<Rule>) -> QueryError {
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

fn invalid(span: Span<'_>, message: &str) -> QueryError {
    let (line, column) = span.start_pos().line_col();
    QueryError::Validation(format!("line {line}, column {column}: {message}"))
}
