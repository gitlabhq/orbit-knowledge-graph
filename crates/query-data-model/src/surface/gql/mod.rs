mod ast;
mod errors;
mod lower;
mod syntax;

use pest_derive::Parser;

use super::{Statement, SurfaceError};
use crate::plan::logical::LogicalCatalog;
use crate::plan::logical::limits::{MAX_IDENTIFIER, MAX_NESTING};
use errors::{invalid, parse_failure, syntax_error};

#[derive(Parser)]
#[grammar = "surface/gql/query.pest"]
struct QueryParser;

pub const GRAMMAR: &str = include_str!("query.pest");

const MAX_QUERY_BYTES: usize = 32 * 1024;

pub fn parse(raw: &str, catalog: LogicalCatalog<'_>) -> Result<Statement, SurfaceError> {
    match statement(raw)? {
        ast::Statement::Query(query) => lower::lower(*query, catalog),
        ast::Statement::SchemaCall => {
            Err(SurfaceError::schema("schema calls are not graph queries"))
        }
    }
}

fn statement(raw: &str) -> Result<ast::Statement<'_>, SurfaceError> {
    bounds(raw)?;
    let statement = <QueryParser as pest_consume::Parser>::parse(Rule::Statement, raw)
        .map_err(parse_failure)?
        .single()
        .map_err(syntax_error)?;
    QueryParser::Statement(statement).map_err(syntax_error)
}

fn bounds(query: &str) -> Result<(), SurfaceError> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(SurfaceError::Limit(format!(
            "query must not exceed {MAX_QUERY_BYTES} bytes"
        )));
    }
    let tokens = <QueryParser as pest::Parser<Rule>>::parse(Rule::Nesting, query)
        .map_err(parse_failure)?
        .next()
        .into_iter()
        .flat_map(|pair| pair.into_inner());
    let mut depth = 0usize;
    for token in tokens {
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

fn identifier(identifier: &str) -> Result<(), String> {
    let mut chars = identifier.chars();
    let valid = identifier.len() <= MAX_IDENTIFIER
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    match valid {
        true => Ok(()),
        false => Err(format!("invalid identifier {identifier:?}")),
    }
}
