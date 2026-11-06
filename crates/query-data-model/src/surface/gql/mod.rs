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

#[derive(Debug)]
pub enum Routed {
    Query(Statement),
    Schema(Option<String>),
}

pub fn route(raw: &str, catalog: LogicalCatalog<'_>) -> Result<Routed, SurfaceError> {
    match statement(raw)? {
        ast::Statement::Query(query) => lower::lower(*query, catalog).map(Routed::Query),
        ast::Statement::SchemaCall(node) => Ok(Routed::Schema(node)),
    }
}

pub fn parse(raw: &str, catalog: LogicalCatalog<'_>) -> Result<Statement, SurfaceError> {
    match route(raw, catalog)? {
        Routed::Query(statement) => Ok(statement),
        Routed::Schema(_) => Err(SurfaceError::schema("schema calls are not graph queries")),
    }
}

pub fn outline(query: &str) -> Option<Vec<(usize, String)>> {
    fn visit(
        pairs: pest::iterators::Pairs<'_, Rule>,
        depth: usize,
        out: &mut Vec<(usize, String)>,
    ) {
        for pair in pairs {
            out.push((depth, format!("{:?}", pair.as_rule())));
            visit(pair.into_inner(), depth + 1, out);
        }
    }
    bounds(query).ok()?;
    let pairs = <QueryParser as pest::Parser<Rule>>::parse(Rule::Query, query).ok()?;
    let mut outline = Vec::new();
    visit(pairs, 0, &mut outline);
    Some(outline)
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
