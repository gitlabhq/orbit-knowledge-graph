mod ast;
mod ceilings;
mod errors;
mod lower;
mod syntax;

use orbit_server_config::QueryLimits;
use pest_derive::Parser;

use super::{Statement, SurfaceError};
use crate::plan::logical::LogicalCatalog;
use errors::{invalid, parse_failure, syntax_error};

pub use ceilings::{admits, ceilings};

#[derive(Parser)]
#[grammar = "surface/gql/query.pest"]
struct QueryParser;

pub const GRAMMAR: &str = include_str!("query.pest");

#[derive(Debug)]
pub enum Routed {
    Query(Statement),
    Schema(Option<String>),
}

pub fn route(raw: &str, catalog: LogicalCatalog<'_>) -> Result<Routed, SurfaceError> {
    match statement(raw, catalog.limits())? {
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

pub fn outline(query: &str, limits: &QueryLimits) -> Option<Vec<(usize, String)>> {
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
    bounds(query, limits).ok()?;
    let pairs = <QueryParser as pest::Parser<Rule>>::parse(Rule::Query, query).ok()?;
    let mut outline = Vec::new();
    visit(pairs, 0, &mut outline);
    Some(outline)
}

fn statement<'i>(
    raw: &'i str,
    limits: &'i QueryLimits,
) -> Result<ast::Statement<'i>, SurfaceError> {
    bounds(raw, limits)?;
    let statement =
        <QueryParser as pest_consume::Parser>::parse_with_userdata(Rule::Statement, raw, limits)
            .map_err(|error| parse_failure(error, limits))?
            .single()
            .map_err(syntax_error)?;
    QueryParser::Statement(statement).map_err(syntax_error)
}

fn bounds(query: &str, limits: &QueryLimits) -> Result<(), SurfaceError> {
    if query.len() > limits.query_bytes {
        return Err(SurfaceError::Limit(format!(
            "query must not exceed {} bytes",
            limits.query_bytes
        )));
    }
    let tokens = <QueryParser as pest::Parser<Rule>>::parse(Rule::Nesting, query)
        .map_err(|error| parse_failure(error, limits))?
        .next()
        .into_iter()
        .flat_map(|pair| pair.into_inner());
    let mut depth = 0usize;
    for token in tokens {
        match token.as_rule() {
            Rule::OpenDelimiter => {
                depth += 1;
                if depth > limits.nesting {
                    return Err(invalid(token.as_span(), "expression nesting is too deep"));
                }
            }
            Rule::CloseDelimiter => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

fn identifier(identifier: &str, max: usize) -> Result<(), String> {
    let mut chars = identifier.chars();
    let valid = identifier.len() <= max
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    match valid {
        true => Ok(()),
        false => Err(format!("invalid identifier {identifier:?}")),
    }
}
