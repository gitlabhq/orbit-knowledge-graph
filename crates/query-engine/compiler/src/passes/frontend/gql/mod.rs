mod ast;
mod lower;
mod syntax;

use std::sync::Arc;

use crate::config::{self, CompilerCtx as _};
use crate::metrics::CountErr;
use crate::{CompiledQueryContext, Input, Ontology, QueryError, Result, SecurityContext};
use ontology::introspection::{
    IntrospectionScope, SchemaResponse, build_node_schema_response, build_schema_response,
};
use pest::Span;
use pest::error::{ErrorVariant, LineColLocation};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "passes/frontend/gql/query.pest"]
struct QueryParser;

pub const GRAMMAR: &str = include_str!("query.pest");

pub fn pair_outline(query: &str) -> Option<Vec<(usize, String)>> {
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
    check_bounds(query).ok()?;
    let pairs = <QueryParser as pest::Parser<Rule>>::parse(Rule::Query, query).ok()?;
    let mut outline = Vec::new();
    visit(pairs, 0, &mut outline);
    Some(outline)
}

const MAX_QUERY_BYTES: usize = 32 * 1024;
const MAX_NESTING: usize = 32;

const INVARIANT_PREFIXES: [&str; 3] = [
    "grammar produced",
    "Nodes didn't match any pattern",
    "pest_consume::parser",
];

#[derive(Debug)]
pub enum PreparedStatement {
    Query(Box<CompiledQueryContext>),
    Schema(SchemaResponse),
}

#[must_use = "the prepared statement should be used"]
pub fn prepare(
    raw: &str,
    ontology: &Ontology,
    security_context: &SecurityContext,
    scope: IntrospectionScope,
) -> Result<PreparedStatement> {
    match parse_statement(raw).count_err()? {
        ast::Statement::Query(query) => {
            let input = lower::lower(raw, *query).count_err()?;
            compile_query(input, ontology, security_context)
                .map(|compiled| PreparedStatement::Query(Box::new(compiled)))
        }
        ast::Statement::SchemaCall { node } => resolve_schema(node, ontology, scope)
            .count_err()
            .map(PreparedStatement::Schema),
    }
}

pub fn parse(raw: &str) -> Result<Input> {
    match parse_statement(raw)? {
        ast::Statement::Query(query) => lower::lower(raw, *query),
        ast::Statement::SchemaCall { .. } => Err(QueryError::Validation(
            "schema calls are not graph queries; use gql::prepare instead".into(),
        )),
    }
}

fn compile_query(
    input: Input,
    ontology: &Ontology,
    security_context: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut ctx =
        config::ClickhouseGqlCtx::new(Arc::new(ontology.clone()), security_context.clone());
    ctx.set_input(input);
    crate::finish(&mut ctx, config::run_clickhouse_gql)
}

fn resolve_schema(
    node: Option<String>,
    ontology: &Ontology,
    scope: IntrospectionScope,
) -> Result<SchemaResponse> {
    let Some(name) = node else {
        return Ok(build_schema_response(ontology, scope, &[]));
    };
    if name == "*"
        || ontology.get_node(&name).is_none()
        || (scope == IntrospectionScope::Local
            && !ontology.local_entity_names().contains(&name.as_str()))
    {
        return Err(QueryError::Validation(format!(
            "schema node '{name}' is unknown or unavailable in this scope"
        )));
    }
    Ok(build_node_schema_response(ontology, scope, &name))
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
