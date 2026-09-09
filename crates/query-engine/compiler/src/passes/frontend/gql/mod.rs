mod lower;
mod value;

use crate::{Input, QueryError, Result};
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "passes/frontend/gql/query.pest"]
struct QueryParser;

const MAX_QUERY_BYTES: usize = 32 * 1024;
const MAX_NESTING: usize = 32;

pub fn parse(query: &str) -> Result<Input> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(QueryError::LimitExceeded(format!(
            "query must not exceed {MAX_QUERY_BYTES} bytes"
        )));
    }
    check_nesting(query)?;
    let statement = QueryParser::parse(Rule::Query, query)
        .map_err(|error| {
            QueryError::Validation(format!(
                "Orbit query syntax: {error}\nExpected one MATCH ... [WHERE] RETURN [ORDER BY] [LIMIT] statement; only AND predicates, named nodes, and bounded paths are supported."
            ))
        })?
        .next()
        .expect("Query produces one pair");
    lower::lower(statement)
}

fn check_nesting(query: &str) -> Result<()> {
    let tokens = QueryParser::parse(Rule::Nesting, query)
        .expect("Nesting accepts every character")
        .next()
        .expect("Nesting produces one pair");
    let mut depth = 0usize;
    for token in tokens.into_inner() {
        match token.as_rule() {
            Rule::OpenDelimiter => {
                depth += 1;
                if depth > MAX_NESTING {
                    return Err(invalid(&token, "expression nesting is too deep"));
                }
            }
            Rule::CloseDelimiter => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

fn invalid(pair: &Pair<'_, Rule>, message: &str) -> QueryError {
    let (line, column) = pair.line_col();
    QueryError::Validation(format!("line {line}, column {column}: {message}"))
}

fn unexpected(pair: &Pair<'_, Rule>) -> QueryError {
    QueryError::PipelineInvariant(format!(
        "grammar produced {:?} where the lowering has no arm",
        pair.as_rule()
    ))
}

fn name(pair: Pair<'_, Rule>) -> Result<String> {
    let name = unescape(pair.as_str());
    crate::input_validation::validate_identifier(&name)
        .map_err(|error| invalid(&pair, &error.to_string()))?;
    Ok(name)
}

fn unescape(raw: &str) -> String {
    raw.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .map_or_else(|| raw.to_owned(), |s| s.replace("``", "`"))
}

fn property(pair: Pair<'_, Rule>) -> Result<crate::input::PropertyRef> {
    let mut parts = pair.into_inner();
    Ok(crate::input::PropertyRef {
        node: name(parts.next().expect("property has a variable"))?,
        property: name(parts.next().expect("property has a name"))?,
    })
}
