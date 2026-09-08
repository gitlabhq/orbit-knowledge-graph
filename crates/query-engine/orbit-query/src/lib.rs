mod lower;
mod value;

use std::collections::HashMap;

use compiler::{CompiledQueryContext, Input, Ontology, QueryError, Result, SecurityContext};
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;
use serde_json::Value;

#[derive(Parser)]
#[grammar = "query.pest"]
struct QueryParser;

pub type Parameters = HashMap<String, Value>;

const MAX_QUERY_BYTES: usize = 32 * 1024;
const MAX_NESTING: usize = 32;

pub fn parse(query: &str, parameters: &Parameters) -> Result<Input> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(QueryError::LimitExceeded(format!(
            "query must not exceed {MAX_QUERY_BYTES} bytes"
        )));
    }
    let footprints = check_parameters(parameters)?;
    check_nesting(query)?;
    let statement = QueryParser::parse(Rule::Query, query)
        .map_err(|error| {
            QueryError::Validation(format!(
                "Orbit query syntax: {error}\nExpected one MATCH ... [WHERE] RETURN [ORDER BY] [LIMIT] statement; only AND predicates, named nodes, and bounded paths are supported."
            ))
        })?
        .next()
        .expect("Query produces one pair");
    lower::lower(statement, value::Bindings::new(parameters, footprints))
}

pub fn compile(
    query: &str,
    parameters: &Parameters,
    ontology: &Ontology,
    context: &SecurityContext,
) -> Result<CompiledQueryContext> {
    compiler::compile_from_input(parse(query, parameters)?, ontology, context)
}

fn check_parameters(parameters: &Parameters) -> Result<HashMap<&str, usize>> {
    let mut total: usize = 0;
    let mut footprints = HashMap::with_capacity(parameters.len());
    for (key, root) in parameters {
        let mut bytes = key.len();
        let mut pending = vec![(root, 0)];
        while let Some((value, depth)) = pending.pop() {
            if depth > MAX_NESTING || bytes > MAX_QUERY_BYTES || pending.len() > MAX_QUERY_BYTES {
                return Err(QueryError::LimitExceeded(
                    "parameter payload is too large or too deeply nested".into(),
                ));
            }
            bytes = bytes.saturating_add(1);
            match value {
                Value::Array(values) => pending.extend(values.iter().map(|v| (v, depth + 1))),
                Value::String(text) => bytes = bytes.saturating_add(text.len()),
                Value::Number(number) => bytes = bytes.saturating_add(number.to_string().len()),
                Value::Bool(_) => {}
                _ => {
                    return Err(QueryError::Validation(
                        "parameters must be strings, numbers, booleans, or lists".into(),
                    ));
                }
            }
        }
        total = total.saturating_add(bytes);
        if total > MAX_QUERY_BYTES {
            return Err(QueryError::LimitExceeded(
                "parameter payload is too large".into(),
            ));
        }
        footprints.insert(key.as_str(), bytes);
    }
    Ok(footprints)
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
    compiler::input_validation::validate_identifier(&name)
        .map_err(|error| invalid(&pair, &error.to_string()))?;
    Ok(name)
}

fn unescape(raw: &str) -> String {
    raw.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .map_or_else(|| raw.to_owned(), |s| s.replace("``", "`"))
}

fn property(pair: Pair<'_, Rule>) -> Result<compiler::input::PropertyRef> {
    let mut parts = pair.into_inner();
    Ok(compiler::input::PropertyRef {
        node: name(parts.next().expect("property has a variable"))?,
        property: name(parts.next().expect("property has a name"))?,
    })
}
