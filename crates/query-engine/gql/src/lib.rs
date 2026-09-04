//! Read-only ISO GQL frontend for the graph query compiler.

mod assemble;
mod grammar;
pub mod limits;

use std::collections::HashMap;

use compiler::passes::codegen::CompiledQueryContext;
use compiler::{Input, QueryError, SecurityContext};
use ontology::Ontology;
use serde_json::Value;

pub type Params = HashMap<String, Value>;

pub type Projection = Vec<OutputColumn>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumn {
    pub name: String,
    pub source: ColumnSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSource {
    Node(String),
    Property {
        node: String,
        property: String,
    },
    EdgeProperty {
        relationship: String,
        property: String,
    },
    Aggregate,
}

impl OutputColumn {
    fn node_as(var: &str, alias: Option<String>) -> Self {
        Self {
            name: alias.unwrap_or_else(|| var.to_string()),
            source: ColumnSource::Node(var.to_string()),
        }
    }

    fn property(node: &str, property: &str, alias: Option<String>) -> Self {
        Self {
            name: alias.unwrap_or_else(|| format!("{node}.{property}")),
            source: ColumnSource::Property {
                node: node.to_string(),
                property: property.to_string(),
            },
        }
    }

    fn edge_property(relationship: &str, property: &str, alias: Option<String>) -> Self {
        Self {
            name: alias.unwrap_or_else(|| format!("{relationship}.{property}")),
            source: ColumnSource::EdgeProperty {
                relationship: relationship.to_string(),
                property: property.to_string(),
            },
        }
    }

    fn aggregate(name: String) -> Self {
        Self {
            name,
            source: ColumnSource::Aggregate,
        }
    }
}

#[derive(Debug)]
pub struct Parsed {
    pub input: Input,
    pub projection: Projection,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("syntax error at line {line}, column {column}: expected {expected}")]
    Syntax {
        line: usize,
        column: usize,
        expected: String,
    },
    #[error("unsupported GQL: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Semantic(String),
    #[error("{0}")]
    Depth(String),
    #[error("{0}")]
    Limit(String),
}

impl From<Error> for QueryError {
    fn from(e: Error) -> Self {
        match e {
            Error::Depth(msg) => QueryError::DepthExceeded(msg),
            Error::Limit(msg) => QueryError::LimitExceeded(msg),
            other => QueryError::Validation(other.to_string()),
        }
    }
}

/// Parse a GQL query into the compiler's [`Input`] without compiling it.
pub fn parse(gql: &str, params: &Params) -> Result<Parsed, Error> {
    match grammar::gql::query(gql, params) {
        Ok(parsed) => {
            let parsed = parsed?;
            limits::check(&parsed.input)?;
            Ok(parsed)
        }
        Err(e) => Err(Error::Syntax {
            line: e.location.line,
            column: e.location.column,
            expected: e.expected.to_string(),
        }),
    }
}

/// Parse and compile a GQL query through the full security pipeline.
#[must_use = "the compiled query context should be used"]
pub fn compile_gql(
    gql: &str,
    params: &Params,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> compiler::Result<CompiledQueryContext> {
    let parsed = parse(gql, params)?;
    compiler::compile_structured(parsed.input, ontology, ctx)
}
