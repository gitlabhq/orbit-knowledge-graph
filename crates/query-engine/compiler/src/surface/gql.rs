use std::sync::Arc;

use query_data_model::introspection::{
    IntrospectionScope, SchemaResponse, build_compact_schema_response,
};
use query_data_model::surface::Statement;
use query_data_model::surface::gql::{self, Routed};
use query_data_model::{Catalog, ClickHouse, Model};

use crate::metrics::CountErr;
use crate::{CompiledQueryContext, QueryError, Result};
use orbit_authorization::Grants;
use orbit_server_config::QueryLimits;

pub use query_data_model::surface::gql::{GRAMMAR, outline};

#[derive(Debug)]
pub struct ParsedQuery {
    pub statement: Statement,
}

#[derive(Debug)]
pub enum RoutedStatement {
    Query(Box<ParsedQuery>),
    Schema(SchemaResponse),
}

#[derive(Debug)]
pub enum PreparedStatement {
    Query(Box<CompiledQueryContext>),
    Schema(SchemaResponse),
}

#[must_use = "the prepared statement should be used"]
pub fn prepare(
    raw: &str,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    scope: IntrospectionScope,
    limits: &QueryLimits,
) -> Result<PreparedStatement> {
    let connections = crate::graph_only(data_model);
    match route(
        raw,
        data_model,
        &connections,
        grants.caller().admin(),
        scope,
        limits,
    )? {
        RoutedStatement::Query(query) => compile_query(*query, data_model, grants, limits)
            .map(|compiled| PreparedStatement::Query(Box::new(compiled))),
        RoutedStatement::Schema(response) => Ok(PreparedStatement::Schema(response)),
    }
}

pub fn route(
    raw: &str,
    data_model: &Catalog<ClickHouse>,
    connections: &crate::dag::Connections,
    admin: bool,
    scope: IntrospectionScope,
    limits: &QueryLimits,
) -> Result<RoutedStatement> {
    let catalog = super::catalog(data_model, connections, limits, admin);
    match gql::route(raw, catalog)
        .map_err(QueryError::from)
        .count_err()?
    {
        Routed::Query(statement) => Ok(RoutedStatement::Query(Box::new(ParsedQuery { statement }))),
        Routed::Schema(node) => resolve_schema(node, data_model.model(), scope)
            .count_err()
            .map(RoutedStatement::Schema),
    }
}

pub fn compile_query(
    query: ParsedQuery,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<CompiledQueryContext> {
    let stage = crate::split(
        crate::dag::Source::Parsed(query.statement),
        data_model,
        grants,
        limits,
    )?
    .into_single()?;
    crate::compile_stage(stage, data_model, grants, limits)
}

fn resolve_schema(
    node: Option<String>,
    model: &Model,
    scope: IntrospectionScope,
) -> Result<SchemaResponse> {
    if let Some(name) = &node
        && (name == "*" || !scope.includes(model, name))
    {
        return Err(QueryError::Validation(format!(
            "schema node '{name}' is unknown or unavailable in this scope"
        )));
    }
    Ok(build_compact_schema_response(model, scope, node.as_deref()))
}
