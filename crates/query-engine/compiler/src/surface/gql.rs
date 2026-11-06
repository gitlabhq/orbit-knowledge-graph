use std::sync::Arc;

use ontology::introspection::{IntrospectionScope, SchemaResponse, build_compact_schema_response};
use query_data_model::ClickHouseDataModel;
use query_data_model::surface::Statement;
use query_data_model::surface::gql::{self, Routed};

use crate::config::{self, CompilerCtx};
use crate::metrics::CountErr;
use crate::{CompiledQueryContext, Ontology, QueryError, Result, SecurityContext};

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
    ontology: &Ontology,
    data_model: &Arc<ClickHouseDataModel>,
    security_context: &SecurityContext,
    scope: IntrospectionScope,
) -> Result<PreparedStatement> {
    match route(raw, ontology, data_model, security_context.admin, scope)? {
        RoutedStatement::Query(query) => compile_query(*query, data_model, security_context)
            .map(|compiled| PreparedStatement::Query(Box::new(compiled))),
        RoutedStatement::Schema(response) => Ok(PreparedStatement::Schema(response)),
    }
}

pub fn route(
    raw: &str,
    ontology: &Ontology,
    data_model: &ClickHouseDataModel,
    admin: bool,
    scope: IntrospectionScope,
) -> Result<RoutedStatement> {
    let catalog = super::catalog(data_model.catalog().model(), admin);
    match gql::route(raw, catalog)
        .map_err(QueryError::from)
        .count_err()?
    {
        Routed::Query(statement) => Ok(RoutedStatement::Query(Box::new(ParsedQuery { statement }))),
        Routed::Schema(node) => resolve_schema(node, ontology, scope)
            .count_err()
            .map(RoutedStatement::Schema),
    }
}

pub fn compile_query(
    query: ParsedQuery,
    data_model: &Arc<ClickHouseDataModel>,
    security_context: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut ctx = config::ClickhouseGqlCtx::new(security_context.clone(), Arc::clone(data_model));
    ctx.set_statement(query.statement);
    crate::finish(&mut ctx, config::run_clickhouse_gql)
}

fn resolve_schema(
    node: Option<String>,
    ontology: &Ontology,
    scope: IntrospectionScope,
) -> Result<SchemaResponse> {
    if let Some(name) = &node
        && (name == "*" || !scope.includes(ontology, name))
    {
        return Err(QueryError::Validation(format!(
            "schema node '{name}' is unknown or unavailable in this scope"
        )));
    }
    Ok(build_compact_schema_response(
        ontology,
        scope,
        node.as_deref(),
    ))
}
