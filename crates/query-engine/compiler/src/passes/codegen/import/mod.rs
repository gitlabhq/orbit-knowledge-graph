mod aggregate;
mod convert;
mod expr;
mod query;
mod scope;
mod sources;
mod tail;

use query_data_model::Catalog;
use query_data_model::implementations::clickhouse::ClickHouse;
use query_data_model::implementations::duckdb::DuckDb;
use query_data_model::implementations::relational::plan::{RelationalError, RelationalPlan};

use crate::ast::Node;
use crate::passes::codegen::SqlDialect;

pub fn clickhouse(
    ast: &Node,
    catalog: &Catalog<ClickHouse>,
    settings: Vec<(String, String)>,
) -> Result<RelationalPlan<ClickHouse>, RelationalError> {
    let root = query(ast)?;
    catalog.build(|q| {
        q.settings(settings)?;
        query::Importer::new(SqlDialect::ClickHouse).statement(q, root)
    })
}

pub fn duckdb(
    ast: &Node,
    catalog: &Catalog<DuckDb>,
) -> Result<RelationalPlan<DuckDb>, RelationalError> {
    let root = query(ast)?;
    catalog.build(|q| query::Importer::new(SqlDialect::DuckDb).statement(q, root))
}

fn query(ast: &Node) -> Result<&crate::ast::Query, RelationalError> {
    match ast {
        Node::Query(root) => Ok(root),
        Node::Insert(_) => Err(RelationalError::Unsupported("inserts")),
    }
}
