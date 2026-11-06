mod aggregate;
mod convert;
mod expr;
mod query;
mod scope;
mod sources;
mod tail;

use query_data_model::Catalog;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{RelationalError, RelationalPlan};

use crate::ast::Node;
use crate::passes::codegen::SqlDialect;

pub fn build<B: RelationalBackend>(
    ast: &Node,
    catalog: &Catalog<B>,
    dialect: SqlDialect,
) -> Result<RelationalPlan<B>, RelationalError> {
    let root = query(ast)?;
    catalog.build(|q| query::Importer::new(dialect).statement(q, root))
}

fn query(ast: &Node) -> Result<&crate::ast::Query, RelationalError> {
    match ast {
        Node::Query(root) => Ok(root),
        Node::Insert(_) => Err(RelationalError::Unsupported("inserts")),
    }
}
