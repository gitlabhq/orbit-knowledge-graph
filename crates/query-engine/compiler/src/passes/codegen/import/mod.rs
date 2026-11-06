mod aggregate;
mod convert;
mod expr;
mod query;
mod scope;
mod sources;
mod tail;

use query_data_model::Catalog;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, Relation, RelationalBuilder, RelationalError, RelationalPlan,
};

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

pub fn resolve<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    dialect: SqlDialect,
    expr: &crate::ast::Expr,
    rows: &Relation,
) -> Result<Expr, RelationalError> {
    query::Importer::new(dialect).expr(q, expr, &mut scope::Scope::new(vec![rows]))
}

fn query(ast: &Node) -> Result<&crate::ast::Query, RelationalError> {
    match ast {
        Node::Query(root) => Ok(root),
        Node::Insert(_) => Err(RelationalError::Unsupported("inserts")),
    }
}
