use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Column, Order, Relation, RelationalBuilder, RelationalError,
};

use super::query::{Imported, Importer};
use super::scope::{Binding, Scope};
use crate::ast::{self, OrderExpr};
use crate::passes::codegen::SqlDialect;

pub(super) fn versioning(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::Column { column, .. } if column == "_version" || column == "_deleted")
}

impl Importer {
    pub(super) fn tail<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
        mut rows: Relation,
        groups: &[Binding],
        measures: &[Binding],
    ) -> Imported<Relation> {
        let local = self.dialect == SqlDialect::DuckDb;
        let orders: Vec<_> = query
            .order_by
            .iter()
            .filter(|order| !local || !matches!(&order.expr, ast::Expr::Column { column, .. } if column == "_version"))
            .collect();
        let limit_by = query.limit_by.as_ref().filter(|_| !local);
        match latest(&orders, limit_by) {
            Some((keys, version)) => {
                let scope = Scope::aggregated(&rows, groups, measures);
                let keys = keys
                    .iter()
                    .map(|key| column(&scope, key))
                    .collect::<Imported<Vec<_>>>()?;
                let version = column(&scope, version)?;
                rows = q.latest(rows, keys, version)?;
            }
            None => {
                let mut scope = Scope::aggregated(&rows, groups, measures);
                let mut converted = Vec::new();
                for order in &orders {
                    converted.push(Order {
                        value: self.expr(q, &order.expr, &mut scope)?,
                        descending: order.desc,
                    });
                }
                let by = match limit_by {
                    Some((count, by)) => {
                        let by = by
                            .iter()
                            .map(|value| self.expr(q, value, &mut scope))
                            .collect::<Imported<Vec<_>>>()?;
                        Some((*count, by))
                    }
                    None => None,
                };
                if !converted.is_empty() {
                    rows = q.order(rows, converted)?;
                }
                if let Some((count, by)) = by {
                    rows = q.limit_by(rows, count, by)?;
                }
            }
        }
        if query.limit.is_some() || query.offset.is_some() {
            rows = q.limit(rows, query.limit, query.offset)?;
        }
        Ok(rows)
    }
}

fn latest<'a>(
    orders: &[&'a OrderExpr],
    limit_by: Option<&'a (u32, Vec<ast::Expr>)>,
) -> Option<(&'a [ast::Expr], &'a ast::Expr)> {
    let (1, keys) = limit_by? else {
        return None;
    };
    let (version, prefix) = orders.split_last()?;
    let ordered = prefix.len() == keys.len()
        && prefix
            .iter()
            .zip(keys)
            .all(|(order, key)| !order.desc && order.expr == *key);
    let column = |expr: &ast::Expr| matches!(expr, ast::Expr::Column { .. });
    (ordered && version.desc && versioning(&version.expr) && keys.iter().all(column))
        .then_some((keys.as_slice(), &version.expr))
}

fn column(scope: &Scope<'_>, expr: &ast::Expr) -> Imported<Column> {
    match expr {
        ast::Expr::Column { table, column } => scope.qualified(table, column),
        _ => Err(RelationalError::Unsupported("computed latest keys")),
    }
}
