use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Expr, RelationalBuilder, RelationalError,
};
use serde_json::Value;

use super::convert::{binary, deleted, function, literal, text_match, time_unit, unary};
use super::query::{Imported, Importer};
use super::scope::Scope;
use crate::ast::{self, SqlType};
use crate::passes::codegen::SqlDialect;

impl Importer {
    pub(super) fn expr<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        expr: &ast::Expr,
        scope: &mut Scope<'_>,
    ) -> Imported<Expr> {
        if let Some(column) =
            Scope::bound(scope.groups, expr).or(Scope::bound(scope.measures, expr))
        {
            return Ok(column.expr());
        }
        if self.dialect == SqlDialect::DuckDb && deleted(expr) {
            return Ok(Expr::Bool(true));
        }
        let mut boxed = |this: &mut Self, value: &ast::Expr, scope: &mut Scope<'_>| {
            this.expr(q, value, scope).map(Box::new)
        };
        Ok(match expr {
            ast::Expr::Column { table, column } => scope.qualified(table, column)?.expr(),
            ast::Expr::Identifier(name) if scope.lambdas.contains(name) => {
                Expr::Variable(name.clone())
            }
            ast::Expr::Identifier(name) => scope.named(name)?.expr(),
            ast::Expr::Literal(value) => literal(value),
            ast::Expr::Param { data_type, value } => self.parameter(*data_type, value),
            ast::Expr::EmptyTupleArray(types) => Expr::EmptyArray(types.clone()),
            ast::Expr::TimeBucket { unit, value } => Expr::TimeBucket {
                unit: time_unit(*unit),
                value: boxed(self, value, scope)?,
            },
            ast::Expr::TextSearch { mode, value, query } => Expr::TextSearch {
                mode: text_match(*mode),
                value: boxed(self, value, scope)?,
                query: boxed(self, query, scope)?,
            },
            ast::Expr::Lambda { param, body } => {
                scope.lambdas.push(param.clone());
                let body = boxed(self, body, scope);
                scope.lambdas.pop();
                Expr::Lambda {
                    param: param.clone(),
                    body: body?,
                }
            }
            ast::Expr::BinaryOp { op, left, right } => Expr::Binary {
                op: binary(*op)?,
                left: boxed(self, left, scope)?,
                right: boxed(self, right, scope)?,
            },
            ast::Expr::UnaryOp { op, expr } => Expr::Unary {
                op: unary(*op)?,
                value: boxed(self, expr, scope)?,
            },
            ast::Expr::FuncCall { name, args } => {
                let mut converted = Vec::new();
                for arg in args {
                    converted.push(self.expr(q, arg, scope)?);
                }
                Expr::Call {
                    function: function(*name)?,
                    args: converted,
                }
            }
            ast::Expr::InSubquery {
                expr,
                cte_name,
                columns,
            } => {
                let value = self.expr(q, expr, scope)?;
                let body = self.cte(cte_name)?.clone();
                let keys = q.subquery(cte_name, |q| {
                    let rows = q.read(&body, None)?;
                    let keys = columns
                        .iter()
                        .map(|column| rows.column(column))
                        .collect::<Imported<Vec<_>>>()?;
                    q.select(rows, keys)
                })?;
                keys.contains(value)
            }
            ast::Expr::InSelect { expr, query } => {
                let value = self.expr(q, expr, scope)?;
                self.subquery(q, query)?.contains(value)
            }
            ast::Expr::Scalar(query) => self.subquery(q, query)?.scalar(),
            ast::Expr::Aggregate { .. } | ast::Expr::Star => {
                return Err(RelationalError::Unsupported(
                    "aggregates outside an aggregation",
                ));
            }
        })
    }

    fn parameter(&self, data_type: SqlType, value: &Value) -> Expr {
        match value {
            Value::Null => Expr::Null,
            Value::Array(items)
                if !matches!(data_type, SqlType::Array(_))
                    && self.dialect == SqlDialect::ClickHouse =>
            {
                Expr::List(
                    items
                        .iter()
                        .map(|item| Expr::Parameter {
                            data_type,
                            value: item.clone(),
                        })
                        .collect(),
                )
            }
            value => Expr::Parameter {
                data_type,
                value: value.clone(),
            },
        }
    }
}
