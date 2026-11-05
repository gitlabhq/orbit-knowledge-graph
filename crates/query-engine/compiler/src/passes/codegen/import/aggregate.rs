use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    AggregateFunction, Measure, Relation, RelationalBuilder, RelationalError,
};

use super::query::{Imported, Importer};
use super::scope::{Binding, Scope};
use crate::ast::{self, Function, visit::visit_expressions};
use crate::input::AggFunction;

pub(super) struct Grouped {
    pub(super) rows: Relation,
    pub(super) groups: Vec<Binding>,
    pub(super) measures: Vec<Binding>,
}

pub(super) fn aggregates(query: &ast::Query) -> Vec<&ast::Expr> {
    let mut found: Vec<&ast::Expr> = Vec::new();
    let values = query
        .select
        .iter()
        .map(|item| &item.expr)
        .chain(&query.having)
        .chain(query.order_by.iter().map(|order| &order.expr));
    for value in values {
        let _ = visit_expressions(value, &mut |expr| {
            let aggregate = matches!(
                expr,
                ast::Expr::Aggregate { .. }
                    | ast::Expr::FuncCall {
                        name: Function::ArgMax | Function::ArgMaxOrNull,
                        ..
                    }
            );
            if aggregate && !found.contains(&expr) {
                found.push(expr);
            }
            Ok(())
        });
    }
    found
}

impl Importer {
    pub(super) fn group<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        query: &ast::Query,
        rows: Relation,
    ) -> Imported<Grouped> {
        let found = aggregates(query);
        if query.group_by.is_empty() && found.is_empty() {
            return Ok(Grouped {
                rows,
                groups: Vec::new(),
                measures: Vec::new(),
            });
        }
        let mut scope = Scope::new(vec![&rows]);
        let mut groups = Vec::new();
        for group in &query.group_by {
            groups.push(self.expr(q, group, &mut scope)?);
        }
        let mut measures = Vec::new();
        for aggregate in &found {
            measures.push(self.measure(q, aggregate, &mut scope)?);
        }
        let aggregated = q.aggregate(rows, groups, measures)?;
        Ok(Grouped {
            rows: aggregated.rows,
            groups: query
                .group_by
                .iter()
                .cloned()
                .zip(aggregated.groups)
                .collect(),
            measures: found
                .into_iter()
                .cloned()
                .zip(aggregated.measures)
                .collect(),
        })
    }

    fn measure<B: RelationalBackend>(
        &mut self,
        q: &mut RelationalBuilder<'_, B>,
        aggregate: &ast::Expr,
        scope: &mut Scope<'_>,
    ) -> Imported<Measure> {
        let (function, args, distinct, condition) = match aggregate {
            ast::Expr::Aggregate {
                function,
                argument,
                distinct,
                condition,
            } => (
                aggregate_function(*function),
                argument
                    .iter()
                    .map(|value| value.as_ref())
                    .collect::<Vec<_>>(),
                *distinct,
                condition.as_deref(),
            ),
            ast::Expr::FuncCall { name, args } => {
                let function = match name {
                    Function::ArgMax => AggregateFunction::ArgMax,
                    _ => AggregateFunction::ArgMaxOrNull,
                };
                (function, args.iter().collect(), false, None)
            }
            _ => return Err(RelationalError::Unsupported("non-aggregate measures")),
        };
        let mut converted = Vec::new();
        for arg in args {
            converted.push(self.expr(q, arg, scope)?);
        }
        let condition = match condition {
            Some(condition) => Some(self.expr(q, condition, scope)?),
            None => None,
        };
        Ok(Measure {
            function,
            args: converted,
            distinct,
            condition,
        })
    }
}

fn aggregate_function(function: AggFunction) -> AggregateFunction {
    match function {
        AggFunction::Count => AggregateFunction::Count,
        AggFunction::Sum => AggregateFunction::Sum,
        AggFunction::Avg => AggregateFunction::Avg,
        AggFunction::Min => AggregateFunction::Min,
        AggFunction::Max => AggregateFunction::Max,
        AggFunction::Collect => AggregateFunction::Collect,
    }
}
