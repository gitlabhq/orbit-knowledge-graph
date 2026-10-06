use crate::ast::{Cte, Expr, OrderExpr, Query, SelectExpr, TableRef};
use crate::passes::plan::physical::{ExecutionPlan, PhysicalPlan, PhysicalSource};
use query_data_model::bindings::{ColumnRef, ScopeId};

use super::requirements::{column, predicate, projections};

pub(super) fn execute(scope: ScopeId, plan: &ExecutionPlan) -> Query {
    let mut query = source(scope, &plan.source);
    query.select = projections(&plan.outputs);
    query.ctes = plan
        .definitions
        .iter()
        .map(|(name, body)| Cte::new(name, physical_query(body)))
        .collect();
    query
}

pub(super) fn physical_query(plan: &PhysicalPlan) -> Query {
    let mut query = match &plan.source {
        PhysicalSource::Latest {
            scope,
            input,
            sort_key,
            version,
            aggregate_condition,
            ..
        } => latest_query(*scope, input, sort_key, *version, aggregate_condition),
        input => source(plan.scope, input),
    };
    query.select = if plan.outputs.is_empty() {
        vec![SelectExpr::star()]
    } else {
        projections(&plan.outputs)
    };
    query
}

fn latest_query(
    scope: ScopeId,
    input: &PhysicalSource,
    keys: &[ColumnRef],
    version: ColumnRef,
    condition: &[crate::passes::plan::requirements::Predicate],
) -> Query {
    let mut query = source(scope, input);
    query.where_clause = Expr::and_all(
        std::iter::once(query.where_clause.take())
            .chain(condition.iter().map(|value| Some(predicate(value)))),
    );
    let keys: Vec<_> = keys.iter().map(column).collect();
    query.select = vec![SelectExpr::star()];
    query.order_by = keys.iter().cloned().map(OrderExpr::asc).collect();
    query.order_by.push(OrderExpr::desc(Expr::Column(version)));
    query.limit_by = Some((1, keys));
    query
}

fn source(scope: ScopeId, plan: &PhysicalSource) -> Query {
    let from = match plan {
        PhysicalSource::Scan {
            relation,
            final_,
            relationship,
        } => TableRef::Scan {
            relation: *relation,
            final_: *final_,
            relationship: *relationship,
        },
        PhysicalSource::Union {
            relation,
            arms,
            relationship,
        } => TableRef::union_all(arms.iter().map(physical_query).collect(), *relation)
            .with_relationship(*relationship),
        PhysicalSource::Filter { predicates, input } => {
            let mut query = source(scope, input);
            query.where_clause = Expr::and_all(
                std::iter::once(query.where_clause.take())
                    .chain(predicates.iter().map(|value| Some(predicate(value)))),
            );
            return query;
        }
        PhysicalSource::KeyFilter { value, keys, input } => {
            let mut query = source(scope, input);
            let filter = Expr::InSelect {
                expr: Box::new(column(value)),
                query: Box::new(physical_query(keys)),
            };
            query.where_clause = Expr::and_all([query.where_clause.take(), Some(filter)]);
            return query;
        }
        PhysicalSource::Scope { relation, input } => {
            TableRef::subquery(physical_query(input), *relation)
        }
        PhysicalSource::Latest {
            relation,
            scope: body,
            input,
            sort_key,
            version,
            aggregate_condition,
        } => TableRef::subquery(
            latest_query(*body, input, sort_key, *version, aggregate_condition),
            *relation,
        ),
        PhysicalSource::Join {
            endpoints,
            predicates,
            left,
            right,
        } => {
            let left = source(scope, left);
            let right = source(scope, right);
            let condition = predicates.iter().map(predicate).fold(
                Expr::eq(column(&endpoints.0), column(&endpoints.1)),
                Expr::and,
            );
            let mut query = Query::new(
                scope,
                TableRef::join(
                    crate::ast::JoinType::Inner,
                    left.from,
                    right.from,
                    condition,
                ),
            );
            query.where_clause = Expr::and_all([left.where_clause, right.where_clause]);
            return query;
        }
    };
    Query::new(scope, from)
}
