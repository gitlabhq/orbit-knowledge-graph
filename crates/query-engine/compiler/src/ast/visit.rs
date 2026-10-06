use super::{Expr, Query, TableRef};
use crate::error::Result;

pub fn visit_queries(query: &Query, callback: &mut impl FnMut(&Query) -> Result<()>) -> Result<()> {
    for cte in &query.ctes {
        visit_queries(&cte.query, callback)?;
    }
    visit_table_queries_ref(&query.from, callback)?;
    for expression in query
        .select
        .iter()
        .map(|select| &select.expr)
        .chain(query.where_clause.iter())
        .chain(query.having.iter())
        .chain(&query.group_by)
        .chain(query.order_by.iter().map(|order| &order.expr))
        .chain(query.limit_by.iter().flat_map(|(_, keys)| keys))
    {
        visit_expr_queries_ref(expression, callback)?;
    }
    for arm in &query.union_all {
        visit_queries(arm, callback)?;
    }
    callback(query)
}

fn visit_table_queries_ref(
    table: &TableRef,
    callback: &mut impl FnMut(&Query) -> Result<()>,
) -> Result<()> {
    match table {
        TableRef::Scan { .. } | TableRef::Cte { .. } => Ok(()),
        TableRef::Subquery { query, .. } => visit_queries(query, callback),
        TableRef::Union { queries, .. } => {
            for query in queries {
                visit_queries(query, callback)?;
            }
            Ok(())
        }
        TableRef::Join {
            left, right, on, ..
        } => {
            visit_table_queries_ref(left, callback)?;
            visit_table_queries_ref(right, callback)?;
            visit_expr_queries_ref(on, callback)
        }
    }
}

fn visit_expr_queries_ref(
    expression: &Expr,
    callback: &mut impl FnMut(&Query) -> Result<()>,
) -> Result<()> {
    visit_expressions(expression, &mut |expression| match expression {
        Expr::InSelect { query, .. } | Expr::Scalar(query) => visit_queries(query, callback),
        _ => Ok(()),
    })
}

pub fn visit_expressions<'a>(
    expression: &'a Expr,
    callback: &mut impl FnMut(&'a Expr) -> Result<()>,
) -> Result<()> {
    match expression {
        Expr::BinaryOp { left, right, .. }
        | Expr::TokenSearch {
            value: left,
            query: right,
            ..
        } => {
            visit_expressions(left, callback)?;
            visit_expressions(right, callback)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::TimeBucket { value: expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::InSelect { expr, .. }
        | Expr::Lambda { body: expr, .. } => {
            visit_expressions(expr, callback)?;
        }
        Expr::FuncCall { args, .. } => {
            for argument in args {
                visit_expressions(argument, callback)?;
            }
        }
        Expr::Aggregate {
            argument,
            condition,
            ..
        } => {
            for child in argument.iter().chain(condition.iter()) {
                visit_expressions(child, callback)?;
            }
        }
        Expr::Column { .. }
        | Expr::Output(_)
        | Expr::EmptyTupleArray(_)
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Scalar(_)
        | Expr::Star => {}
    }
    callback(expression)
}

pub fn visit_relations<'a>(table: &'a TableRef, callback: &mut impl FnMut(&'a TableRef)) {
    match table {
        TableRef::Join { left, right, .. } => {
            visit_relations(left, callback);
            visit_relations(right, callback);
        }
        TableRef::Scan { .. }
        | TableRef::Cte { .. }
        | TableRef::Subquery { .. }
        | TableRef::Union { .. } => callback(table),
    }
}

pub fn visit_queries_mut(
    query: &mut Query,
    callback: &mut impl FnMut(&mut Query) -> Result<()>,
) -> Result<()> {
    for cte in &mut query.ctes {
        visit_queries_mut(&mut cte.query, callback)?;
    }
    visit_table_queries(&mut query.from, callback)?;
    for expression in query
        .select
        .iter_mut()
        .map(|select| &mut select.expr)
        .chain(query.where_clause.iter_mut())
        .chain(query.having.iter_mut())
        .chain(&mut query.group_by)
        .chain(query.order_by.iter_mut().map(|order| &mut order.expr))
        .chain(query.limit_by.iter_mut().flat_map(|(_, keys)| keys))
    {
        visit_expr_queries(expression, callback)?;
    }
    for arm in &mut query.union_all {
        visit_queries_mut(arm, callback)?;
    }
    callback(query)
}

fn visit_table_queries(
    table: &mut TableRef,
    callback: &mut impl FnMut(&mut Query) -> Result<()>,
) -> Result<()> {
    match table {
        TableRef::Scan { .. } | TableRef::Cte { .. } => Ok(()),
        TableRef::Subquery { query, .. } => visit_queries_mut(query, callback),
        TableRef::Union { queries, .. } => {
            for query in queries {
                visit_queries_mut(query, callback)?;
            }
            Ok(())
        }
        TableRef::Join {
            left, right, on, ..
        } => {
            visit_table_queries(left, callback)?;
            visit_table_queries(right, callback)?;
            visit_expr_queries(on, callback)
        }
    }
}

fn visit_expr_queries(
    expression: &mut Expr,
    callback: &mut impl FnMut(&mut Query) -> Result<()>,
) -> Result<()> {
    match expression {
        Expr::InSelect { expr, query } => {
            visit_expr_queries(expr, callback)?;
            visit_queries_mut(query, callback)
        }
        Expr::Scalar(query) => visit_queries_mut(query, callback),
        Expr::BinaryOp { left, right, .. }
        | Expr::TokenSearch {
            value: left,
            query: right,
            ..
        } => {
            visit_expr_queries(left, callback)?;
            visit_expr_queries(right, callback)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::TimeBucket { value: expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::Lambda { body: expr, .. } => visit_expr_queries(expr, callback),
        Expr::FuncCall { args, .. } => {
            for argument in args {
                visit_expr_queries(argument, callback)?;
            }
            Ok(())
        }
        Expr::Aggregate {
            argument,
            condition,
            ..
        } => {
            for child in argument.iter_mut().chain(condition.iter_mut()) {
                visit_expr_queries(child, callback)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::Output(_)
        | Expr::EmptyTupleArray(_)
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => Ok(()),
    }
}
