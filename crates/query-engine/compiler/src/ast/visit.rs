use super::{Expr, Query, TableRef};
use crate::error::Result;

pub fn visit_queries(query: &Query, callback: &mut impl FnMut(&Query) -> Result<()>) -> Result<()> {
    for cte in &query.ctes {
        visit_queries(&cte.query, callback)?;
    }
    visit_table_queries_ref(&query.from, callback)?;
    for select in &query.select {
        visit_expr_queries_ref(&select.expr, callback)?;
    }
    for expression in query.where_clause.iter().chain(query.having.iter()) {
        visit_expr_queries_ref(expression, callback)?;
    }
    for expression in &query.group_by {
        visit_expr_queries_ref(expression, callback)?;
    }
    for order in &query.order_by {
        visit_expr_queries_ref(&order.expr, callback)?;
    }
    if let Some((_, keys)) = &query.limit_by {
        for key in keys {
            visit_expr_queries_ref(key, callback)?;
        }
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
        TableRef::Scan { .. } => Ok(()),
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
    match expression {
        Expr::InSelect { expr, query } => {
            visit_expr_queries_ref(expr, callback)?;
            visit_queries(query, callback)
        }
        Expr::Scalar(query) => visit_queries(query, callback),
        Expr::BinaryOp { left, right, .. } => {
            visit_expr_queries_ref(left, callback)?;
            visit_expr_queries_ref(right, callback)
        }
        Expr::UnaryOp { expr, .. } | Expr::InSubquery { expr, .. } => {
            visit_expr_queries_ref(expr, callback)
        }
        Expr::Lambda { body, .. } => visit_expr_queries_ref(body, callback),
        Expr::FuncCall { args, .. } => {
            for argument in args {
                visit_expr_queries_ref(argument, callback)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => Ok(()),
    }
}

pub fn visit_relations<'a>(table: &'a TableRef, callback: &mut impl FnMut(&'a TableRef)) {
    match table {
        TableRef::Join { left, right, .. } => {
            visit_relations(left, callback);
            visit_relations(right, callback);
        }
        TableRef::Scan { .. } | TableRef::Subquery { .. } | TableRef::Union { .. } => {
            callback(table)
        }
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
    for select in &mut query.select {
        visit_expr_queries(&mut select.expr, callback)?;
    }
    for expression in query.where_clause.iter_mut().chain(query.having.iter_mut()) {
        visit_expr_queries(expression, callback)?;
    }
    for expression in &mut query.group_by {
        visit_expr_queries(expression, callback)?;
    }
    for order in &mut query.order_by {
        visit_expr_queries(&mut order.expr, callback)?;
    }
    if let Some((_, keys)) = &mut query.limit_by {
        for key in keys {
            visit_expr_queries(key, callback)?;
        }
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
        TableRef::Scan { .. } => Ok(()),
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
        Expr::BinaryOp { left, right, .. } => {
            visit_expr_queries(left, callback)?;
            visit_expr_queries(right, callback)
        }
        Expr::UnaryOp { expr, .. } | Expr::InSubquery { expr, .. } => {
            visit_expr_queries(expr, callback)
        }
        Expr::Lambda { body, .. } => visit_expr_queries(body, callback),
        Expr::FuncCall { args, .. } => {
            for argument in args {
                visit_expr_queries(argument, callback)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => Ok(()),
    }
}
