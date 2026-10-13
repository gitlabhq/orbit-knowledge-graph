use super::{Expr, Select, TableRef};
use crate::error::Result;

pub fn visit_selects(
    select: &Select,
    callback: &mut impl FnMut(&Select) -> Result<()>,
) -> Result<()> {
    for cte in &select.ctes {
        visit_selects(&cte.select, callback)?;
    }
    visit_table_selects(&select.from, callback)?;
    for expression in select
        .select
        .iter()
        .map(|select| &select.expr)
        .chain(select.where_clause.iter())
        .chain(select.having.iter())
        .chain(&select.group_by)
        .chain(select.order_by.iter().map(|order| &order.expr))
        .chain(select.limit_by.iter().flat_map(|(_, keys)| keys))
    {
        visit_expr_selects(expression, callback)?;
    }
    for arm in &select.union_all {
        visit_selects(arm, callback)?;
    }
    callback(select)
}

fn visit_table_selects(
    table: &TableRef,
    callback: &mut impl FnMut(&Select) -> Result<()>,
) -> Result<()> {
    match table {
        TableRef::Scan { .. } => Ok(()),
        TableRef::Subquery { select, .. } => visit_selects(select, callback),
        TableRef::Union { selects, .. } => {
            for select in selects {
                visit_selects(select, callback)?;
            }
            Ok(())
        }
        TableRef::Join {
            left, right, on, ..
        } => {
            visit_table_selects(left, callback)?;
            visit_table_selects(right, callback)?;
            visit_expr_selects(on, callback)
        }
    }
}

fn visit_expr_selects(
    expression: &Expr,
    callback: &mut impl FnMut(&Select) -> Result<()>,
) -> Result<()> {
    visit_expressions(expression, &mut |expression| match expression {
        Expr::InSelect { select, .. } | Expr::Scalar(select) => visit_selects(select, callback),
        _ => Ok(()),
    })
}

pub fn visit_expressions<'a>(
    expression: &'a Expr,
    callback: &mut impl FnMut(&'a Expr) -> Result<()>,
) -> Result<()> {
    match expression {
        Expr::BinaryOp { left, right, .. }
        | Expr::TextSearch {
            value: left,
            pattern: right,
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
        TableRef::Scan { .. } | TableRef::Subquery { .. } | TableRef::Union { .. } => {
            callback(table)
        }
    }
}

pub fn visit_selects_mut(
    select: &mut Select,
    callback: &mut impl FnMut(&mut Select) -> Result<()>,
) -> Result<()> {
    for cte in &mut select.ctes {
        visit_selects_mut(&mut cte.select, callback)?;
    }
    visit_table_selects_mut(&mut select.from, callback)?;
    for expression in select
        .select
        .iter_mut()
        .map(|select| &mut select.expr)
        .chain(select.where_clause.iter_mut())
        .chain(select.having.iter_mut())
        .chain(&mut select.group_by)
        .chain(select.order_by.iter_mut().map(|order| &mut order.expr))
        .chain(select.limit_by.iter_mut().flat_map(|(_, keys)| keys))
    {
        visit_expr_selects_mut(expression, callback)?;
    }
    for arm in &mut select.union_all {
        visit_selects_mut(arm, callback)?;
    }
    callback(select)
}

fn visit_table_selects_mut(
    table: &mut TableRef,
    callback: &mut impl FnMut(&mut Select) -> Result<()>,
) -> Result<()> {
    match table {
        TableRef::Scan { .. } => Ok(()),
        TableRef::Subquery { select, .. } => visit_selects_mut(select, callback),
        TableRef::Union { selects, .. } => {
            for select in selects {
                visit_selects_mut(select, callback)?;
            }
            Ok(())
        }
        TableRef::Join {
            left, right, on, ..
        } => {
            visit_table_selects_mut(left, callback)?;
            visit_table_selects_mut(right, callback)?;
            visit_expr_selects_mut(on, callback)
        }
    }
}

fn visit_expr_selects_mut(
    expression: &mut Expr,
    callback: &mut impl FnMut(&mut Select) -> Result<()>,
) -> Result<()> {
    match expression {
        Expr::InSelect { expr, select } => {
            visit_expr_selects_mut(expr, callback)?;
            visit_selects_mut(select, callback)
        }
        Expr::Scalar(select) => visit_selects_mut(select, callback),
        Expr::BinaryOp { left, right, .. }
        | Expr::TextSearch {
            value: left,
            pattern: right,
            ..
        } => {
            visit_expr_selects_mut(left, callback)?;
            visit_expr_selects_mut(right, callback)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::TimeBucket { value: expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::Lambda { body: expr, .. } => visit_expr_selects_mut(expr, callback),
        Expr::FuncCall { args, .. } => {
            for argument in args {
                visit_expr_selects_mut(argument, callback)?;
            }
            Ok(())
        }
        Expr::Aggregate {
            argument,
            condition,
            ..
        } => {
            for child in argument.iter_mut().chain(condition.iter_mut()) {
                visit_expr_selects_mut(child, callback)?;
            }
            Ok(())
        }
        Expr::Column { .. }
        | Expr::EmptyTupleArray(_)
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => Ok(()),
    }
}
