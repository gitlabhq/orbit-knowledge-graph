use std::collections::HashSet;

use super::{Expr, Node, Query, TableRef};
use crate::bindings::Definition;
use crate::error::{QueryError, Result};

pub fn validate_definitions(node: &Node) -> Result<()> {
    match node {
        Node::Query(query) => validate_query(query, &HashSet::new()),
        Node::Insert(_) => Ok(()),
    }
}

fn validate_query(query: &Query, inherited: &HashSet<Definition>) -> Result<()> {
    let mut visible = inherited.clone();
    for cte in &query.ctes {
        if visible.contains(&cte.name) {
            return Err(QueryError::Codegen(
                "CTE handle declared more than once in its scope".into(),
            ));
        }
        if cte.recursive {
            visible.insert(cte.name.clone());
        }
        validate_query(&cte.query, &visible)?;
        visible.insert(cte.name.clone());
    }
    validate_table(&query.from, &visible)?;
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
        validate_expression(expression, &visible)?;
    }
    for arm in &query.union_all {
        validate_query(arm, &visible)?;
    }
    Ok(())
}

fn require_visible(definition: &Definition, visible: &HashSet<Definition>) -> Result<()> {
    if visible.contains(definition) {
        return Ok(());
    }
    Err(QueryError::Codegen(format!(
        "CTE '{}' is outside its definition scope",
        definition.hint()
    )))
}

fn validate_table(table: &TableRef, visible: &HashSet<Definition>) -> Result<()> {
    match table {
        TableRef::Scan { .. } => Ok(()),
        TableRef::Cte { definition, .. } => require_visible(definition, visible),
        TableRef::Subquery { query, .. } => validate_query(query, visible),
        TableRef::Union { queries, .. } => {
            for query in queries {
                validate_query(query, visible)?;
            }
            Ok(())
        }
        TableRef::Join {
            left, right, on, ..
        } => {
            validate_table(left, visible)?;
            validate_table(right, visible)?;
            validate_expression(on, visible)
        }
    }
}

fn validate_expression(expression: &Expr, visible: &HashSet<Definition>) -> Result<()> {
    super::visit::visit_expressions(expression, &mut |expression| match expression {
        Expr::InSubquery { cte_name, .. } => require_visible(cte_name, visible),
        Expr::InSelect { query, .. } | Expr::Scalar(query) => validate_query(query, visible),
        _ => Ok(()),
    })
}
