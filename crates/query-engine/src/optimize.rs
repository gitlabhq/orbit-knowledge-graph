//! AST optimization pass.
//!
//! Runs after lowering, enforce, and security — right before `check_ast`.
//! Rewrites the AST for better ClickHouse performance without changing
//! query semantics.
//!
//! ## `fold_filters_into_aggregates`
//!
//! Converts WHERE-filtered aggregations into ClickHouse `-If` combinators:
//!
//! ```sql
//! -- before
//! SELECT p.name, COUNT(mr.id) FROM ... WHERE mr.state = 'merged' GROUP BY p.name
//!
//! -- after
//! SELECT p.name, countIf(mr.id, mr.state = 'merged') FROM ... GROUP BY p.name
//! ```
//!
//! This avoids materializing per-filter hash tables in the aggregation engine.
//! Each `-If` aggregate maintains one counter per group regardless of data volume.
//! See: <https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if>

use crate::ast::{Expr, Node, Op, Query};

/// Apply all optimization passes to the AST.
pub fn optimize(node: &mut Node) {
    match node {
        Node::Query(q) => optimize_query(q),
    }
}

fn optimize_query(q: &mut Query) {
    let has_aggregates = q
        .select
        .iter()
        .any(|sel| matches!(&sel.expr, Expr::FuncCall { name, .. } if is_aggregate(name)));
    if !has_aggregates {
        return;
    }
    fold_filters_into_aggregates(q);
}

/// Rewrite `AGG(arg) ... WHERE <target_conds> AND <other_conds>`
/// into `AGGIf(arg, <target_conds>) ... WHERE <other_conds>`.
///
/// A WHERE conjunct is "foldable" into an aggregate if it references
/// only columns from the aggregate's target table (i.e. the table alias
/// of the aggregate's first argument). Structural predicates (JOINs,
/// security filters, group-by node filters) stay in WHERE.
fn fold_filters_into_aggregates(q: &mut Query) {
    let where_clause = match q.where_clause.take() {
        Some(w) => w,
        None => return,
    };

    let conjuncts = flatten_and(where_clause);

    // Collect (target_alias, agg_function_name) for each aggregate in SELECT.
    let agg_targets: Vec<Option<String>> = q
        .select
        .iter()
        .map(|sel| extract_agg_target_alias(&sel.expr))
        .collect();

    // Partition conjuncts: for each aggregate, extract conjuncts that
    // reference only its target alias. A conjunct is shared across all
    // aggregates that target the same alias.
    //
    // A conjunct is "kept" in WHERE if:
    //   - it references columns from multiple aliases, or
    //   - it references an alias that isn't an aggregation target, or
    //   - it's a security filter (startsWith on traversal_path)
    let mut folded_by_alias: std::collections::HashMap<String, Vec<Expr>> =
        std::collections::HashMap::new();
    let mut remaining: Vec<Expr> = Vec::new();

    // Collect all group_by aliases to avoid folding their filters.
    let group_aliases: std::collections::HashSet<String> = q
        .group_by
        .iter()
        .filter_map(|e| match e {
            Expr::Column { table, .. } => Some(table.clone()),
            _ => None,
        })
        .collect();

    let target_aliases: std::collections::HashSet<String> =
        agg_targets.iter().filter_map(|a| a.clone()).collect();

    for conjunct in conjuncts {
        let aliases = collect_column_aliases(&conjunct);

        // Keep in WHERE if:
        // - references no columns (constant expression)
        // - references multiple aliases (cross-table predicate)
        // - references a group_by alias (group node filter must stay)
        // - references an alias that isn't an aggregation target
        // - is a security filter (startsWith on traversal_path)
        let should_keep = aliases.is_empty()
            || aliases.len() > 1
            || aliases.iter().any(|a| group_aliases.contains(a))
            || aliases.iter().any(|a| !target_aliases.contains(a))
            || is_security_filter(&conjunct);

        if should_keep {
            remaining.push(conjunct);
        } else {
            let alias = aliases.into_iter().next().unwrap();
            folded_by_alias.entry(alias).or_default().push(conjunct);
        }
    }

    // Nothing to fold — restore original WHERE.
    if folded_by_alias.is_empty() {
        q.where_clause = rebuild_and(remaining);
        return;
    }

    // Rewrite each aggregate in SELECT: AGG(arg) → AGGIf(arg, folded_conds).
    for (i, sel) in q.select.iter_mut().enumerate() {
        let target_alias = match agg_targets.get(i) {
            Some(Some(alias)) => alias,
            _ => continue,
        };
        let conds = match folded_by_alias.get(target_alias) {
            Some(c) if !c.is_empty() => c,
            _ => continue,
        };

        sel.expr = rewrite_agg_to_if(&sel.expr, conds);
    }

    // Also rewrite ORDER BY expressions that reference the same aggregates.
    for ord in &mut q.order_by {
        if let Some(alias) = extract_agg_target_alias(&ord.expr) {
            if let Some(conds) = folded_by_alias.get(&alias) {
                if !conds.is_empty() {
                    ord.expr = rewrite_agg_to_if(&ord.expr, conds);
                }
            }
        }
    }

    q.where_clause = rebuild_and(remaining);
}

/// Rewrite `AGG(arg)` to `AGGIf(arg, cond1 AND cond2 AND ...)`.
fn rewrite_agg_to_if(expr: &Expr, conditions: &[Expr]) -> Expr {
    match expr {
        Expr::FuncCall { name, args } => {
            let if_name = match agg_if_name(name) {
                Some(n) => n,
                None => return expr.clone(),
            };
            let condition = conditions
                .iter()
                .cloned()
                .reduce(|a, b| Expr::and(a, b))
                .unwrap();

            let mut new_args = args.clone();
            new_args.push(condition);
            Expr::FuncCall {
                name: if_name.to_string(),
                args: new_args,
            }
        }
        _ => expr.clone(),
    }
}

/// Map standard aggregate function names to their `-If` combinator.
fn agg_if_name(name: &str) -> Option<&'static str> {
    match name {
        "COUNT" => Some("countIf"),
        "SUM" => Some("sumIf"),
        "AVG" => Some("avgIf"),
        "MIN" => Some("minIf"),
        "MAX" => Some("maxIf"),
        _ => None,
    }
}

/// Flatten nested AND expressions into a flat list of conjuncts.
fn flatten_and(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => {
            let mut out = flatten_and(*left);
            out.extend(flatten_and(*right));
            out
        }
        other => vec![other],
    }
}

/// Rebuild an AND chain from conjuncts. Returns None if empty.
fn rebuild_and(mut conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }
    let first = conjuncts.remove(0);
    Some(conjuncts.into_iter().fold(first, Expr::and))
}

/// Collect all unique table aliases referenced by column expressions.
fn collect_column_aliases(expr: &Expr) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    collect_aliases_inner(expr, &mut aliases);
    aliases
}

fn collect_aliases_inner(expr: &Expr, aliases: &mut std::collections::HashSet<String>) {
    match expr {
        Expr::Column { table, .. } => {
            aliases.insert(table.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_aliases_inner(left, aliases);
            collect_aliases_inner(right, aliases);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_aliases_inner(inner, aliases);
        }
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_aliases_inner(arg, aliases);
            }
        }
        Expr::Literal(_) | Expr::Param { .. } => {}
    }
}

/// Check if an expression is a security filter (startsWith on traversal_path).
fn is_security_filter(expr: &Expr) -> bool {
    match expr {
        Expr::FuncCall { name, args } if name == "startsWith" => args
            .iter()
            .any(|a| matches!(a, Expr::Column { column, .. } if column == "traversal_path")),
        _ => false,
    }
}

/// Extract the table alias from the first argument of an aggregate FuncCall.
/// Returns None if the expression isn't an aggregate or has no column arg.
fn extract_agg_target_alias(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FuncCall { name, args } if is_aggregate(name) => {
            args.first().and_then(|arg| match arg {
                Expr::Column { table, .. } => Some(table.clone()),
                _ => None,
            })
        }
        _ => None,
    }
}

fn is_aggregate(name: &str) -> bool {
    matches!(name, "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "groupArray")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{OrderExpr, SelectExpr, TableRef};

    fn count_expr(table: &str, col: &str) -> Expr {
        Expr::func("COUNT", vec![Expr::col(table, col)])
    }

    fn sum_expr(table: &str, col: &str) -> Expr {
        Expr::func("SUM", vec![Expr::col(table, col)])
    }

    fn eq_filter(table: &str, col: &str, val: &str) -> Expr {
        Expr::eq(
            Expr::col(table, col),
            Expr::Param {
                data_type: crate::ast::ChType::String,
                value: serde_json::Value::String(val.to_string()),
            },
        )
    }

    fn security_filter(table: &str, path: &str) -> Expr {
        Expr::func(
            "startsWith",
            vec![
                Expr::col(table, "traversal_path"),
                Expr::Literal(serde_json::Value::String(path.to_string())),
            ],
        )
    }

    #[test]
    fn folds_target_filter_into_count_if() {
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                security_filter("mr", "1/2/"),
                eq_filter("mr", "state", "merged"),
            )),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        optimize_query(&mut q);

        // The target filter should be folded into countIf.
        let agg = &q.select[1].expr;
        match agg {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2); // arg + condition
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }

        // Security filter stays in WHERE.
        assert!(q.where_clause.is_some());
        assert!(is_security_filter(q.where_clause.as_ref().unwrap()));
    }

    #[test]
    fn keeps_group_by_node_filters_in_where() {
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                eq_filter("p", "name", "my-project"),
                eq_filter("mr", "state", "merged"),
            )),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        optimize_query(&mut q);

        // mr.state should be folded, p.name should stay in WHERE.
        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "countIf"),
            other => panic!("expected countIf, got {other:?}"),
        }

        // WHERE should still have the p.name filter.
        let where_aliases = collect_column_aliases(q.where_clause.as_ref().unwrap());
        assert!(where_aliases.contains("p"));
    }

    #[test]
    fn no_group_by_still_folds() {
        let mut q = Query {
            select: vec![SelectExpr::new(count_expr("mr", "id"), "total")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        };

        optimize_query(&mut q);

        // Aggregate without GROUP BY still folds target filters.
        match &q.select[0].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected countIf, got {other:?}"),
        }
        assert!(q.where_clause.is_none());
    }

    #[test]
    fn non_aggregate_query_skips_optimization() {
        let mut q = Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "mr_id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        };

        let original_where = q.where_clause.clone();
        optimize_query(&mut q);

        // No aggregate functions in SELECT → no rewrite.
        assert_eq!(q.where_clause, original_where);
    }

    #[test]
    fn folds_multiple_conditions() {
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                eq_filter("mr", "state", "merged"),
                eq_filter("mr", "draft", "false"),
            )),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        optimize_query(&mut q);

        // Both mr conditions folded into a single countIf with AND.
        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
                // Second arg should be AND of both conditions.
                match &args[1] {
                    Expr::BinaryOp { op: Op::And, .. } => {}
                    other => panic!("expected AND condition, got {other:?}"),
                }
            }
            other => panic!("expected countIf, got {other:?}"),
        }

        // WHERE should be empty (no remaining conditions).
        assert!(q.where_clause.is_none());
    }

    #[test]
    fn rewrites_order_by_to_match() {
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            order_by: vec![OrderExpr {
                expr: count_expr("mr", "id"),
                desc: true,
            }],
            ..Default::default()
        };

        optimize_query(&mut q);

        // ORDER BY should also use countIf.
        match &q.order_by[0].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "countIf"),
            other => panic!("expected countIf in ORDER BY, got {other:?}"),
        }
    }

    #[test]
    fn folds_sum_if() {
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(sum_expr("mr", "additions"), "total_additions"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        optimize_query(&mut q);

        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "sumIf");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected sumIf, got {other:?}"),
        }
    }

    #[test]
    fn no_foldable_conditions_is_noop() {
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(security_filter("mr", "1/2/")),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        optimize_query(&mut q);

        // Security filter is not foldable, COUNT stays as-is.
        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "COUNT"),
            other => panic!("expected COUNT, got {other:?}"),
        }
    }
}
