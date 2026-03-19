//! AST optimization pass.
//!
//! Runs after lowering — before enforce, security, and check.
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

use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, Node, Op, Query};
use crate::input::{Input, QueryType};

/// Apply all optimization passes to the AST.
pub fn optimize(node: &mut Node, input: &Input) {
    if input.query_type != QueryType::Aggregation {
        return;
    }
    match node {
        Node::Query(q) => fold_filters_into_aggregates(q, input),
    }
}

/// Rewrite `AGG(arg) ... WHERE <target_conds> AND <other_conds>`
/// into `AGGIf(arg, <target_conds>) ... WHERE <other_conds>`.
///
/// A WHERE conjunct is "foldable" into an aggregate if it references
/// only columns from the aggregate's target table (i.e. the table alias
/// of the aggregate's first argument). Structural predicates (JOINs,
/// group-by node filters) stay in WHERE.
fn fold_filters_into_aggregates(q: &mut Query, input: &Input) {
    let where_clause = match q.where_clause.take() {
        Some(w) => w,
        None => return,
    };

    let conjuncts = flatten_and(where_clause);

    // Build target alias set from Input aggregations (node ID = table alias after lowering).
    let target_aliases: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.target.as_deref())
        .collect();

    // Build group-by alias set to avoid folding their filters.
    let group_aliases: HashSet<&str> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.as_deref())
        .collect();

    // Build mapping from SQL function name -> -If name using AggFunction.
    let if_names: HashMap<&str, &str> = input
        .aggregations
        .iter()
        .filter_map(|agg| {
            agg.function
                .as_sql_if()
                .map(|if_n| (agg.function.as_sql(), if_n))
        })
        .collect();

    let mut folded_by_alias: HashMap<String, Vec<Expr>> = HashMap::new();
    let mut remaining: Vec<Expr> = Vec::new();

    for conjunct in conjuncts {
        let aliases = collect_column_aliases(&conjunct);

        // Keep in WHERE if:
        //   - references no columns (constant expression)
        //   - references multiple aliases (cross-table predicate)
        //   - references a group_by alias (group node filter must stay)
        //   - references an alias that isn't an aggregation target
        let should_keep = aliases.is_empty()
            || aliases.len() > 1
            || aliases.iter().any(|a| group_aliases.contains(a.as_str()))
            || aliases.iter().any(|a| !target_aliases.contains(a.as_str()));

        if should_keep {
            remaining.push(conjunct);
        } else if let Some(alias) = aliases.into_iter().next() {
            folded_by_alias.entry(alias).or_default().push(conjunct);
        }
    }

    if folded_by_alias.is_empty() {
        q.where_clause = rebuild_and(remaining);
        return;
    }

    // Rewrite each aggregate in SELECT: AGG(arg) → AGGIf(arg, folded_conds).
    for sel in &mut q.select {
        if let Some((_alias, conds)) = extract_and_match(&sel.expr, &folded_by_alias) {
            sel.expr = rewrite_agg_to_if(&sel.expr, &if_names, conds);
        }
    }

    // Also rewrite ORDER BY expressions that reference the same aggregates.
    for ord in &mut q.order_by {
        if let Some((_alias, conds)) = extract_and_match(&ord.expr, &folded_by_alias) {
            ord.expr = rewrite_agg_to_if(&ord.expr, &if_names, conds);
        }
    }

    q.where_clause = rebuild_and(remaining);
}

/// Check if an expression is an aggregate targeting an alias with folded conditions.
fn extract_and_match<'a>(
    expr: &Expr,
    folded: &'a HashMap<String, Vec<Expr>>,
) -> Option<(String, &'a [Expr])> {
    let alias = extract_agg_target_alias(expr)?;
    let conds = folded.get(&alias)?;
    if conds.is_empty() {
        return None;
    }
    Some((alias, conds))
}

/// Rewrite `AGG(arg)` to `AGGIf(arg, cond1 AND cond2 AND ...)`.
fn rewrite_agg_to_if(expr: &Expr, if_names: &HashMap<&str, &str>, conditions: &[Expr]) -> Expr {
    match expr {
        Expr::FuncCall { name, args } => {
            let if_name = match if_names.get(name.as_str()) {
                Some(n) => *n,
                None => return expr.clone(),
            };
            let condition = match conditions.iter().cloned().reduce(Expr::and) {
                Some(c) => c,
                None => return expr.clone(),
            };

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

/// Extract the table alias from the first argument of a FuncCall.
fn extract_agg_target_alias(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FuncCall { args, .. } => args.first().and_then(|arg| match arg {
            Expr::Column { table, .. } => Some(table.clone()),
            _ => None,
        }),
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
fn collect_column_aliases(expr: &Expr) -> HashSet<String> {
    let mut aliases = HashSet::new();
    collect_aliases_inner(expr, &mut aliases);
    aliases
}

fn collect_aliases_inner(expr: &Expr, aliases: &mut HashSet<String>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{OrderExpr, SelectExpr, TableRef};
    use crate::input::{AggFunction, InputAggregation};

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

    fn agg_input(aggs: Vec<InputAggregation>) -> Input {
        Input {
            query_type: QueryType::Aggregation,
            aggregations: aggs,
            ..Default::default()
        }
    }

    fn count_agg(target: &str, group_by: Option<&str>) -> InputAggregation {
        InputAggregation {
            function: AggFunction::Count,
            target: Some(target.to_string()),
            group_by: group_by.map(str::to_string),
            property: None,
            alias: Some("count".to_string()),
        }
    }

    fn sum_agg(target: &str, property: &str, group_by: Option<&str>) -> InputAggregation {
        InputAggregation {
            function: AggFunction::Sum,
            target: Some(target.to_string()),
            group_by: group_by.map(str::to_string),
            property: Some(property.to_string()),
            alias: Some("total".to_string()),
        }
    }

    #[test]
    fn folds_target_filter_into_count_if() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
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

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }

        // Group-by node filter stays in WHERE.
        let where_aliases = collect_column_aliases(q.where_clause.as_ref().unwrap());
        assert!(where_aliases.contains("p"));
        assert!(!where_aliases.contains("mr"));
    }

    #[test]
    fn keeps_group_by_node_filters_in_where() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
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

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "countIf"),
            other => panic!("expected countIf, got {other:?}"),
        }

        let where_aliases = collect_column_aliases(q.where_clause.as_ref().unwrap());
        assert!(where_aliases.contains("p"));
    }

    #[test]
    fn no_group_by_still_folds() {
        let input = agg_input(vec![count_agg("mr", None)]);
        let mut q = Query {
            select: vec![SelectExpr::new(count_expr("mr", "id"), "total")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

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
        let input = Input {
            query_type: QueryType::Traversal,
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "mr_id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(eq_filter("mr", "state", "merged")),
            ..Default::default()
        }));

        let original = match &node {
            Node::Query(q) => q.where_clause.clone(),
        };
        optimize(&mut node, &input);

        match &node {
            Node::Query(q) => assert_eq!(q.where_clause, original),
        }
    }

    #[test]
    fn folds_multiple_conditions() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
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

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, args } => {
                assert_eq!(name, "countIf");
                assert_eq!(args.len(), 2);
                match &args[1] {
                    Expr::BinaryOp { op: Op::And, .. } => {}
                    other => panic!("expected AND condition, got {other:?}"),
                }
            }
            other => panic!("expected countIf, got {other:?}"),
        }

        assert!(q.where_clause.is_none());
    }

    #[test]
    fn rewrites_order_by_to_match() {
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
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

        fold_filters_into_aggregates(&mut q, &input);

        match &q.order_by[0].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "countIf"),
            other => panic!("expected countIf in ORDER BY, got {other:?}"),
        }
    }

    #[test]
    fn folds_sum_if() {
        let input = agg_input(vec![sum_agg("mr", "additions", Some("p"))]);
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

        fold_filters_into_aggregates(&mut q, &input);

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
        let input = agg_input(vec![count_agg("mr", Some("p"))]);
        let cross_table = Expr::eq(Expr::col("mr", "author_id"), Expr::col("p", "creator_id"));
        let mut q = Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(count_expr("mr", "id"), "mr_count"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(cross_table),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        };

        fold_filters_into_aggregates(&mut q, &input);

        match &q.select[1].expr {
            Expr::FuncCall { name, .. } => assert_eq!(name, "COUNT"),
            other => panic!("expected COUNT, got {other:?}"),
        }
        assert!(q.where_clause.is_some());
    }
}
