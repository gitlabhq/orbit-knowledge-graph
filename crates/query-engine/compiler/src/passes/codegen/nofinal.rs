//! Proof of concept: replace `FINAL` scans with find-then-check latest-row reads.
//! Off unless `ORBIT_NOFINAL_KEYS` names a JSON file of `{table: [sorting key columns]}`.

use std::collections::HashMap;

use crate::ast::visit::{visit_expressions, visit_queries_mut};
use crate::ast::{Expr, Function, JoinType, Op, OrderExpr, Query, SelectExpr, TableRef};

type Keys = HashMap<String, Vec<String>>;
type ColumnRef = (String, String);

const TRIVIAL: [&str; 3] = ["traversal_path", "_deleted", "project_id"];

/// Sort-key columns whose filters still match a large share of rows.
const BROAD_KEYS: [&str; 6] = [
    "traversal_path",
    "relationship_kind",
    "source_kind",
    "target_kind",
    "project_id",
    "branch",
];

pub fn rewrite(query: &mut Query) {
    let Ok(path) = std::env::var("ORBIT_NOFINAL_KEYS") else {
        return;
    };
    let keys: Keys = serde_json::from_str(&std::fs::read_to_string(path).expect("keys file"))
        .expect("keys json");
    for _ in 0..3 {
        let _ = visit_queries_mut(query, &mut |q| {
            push_outer_filters(q);
            Ok(())
        });
    }
    for _ in 0..3 {
        let _ = visit_queries_mut(query, &mut |q| {
            push_join_keys(q, &keys);
            Ok(())
        });
    }
    let _ = visit_queries_mut(query, &mut |q| {
        rewrite_query(q, &keys);
        Ok(())
    });
}

/// ClickHouse pushes outer filters into FINAL scans, but not through `LIMIT 1 BY`,
/// so push them into plain `SELECT` subqueries and `UNION ALL` arms first.
fn push_outer_filters(q: &mut Query) {
    let parent: Vec<Expr> = q.where_clause.clone().map(flatten_and).unwrap_or_default();
    if !parent.is_empty() {
        push_into(&mut q.from, &parent);
    }
}

fn push_into(table: &mut TableRef, parent: &[Expr]) {
    match table {
        TableRef::Join {
            join_type: JoinType::Inner,
            left,
            right,
            ..
        } => {
            push_into(left, parent);
            push_into(right, parent);
        }
        TableRef::Subquery { query, alias } => {
            let alias = alias.clone();
            push_into_query(query, &alias, parent);
        }
        TableRef::Union { queries, alias } => {
            let alias = alias.clone();
            for arm in queries {
                push_into_query(arm, &alias, parent);
            }
        }
        _ => {}
    }
}

fn push_into_query(query: &mut Query, alias: &str, parent: &[Expr]) {
    if !query.group_by.is_empty()
        || query.limit_by.is_some()
        || query.limit.is_some()
        || query.distinct
        || !query.union_all.is_empty()
    {
        return;
    }
    let existing: Vec<Expr> = query
        .where_clause
        .clone()
        .map(flatten_and)
        .unwrap_or_default();
    let mut added = Vec::new();
    for conjunct in own(parent, alias) {
        let Some(mapped) = map_columns(&conjunct, alias, query) else {
            continue;
        };
        if !existing.contains(&mapped) && !added.contains(&mapped) {
            added.push(mapped);
        }
    }
    if !added.is_empty() {
        query.where_clause =
            Expr::conjoin(query.where_clause.take().into_iter().chain(added).collect());
    }
}

fn map_columns(expr: &Expr, alias: &str, query: &Query) -> Option<Expr> {
    let star = query.select.iter().any(|s| matches!(s.expr, Expr::Star));
    let inner = match &query.from {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => alias.clone(),
        _ => return None,
    };
    let lookup = |column: &str| -> Option<Expr> {
        let hits: Vec<&Expr> = query
            .select
            .iter()
            .filter(|s| s.alias.as_deref() == Some(column))
            .map(|s| &s.expr)
            .collect();
        match hits.as_slice() {
            [expr @ Expr::Column { .. }] => Some((*expr).clone()),
            [] if star => Some(Expr::col(&inner, column)),
            _ => None,
        }
    };
    map_expr(expr, alias, &lookup)
}

fn map_expr(expr: &Expr, alias: &str, lookup: &dyn Fn(&str) -> Option<Expr>) -> Option<Expr> {
    let map = |e: &Expr| map_expr(e, alias, lookup).map(Box::new);
    Some(match expr {
        Expr::Column { table, column } if table == alias => lookup(column)?,
        Expr::Column { .. } => return None,
        Expr::FuncCall { name, args } => Expr::FuncCall {
            name: *name,
            args: args
                .iter()
                .map(|a| map_expr(a, alias, lookup))
                .collect::<Option<_>>()?,
        },
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op: *op,
            left: map(left)?,
            right: map(right)?,
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: map(expr)?,
        },
        Expr::InSelect { expr, query } => Expr::InSelect {
            expr: map(expr)?,
            query: query.clone(),
        },
        Expr::Literal(_) | Expr::Param { .. } | Expr::Scalar(_) => expr.clone(),
        _ => return None,
    })
}

struct Leaf {
    alias: String,
    table: String,
    conjuncts: Vec<Expr>,
    wrapped: bool,
}

fn push_join_keys(q: &mut Query, keys: &Keys) {
    let parent: Vec<Expr> = q.where_clause.clone().map(flatten_and).unwrap_or_default();
    let mut leaves = Vec::new();
    let mut pairs = Vec::new();
    collect(&q.from, &parent, keys, &mut leaves, &mut pairs);
    for ((ta, tc), (sa, sc)) in pairs.iter().flat_map(|(l, r)| [(l, r), (r, l)]) {
        let (Some(target), Some(source)) = (
            leaves.iter().find(|l| &l.alias == ta),
            leaves.iter().find(|l| &l.alias == sa),
        ) else {
            continue;
        };
        if selective(target) || !selective(source) || !keys[&target.table].contains(tc) {
            continue;
        }
        let sip = Expr::InSelect {
            expr: Box::new(Expr::col(ta, tc)),
            query: Box::new(Query {
                select: vec![SelectExpr::col(sa, sc)],
                from: scan(&source.table, sa),
                where_clause: Expr::conjoin(source.conjuncts.clone()),
                ..Default::default()
            }),
        };
        if target.wrapped {
            if let Some(sub) = find_subquery(&mut q.from, ta) {
                sub.where_clause =
                    Expr::conjoin(sub.where_clause.take().into_iter().chain([sip]).collect());
            }
        } else {
            q.where_clause =
                Expr::conjoin(q.where_clause.take().into_iter().chain([sip]).collect());
        }
        return;
    }
}

fn collect(
    table: &TableRef,
    parent: &[Expr],
    keys: &Keys,
    leaves: &mut Vec<Leaf>,
    pairs: &mut Vec<(ColumnRef, ColumnRef)>,
) {
    match table {
        TableRef::Join {
            join_type,
            left,
            right,
            on,
        } => {
            collect(left, parent, keys, leaves, pairs);
            collect(right, parent, keys, leaves, pairs);
            if *join_type == JoinType::Inner {
                pairs.extend(flatten_and(on.clone()).iter().filter_map(column_eq));
            }
        }
        TableRef::Scan {
            table,
            alias,
            final_: true,
            ..
        } if keys.contains_key(table) => leaves.push(Leaf {
            alias: alias.clone(),
            table: table.clone(),
            conjuncts: own(parent, alias),
            wrapped: false,
        }),
        TableRef::Subquery { query, alias } => {
            if let TableRef::Scan {
                table,
                alias: inner,
                final_: true,
                ..
            } = &query.from
                && inner == alias
                && keys.contains_key(table)
                && query.group_by.is_empty()
                && query.limit_by.is_none()
                && query.limit.is_none()
            {
                let mut conjuncts = own(parent, alias);
                conjuncts.extend(
                    query
                        .where_clause
                        .clone()
                        .map(flatten_and)
                        .unwrap_or_default(),
                );
                leaves.push(Leaf {
                    alias: alias.clone(),
                    table: table.clone(),
                    conjuncts,
                    wrapped: true,
                });
            }
        }
        _ => {}
    }
}

fn find_subquery<'a>(table: &'a mut TableRef, alias: &str) -> Option<&'a mut Query> {
    match table {
        TableRef::Join { left, right, .. } => {
            find_subquery(left, alias).or_else(|| find_subquery(right, alias))
        }
        TableRef::Subquery { query, alias: a } if a == alias => Some(query),
        _ => None,
    }
}

fn column_eq(expr: &Expr) -> Option<(ColumnRef, ColumnRef)> {
    if let Expr::BinaryOp {
        op: Op::Eq,
        left,
        right,
    } = expr
        && let (
            Expr::Column {
                table: lt,
                column: lc,
            },
            Expr::Column {
                table: rt,
                column: rc,
            },
        ) = (left.as_ref(), right.as_ref())
        && lt != rt
    {
        return Some(((lt.clone(), lc.clone()), (rt.clone(), rc.clone())));
    }
    None
}

fn selective(leaf: &Leaf) -> bool {
    leaf.conjuncts
        .iter()
        .any(|c| !columns_within(c, &leaf.alias, &TRIVIAL))
}

fn rewrite_query(q: &mut Query, keys: &Keys) {
    let conjuncts = q.where_clause.clone().map(flatten_and).unwrap_or_default();
    let mut deleted = Vec::new();
    replace_final_scans(&mut q.from, &conjuncts, keys, &mut deleted);
    if deleted.is_empty() {
        return;
    }
    let mut parts: Vec<Expr> = q.where_clause.take().into_iter().collect();
    parts.extend(
        deleted
            .iter()
            .map(|alias| Expr::eq(Expr::col(alias, "_deleted"), Expr::lit(false))),
    );
    q.where_clause = Expr::conjoin(parts);
}

fn replace_final_scans(
    table: &mut TableRef,
    conjuncts: &[Expr],
    keys: &Keys,
    deleted: &mut Vec<String>,
) {
    match table {
        TableRef::Join { left, right, .. } => {
            replace_final_scans(left, conjuncts, keys, deleted);
            replace_final_scans(right, conjuncts, keys, deleted);
        }
        TableRef::Scan {
            table: name,
            alias,
            final_: true,
            ..
        } => {
            let Some(key) = keys.get(name.as_str()) else {
                return;
            };
            let (name, alias) = (name.clone(), alias.clone());
            if let Some(query) = latest(&name, &alias, key, conjuncts) {
                *table = TableRef::subquery(query, &alias);
                deleted.push(alias);
            }
        }
        _ => {}
    }
}

/// `None` keeps FINAL: a broad scan (path scope only) is cheaper merged by FINAL.
fn latest(table: &str, alias: &str, key: &[String], conjuncts: &[Expr]) -> Option<Query> {
    let key_refs: Vec<&str> = key.iter().map(String::as_str).collect();
    let own = own(conjuncts, alias);
    let mut check: Vec<Expr> = own
        .iter()
        .filter(|c| columns_within(c, alias, &key_refs))
        .cloned()
        .collect();
    let needs_find = own
        .iter()
        .any(|c| !columns_within(c, alias, &key_refs) && !columns_within(c, alias, &TRIVIAL));
    let keyed = check.iter().any(|c| !columns_within(c, alias, &BROAD_KEYS));
    if !needs_find && !keyed {
        return None;
    }
    let key_cols: Vec<Expr> = key.iter().map(|k| Expr::col(alias, k)).collect();
    if needs_find {
        check.push(Expr::InSelect {
            expr: Box::new(match key_cols.as_slice() {
                [one] => one.clone(),
                many => Expr::func(Function::Tuple, many.to_vec()),
            }),
            query: Box::new(Query {
                select: key.iter().map(|k| SelectExpr::col(alias, k)).collect(),
                from: scan(table, alias),
                where_clause: Expr::conjoin(own),
                ..Default::default()
            }),
        });
    }
    let mut order_by: Vec<OrderExpr> = key_cols.iter().cloned().map(OrderExpr::asc).collect();
    order_by.push(OrderExpr::desc(Expr::col(alias, "_version")));
    Some(Query {
        select: vec![SelectExpr::star()],
        from: scan(table, alias),
        where_clause: Expr::conjoin(check),
        order_by,
        limit_by: Some((1, key_cols)),
        ..Default::default()
    })
}

fn scan(table: &str, alias: &str) -> TableRef {
    TableRef::Scan {
        table: table.into(),
        alias: alias.into(),
        final_: false,
        relationship: None,
    }
}

fn own(conjuncts: &[Expr], alias: &str) -> Vec<Expr> {
    conjuncts
        .iter()
        .filter(|c| columns_within(c, alias, &[]))
        .cloned()
        .collect()
}

/// An empty `allowed` accepts any column of `alias`.
fn columns_within(expr: &Expr, alias: &str, allowed: &[&str]) -> bool {
    let mut columns = 0;
    let mut foreign = false;
    let _ = visit_expressions(expr, &mut |e| {
        if let Expr::Column { table, column } = e {
            columns += 1;
            foreign |=
                table != alias || (!allowed.is_empty() && !allowed.contains(&column.as_str()));
        }
        Ok(())
    });
    columns > 0 && !foreign
}

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
