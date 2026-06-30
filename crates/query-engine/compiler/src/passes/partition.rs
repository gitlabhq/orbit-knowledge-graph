//! Partition-pruning pass. Wherever a scan's filter confines an alias to one or
//! more namespaces via `startsWith(alias.traversal_path, '<prefix>')`, ANDs a
//! predicate on the `_partition_id` virtual column so ClickHouse prunes parts
//! before reading data. Pure optimization: the security pass owns the
//! authorization filters, so over-scanning (emitting nothing, or a superset of
//! buckets) is always safe; the predicate can never drop an authorized row
//! because every pinning prefix's rows live in the matched bucket. Runs after
//! `security`.
//!
//! `_partition_id` is ClickHouse's per-part virtual column holding the formatted
//! partition value, so the row side is free (resolved from part metadata, no
//! column read). The bucket constant is computed in ClickHouse from the prefix
//! via the same expression the DDL uses, so the two stay identical by
//! construction.
//!
//! A namespaced scan with no pinning `startsWith` (a far node reached through a
//! cross-namespace edge) still cannot hold rows outside the caller's authorized
//! top-level namespaces, so it gets an `_partition_id IN (<authorized bucket
//! set>)` predicate — over-scanning within the tenant instead of all buckets.
//! This is skipped when the authorized set is unbounded (an org-only prefix
//! pins no namespace) or covers as many buckets as the table has anyway.

use std::collections::BTreeSet;

use ontology::{Ontology, PartitionStrategy};
use serde_json::Value;

use crate::ast::{Expr, Node, Op, Query, TableRef};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::Result;
use crate::passes::security::collect_aliased_tables;
use crate::types::SecurityContext;

/// ClickHouse virtual column holding a part's formatted partition value.
const PARTITION_ID_COLUMN: &str = "_partition_id";

pub fn apply_partition_pruning(
    node: &mut Node,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> Result<()> {
    let Some(partition) = ontology.partition() else {
        return Ok(());
    };
    let authorized = authorized_bucket_ids(&partition.strategy, security_ctx);
    if let Node::Query(q) = node {
        prune_query(q, &partition.strategy, authorized.as_deref());
    }
    Ok(())
}

fn prune_query(q: &mut Query, strategy: &PartitionStrategy, authorized: Option<&[Expr]>) {
    if let Some(where_clause) = &q.where_clause {
        let pinned = pinning_prefixes_by_alias(where_clause, strategy);
        let mut preds: Vec<Expr> = pinned
            .iter()
            .map(|(alias, prefixes)| partition_id_predicate(strategy, alias, prefixes))
            .collect();
        // Namespaced scans in this scope without a pinning prefix fall back to
        // the authorized bucket set (the far-node case).
        if let Some(buckets) = authorized {
            for (alias, _) in collect_aliased_tables(&q.from) {
                if !pinned.iter().any(|(a, _)| *a == alias) {
                    preds.push(in_bucket_set(&alias, buckets));
                }
            }
        }
        if !preds.is_empty() {
            q.where_clause = Expr::and_all(
                preds
                    .into_iter()
                    .map(Some)
                    .chain(std::iter::once(q.where_clause.take())),
            );
        }
    }

    for cte in &mut q.ctes {
        prune_query(&mut cte.query, strategy, authorized);
    }
    prune_query_from(&mut q.from, strategy, authorized);
    // Cascade-anchor / narrowing subqueries embedded in the WHERE clause
    // (`… IN (SELECT … FROM gl_edge AS e0p WHERE startsWith(…))`) are their own
    // scans; prune each in its own scope so a confined cascade alias is not left
    // reading every bucket.
    if let Some(where_clause) = &mut q.where_clause {
        prune_subqueries_in_expr(where_clause, strategy, authorized);
    }
    for arm in &mut q.union_all {
        prune_query(arm, strategy, authorized);
    }
}

fn prune_subqueries_in_expr(
    expr: &mut Expr,
    strategy: &PartitionStrategy,
    authorized: Option<&[Expr]>,
) {
    match expr {
        Expr::InSelect { expr, query } => {
            prune_subqueries_in_expr(expr, strategy, authorized);
            prune_query(query, strategy, authorized);
        }
        Expr::FuncCall { args, .. } => {
            for a in args {
                prune_subqueries_in_expr(a, strategy, authorized);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            prune_subqueries_in_expr(left, strategy, authorized);
            prune_subqueries_in_expr(right, strategy, authorized);
        }
        Expr::UnaryOp { expr, .. } | Expr::InSubquery { expr, .. } => {
            prune_subqueries_in_expr(expr, strategy, authorized);
        }
        Expr::Lambda { body, .. } => prune_subqueries_in_expr(body, strategy, authorized),
        _ => {}
    }
}

fn prune_query_from(
    from: &mut TableRef,
    strategy: &PartitionStrategy,
    authorized: Option<&[Expr]>,
) {
    match from {
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            prune_query_from(left, strategy, authorized);
            prune_query_from(right, strategy, authorized);
        }
        TableRef::Subquery { query, .. } => prune_query(query, strategy, authorized),
        TableRef::Union { queries, .. } => {
            for q in queries {
                prune_query(q, strategy, authorized);
            }
        }
    }
}

/// The bucket-id constant expressions for every top-level namespace the caller
/// is authorized for. `None` when the set cannot prune: any authorized path is
/// org-only (pins no namespace), or the distinct buckets cover the whole table.
/// Derived from the full authorized set, so it is always a superset of any
/// role-floored subset a given alias sees — a safe over-scan.
fn authorized_bucket_ids(strategy: &PartitionStrategy, ctx: &SecurityContext) -> Option<Vec<Expr>> {
    if ctx.admin {
        return None;
    }
    let mut tlns: BTreeSet<String> = BTreeSet::new();
    for tp in &ctx.traversal_paths {
        let segments: Vec<&str> = tp.path.split('/').filter(|s| !s.is_empty()).collect();
        // An org-only path (`1/`) pins no top-level namespace, so the authorized
        // set spans every bucket; bail rather than emit a useless full set.
        if segments.len() < 2 {
            return None;
        }
        tlns.insert(segments[1].to_string());
    }
    // More distinct namespaces than buckets guarantees near-full coverage, so
    // the IN-list buys nothing; bail. Bounding on the TLN count (not the hashed
    // bucket count) keeps the hash entirely in ClickHouse — no Rust sipHash to
    // drift from the DDL.
    if tlns.is_empty() || tlns.len() >= bucket_count(strategy) {
        return None;
    }
    Some(
        tlns.iter()
            .map(|tln| {
                Expr::func(
                    "toString",
                    // Reuse the DDL strategy expression over the `<org>/<tln>/`
                    // shape it hashes from; only segment 2 (the tln) is read.
                    vec![partition_expr(strategy, Expr::string(format!("0/{tln}/")))],
                )
            })
            .collect(),
    )
}

fn bucket_count(strategy: &PartitionStrategy) -> usize {
    match strategy {
        PartitionStrategy::HashBucket { buckets, .. } => usize::from(*buckets),
    }
}

/// `alias._partition_id IN tuple(<bucket exprs>)`.
fn in_bucket_set(alias: &str, buckets: &[Expr]) -> Expr {
    Expr::binary(
        Op::In,
        Expr::col(alias, PARTITION_ID_COLUMN),
        Expr::func("tuple", buckets.to_vec()),
    )
}

/// Per alias, the distinct set of pinning prefixes from its
/// `startsWith(alias.traversal_path, '<prefix>')` filters. An alias filtered by
/// several prefixes (a multi-namespace `OR`) is kept: its rows span exactly
/// those prefixes' buckets, so an `IN` over that bucket set is exhaustive.
fn pinning_prefixes_by_alias(
    expr: &Expr,
    strategy: &PartitionStrategy,
) -> Vec<(String, Vec<String>)> {
    let mut found: Vec<(String, String)> = Vec::new();
    collect_pinning(expr, strategy, &mut found);
    found.sort();
    found.dedup();

    let mut by_alias: Vec<(String, Vec<String>)> = Vec::new();
    for (alias, prefix) in found {
        match by_alias.last_mut() {
            Some((a, prefixes)) if *a == alias => prefixes.push(prefix),
            _ => by_alias.push((alias, vec![prefix])),
        }
    }
    by_alias
}

fn collect_pinning(expr: &Expr, strategy: &PartitionStrategy, out: &mut Vec<(String, String)>) {
    match expr {
        Expr::FuncCall { name, args } if name == "startsWith" && args.len() == 2 => {
            if let (
                Expr::Column { table, column },
                Expr::Param {
                    value: Value::String(prefix),
                    ..
                },
            ) = (&args[0], &args[1])
                && column == TRAVERSAL_PATH_COLUMN
                && prefix_pins_partition(strategy, prefix)
            {
                out.push((table.clone(), prefix.clone()));
            }
        }
        Expr::FuncCall { args, .. } => {
            for a in args {
                collect_pinning(a, strategy, out);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_pinning(left, strategy, out);
            collect_pinning(right, strategy, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::InSelect { expr, .. } => collect_pinning(expr, strategy, out),
        Expr::Lambda { body, .. } => collect_pinning(body, strategy, out),
        _ => {}
    }
}

/// `alias._partition_id = bucket(prefix)` for one prefix, or
/// `alias._partition_id IN tuple(bucket(p1), …)` for several. ClickHouse folds
/// each bucket constant and prunes parts on the virtual column.
fn partition_id_predicate(strategy: &PartitionStrategy, alias: &str, prefixes: &[String]) -> Expr {
    let col = Expr::col(alias, PARTITION_ID_COLUMN);
    let mut buckets = prefixes.iter().map(|p| bucket_id(strategy, p));
    let first = buckets
        .next()
        .expect("alias has at least one pinning prefix");
    match buckets.next() {
        None => Expr::eq(col, first),
        Some(second) => {
            let elems = std::iter::once(first)
                .chain(std::iter::once(second))
                .chain(buckets)
                .collect();
            Expr::binary(Op::In, col, Expr::func("tuple", elems))
        }
    }
}

/// `toString(<partition expression over the prefix literal>)`, matching the
/// `_partition_id` string form. Computed in ClickHouse so it is byte-identical
/// to the DDL `PARTITION BY`.
fn bucket_id(strategy: &PartitionStrategy, prefix: &str) -> Expr {
    Expr::func(
        "toString",
        vec![partition_expr(strategy, Expr::string(prefix))],
    )
}

/// Lowers a strategy to its partition expression over `input`; shared with the
/// DDL `PARTITION BY` so the two stay identical.
pub fn partition_expr(strategy: &PartitionStrategy, input: Expr) -> Expr {
    match strategy {
        PartitionStrategy::HashBucket { buckets, .. } => Expr::func(
            "modulo",
            vec![
                Expr::func(
                    "sipHash64",
                    vec![Expr::func(
                        "toUInt64OrZero",
                        vec![Expr::func(
                            "arrayElement",
                            vec![
                                Expr::func("splitByChar", vec![Expr::string("/"), input]),
                                Expr::int(2),
                            ],
                        )],
                    )],
                ),
                Expr::int(i64::from(*buckets)),
            ],
        ),
    }
}

/// Whether `prefix` pins the strategy's hashed value (hash-bucket needs `org/top_level/`).
fn prefix_pins_partition(strategy: &PartitionStrategy, prefix: &str) -> bool {
    match strategy {
        PartitionStrategy::HashBucket { .. } => {
            prefix.split('/').filter(|s| !s.is_empty()).count() >= 2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, SelectExpr, TableRef};

    /// Org-only scope: `authorized_bucket_ids` returns `None`, so the far-node
    /// fallback is off and these tests isolate the pinning-prefix behavior.
    fn org_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    fn prune(node: &mut Node, ctx: &SecurityContext) {
        apply_partition_pruning(node, &Ontology::load_embedded().unwrap(), ctx).unwrap();
    }

    fn where_with(filter: Expr) -> Option<Expr> {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(filter),
            limit: Some(10),
            ..Default::default()
        }));
        prune(&mut node, &org_ctx());
        let Node::Query(q) = node else { unreachable!() };
        q.where_clause
    }

    /// `_partition_id` predicates referencing `alias`, returned as `(is_in, n_buckets)`.
    fn partition_id_pred(expr: &Expr, alias: &str) -> Option<(bool, usize)> {
        fn refs_partition_col(e: &Expr, alias: &str) -> bool {
            matches!(e, Expr::Column { table, column } if table == alias && column == PARTITION_ID_COLUMN)
        }
        fn count_buckets(rhs: &Expr) -> usize {
            match rhs {
                Expr::FuncCall { name, args } if name == "tuple" => args.len(),
                _ => 1,
            }
        }
        match expr {
            Expr::BinaryOp {
                op: Op::Eq,
                left,
                right,
            } if refs_partition_col(left, alias) => {
                let _ = right;
                Some((false, count_buckets(right)))
            }
            Expr::BinaryOp {
                op: Op::In,
                left,
                right,
            } if refs_partition_col(left, alias) => Some((true, count_buckets(right))),
            Expr::BinaryOp { left, right, .. } => {
                partition_id_pred(left, alias).or_else(|| partition_id_pred(right, alias))
            }
            Expr::FuncCall { args, .. } => args.iter().find_map(|a| partition_id_pred(a, alias)),
            Expr::UnaryOp { expr, .. }
            | Expr::InSubquery { expr, .. }
            | Expr::InSelect { expr, .. } => partition_id_pred(expr, alias),
            Expr::Lambda { body, .. } => partition_id_pred(body, alias),
            _ => None,
        }
    }

    fn has_partition_pred(expr: &Expr, alias: &str) -> bool {
        partition_id_pred(expr, alias).is_some()
    }

    fn starts_with(alias: &str, prefix: &str) -> Expr {
        Expr::func(
            "startsWith",
            vec![Expr::col(alias, "traversal_path"), Expr::string(prefix)],
        )
    }

    #[test]
    fn prunes_alias_with_pinning_startswith() {
        let where_clause = where_with(starts_with("p", "1/100/1000/")).unwrap();
        assert_eq!(partition_id_pred(&where_clause, "p"), Some((false, 1)));
    }

    #[test]
    fn no_prune_for_org_only_prefix() {
        let where_clause = where_with(starts_with("p", "1/")).unwrap();
        assert!(!has_partition_pred(&where_clause, "p"));
    }

    #[test]
    fn no_prune_without_startswith() {
        let where_clause = where_with(Expr::eq(Expr::col("p", "id"), Expr::int(1))).unwrap();
        assert!(!has_partition_pred(&where_clause, "p"));
    }

    #[test]
    fn multi_namespace_or_prunes_to_bucket_set() {
        let filter = Expr::binary(
            Op::Or,
            starts_with("p", "1/100/1000/"),
            starts_with("p", "1/200/2000/"),
        );
        let where_clause = where_with(filter).unwrap();
        assert_eq!(
            partition_id_pred(&where_clause, "p"),
            Some((true, 2)),
            "a multi-namespace alias prunes to the IN-set of its buckets"
        );
    }

    #[test]
    fn prunes_edge_alias_in_join() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::subquery(
                    Query {
                        select: vec![SelectExpr {
                            expr: Expr::Star,
                            alias: None,
                        }],
                        from: TableRef::scan("gl_edge", "e"),
                        where_clause: Some(starts_with("e", "1/100/1000/")),
                        ..Default::default()
                    },
                    "e",
                ),
                Expr::lit(true),
            ),
            where_clause: Some(starts_with("p", "1/100/1000/")),
            limit: Some(10),
            ..Default::default()
        }));
        prune(&mut node, &org_ctx());
        let Node::Query(q) = node else { unreachable!() };
        assert!(has_partition_pred(q.where_clause.as_ref().unwrap(), "p"));
        let TableRef::Join { right, .. } = &q.from else {
            unreachable!()
        };
        let TableRef::Subquery { query, .. } = right.as_ref() else {
            unreachable!()
        };
        assert!(has_partition_pred(
            query.where_clause.as_ref().unwrap(),
            "e"
        ));
    }

    #[test]
    fn prunes_alias_inside_in_subquery() {
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::col("e0p", "source_id"),
                alias: None,
            }],
            from: TableRef::scan("gl_edge", "e0p"),
            where_clause: Some(starts_with("e0p", "1/24/23/")),
            ..Default::default()
        };
        let where_clause = Expr::binary(
            Op::In,
            Expr::col("e", "source_id"),
            Expr::InSelect {
                expr: Box::new(Expr::col("e", "source_id")),
                query: Box::new(inner),
            },
        );
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_edge", "e"),
            where_clause: Some(where_clause),
            ..Default::default()
        }));
        prune(&mut node, &org_ctx());
        let Node::Query(q) = node else { unreachable!() };
        let Some(Expr::BinaryOp { right, .. }) = q.where_clause.as_ref() else {
            unreachable!()
        };
        let Expr::InSelect { query, .. } = right.as_ref() else {
            unreachable!()
        };
        assert!(
            has_partition_pred(query.where_clause.as_ref().unwrap(), "e0p"),
            "a confined alias inside an IN-subquery must still be pruned"
        );
    }

    #[test]
    fn no_op_without_partition_config() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(starts_with("p", "1/100/1000/")),
            ..Default::default()
        }));
        let ontology = Ontology::new();
        apply_partition_pruning(&mut node, &ontology, &org_ctx()).unwrap();
        let Node::Query(q) = node else { unreachable!() };
        assert!(!has_partition_pred(q.where_clause.as_ref().unwrap(), "p"));
    }

    #[test]
    fn unscoped_alias_falls_back_to_authorized_bucket_set() {
        let ctx = SecurityContext::new(1, vec!["1/100/".into(), "1/200/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "source_id"),
                alias: None,
            }],
            from: TableRef::scan("gl_edge", "e"),
            // No pinning startsWith: only the broad org filter.
            where_clause: Some(starts_with("e", "1/")),
            ..Default::default()
        }));
        prune(&mut node, &ctx);
        let Node::Query(q) = node else { unreachable!() };
        assert_eq!(
            partition_id_pred(q.where_clause.as_ref().unwrap(), "e"),
            Some((true, 2)),
            "a far node with no pinning prefix prunes to the caller's authorized TLN buckets"
        );
    }

    #[test]
    fn org_only_scope_yields_no_authorized_fallback() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "source_id"),
                alias: None,
            }],
            from: TableRef::scan("gl_edge", "e"),
            where_clause: Some(starts_with("e", "1/")),
            ..Default::default()
        }));
        prune(&mut node, &org_ctx());
        let Node::Query(q) = node else { unreachable!() };
        assert!(
            !has_partition_pred(q.where_clause.as_ref().unwrap(), "e"),
            "an org-only authorized path pins no namespace, so no bucket set"
        );
    }
}
