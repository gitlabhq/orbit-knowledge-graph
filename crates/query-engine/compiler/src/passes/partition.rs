//! Partition-pruning pass: ANDs a `_partition_id` predicate on namespaced scans
//! so ClickHouse prunes parts. Over-scanning is always safe (the security pass
//! owns authorization); the bucket constant is computed in ClickHouse, matching
//! the DDL. Runs after `security`.

use std::collections::BTreeSet;

use ontology::{Ontology, PartitionConfig, PartitionStrategy};
use serde_json::Value;

use crate::ast::{Expr, Node, Op, Query, TableRef};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::Result;
use crate::passes::security::collect_aliased_tables;
use crate::types::SecurityContext;

/// ClickHouse per-part virtual column holding the formatted partition value.
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
        prune_query(q, partition, authorized.as_deref());
    }
    Ok(())
}

fn prune_query(q: &mut Query, partition: &PartitionConfig, authorized: Option<&[Expr]>) {
    let strategy = &partition.strategy;
    if let Some(where_clause) = &q.where_clause {
        let partitioned: std::collections::HashSet<String> = collect_aliased_tables(&q.from)
            .into_iter()
            .filter(|(_, table)| partition.is_partitioned(table))
            .map(|(alias, _)| alias)
            .collect();
        let pinned = pinning_prefixes_by_alias(where_clause, strategy);
        let mut preds: Vec<Expr> = pinned
            .iter()
            .filter(|(alias, _)| partitioned.contains(alias))
            .map(|(alias, prefixes)| {
                let buckets = prefixes.iter().map(|p| bucket_id(strategy, p)).collect();
                partition_id_predicate(alias, buckets)
            })
            .collect();
        // Namespaced scans with no pinning prefix fall back to the tenant set.
        if let Some(buckets) = authorized {
            for alias in &partitioned {
                if !pinned.iter().any(|(a, _)| a == alias) {
                    preds.push(partition_id_predicate(alias, buckets.to_vec()));
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
        prune_query(&mut cte.query, partition, authorized);
    }
    prune_query_from(&mut q.from, partition, authorized);
    // Subqueries in the WHERE (cascade/narrowing) are their own scans.
    if let Some(where_clause) = &mut q.where_clause {
        prune_subqueries_in_expr(where_clause, partition, authorized);
    }
    for arm in &mut q.union_all {
        prune_query(arm, partition, authorized);
    }
}

fn prune_subqueries_in_expr(
    expr: &mut Expr,
    partition: &PartitionConfig,
    authorized: Option<&[Expr]>,
) {
    match expr {
        Expr::InSelect { expr, query } => {
            prune_subqueries_in_expr(expr, partition, authorized);
            prune_query(query, partition, authorized);
        }
        Expr::FuncCall { args, .. } => {
            for a in args {
                prune_subqueries_in_expr(a, partition, authorized);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            prune_subqueries_in_expr(left, partition, authorized);
            prune_subqueries_in_expr(right, partition, authorized);
        }
        Expr::UnaryOp { expr, .. } | Expr::InSubquery { expr, .. } => {
            prune_subqueries_in_expr(expr, partition, authorized);
        }
        Expr::Lambda { body, .. } => prune_subqueries_in_expr(body, partition, authorized),
        _ => {}
    }
}

fn prune_query_from(from: &mut TableRef, partition: &PartitionConfig, authorized: Option<&[Expr]>) {
    match from {
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            prune_query_from(left, partition, authorized);
            prune_query_from(right, partition, authorized);
        }
        TableRef::Subquery { query, .. } => prune_query(query, partition, authorized),
        TableRef::Union { queries, .. } => {
            for q in queries {
                prune_query(q, partition, authorized);
            }
        }
    }
}

/// Bucket-id constants for every top-level namespace the caller is authorized
/// for, or `None` when it cannot prune (admin, org-only path, or more
/// namespaces than buckets). The full set is a safe superset of any alias's
/// role-floored subset.
fn authorized_bucket_ids(strategy: &PartitionStrategy, ctx: &SecurityContext) -> Option<Vec<Expr>> {
    if ctx.admin {
        return None;
    }
    let mut tlns: BTreeSet<String> = BTreeSet::new();
    for tp in &ctx.traversal_paths {
        let segments: Vec<&str> = tp.path.split('/').filter(|s| !s.is_empty()).collect();
        // An org-only path pins no namespace, so it spans every bucket.
        if segments.len() < 2 {
            return None;
        }
        tlns.insert(segments[1].to_string());
    }
    // Count TLNs, not hashed buckets, so the hash stays in ClickHouse.
    if tlns.is_empty() || tlns.len() >= bucket_count(strategy) {
        return None;
    }
    Some(
        tlns.iter()
            .map(|tln| bucket_id(strategy, &format!("0/{tln}/")))
            .collect(),
    )
}

fn bucket_count(strategy: &PartitionStrategy) -> usize {
    match strategy {
        PartitionStrategy::HashBucket { buckets, .. } => usize::from(*buckets),
    }
}

/// Per alias, the distinct pinning prefixes from its `startsWith` filters.
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

/// `alias._partition_id = bucket` for one bucket, else `IN tuple(bucket…)`.
fn partition_id_predicate(alias: &str, mut buckets: Vec<Expr>) -> Expr {
    let col = Expr::col(alias, PARTITION_ID_COLUMN);
    if buckets.len() == 1 {
        Expr::eq(col, buckets.pop().expect("len checked"))
    } else {
        Expr::binary(Op::In, col, Expr::func("tuple", buckets))
    }
}

/// `toString(partition_expr(prefix))`, matching the `_partition_id` string form.
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

    /// Org-only scope: no far-node fallback, so tests isolate pinning behavior.
    fn org_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    /// The pass logic is exercised against a fixed partitioned config so these
    /// tests stay meaningful even when the shipped ontology disables partitioning
    /// (the break-glass path). Coverage of the shipped config lives in the DDL
    /// and integration tests, which gate on `ontology.partition()`. The bucket
    /// count is only ever compared against the (smaller) authorized-TLN set, so
    /// any value larger than the test prefixes' namespaces works.
    fn partitioned_ontology() -> Ontology {
        Ontology::load_embedded()
            .unwrap()
            .with_partition(PartitionConfig {
                strategy: PartitionStrategy::HashBucket {
                    buckets: 50,
                    column: TRAVERSAL_PATH_COLUMN.to_string(),
                },
                partitioned_tables: ["gl_edge", "gl_merge_request"]
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
            })
    }

    fn prune(node: &mut Node, ctx: &SecurityContext) {
        apply_partition_pruning(node, &partitioned_ontology(), ctx).unwrap();
    }

    fn where_with(filter: Expr) -> Option<Expr> {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_merge_request", "p"),
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
            } if refs_partition_col(left, alias) => Some((false, count_buckets(right))),
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
    fn no_prune_for_unpartitioned_table() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(starts_with("p", "1/100/1000/")),
            limit: Some(10),
            ..Default::default()
        }));
        prune(&mut node, &org_ctx());
        let Node::Query(q) = node else { unreachable!() };
        assert!(!has_partition_pred(q.where_clause.as_ref().unwrap(), "p"));
    }

    #[test]
    fn unpartitioned_table_skipped_by_authorized_fallback() {
        let ctx = SecurityContext::new(1, vec!["1/100/".into(), "1/200/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(starts_with("p", "1/")),
            ..Default::default()
        }));
        prune(&mut node, &ctx);
        let Node::Query(q) = node else { unreachable!() };
        assert!(!has_partition_pred(q.where_clause.as_ref().unwrap(), "p"));
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
                TableRef::scan("gl_merge_request", "p"),
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
            from: TableRef::scan("gl_merge_request", "p"),
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
