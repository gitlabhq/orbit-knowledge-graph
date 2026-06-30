//! Partition-pruning pass. Wherever a scan's filter already confines an alias
//! to a single namespace via `startsWith(alias.traversal_path, '<prefix>')`,
//! ANDs the table's partition expression equated to that prefix so ClickHouse
//! prunes to one bucket. Pure optimization: the security pass owns the
//! authorization filters, so over-scanning (emitting nothing) is always safe;
//! a tight `startsWith` proves the rows can only live in the matched bucket,
//! so the predicate can never drop an authorized row. Runs after `security`.

use ontology::{Ontology, PartitionStrategy};
use serde_json::Value;

use crate::ast::{Expr, Node, Query, TableRef};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::Result;

pub fn apply_partition_pruning(node: &mut Node, ontology: &Ontology) -> Result<()> {
    let Some(partition) = ontology.partition() else {
        return Ok(());
    };
    if let Node::Query(q) = node {
        prune_query(q, &partition.strategy);
    }
    Ok(())
}

fn prune_query(q: &mut Query, strategy: &PartitionStrategy) {
    if let Some(where_clause) = &q.where_clause {
        let preds: Vec<Expr> = pinning_prefixes(where_clause, strategy)
            .into_iter()
            .map(|(alias, prefix)| bucket_predicate(strategy, &alias, &prefix))
            .collect();
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
        prune_query(&mut cte.query, strategy);
    }
    prune_query_from(&mut q.from, strategy);
    for arm in &mut q.union_all {
        prune_query(arm, strategy);
    }
}

fn prune_query_from(from: &mut TableRef, strategy: &PartitionStrategy) {
    match from {
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            prune_query_from(left, strategy);
            prune_query_from(right, strategy);
        }
        TableRef::Subquery { query, .. } => prune_query(query, strategy),
        TableRef::Union { queries, .. } => {
            for q in queries {
                prune_query(q, strategy);
            }
        }
    }
}

/// Collects `(alias, prefix)` for every `startsWith(alias.traversal_path,
/// '<prefix>')` in `expr` whose prefix pins the strategy's partition input.
/// Deduplicated so an alias filtered more than once yields one predicate.
fn pinning_prefixes(expr: &Expr, strategy: &PartitionStrategy) -> Vec<(String, String)> {
    let mut found: Vec<(String, String)> = Vec::new();
    collect_pinning(expr, strategy, &mut found);
    found.sort();
    found.dedup();
    found
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

/// `expr(column) = expr(prefix)`; ClickHouse folds the constant side and prunes.
fn bucket_predicate(strategy: &PartitionStrategy, alias: &str, prefix: &str) -> Expr {
    Expr::eq(
        partition_expr(strategy, Expr::col(alias, strategy.column())),
        partition_expr(strategy, Expr::string(prefix)),
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
        apply_partition_pruning(&mut node, &Ontology::load_embedded().unwrap()).unwrap();
        let Node::Query(q) = node else { unreachable!() };
        q.where_clause
    }

    fn has_modulo_over(expr: &Expr, alias: &str) -> bool {
        fn refs(e: &Expr, alias: &str) -> bool {
            match e {
                Expr::Column { table, .. } => table == alias,
                Expr::FuncCall { args, .. } => args.iter().any(|a| refs(a, alias)),
                Expr::BinaryOp { left, right, .. } => refs(left, alias) || refs(right, alias),
                _ => false,
            }
        }
        match expr {
            Expr::FuncCall { name, args } if name == "modulo" => {
                args.iter().any(|a| refs(a, alias))
            }
            Expr::FuncCall { args, .. } => args.iter().any(|a| has_modulo_over(a, alias)),
            Expr::BinaryOp { left, right, .. } => {
                has_modulo_over(left, alias) || has_modulo_over(right, alias)
            }
            _ => false,
        }
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
        assert!(has_modulo_over(&where_clause, "p"));
    }

    #[test]
    fn no_prune_for_org_only_prefix() {
        let where_clause = where_with(starts_with("p", "1/")).unwrap();
        assert!(!has_modulo_over(&where_clause, "p"));
    }

    #[test]
    fn no_prune_without_startswith() {
        let where_clause = where_with(Expr::eq(Expr::col("p", "id"), Expr::int(1))).unwrap();
        assert!(!has_modulo_over(&where_clause, "p"));
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
        apply_partition_pruning(&mut node, &Ontology::load_embedded().unwrap()).unwrap();
        let Node::Query(q) = node else { unreachable!() };
        assert!(has_modulo_over(q.where_clause.as_ref().unwrap(), "p"));
        let TableRef::Join { right, .. } = &q.from else {
            unreachable!()
        };
        let TableRef::Subquery { query, .. } = right.as_ref() else {
            unreachable!()
        };
        assert!(has_modulo_over(query.where_clause.as_ref().unwrap(), "e"));
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
        apply_partition_pruning(&mut node, &ontology).unwrap();
        let Node::Query(q) = node else { unreachable!() };
        assert!(!has_modulo_over(q.where_clause.as_ref().unwrap(), "p"));
    }
}
