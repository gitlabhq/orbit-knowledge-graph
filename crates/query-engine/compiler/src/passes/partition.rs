//! Partition-pruning pass: emits the table's partition expression as a
//! predicate for a query scoped to a single top-level namespace, so ClickHouse
//! prunes to one bucket. Runs after `security`.

use ontology::{Ontology, PartitionStrategy};

use crate::ast::{Expr, Node, Query};
use crate::error::Result;
use crate::passes::security::collect_aliased_tables;
use crate::types::SecurityContext;

pub fn apply_partition_pruning(
    node: &mut Node,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    let Some(partition) = ontology.partition() else {
        return Ok(());
    };
    let Node::Query(q) = node else {
        return Ok(());
    };
    for cte in &mut q.ctes {
        prune_query(&mut cte.query, ctx, ontology, &partition.strategy);
    }
    prune_query(q, ctx, ontology, &partition.strategy);
    Ok(())
}

fn prune_query(
    q: &mut Query,
    ctx: &SecurityContext,
    ontology: &Ontology,
    strategy: &PartitionStrategy,
) {
    let preds: Vec<Expr> = collect_aliased_tables(&q.from)
        .into_iter()
        .filter(|(_, table)| ontology.is_table_path_scopable(table))
        .filter_map(|(alias, _)| {
            let prefix = ctx.scope_prefixes.get(&alias)?;
            prefix_pins_partition(strategy, prefix)
                .then(|| bucket_predicate(strategy, &alias, prefix))
        })
        .collect();

    if !preds.is_empty() {
        q.where_clause = Expr::and_all(
            preds
                .into_iter()
                .map(Some)
                .chain(std::iter::once(q.where_clause.take())),
        );
    }

    for arm in &mut q.union_all {
        prune_query(arm, ctx, ontology, strategy);
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
    use crate::ast::{SelectExpr, TableRef};

    fn scoped_project_where(prefix: Option<&str>) -> Option<Expr> {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let ctx = match prefix {
            Some(p) => ctx.with_scope_prefixes([("p".to_string(), p.to_string())].into()),
            None => ctx,
        };
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            limit: Some(10),
            ..Default::default()
        }));
        apply_partition_pruning(&mut node, &ctx, &Ontology::load_embedded().unwrap()).unwrap();
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

    #[test]
    fn prunes_when_prefix_pins_namespace() {
        let where_clause = scoped_project_where(Some("1/100/1000/")).unwrap();
        assert!(has_modulo_over(&where_clause, "p"));
    }

    #[test]
    fn no_prune_for_org_only_prefix() {
        assert!(scoped_project_where(Some("1/")).is_none());
    }

    #[test]
    fn no_prune_without_scope_prefix() {
        assert!(scoped_project_where(None).is_none());
    }
}
