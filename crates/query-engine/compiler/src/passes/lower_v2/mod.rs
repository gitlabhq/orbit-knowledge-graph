//! `PhysOp` → SQL AST.
//!
//! One `Query` block accumulates operators bottom-up until an operator would
//! conflict with what the block already holds (a second projection, a filter
//! after a projection or dedup, a sort after a limit). Then the block closes
//! as a derived table and a fresh block continues:
//!
//! ```text
//!   Filter(Project(Filter(Scan)))
//!   ⇒ SELECT * FROM (SELECT cols FROM t WHERE p1) AS t WHERE p2
//! ```

mod expr;

use crate::ast::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::plan_v2::*;
use crate::passes::shared::filter_to_expr;
use expr::expr;
use ontology::constants::*;

pub fn lower(op: PhysOp, input: &Input) -> Result<Node> {
    let mut q = emit(op, input);
    if q.select.is_empty() {
        q.select = default_select(&q.from);
    }
    for jp in &input.join_predicates {
        let filter = InputFilter {
            op: Some(jp.op),
            rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
            ..Default::default()
        };
        and_where(&mut q, filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter));
    }
    Ok(Node::Query(Box::new(q)))
}

fn emit(op: PhysOp, input: &Input) -> Query {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } => match dedup {
            Dedup::None => Query {
                from: TableRef::scan(&table, &alias),
                ..Default::default()
            },
            Dedup::Final => Query {
                from: TableRef::scan_final(&table, &alias),
                ..Default::default()
            },
            Dedup::LimitBy => {
                let sort_key = input
                    .compiler
                    .table_sort_keys
                    .get(&table)
                    .cloned()
                    .unwrap_or_else(|| vec![DEFAULT_PRIMARY_KEY.to_string()]);
                let key: Vec<Expr> = sort_key.iter().map(|c| Expr::col(&alias, c)).collect();
                let mut order_by: Vec<OrderExpr> =
                    key.iter().cloned().map(OrderExpr::asc).collect();
                order_by.push(OrderExpr::desc(Expr::col(&alias, VERSION_COLUMN)));
                Query {
                    from: TableRef::scan(&table, &alias),
                    order_by,
                    limit_by: Some((1, key)),
                    ..Default::default()
                }
            }
        },

        PhysOp::Filter {
            input: child,
            predicates,
        } => {
            let mut q = emit(*child, input);
            if is_closed(&q) {
                q = close(q);
            }
            for p in predicates {
                and_where(&mut q, expr(&p));
            }
            q
        }

        PhysOp::Project {
            input: child,
            columns,
        } => {
            let mut q = emit(*child, input);
            if is_closed(&q) {
                q = close(q);
            }
            q.select = columns
                .into_iter()
                .map(|(e, a)| SelectExpr::new(expr(&e), a))
                .collect();
            q
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Inner,
        } => {
            let (lhs, where_clause, mut ctes) = join_operand(emit(*left, input), true);
            let (rhs, _, rctes) = join_operand(emit(*right, input), false);
            ctes.extend(rctes);
            let cond = on
                .iter()
                .map(|(x, y)| Expr::eq(Expr::col(&x.0, &x.1), Expr::col(&y.0, &y.1)))
                .reduce(Expr::and);
            let join_type = if cond.is_some() {
                JoinType::Inner
            } else {
                JoinType::Cross
            };
            Query {
                from: TableRef::join(join_type, lhs, rhs, cond.unwrap_or_else(|| Expr::lit(1))),
                where_clause,
                ctes,
                ..Default::default()
            }
        }

        PhysOp::Join {
            left,
            right,
            on,
            kind: JoinKind::Semi,
        } => {
            let mut lq = emit(*left, input);
            if is_closed(&lq) {
                lq = close(lq);
            }
            let (consumer, producer) = on.into_iter().next().expect("semi join equality");
            let rq = emit(*right, input);
            let bare_ref = rq.select.is_empty()
                && rq.where_clause.is_none()
                && matches!(rq.from, TableRef::Scan { .. });
            let pred = if let (true, TableRef::Scan { table, .. }) = (bare_ref, &rq.from) {
                Expr::InSubquery {
                    expr: Box::new(Expr::col(&consumer.0, &consumer.1)),
                    cte_name: table.clone(),
                    column: producer.1,
                }
            } else {
                let mut body = rq;
                body.select = vec![SelectExpr::new(
                    Expr::col(&producer.0, &producer.1),
                    producer.1.clone(),
                )];
                Expr::InSelect {
                    expr: Box::new(Expr::col(&consumer.0, &consumer.1)),
                    query: Box::new(body),
                }
            };
            and_where(&mut lq, pred);
            lq
        }

        PhysOp::Aggregate {
            input: child,
            group_by,
            metrics,
        } => {
            let mut q = emit(*child, input);
            if is_closed(&q) {
                q = close(q);
            }
            for (e, a) in &group_by {
                let e = expr(e);
                q.select.push(SelectExpr::new(e.clone(), a));
                if !q.group_by.contains(&e) {
                    q.group_by.push(e);
                }
            }
            q.select
                .extend(metrics.iter().map(|(e, a)| SelectExpr::new(expr(e), a)));
            q
        }

        PhysOp::Union { arms, alias } => {
            let queries = arms
                .into_iter()
                .map(|a| {
                    let mut q = emit(a, input);
                    if q.select.is_empty() {
                        q.select.push(SelectExpr::star());
                    }
                    q
                })
                .collect();
            Query {
                from: TableRef::union_all(queries, alias),
                ..Default::default()
            }
        }

        PhysOp::Sort { input: child, keys } => {
            let mut q = emit(*child, input);
            if q.limit.is_some() || q.limit_by.is_some() {
                q = close(q);
            }
            q.order_by.extend(keys.iter().map(|(e, desc)| {
                if *desc {
                    OrderExpr::desc(expr(e))
                } else {
                    OrderExpr::asc(expr(e))
                }
            }));
            q
        }

        PhysOp::Limit {
            input: child,
            count,
        } => {
            let mut q = emit(*child, input);
            if q.limit.is_some() {
                q = close(q);
            }
            q.limit = Some(count);
            q
        }

        PhysOp::With { ctes, input: child } => {
            let mut q = emit(*child, input);
            let mut all: Vec<Cte> = ctes
                .into_iter()
                .map(|(name, body)| {
                    let mut bq = emit(body, input);
                    if bq.select.is_empty() {
                        bq.select.push(SelectExpr::star());
                    }
                    Cte::new(name, bq)
                })
                .collect();
            all.append(&mut q.ctes);
            q.ctes = all;
            q
        }
    }
}

// ── Blocks ────────────────────────────────────────────────────────────────────

/// The block already projects, groups, or limits: further filters and
/// projections apply to its result.
fn is_closed(q: &Query) -> bool {
    !q.select.is_empty() || !q.group_by.is_empty() || q.limit.is_some()
}

fn close(q: Query) -> Query {
    let alias = alias_of(&q).to_string();
    Query {
        from: as_table(q, &alias),
        ..Default::default()
    }
}

fn as_table(mut q: Query, alias: &str) -> TableRef {
    if q.select.is_empty() {
        q.select.push(SelectExpr::star());
    }
    TableRef::subquery(q, alias)
}

/// A block that is only `FROM` (+ `WHERE` when `keep_where`) can be a join
/// operand directly; anything richer becomes a derived table. Returns the
/// operand, a `WHERE` to hoist, and CTEs to hoist.
fn join_operand(q: Query, keep_where: bool) -> (TableRef, Option<Expr>, Vec<Cte>) {
    let simple = q.select.is_empty()
        && q.group_by.is_empty()
        && q.order_by.is_empty()
        && q.limit_by.is_none()
        && q.limit.is_none()
        && (keep_where || q.where_clause.is_none() && matches!(q.from, TableRef::Scan { .. }));
    if simple {
        (q.from, q.where_clause, q.ctes)
    } else {
        let alias = alias_of(&q).to_string();
        let mut q = q;
        let ctes = std::mem::take(&mut q.ctes);
        (as_table(q, &alias), None, ctes)
    }
}

fn alias_of(q: &Query) -> &str {
    match &q.from {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => alias,
        TableRef::Join { .. } => "_q",
    }
}

fn and_where(q: &mut Query, pred: Expr) {
    q.where_clause = Some(match q.where_clause.take() {
        Some(existing) => Expr::and(existing, pred),
        None => pred,
    });
}

/// A union's output columns are named after its first arm's aliases; project
/// them explicitly so later passes can see which aliases the query returns.
fn default_select(from: &TableRef) -> Vec<SelectExpr> {
    if let TableRef::Union { queries, alias } = from
        && let Some(first) = queries.first()
        && first.select.iter().all(|s| s.alias.is_some())
    {
        return first
            .select
            .iter()
            .map(|s| SelectExpr::col(alias.as_str(), s.alias.as_deref().unwrap()))
            .collect();
    }
    vec![SelectExpr::star()]
}
