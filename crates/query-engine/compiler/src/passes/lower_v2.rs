use crate::ast::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::plan_v2::PhysOp;
use crate::passes::shared::filter_to_expr;

pub fn lower(op: PhysOp, input: &Input) -> Result<Node> {
    let mut q = emit(op);
    for jp in &input.join_predicates {
        let filter = InputFilter {
            op: Some(jp.op),
            rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
            ..Default::default()
        };
        let pred = filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter);
        q.where_clause = Some(match q.where_clause.take() {
            Some(existing) => Expr::and(existing, pred),
            None => pred,
        });
    }
    Ok(Node::Query(Box::new(q)))
}

fn emit(op: PhysOp) -> Query {
    match op {
        PhysOp::Scan { table, alias, dedup, predicates, select } => {
            let from = if dedup {
                TableRef::scan_final(&table, &alias)
            } else {
                TableRef::scan(&table, &alias)
            };
            let sel = if select.is_empty() { vec![SelectExpr::star()] } else { select };
            Query {
                select: sel,
                from,
                where_clause: Expr::conjoin(predicates),
                ..Default::default()
            }
        }

        PhysOp::Join { left, right, on } => {
            let lq = emit(*left);
            let rq = emit(*right);
            let rhs = subquery_wrap(rq);
            Query {
                select: lq.select,
                from: TableRef::join(JoinType::Inner, lq.from, rhs, on),
                where_clause: lq.where_clause,
                ctes: lq.ctes,
                ..Default::default()
            }
        }

        PhysOp::Union { arms, alias } => {
            let queries: Vec<Query> = arms.into_iter().map(emit).collect();
            Query {
                from: TableRef::union_all(queries, &alias),
                ..Default::default()
            }
        }

        PhysOp::UnionQueries { arms, alias, outer_predicates } => Query {
            from: TableRef::union_all(arms, &alias),
            where_clause: Expr::conjoin(outer_predicates),
            ..Default::default()
        },

        PhysOp::Cte { name, body, consumer } => {
            let body_q = emit(*body);
            let mut con_q = emit(*consumer);
            con_q.ctes.insert(0, Cte::new(&name, body_q));
            con_q
        }

        PhysOp::TopN { input, select, order_by, limit } => {
            let mut q = emit(*input);
            q.select.extend(select);
            q.order_by = order_by;
            q.limit = Some(limit);
            q
        }

        PhysOp::Aggregate { input, select, group_by, order_by, limit } => {
            let mut q = emit(*input);
            q.select.extend(select);
            q.group_by = group_by;
            q.order_by = order_by;
            q.limit = Some(limit);
            q
        }
    }
}

fn subquery_wrap(q: Query) -> TableRef {
    let alias = match &q.from {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => alias.clone(),
        TableRef::Join { .. } => "_j".to_string(),
    };
    TableRef::subquery(q, &alias)
}
