use crate::ast::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::plan_v2::PhysOp;

pub fn lower(op: PhysOp, input: &Input) -> Result<Node> {
    let mut q = emit(op);
    for jp in &input.join_predicates {
        let filter = InputFilter {
            op: Some(jp.op),
            rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
            ..Default::default()
        };
        let pred = crate::passes::shared::filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter);
        q.where_clause = Some(match q.where_clause.take() {
            Some(existing) => Expr::and(existing, pred),
            None => pred,
        });
    }
    Ok(Node::Query(Box::new(q)))
}

fn emit(op: PhysOp) -> Query {
    match op {
        PhysOp::Scan { table, alias, final_, predicates, select } => Query {
            select,
            from: if final_ {
                TableRef::scan_final(&table, &alias)
            } else {
                TableRef::scan(&table, &alias)
            },
            where_clause: Expr::conjoin(predicates),
            ..Default::default()
        },

        PhysOp::Join { left, right, on } => {
            let lq = emit(*left);
            let rq = emit(*right);
            let (rhs_from, rhs_where) = wrap_if_needed(rq);
            Query {
                select: lq.select,
                from: TableRef::join(JoinType::Inner, lq.from, rhs_from, on),
                where_clause: merge_where(lq.where_clause, rhs_where),
                ctes: lq.ctes,
                ..Default::default()
            }
        }

        PhysOp::MultiHopUnion { arms, alias, outer_predicates, select } => Query {
            select,
            from: TableRef::union_all(arms, &alias),
            where_clause: Expr::conjoin(outer_predicates),
            ..Default::default()
        },

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

        PhysOp::Raw(q) => q,
    }
}

fn wrap_if_needed(q: Query) -> (TableRef, Option<Expr>) {
    let alias = extract_alias(&q.from);
    if q.where_clause.is_some() || !q.ctes.is_empty() || q.limit.is_some() {
        let a = alias.unwrap_or_else(|| "_r".to_string());
        let mut wrapped = q;
        if wrapped.select.is_empty() {
            wrapped.select.push(SelectExpr::star());
        }
        (TableRef::subquery(wrapped, &a), None)
    } else {
        (q.from, None)
    }
}

fn extract_alias(tr: &TableRef) -> Option<String> {
    match tr {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => Some(alias.clone()),
        TableRef::Join { .. } => None,
    }
}

fn merge_where(a: Option<Expr>, b: Option<Expr>) -> Option<Expr> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Expr::and(a, b)),
        (a, b) => a.or(b),
    }
}
