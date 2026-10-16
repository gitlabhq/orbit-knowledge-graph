//! Under FINAL, ClickHouse expands skip-index selections back to every granule
//! whose primary-key range overlaps them, so a range on a time column can read
//! the whole table. When the column rises with `id`, the same range gives an
//! `id` lower bound that the primary key prunes on directly. The bound reads
//! without FINAL and skips tombstones: the latest live version of every
//! matching row is in that set, so no row is lost.

use ontology::constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN};
use query_data_model::OrbitQueryModel;

use crate::ast::visit::{visit_queries_mut, visit_relations};
use crate::ast::{Expr, Node, Op, Query, SelectExpr, TableRef};
use crate::error::Result;
use crate::input::AggFunction;

const BOUND_ALIAS: &str = "_id_bound";

pub fn apply(node: &mut Node, model: &(impl OrbitQueryModel + ?Sized)) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    visit_queries_mut(query, &mut |query| {
        bound_final_scans(query, model);
        Ok(())
    })
}

fn bound_final_scans(query: &mut Query, model: &(impl OrbitQueryModel + ?Sized)) {
    let Some(where_clause) = &query.where_clause else {
        return;
    };

    let mut bounds = Vec::new();
    visit_relations(&query.from, &mut |relation| {
        if let TableRef::Scan {
            table,
            alias,
            final_: true,
            ..
        } = relation
            && model.in_sort_key(table, DEFAULT_PRIMARY_KEY)
        {
            for (column, op, value) in lower_bounds(where_clause, alias) {
                if model.column_rises_with_id(table, column) {
                    bounds.push(id_lower_bound(table, alias, column, op, value));
                }
            }
        }
    });

    query.where_clause = Expr::and_all(
        std::iter::once(query.where_clause.take()).chain(bounds.into_iter().map(Some)),
    );
}

fn lower_bounds<'a>(expr: &'a Expr, alias: &str) -> Vec<(&'a str, Op, &'a Expr)> {
    match expr {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => [lower_bounds(left, alias), lower_bounds(right, alias)].concat(),
        Expr::BinaryOp {
            op: op @ (Op::Ge | Op::Gt),
            left,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { table, column }, value @ (Expr::Param { .. } | Expr::Literal(_)))
                if table == alias =>
            {
                vec![(column.as_str(), *op, value)]
            }
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn id_lower_bound(table: &str, alias: &str, column: &str, op: Op, value: &Expr) -> Expr {
    let min_id = Query {
        select: vec![SelectExpr::new(
            Expr::aggregate(
                AggFunction::Min,
                Some(Expr::col(BOUND_ALIAS, DEFAULT_PRIMARY_KEY)),
            ),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(table, BOUND_ALIAS),
        where_clause: Some(Expr::and(
            Expr::binary(op, Expr::col(BOUND_ALIAS, column), value.clone()),
            Expr::eq(Expr::col(BOUND_ALIAS, DELETED_COLUMN), Expr::lit(false)),
        )),
        ..Default::default()
    };
    Expr::binary(
        Op::Ge,
        Expr::col(alias, DEFAULT_PRIMARY_KEY),
        Expr::Scalar(Box::new(min_id)),
    )
}
