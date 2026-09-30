use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::error::Result;
use crate::passes::plan::*;
use crate::passes::shared::latest_row_dedup;
pub(super) use crate::passes::shared::{latest_node_predicates, node_select_columns};

pub(super) enum NarrowSource {
    Cte(String),
}

pub(super) fn emit_node_join_with_narrowing(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    node_column: &str,
    narrow: Option<NarrowSource>,
    sort_key: &[String],
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let alias = &np.alias;
    let in_predicate = narrow.map(|NarrowSource::Cte(cte_name)| Expr::InSubquery {
        expr: Box::new(Expr::col(alias, node_column)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    });
    let scan = super::physical::emit(&physical::PhysicalPlan::node_scan(
        np,
        in_predicate,
        sort_key,
    )?)?;
    let joined = TableRef::join(
        JoinType::Inner,
        from,
        scan.from,
        Expr::eq(
            Expr::col(alias, node_column),
            Expr::col(edge_alias, edge_col),
        ),
    );
    Ok((joined, scan.select, vec![]))
}

pub(super) fn emit_filter_subquery(
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    node_column: &str,
    ctes: &mut Vec<Cte>,
) -> Result<Vec<Expr>> {
    let alias = &np.alias;
    let cte_name = format!("_filter_{alias}");
    let keys = physical::PhysicalPlan::filtered_keys(np, node_column)?;
    ctes.push(Cte::new(&cte_name, super::physical::query(&keys)));
    Ok(vec![Expr::InSubquery {
        expr: Box::new(Expr::col(edge_alias, edge_col)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    }])
}

pub(super) fn limit_by_scan(
    table: &str,
    alias: &str,
    select: Vec<SelectExpr>,
    sort_key: &[String],
    where_predicates: Vec<Expr>,
) -> TableRef {
    let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
    let query = Query {
        select,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(where_predicates),
        order_by,
        limit_by,
        ..Default::default()
    };
    TableRef::subquery(query, alias)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_filter_narrowing(
    where_parts: &mut Vec<Expr>,
    hop: &Hop,
    plan: &physical::FlatPlan,
    edge_alias: &str,
    start_col: &str,
    end_col: &str,
    ctes: &mut Vec<Cte>,
    narrowed: &mut HashSet<String>,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(keys) = plan.narrowing.get(node_alias) else {
            continue;
        };
        let cte_name = format!("_filter_{node_alias}");
        if narrowed.insert(node_alias.clone()) {
            ctes.push(Cte::new(&cte_name, super::physical::query(keys)));
        }
        where_parts.push(Expr::InSubquery {
            expr: Box::new(Expr::col(edge_alias, id_col)),
            cte_name,
            column: "id".to_string(),
        });
    }
}
