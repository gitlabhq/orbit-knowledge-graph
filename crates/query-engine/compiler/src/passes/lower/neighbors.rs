//! Neighbors emit: single-hop edge scan for adjacent entities.
//!
//! For Direction::Both, produces outgoing UNION ALL incoming.
//! The enforce pass handles _gkg_* column injection (Neighbors emits
//! its own redaction columns directly since the center edge column
//! differs per direction arm).

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;

use crate::passes::plan::{EdgeTableConfig, Plan};
use crate::passes::shared::{
    dedup_subquery, deleted_false, denorm_tag_expr, edge_table_scan, filter_to_expr,
    id_list_predicate, id_range_predicate, rel_kind_filter,
};

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_neighbors(
    plan: &Plan,
    center_alias: &str,
    direction: Direction,
    edge: &EdgeTableConfig,
    has_non_denorm: bool,
) -> Result<Node> {
    let cnp = &plan.nodes[center_alias];
    let center_id = center_alias.to_string();
    let center_entity = cnp.entity.clone().unwrap_or_default();
    let center_table = cnp.table.clone().unwrap_or_default();
    let center_uses_default_pk = cnp.uses_default_pk();
    let center_redaction_col = cnp.redaction_id_column.clone();
    let center_node_ids = cnp.node_ids.clone();
    let center_filters = cnp.filters.clone();
    let center_id_range = cnp.id_range.clone();
    let edge_table = edge.tables.clone();
    let edge_alias = "e";

    fn build_center_dedup(
        alias: &str,
        table: &str,
        filters: &[(String, InputFilter)],
        node_ids: &[i64],
        id_range: Option<&InputIdRange>,
    ) -> (TableRef, Expr) {
        let mut scan_where = Vec::new();
        for (prop, filter) in filters {
            scan_where.push(filter_to_expr(alias, prop, filter));
        }
        if !node_ids.is_empty() {
            scan_where.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, node_ids));
        }
        if let Some(range) = id_range {
            scan_where.push(id_range_predicate(alias, range));
        }
        let select = vec![
            SelectExpr::col(alias, DEFAULT_PRIMARY_KEY),
            SelectExpr::col(alias, DELETED_COLUMN),
        ];
        dedup_subquery(alias, table, select, scan_where, DEFAULT_PRIMARY_KEY)
    }

    let edge_tiebreakers = || -> Vec<OrderExpr> {
        vec![
            OrderExpr::asc(Expr::col(edge_alias, SOURCE_ID_COLUMN)),
            OrderExpr::asc(Expr::col(edge_alias, TARGET_ID_COLUMN)),
            OrderExpr::asc(Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN)),
        ]
    };
    let order_by = match &plan.order_by {
        Some(ob) => {
            let mut exprs = vec![if ob.direction == OrderDirection::Desc {
                OrderExpr::desc(Expr::col(&ob.node, &ob.property))
            } else {
                OrderExpr::asc(Expr::col(&ob.node, &ob.property))
            }];
            if plan.cursor.is_some() {
                exprs.extend(edge_tiebreakers());
            }
            exprs
        }
        None if plan.cursor.is_some() => edge_tiebreakers(),
        None => vec![],
    };

    let build_arm = |dir: Direction| -> Query {
        let (center_edge_col, center_kind_col, neighbor_id, neighbor_type, is_outgoing) = match dir
        {
            Direction::Outgoing => (
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                1i64,
            ),
            Direction::Incoming => (
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                0i64,
            ),
            Direction::Both => unreachable!(),
        };

        let mut where_parts: Vec<Expr> = Vec::new();

        // Center entity kind.
        where_parts.push(Expr::eq(
            Expr::col(edge_alias, center_kind_col),
            Expr::string(&center_entity),
        ));

        // Push node_ids directly on edge column.
        if !center_node_ids.is_empty() {
            where_parts.push(id_list_predicate(
                edge_alias,
                center_edge_col,
                &center_node_ids,
            ));
        }

        let denorm_dir = if dir == Direction::Outgoing {
            "source"
        } else {
            "target"
        };
        for (prop, filter) in &center_filters {
            let key = (center_entity.clone(), prop.clone(), denorm_dir.to_string());
            if let Some((tag_col, tag_key)) = plan.denorm_columns.get(&key)
                && let Some(expr) = denorm_tag_expr(edge_alias, tag_col, tag_key, filter)
            {
                where_parts.push(expr);
            }
        }

        if let Some(ref types) = edge.rel_type_filter
            && let Some(f) = rel_kind_filter(edge_alias, types)
        {
            where_parts.push(f);
        }

        where_parts.push(deleted_false(edge_alias));

        let mut select = vec![
            SelectExpr::new(Expr::col(edge_alias, neighbor_id), neighbor_id_column()),
            SelectExpr::new(Expr::col(edge_alias, neighbor_type), neighbor_type_column()),
            SelectExpr::new(
                Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN),
                relationship_type_column(),
            ),
            SelectExpr::new(Expr::int(is_outgoing), neighbor_is_outgoing_column()),
        ];

        let mut from: TableRef = edge_table_scan(&edge_table, edge_alias);

        // Non-denorm filters: inline dedup JOIN instead of CTE.
        if has_non_denorm {
            let (center_subq, deleted_filter) = build_center_dedup(
                &center_id,
                &center_table,
                &center_filters,
                &center_node_ids,
                center_id_range.as_ref(),
            );
            from = TableRef::join(
                JoinType::Inner,
                from,
                center_subq,
                Expr::eq(
                    Expr::col(edge_alias, center_edge_col),
                    Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                ),
            );
            where_parts.push(deleted_filter);
        }

        if center_uses_default_pk {
            select.push(SelectExpr::new(
                Expr::col(edge_alias, center_edge_col),
                redaction_id_column(&center_id),
            ));
        } else {
            from = TableRef::join(
                JoinType::Inner,
                from,
                TableRef::scan(&center_table, &center_id),
                Expr::eq(
                    Expr::col(edge_alias, center_edge_col),
                    Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                ),
            );
            select.push(SelectExpr::new(
                Expr::col(&center_id, &center_redaction_col),
                redaction_id_column(&center_id),
            ));
            select.push(SelectExpr::new(
                Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                primary_key_column(&center_id),
            ));
        }
        select.push(SelectExpr::new(
            Expr::string(&center_entity),
            redaction_type_column(&center_id),
        ));

        Query {
            select,
            from,
            where_clause: Expr::conjoin(where_parts),
            ..Default::default()
        }
    };

    if direction == Direction::Both {
        let mut outgoing = build_arm(Direction::Outgoing);
        outgoing.union_all = vec![build_arm(Direction::Incoming)];
        outgoing.order_by = order_by;
        outgoing.limit = Some(plan.limit);
        Ok(Node::Query(Box::new(outgoing)))
    } else {
        let mut arm = build_arm(direction);
        arm.order_by = order_by;
        arm.limit = Some(plan.limit);
        Ok(Node::Query(Box::new(arm)))
    }
}
