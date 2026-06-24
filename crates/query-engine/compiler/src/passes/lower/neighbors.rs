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
    dedup_subquery, deleted_false, denorm_tag_expr, edge_table_scan_filtered, filter_to_expr,
    id_list_predicate, id_range_predicate, rel_kind_filter,
};

use super::pagination::{CursorKey, apply_keyset};

const NEIGHBORS_PAGE_ALIAS: &str = "_gkg_neighbors_page";
const NEIGHBORS_SORT_COLUMN: &str = "_gkg_neighbors_sort_0";

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_neighbors(
    plan: &Plan,
    center_alias: &str,
    direction: Direction,
    edge: &EdgeTableConfig,
    has_non_denorm: bool,
    center_tp_lookup: Option<&(String, String)>,
) -> Result<Node> {
    let cnp = &plan.nodes[center_alias];
    let center_id = center_alias.to_string();
    let center_entity = cnp.entity.clone().unwrap_or_default();
    let center_table = cnp.table.clone().unwrap_or_default();
    let center_uses_default_pk = cnp.uses_default_pk();
    let center_redaction_col = cnp.redaction_id_column.clone();
    let center_has_tp = cnp.has_traversal_path;
    let center_node_ids = cnp.node_ids.clone();
    let center_filters = cnp.filters.clone();
    let center_id_range = cnp.id_range.clone();
    let edge_table: Vec<String> = {
        let mut t = edge.outgoing_tables.clone();
        t.extend(edge.incoming_tables.iter().cloned());
        t.sort();
        t.dedup();
        t
    };
    let edge_alias = "e";

    fn build_center_dedup(
        alias: &str,
        table: &str,
        filters: &[(String, InputFilter)],
        node_ids: &[i64],
        id_range: Option<&InputIdRange>,
        extra_select: &[&str],
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
        let mut select = vec![
            SelectExpr::col(alias, DEFAULT_PRIMARY_KEY),
            SelectExpr::col(alias, DELETED_COLUMN),
        ];
        for col in extra_select {
            select.push(SelectExpr::col(alias, *col));
        }
        dedup_subquery(alias, table, select, scan_where)
    }

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

        let denorm_dir = if dir == Direction::Outgoing {
            "source"
        } else {
            "target"
        };

        let arm_where = |a: &str| -> Vec<Expr> {
            let mut wp = vec![Expr::eq(
                Expr::col(a, center_kind_col),
                Expr::string(&center_entity),
            )];
            if !center_node_ids.is_empty() {
                wp.push(id_list_predicate(a, center_edge_col, &center_node_ids));
            }
            if let Some(ref types) = edge.rel_type_filter
                && let Some(f) = rel_kind_filter(a, types)
            {
                wp.push(f);
            }
            // Incoming edges to a namespace center sit at the center's own tp; pin to the resolved paths for a leading-PK point lookup.
            if dir == Direction::Incoming
                && !center_node_ids.is_empty()
                && let Some((src, key_col)) = center_tp_lookup
            {
                wp.push(Expr::InSelect {
                    expr: Box::new(Expr::col(a, TRAVERSAL_PATH_COLUMN)),
                    query: Box::new(Query {
                        select: vec![SelectExpr::col("_tpd", TRAVERSAL_PATH_COLUMN)],
                        from: TableRef::scan(src.as_str(), "_tpd"),
                        where_clause: Expr::conjoin(vec![
                            id_list_predicate("_tpd", key_col, &center_node_ids),
                            deleted_false("_tpd"),
                        ]),
                        ..Default::default()
                    }),
                });
            }
            wp.push(deleted_false(a));
            wp
        };

        let mut where_parts: Vec<Expr> = Vec::new();
        // Denorm tags aren't in the per-arm projection, so they filter the union output alias.
        for (prop, filter) in &center_filters {
            let key = (center_entity.clone(), prop.clone(), denorm_dir.to_string());
            if let Some((tag_col, tag_key)) = plan.denorm_columns.get(&key)
                && let Some(expr) = denorm_tag_expr(edge_alias, tag_col, tag_key, filter)
            {
                where_parts.push(expr);
            }
        }

        let mut select = vec![
            SelectExpr::new(Expr::col(edge_alias, neighbor_id), neighbor_id_column()),
            SelectExpr::new(Expr::col(edge_alias, neighbor_type), neighbor_type_column()),
            SelectExpr::new(
                Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN),
                relationship_type_column(),
            ),
            SelectExpr::new(Expr::int(is_outgoing), neighbor_is_outgoing_column()),
        ];

        let arm_tables = if dir == Direction::Outgoing {
            &edge.outgoing_tables
        } else {
            &edge.incoming_tables
        };
        let (mut from, outer_pushed) = edge_table_scan_filtered(arm_tables, edge_alias, arm_where);
        where_parts.extend(outer_pushed);
        let needs_center_table = !center_uses_default_pk;

        if has_non_denorm {
            let redaction_col = center_redaction_col.as_str();
            let extra: Vec<&str> = if needs_center_table {
                vec![redaction_col]
            } else {
                Vec::new()
            };
            let (center_subq, deleted_filter) = build_center_dedup(
                &center_id,
                &center_table,
                &center_filters,
                &center_node_ids,
                center_id_range.as_ref(),
                &extra,
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
            if !has_non_denorm {
                from = TableRef::join(
                    JoinType::Inner,
                    from,
                    TableRef::scan_final(&center_table, &center_id),
                    Expr::eq(
                        Expr::col(edge_alias, center_edge_col),
                        Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                    ),
                );
                where_parts.push(deleted_false(&center_id));
            }
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

        if center_has_tp {
            select.push(SelectExpr::new(
                Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
                traversal_path_column(&center_id),
            ));
        }

        Query {
            select,
            from,
            where_clause: Expr::conjoin(where_parts),
            ..Default::default()
        }
    };

    // When the center is default-PK with no non-denormalized filters and a single
    // physical edge table, each arm degenerates to a plain edge scan, so both
    // directions collapse into ONE scan: each matching row emits its applicable
    // neighbor row(s) via arrayJoin over the matched direction tuples. A self-loop
    // (source==target==center) matches both arms and still yields two rows.
    let fused_both_eligible = direction == Direction::Both
        && !has_non_denorm
        && center_uses_default_pk
        && edge_table.len() == 1;

    if fused_both_eligible {
        let q = build_fused_both_arm(
            &center_id,
            &center_entity,
            center_has_tp,
            &center_node_ids,
            &center_filters,
            plan,
            edge,
            &edge_table[0],
            edge_alias,
        );
        let q = page_neighbors(q, plan, &center_id)?;
        Ok(Node::Query(Box::new(q)))
    } else if direction == Direction::Both {
        let mut outgoing = build_arm(Direction::Outgoing);
        outgoing.union_all = vec![build_arm(Direction::Incoming)];
        let outgoing = page_neighbors(outgoing, plan, &center_id)?;
        Ok(Node::Query(Box::new(outgoing)))
    } else {
        let arm = page_neighbors(build_arm(direction), plan, &center_id)?;
        Ok(Node::Query(Box::new(arm)))
    }
}

fn page_neighbors(mut inner: Query, plan: &Plan, center_id: &str) -> Result<Query> {
    if let Some(ob) = &plan.order_by {
        let sort_select = SelectExpr::new(
            Expr::col(&ob.node, &ob.property),
            NEIGHBORS_SORT_COLUMN.to_string(),
        );
        inner.select.push(sort_select.clone());
        for arm in &mut inner.union_all {
            arm.select.push(sort_select.clone());
        }
    }

    let select = inner
        .select
        .iter()
        .filter_map(|item| {
            item.alias
                .as_ref()
                .map(|alias| SelectExpr::new(Expr::col(NEIGHBORS_PAGE_ALIAS, alias), alias.clone()))
        })
        .collect();
    let keys = neighbor_keys(plan, center_id);
    let mut outer = Query {
        select,
        from: TableRef::Subquery {
            query: Box::new(inner),
            alias: NEIGHBORS_PAGE_ALIAS.to_string(),
        },
        limit: Some(plan.limit),
        ..Default::default()
    };
    apply_keyset(&mut outer, &keys, plan.cursor.as_ref(), false)?;
    Ok(outer)
}

fn neighbor_keys(plan: &Plan, center_id: &str) -> Vec<CursorKey> {
    let mut keys = Vec::new();
    if let Some(ob) = &plan.order_by {
        keys.push(CursorKey::new(
            Expr::col(NEIGHBORS_PAGE_ALIAS, NEIGHBORS_SORT_COLUMN),
            crate::passes::shared::data_type_to_ch(ob.data_type.as_ref()),
            ob.direction == OrderDirection::Desc,
            keys.len(),
        ));
    }
    keys.extend([
        CursorKey::new(
            Expr::col(NEIGHBORS_PAGE_ALIAS, redaction_id_column(center_id)),
            ChType::Int64,
            false,
            keys.len(),
        ),
        CursorKey::new(
            Expr::col(NEIGHBORS_PAGE_ALIAS, neighbor_id_column()),
            ChType::Int64,
            false,
            keys.len() + 1,
        ),
        CursorKey::new(
            Expr::col(NEIGHBORS_PAGE_ALIAS, relationship_type_column()),
            ChType::String,
            false,
            keys.len() + 2,
        ),
        CursorKey::new(
            Expr::col(NEIGHBORS_PAGE_ALIAS, neighbor_is_outgoing_column()),
            ChType::Int64,
            false,
            keys.len() + 3,
        ),
    ]);
    keys
}

/// Direction::Both collapsed into a single edge scan (see `fused_both_eligible`).
///
/// Inner query: scan the edge once with `WHERE (source side) OR (target side)`,
/// projecting `arrayJoin(arrayFilter(matched, [out_tuple, in_tuple]))` so each
/// row yields one entry per matched arm. Outer query: project the `_gkg_*`
/// columns out of the tuple. `arrayFilter` (not `multiIf`) keeps self-loop
/// semantics: when both arms match the same edge, both rows are emitted.
#[allow(clippy::too_many_arguments)]
fn build_fused_both_arm(
    center_id: &str,
    center_entity: &str,
    center_has_tp: bool,
    center_node_ids: &[i64],
    center_filters: &[(String, InputFilter)],
    plan: &Plan,
    edge: &EdgeTableConfig,
    edge_table: &str,
    edge_alias: &str,
) -> Query {
    let arm_predicate = |kind_col: &str, id_col: &str, denorm_dir: &str| -> Expr {
        let mut parts = vec![Expr::eq(
            Expr::col(edge_alias, kind_col),
            Expr::string(center_entity),
        )];
        if !center_node_ids.is_empty() {
            parts.push(id_list_predicate(edge_alias, id_col, center_node_ids));
        }
        for (prop, filter) in center_filters {
            let key = (
                center_entity.to_string(),
                prop.clone(),
                denorm_dir.to_string(),
            );
            if let Some((tag_col, tag_key)) = plan.denorm_columns.get(&key)
                && let Some(expr) = denorm_tag_expr(edge_alias, tag_col, tag_key, filter)
            {
                parts.push(expr);
            }
        }
        Expr::conjoin(parts).expect("fused arm predicate always has the center-kind conjunct")
    };

    let source_arm = arm_predicate(SOURCE_KIND_COLUMN, SOURCE_ID_COLUMN, "source");
    let target_arm = arm_predicate(TARGET_KIND_COLUMN, TARGET_ID_COLUMN, "target");

    // (matched, is_outgoing, neighbor_id, neighbor_kind, center_id)
    let out_tuple = Expr::func(
        "tuple",
        vec![
            source_arm.clone(),
            Expr::int(1),
            Expr::col(edge_alias, TARGET_ID_COLUMN),
            Expr::col(edge_alias, TARGET_KIND_COLUMN),
            Expr::col(edge_alias, SOURCE_ID_COLUMN),
        ],
    );
    let in_tuple = Expr::func(
        "tuple",
        vec![
            target_arm.clone(),
            Expr::int(0),
            Expr::col(edge_alias, SOURCE_ID_COLUMN),
            Expr::col(edge_alias, SOURCE_KIND_COLUMN),
            Expr::col(edge_alias, TARGET_ID_COLUMN),
        ],
    );
    let matched_only = Expr::func(
        "arrayFilter",
        vec![
            Expr::lambda(
                "_gkg_arm",
                Expr::func("tupleElement", vec![Expr::ident("_gkg_arm"), Expr::int(1)]),
            ),
            Expr::func("array", vec![out_tuple, in_tuple]),
        ],
    );
    let dir_row = Expr::func("arrayJoin", vec![matched_only]);

    const ROW_COL: &str = "_gkg_arm_row";
    let rel_col = relationship_type_column();
    let tp_col = traversal_path_column(center_id);
    let mut inner_select = vec![
        SelectExpr::new(dir_row, ROW_COL),
        SelectExpr::new(Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN), rel_col),
    ];
    if center_has_tp {
        inner_select.push(SelectExpr::new(
            Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
            tp_col.clone(),
        ));
    }

    let mut where_parts = vec![Expr::binary(Op::Or, source_arm, target_arm)];
    if let Some(ref types) = edge.rel_type_filter
        && let Some(f) = rel_kind_filter(edge_alias, types)
    {
        where_parts.push(f);
    }
    where_parts.push(deleted_false(edge_alias));

    let inner = Query {
        select: inner_select,
        from: TableRef::scan(edge_table, edge_alias),
        where_clause: Expr::conjoin(where_parts),
        ..Default::default()
    };

    let inner_alias = "_gkg_fused";
    let te = |n: i64| {
        Expr::func(
            "tupleElement",
            vec![Expr::col(inner_alias, ROW_COL), Expr::int(n)],
        )
    };
    let mut select = vec![
        SelectExpr::new(te(3), neighbor_id_column()),
        SelectExpr::new(te(4), neighbor_type_column()),
        SelectExpr::new(Expr::col(inner_alias, rel_col), rel_col),
        SelectExpr::new(te(2), neighbor_is_outgoing_column()),
        SelectExpr::new(te(5), redaction_id_column(center_id)),
        SelectExpr::new(
            Expr::string(center_entity),
            redaction_type_column(center_id),
        ),
    ];
    if center_has_tp {
        select.push(SelectExpr::new(
            Expr::col(inner_alias, &tp_col),
            tp_col.clone(),
        ));
    }

    Query {
        select,
        from: TableRef::Subquery {
            query: Box::new(inner),
            alias: inner_alias.to_string(),
        },
        ..Default::default()
    }
}
