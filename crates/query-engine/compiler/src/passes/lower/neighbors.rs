//! Neighbors emits its own redaction columns directly (rather than relying on
//! the enforce pass's _orbit_* injection) since the center edge column differs
//! per direction arm.

use ontology::constants::*;

use super::NodeBinding;
use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;

use super::sql::{
    dedup_query, dedup_subquery, deleted_false, denorm_tag_expr, filter_to_expr, id_list_predicate,
    id_range_predicate, rel_kind_filter,
};
use crate::passes::plan::{EdgeTableConfig, NeighborArm, Neighbors, Plan};
use crate::scope::{PathScopeId, lookup_expr};

pub fn emit_neighbors(plan: &Plan<Neighbors>, input: &Input) -> Result<(Node, NodeBinding)> {
    let Neighbors {
        center: center_alias,
        direction,
        edge,
        has_non_denorm,
        fused_table,
        outgoing,
        incoming,
    } = &plan.operation;
    let direction = *direction;
    let has_non_denorm = *has_non_denorm;
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
    let edge_alias = "e";

    let anchor_where = |alias: &str| -> Vec<Expr> {
        let mut parts: Vec<Expr> = center_filters
            .iter()
            .map(|(prop, filter)| filter_to_expr(alias, prop, filter))
            .collect();
        if !center_node_ids.is_empty() {
            parts.push(id_list_predicate(
                alias,
                DEFAULT_PRIMARY_KEY,
                &center_node_ids,
            ));
        }
        if let Some(range) = &center_id_range {
            parts.push(id_range_predicate(alias, range));
        }
        parts
    };

    let center_lookup = |edge: &str, column: &str, select: &str, alias: &str| {
        let mut scan_where = anchor_where(alias);
        scan_where.push(deleted_false(alias));
        Expr::InSelect {
            expr: Box::new(Expr::col(edge, column)),
            query: Box::new(dedup_query(
                alias,
                &center_table,
                vec![SelectExpr::col(alias, select)],
                scan_where,
            )),
        }
    };
    let center_path_lookup = |edge: &str| match center_node_ids.as_slice() {
        [id] if center_filters.is_empty() && center_id_range.is_none() => Expr::eq(
            Expr::col(edge, TRAVERSAL_PATH_COLUMN),
            lookup_expr(
                &center_table,
                DEFAULT_PRIMARY_KEY,
                &PathScopeId::Numeric(*id),
            ),
        ),
        _ => center_lookup(edge, TRAVERSAL_PATH_COLUMN, TRAVERSAL_PATH_COLUMN, "_tpd"),
    };

    let order_by = match &input.order_by {
        Some(ob) => vec![if ob.direction == OrderDirection::Desc {
            OrderExpr::desc(Expr::col(&ob.node, &ob.property))
        } else {
            OrderExpr::asc(Expr::col(&ob.node, &ob.property))
        }],
        None => vec![],
    };

    let build_arm = |dir: Direction| -> Query {
        let (center_edge_col, center_kind_col, neighbor_id, neighbor_type, is_outgoing, arms) =
            match dir {
                Direction::Outgoing => (
                    SOURCE_ID_COLUMN,
                    SOURCE_KIND_COLUMN,
                    TARGET_ID_COLUMN,
                    TARGET_KIND_COLUMN,
                    1i64,
                    outgoing,
                ),
                Direction::Incoming => (
                    TARGET_ID_COLUMN,
                    TARGET_KIND_COLUMN,
                    SOURCE_ID_COLUMN,
                    SOURCE_KIND_COLUMN,
                    0i64,
                    incoming,
                ),
                Direction::Both => unreachable!(),
            };

        let denorm_direction = if dir == Direction::Outgoing {
            query_data_model::DenormalizedDirection::Source
        } else {
            query_data_model::DenormalizedDirection::Target
        };

        let center_id_lookup =
            |a: &str| center_lookup(a, center_edge_col, DEFAULT_PRIMARY_KEY, "_ctr");
        let arm_where = |a: &str, arm: &NeighborArm| -> Vec<Expr> {
            let mut wp = vec![Expr::eq(
                Expr::col(a, center_kind_col),
                Expr::string(&center_entity),
            )];
            if !center_node_ids.is_empty() {
                wp.push(id_list_predicate(a, center_edge_col, &center_node_ids));
            } else if has_non_denorm {
                wp.push(center_id_lookup(a));
            }
            if let Some(f) = rel_kind_filter(a, &arm.kinds) {
                wp.push(f);
            }
            if arm.anchored {
                wp.push(center_path_lookup(a));
            } else if dir == Direction::Incoming && !center_node_ids.is_empty() && center_has_tp {
                wp.push(center_id_lookup(a));
            }
            wp.push(deleted_false(a));
            wp
        };

        let mut where_parts: Vec<Expr> = Vec::new();
        // Denorm tags aren't in the per-arm projection, so they filter the union output alias.
        for (_, filter) in &center_filters {
            let Some(property) = filter.property else {
                continue;
            };
            let key = query_data_model::DenormalizedKey {
                property,
                direction: denorm_direction,
            };
            if let Some(facts) = plan.denormalized.get(&key)
                && let Some(expr) = denorm_tag_expr(
                    edge_alias,
                    &facts.edge_column,
                    &facts.tag_key,
                    &filter.filter,
                )
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

        let (mut from, outer_pushed) = arm_scans(arms, edge_alias, arm_where);
        where_parts.extend(outer_pushed);
        let needs_center_table = !center_uses_default_pk;

        if has_non_denorm {
            let mut center_select = vec![
                SelectExpr::col(&center_id, DEFAULT_PRIMARY_KEY),
                SelectExpr::col(&center_id, DELETED_COLUMN),
            ];
            if needs_center_table {
                center_select.push(SelectExpr::col(&center_id, &center_redaction_col));
            }
            let (center_subq, deleted_filter) = dedup_subquery(
                &center_id,
                &center_table,
                center_select,
                anchor_where(&center_id),
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

    let query = if let Some(table) = fused_table {
        let mut q = build_fused_both_arm(
            &center_id,
            &center_entity,
            center_has_tp,
            &center_node_ids,
            &center_filters,
            plan,
            edge,
            table,
            edge_alias,
        );
        q.order_by = order_by;
        q.limit = Some(input.limit);
        q
    } else if direction == Direction::Both {
        let mut outgoing = build_arm(Direction::Outgoing);
        outgoing.union_all = vec![build_arm(Direction::Incoming)];
        outgoing.order_by = order_by;
        outgoing.limit = Some(input.limit);
        outgoing
    } else {
        let mut arm = build_arm(direction);
        arm.order_by = order_by;
        arm.limit = Some(input.limit);
        arm
    };
    let role_identity = (!has_non_denorm && center_uses_default_pk).then(|| {
        query
            .select
            .iter()
            .find(|select| {
                select.alias.as_deref() == Some(redaction_id_column(center_alias).as_str())
            })
            .expect("neighbors emits its center identity")
            .expr
            .clone()
    });
    Ok((
        Node::Query(Box::new(query)),
        NodeBinding::Projected { role_identity },
    ))
}

fn arm_scans(
    arms: &[NeighborArm],
    alias: &str,
    arm_where: impl Fn(&str, &NeighborArm) -> Vec<Expr>,
) -> (TableRef, Vec<Expr>) {
    if let [arm] = arms {
        return (TableRef::scan(&arm.table, alias), arm_where(alias, arm));
    }
    let inner_alias = format!("_{alias}");
    let mut common_cols: Vec<SelectExpr> = EDGE_RESERVED_COLUMNS
        .iter()
        .map(|col| SelectExpr::col(&inner_alias, *col))
        .collect();
    common_cols.push(SelectExpr::col(&inner_alias, DELETED_COLUMN));
    let scans: Vec<Query> = arms
        .iter()
        .map(|arm| Query {
            select: common_cols.clone(),
            from: TableRef::scan(&arm.table, &inner_alias),
            where_clause: Expr::conjoin(arm_where(&inner_alias, arm)),
            ..Default::default()
        })
        .collect();
    (TableRef::union_all(scans, alias), Vec::new())
}

/// Direction::Both collapsed into a single edge scan (see `fused_both_eligible`).
///
/// Inner query: scan the edge once with `WHERE (source side) OR (target side)`,
/// projecting `arrayJoin(arrayFilter(matched, [out_tuple, in_tuple]))` so each
/// row yields one entry per matched arm. Outer query: project the `_orbit_*`
/// columns out of the tuple. `arrayFilter` (not `multiIf`) keeps self-loop
/// semantics: when both arms match the same edge, both rows are emitted.
#[allow(clippy::too_many_arguments)]
fn build_fused_both_arm(
    center_id: &str,
    center_entity: &str,
    center_has_tp: bool,
    center_node_ids: &[i64],
    center_filters: &[(String, crate::passes::plan::BoundFilter)],
    plan: &Plan<Neighbors>,
    edge: &EdgeTableConfig,
    edge_table: &str,
    edge_alias: &str,
) -> Query {
    let arm_predicate = |kind_col: &str,
                         id_col: &str,
                         direction: query_data_model::DenormalizedDirection|
     -> Expr {
        let mut parts = vec![Expr::eq(
            Expr::col(edge_alias, kind_col),
            Expr::string(center_entity),
        )];
        if !center_node_ids.is_empty() {
            parts.push(id_list_predicate(edge_alias, id_col, center_node_ids));
        }
        for (_, filter) in center_filters {
            let Some(property) = filter.property else {
                continue;
            };
            let key = query_data_model::DenormalizedKey {
                property,
                direction,
            };
            if let Some(facts) = plan.denormalized.get(&key)
                && let Some(expr) = denorm_tag_expr(
                    edge_alias,
                    &facts.edge_column,
                    &facts.tag_key,
                    &filter.filter,
                )
            {
                parts.push(expr);
            }
        }
        Expr::conjoin(parts).expect("fused arm predicate always has the center-kind conjunct")
    };

    let source_arm = arm_predicate(
        SOURCE_KIND_COLUMN,
        SOURCE_ID_COLUMN,
        query_data_model::DenormalizedDirection::Source,
    );
    let target_arm = arm_predicate(
        TARGET_KIND_COLUMN,
        TARGET_ID_COLUMN,
        query_data_model::DenormalizedDirection::Target,
    );

    // (matched, is_outgoing, neighbor_id, neighbor_kind, center_id)
    let out_tuple = Expr::func(
        Function::Tuple,
        vec![
            source_arm.clone(),
            Expr::int(1),
            Expr::col(edge_alias, TARGET_ID_COLUMN),
            Expr::col(edge_alias, TARGET_KIND_COLUMN),
            Expr::col(edge_alias, SOURCE_ID_COLUMN),
        ],
    );
    let in_tuple = Expr::func(
        Function::Tuple,
        vec![
            target_arm.clone(),
            Expr::int(0),
            Expr::col(edge_alias, SOURCE_ID_COLUMN),
            Expr::col(edge_alias, SOURCE_KIND_COLUMN),
            Expr::col(edge_alias, TARGET_ID_COLUMN),
        ],
    );
    let matched_only = Expr::func(
        Function::ArrayFilter,
        vec![
            Expr::lambda(
                "_orbit_arm",
                Expr::func(
                    Function::TupleElement,
                    vec![Expr::ident("_orbit_arm"), Expr::int(1)],
                ),
            ),
            Expr::func(Function::Array, vec![out_tuple, in_tuple]),
        ],
    );
    let dir_row = Expr::func(Function::Unnest, vec![matched_only]);

    const ROW_COL: &str = "_orbit_arm_row";
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

    let inner_alias = "_orbit_fused";
    let te = |n: i64| {
        Expr::func(
            Function::TupleElement,
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
