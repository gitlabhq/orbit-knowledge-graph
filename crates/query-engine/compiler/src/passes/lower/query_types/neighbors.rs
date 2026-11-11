//! Neighbors emits its own redaction columns directly (rather than relying on
//! the result-contract pass's _orbit_* injection) since the center edge column differs
//! per direction arm.

use ontology::constants::*;

use std::collections::HashMap;

use super::Emitted;
use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::lower::NodeBinding;
use crate::passes::lower::predicates::output_value;

use crate::passes::lower::sql::{
    dedup_subquery, deleted_false, denorm_tag_expr, edge_table_scan_filtered, filter_to_expr,
    id_list_predicate, id_range_predicate, latest_row_dedup, rel_kind_filter,
};
use crate::passes::plan::predicates::Column;
use crate::passes::plan::{Neighbors, Plan, property_value};

pub(crate) fn emit(plan: &Plan<Neighbors>, input: &Input) -> Result<Emitted> {
    let (ast, binding) = emit_neighbors(plan, input)?;
    let stable_order = match plan.operation.direction {
        Direction::Both => vec![
            OrderExpr::asc(Expr::ident(redaction_id_column(&plan.operation.center))),
            OrderExpr::asc(Expr::ident(neighbor_id_column())),
            OrderExpr::asc(Expr::ident(relationship_type_column())),
            OrderExpr::asc(Expr::ident(neighbor_is_outgoing_column())),
        ],
        _ => [
            TRAVERSAL_PATH_COLUMN,
            RELATIONSHIP_KIND_COLUMN,
            SOURCE_ID_COLUMN,
            TARGET_ID_COLUMN,
            SOURCE_KIND_COLUMN,
            TARGET_KIND_COLUMN,
        ]
        .into_iter()
        .map(|column| OrderExpr::asc(Expr::col(neighbor_edge_alias(), column)))
        .collect(),
    };
    Ok(Emitted {
        nodes: HashMap::from([(plan.operation.center.clone(), binding)]),
        stable_order,
        ..Emitted::new(ast)
    })
}

fn emit_neighbors(plan: &Plan<Neighbors>, input: &Input) -> Result<(Node, NodeBinding)> {
    let Neighbors {
        center: center_alias,
        direction,
        edge,
        joins_center,
        fused_table,
        outgoing_kinds,
        incoming_kinds,
        incoming_scope_preserving,
        incoming_cross_scope,
        outgoing_scope_preserving,
        outgoing_cross_scope,
        outgoing_neighbor_paths,
        incoming_neighbor_paths,
        center_sort_key,
    } = &plan.operation;
    let direction = *direction;
    let joins_center = *joins_center;
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
    let checks_center_rows = !center_node_ids.is_empty() && !joins_center;
    let edge_alias = neighbor_edge_alias();
    let center_rows = center_rows_cte();

    let center_join = joins_center.then(|| {
        let mut scan_where: Vec<Expr> = center_filters
            .iter()
            .map(|(prop, filter)| filter_to_expr(&center_id, prop, filter))
            .collect();
        if !center_node_ids.is_empty() {
            scan_where.push(id_list_predicate(
                &center_id,
                DEFAULT_PRIMARY_KEY,
                &center_node_ids,
            ));
        }
        if let Some(range) = &center_id_range {
            scan_where.push(id_range_predicate(&center_id, range));
        }
        let mut select = vec![
            SelectExpr::col(&center_id, DEFAULT_PRIMARY_KEY),
            SelectExpr::col(&center_id, DELETED_COLUMN),
        ];
        if !center_uses_default_pk {
            select.push(SelectExpr::col(&center_id, &center_redaction_col));
        }
        dedup_subquery(&center_id, &center_table, select, scan_where).0
    });

    let order_by = match &input.order_by {
        Some(ob) => {
            let value = output_value(&property_value(
                &plan.nodes,
                &ob.node,
                Column::new(&ob.node, &ob.property),
            ));
            vec![if ob.direction == OrderDirection::Desc {
                OrderExpr::desc(value)
            } else {
                OrderExpr::asc(value)
            }]
        }
        None => vec![],
    };

    let build_arm = |dir: Direction, arm_kinds: &[String], path_bound: bool| -> Query {
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

        let denorm_direction = if dir == Direction::Outgoing {
            query_data_model::DenormalizedDirection::Source
        } else {
            query_data_model::DenormalizedDirection::Target
        };

        let arm_where = |a: &str| -> Vec<Expr> {
            let mut wp = vec![Expr::eq(
                Expr::col(a, center_kind_col),
                Expr::string(&center_entity),
            )];
            if !center_node_ids.is_empty() {
                wp.push(id_list_predicate(a, center_edge_col, &center_node_ids));
            }
            if let Some(f) = rel_kind_filter(a, arm_kinds) {
                wp.push(f);
            }
            if path_bound {
                wp.push(center_rows_lookup(
                    a,
                    TRAVERSAL_PATH_COLUMN,
                    TRAVERSAL_PATH_COLUMN,
                ));
            }
            if checks_center_rows {
                wp.push(center_rows_lookup(a, center_edge_col, DEFAULT_PRIMARY_KEY));
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

        let (arm_tables, arm_neighbor_paths) = if dir == Direction::Outgoing {
            (&edge.outgoing_tables, outgoing_neighbor_paths)
        } else {
            (&edge.incoming_tables, incoming_neighbor_paths)
        };
        select.push(SelectExpr::new(
            neighbor_path(edge_alias, neighbor_type, arm_neighbor_paths),
            neighbor_traversal_path_column(),
        ));
        let (mut from, outer_pushed) = edge_table_scan_filtered(arm_tables, edge_alias, arm_where);
        where_parts.extend(outer_pushed);

        if let Some(center_subq) = &center_join {
            from = TableRef::join(
                JoinType::Inner,
                from,
                center_subq.clone(),
                Expr::eq(
                    Expr::col(edge_alias, center_edge_col),
                    Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                ),
            );
            where_parts.push(deleted_false(&center_id));
        }

        if center_uses_default_pk {
            select.push(SelectExpr::new(
                Expr::col(edge_alias, center_edge_col),
                redaction_id_column(&center_id),
            ));
        } else {
            if !joins_center {
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

    let kinds_for = |dir: Direction| match dir {
        Direction::Incoming => (
            incoming_kinds,
            incoming_scope_preserving,
            incoming_cross_scope,
        ),
        _ => (
            outgoing_kinds,
            outgoing_scope_preserving,
            outgoing_cross_scope,
        ),
    };
    let path_bounded = |dir: Direction| {
        !center_node_ids.is_empty() && center_has_tp && !kinds_for(dir).1.is_empty()
    };
    let directions = match direction {
        Direction::Both => vec![Direction::Outgoing, Direction::Incoming],
        dir => vec![dir],
    };
    let reads_center_paths =
        fused_table.is_none() && directions.iter().any(|dir| path_bounded(*dir));
    let mut query = if let Some(table) = fused_table {
        let mut q = build_fused_both_arm(plan, table, checks_center_rows);
        q.order_by = order_by;
        q.limit = Some(input.limit);
        q
    } else {
        let arms_for = |dir: Direction| -> Vec<Query> {
            let (all_kinds, carrying, rest) = kinds_for(dir);
            if !path_bounded(dir) {
                return vec![build_arm(dir, all_kinds, false)];
            }
            if rest.is_empty() {
                return vec![build_arm(dir, all_kinds, true)];
            }
            vec![build_arm(dir, carrying, true), build_arm(dir, rest, false)]
        };
        let mut arms: Vec<Query> = directions.iter().flat_map(|dir| arms_for(*dir)).collect();
        let mut first = arms.remove(0);
        first.union_all = arms;
        first.order_by = order_by;
        first.limit = Some(input.limit);
        first
    };
    if checks_center_rows || reads_center_paths {
        let (order_by, limit_by) = latest_row_dedup(center_rows, center_sort_key);
        let latest = Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(&center_table, center_rows),
            where_clause: Some(id_list_predicate(
                center_rows,
                DEFAULT_PRIMARY_KEY,
                &center_node_ids,
            )),
            order_by,
            limit_by,
            ..Default::default()
        };
        query.ctes.push(crate::ast::Cte {
            name: center_rows.to_string(),
            query: Box::new(Query {
                select: [DEFAULT_PRIMARY_KEY]
                    .into_iter()
                    .chain(reads_center_paths.then_some(TRAVERSAL_PATH_COLUMN))
                    .map(|column| SelectExpr::col(center_rows, column))
                    .collect(),
                from: TableRef::subquery(latest, center_rows),
                where_clause: Some(deleted_false(center_rows)),
                ..Default::default()
            }),
            recursive: false,
            materialized: true,
        });
    }
    Ok((Node::Query(Box::new(query)), NodeBinding::Projected))
}

fn center_rows_lookup(edge_alias: &str, edge_column: &str, center_column: &str) -> Expr {
    let center_rows = center_rows_cte();
    Expr::InSelect {
        expr: Box::new(Expr::col(edge_alias, edge_column)),
        query: Box::new(Query {
            select: vec![SelectExpr::col(center_rows, center_column)],
            from: TableRef::cte(center_rows, center_rows),
            ..Default::default()
        }),
    }
}

fn neighbor_path(edge_alias: &str, neighbor_type: &str, owned: &[(String, String)]) -> Expr {
    let owned_pair = |(relationship, entity): &(String, String)| {
        Some(Expr::and(
            Expr::eq(
                Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN),
                Expr::string(relationship),
            ),
            Expr::eq(Expr::col(edge_alias, neighbor_type), Expr::string(entity)),
        ))
    };
    match Expr::or_all(owned.iter().map(owned_pair)) {
        Some(owned) => Expr::func(
            Function::If,
            vec![
                owned,
                Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
                Expr::string(""),
            ],
        ),
        None => Expr::string(""),
    }
}

/// Inner query: scan the edge once with `WHERE (source side) OR (target side)`,
/// projecting `arrayJoin(arrayFilter(matched, [out_tuple, in_tuple]))` so each
/// row yields one entry per matched arm. Outer query: project the `_orbit_*`
/// columns out of the tuple. `arrayFilter` (not `multiIf`) keeps self-loop
/// semantics: when both arms match the same edge, both rows are emitted.
fn build_fused_both_arm(
    plan: &Plan<Neighbors>,
    edge_table: &str,
    checks_center_rows: bool,
) -> Query {
    let center_id = plan.operation.center.as_str();
    let center = &plan.nodes[center_id];
    let center_entity = center.entity.as_deref().unwrap_or_default();
    let center_has_tp = center.has_traversal_path;
    let center_node_ids = &center.node_ids;
    let center_filters = &center.filters;
    let edge = &plan.operation.edge;
    let edge_alias = neighbor_edge_alias();
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
        if checks_center_rows {
            parts.push(center_rows_lookup(edge_alias, id_col, DEFAULT_PRIMARY_KEY));
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
        from: TableRef::subquery(inner, inner_alias),
        ..Default::default()
    }
}
