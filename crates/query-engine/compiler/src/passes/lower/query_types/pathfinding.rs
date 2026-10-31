//! Generates a forward frontier CTE (UNION ALL of depth arms) from the start node and keeps
//! the arms that end at the end node. Edge reads key on `source_id`, which the edge primary
//! key covers, so the search runs forward only. Dedup is baked into anchor CTEs.

use std::collections::HashMap;

use ontology::constants::*;
use serde_json::Value;

use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;

use super::Emitted;
use crate::passes::lower::sql::{
    dedup_query, deleted_false, denorm_tag_expr, filter_to_expr, final_edge_table_scan,
    id_list_predicate, id_range_predicate, rel_kind_filter,
};
use crate::passes::plan::{NodePlan, PathFinding, Plan};

pub(crate) fn emit(plan: &Plan<PathFinding>, input: &Input) -> Result<Emitted> {
    let path_text = |column: &str| {
        OrderExpr::asc(Expr::func(
            Function::ToString,
            vec![Expr::col(paths_alias(), column)],
        ))
    };
    Ok(Emitted {
        stable_order: vec![path_text(path_column()), path_text(edge_kinds_column())],
        ..Emitted::new(emit_pathfinding(plan, input)?)
    })
}

fn emit_pathfinding(plan: &Plan<PathFinding>, input: &Input) -> Result<Node> {
    let pf = &plan.operation;
    let start_np = &plan.nodes[&pf.start];
    let end_np = &plan.nodes[&pf.end];

    let mut anchor_ctes: Vec<Cte> = Vec::new();
    let start_anchor = build_anchor(
        start_np,
        SOURCE_ID_COLUMN,
        &mut anchor_ctes,
        pf.scoped_by_tp,
    );
    let end_anchor = build_anchor(end_np, TARGET_ID_COLUMN, &mut anchor_ctes, pf.scoped_by_tp);
    let path_scope = build_scope_cte(&start_anchor, &end_anchor);

    let start_entity = start_np.entity.as_deref().unwrap_or("");
    let end_entity = end_np.entity.as_deref().unwrap_or("");

    let first_hop = hop_alias(1);
    let start_denorm = build_denorm_tags(
        query_data_model::DenormalizedDirection::Source,
        &first_hop,
        &start_np.filters,
        &plan.denormalized,
    );
    let frontier_opts = FrontierOpts {
        rel_type_filter: &pf.edge.rel_type_filter,
        anchor_entity: Some(start_entity),
        edge_tables: &pf.edge.tables,
        scope_cte: path_scope.as_ref().map(|c| c.name.as_str()),
        include_tp: pf.scoped_by_tp,
        anchor_denorm_tags: start_denorm,
    };

    let mut reach_ctes = Vec::new();
    let forward_frontier = Cte::new(
        forward_cte(),
        build_frontier(
            start_anchor.edge_filter,
            pf.max_depth,
            &frontier_opts,
            &mut reach_ctes,
        ),
    );

    let start_tuple = |t: &str| {
        Expr::func(
            Function::Tuple,
            vec![
                Expr::col(t, anchor_node_column()),
                Expr::string(start_entity),
            ],
        )
    };
    let direct_query = Query {
        select: vec![
            SelectExpr::new(
                Expr::col(forward_cte(), anchor_node_column()),
                path_start_column(),
            ),
            SelectExpr::new(
                Expr::col(forward_cte(), end_node_column()),
                path_end_column(),
            ),
            SelectExpr::col(forward_cte(), depth_column()),
            SelectExpr::new(
                Expr::func(
                    Function::ArrayConcat,
                    vec![
                        Expr::func(Function::Array, vec![start_tuple(forward_cte())]),
                        Expr::col(forward_cte(), path_nodes_column()),
                    ],
                ),
                path_column(),
            ),
            SelectExpr::new(
                Expr::col(forward_cte(), frontier_edge_kinds_column()),
                edge_kinds_column(),
            ),
        ],
        from: TableRef::cte(forward_cte(), forward_cte()),
        where_clause: Expr::and_all([
            Some(Expr::eq(
                Expr::col(forward_cte(), end_kind_column()),
                Expr::string(end_entity),
            )),
            endpoint_filter(end_np, forward_cte(), end_node_column()),
        ]),
        ..Default::default()
    };

    let order_by = vec![OrderExpr::asc(Expr::col(paths_alias(), depth_column()))];
    Ok(Node::Query(Box::new(Query {
        ctes: {
            let mut ctes = anchor_ctes;
            if let Some(scope) = path_scope {
                ctes.push(scope);
            }
            ctes.extend(reach_ctes);
            ctes.push(forward_frontier);
            ctes
        },
        select: vec![
            SelectExpr::col(paths_alias(), path_column()),
            SelectExpr::col(paths_alias(), edge_kinds_column()),
            SelectExpr::col(paths_alias(), depth_column()),
        ],
        from: TableRef::subquery(direct_query, paths_alias()),
        order_by,
        limit_by: Some((
            1,
            vec![
                Expr::col(paths_alias(), path_start_column()),
                Expr::col(paths_alias(), path_end_column()),
            ],
        )),
        limit: Some(input.limit),
        ..Default::default()
    })))
}

#[derive(Clone)]
struct Anchor {
    edge_filter: Option<Expr>,
    cte_name: Option<String>,
    has_tp: bool,
}

fn build_anchor(np: &NodePlan, edge_col: &str, ctes: &mut Vec<Cte>, force_cte: bool) -> Anchor {
    let alias = &np.alias;
    let table = np.table.as_deref().unwrap_or("");
    let has_tp = np.has_traversal_path;

    if !force_cte && np.is_id_pinned_only() {
        return Anchor {
            edge_filter: Expr::col_in(
                hop_alias(1),
                edge_col,
                SqlType::Int64,
                np.node_ids.iter().map(|id| Value::from(*id)).collect(),
            ),
            cte_name: None,
            has_tp: false,
        };
    }

    let has_conds = !np.node_ids.is_empty() || !np.filters.is_empty() || np.id_range.is_some();
    if !has_conds {
        return Anchor {
            edge_filter: None,
            cte_name: None,
            has_tp: false,
        };
    }

    let cte_name = node_filter_cte(alias);

    let mut scan_where = Vec::new();
    for (prop, filter) in &np.filters {
        scan_where.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        scan_where.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        scan_where.push(id_range_predicate(alias, range));
    }

    let mut select = vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)];
    if has_tp {
        select.push(SelectExpr::col(alias, TRAVERSAL_PATH_COLUMN));
    }
    select.push(SelectExpr::col(alias, DELETED_COLUMN));

    let dedup_scan = dedup_query(alias, table, select, scan_where);

    let mut outer_select = vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)];
    if has_tp {
        outer_select.push(SelectExpr::col(alias, TRAVERSAL_PATH_COLUMN));
    }

    let cte_query = Query {
        select: outer_select,
        from: TableRef::Subquery {
            query: Box::new(dedup_scan),
            alias: alias.to_string(),
        },
        where_clause: Some(deleted_false(alias)),
        order_by: vec![OrderExpr::asc(Expr::col(alias, DEFAULT_PRIMARY_KEY))],
        limit: Some(crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32),
        ..Default::default()
    };
    ctes.push(Cte::new(&cte_name, cte_query));

    Anchor {
        edge_filter: Some(Expr::InSubquery {
            expr: Box::new(Expr::col(hop_alias(1), edge_col)),
            cte_name: cte_name.clone(),
            columns: vec![DEFAULT_PRIMARY_KEY.into()],
        }),
        cte_name: Some(cte_name),
        has_tp,
    }
}

fn build_scope_cte(start: &Anchor, end: &Anchor) -> Option<Cte> {
    let start_cte = start.cte_name.as_deref()?;
    let end_cte = end.cte_name.as_deref()?;
    if !start.has_tp || !end.has_tp {
        return None;
    }
    // UNION, not intersect: endpoints at different namespace depths are linked
    // by edges carrying only the deeper tp, so equality yields an empty scope.
    let arm = |cte: &str, alias: &str| Query {
        select: vec![SelectExpr::col(alias, TRAVERSAL_PATH_COLUMN)],
        from: TableRef::cte(cte, alias),
        group_by: vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN)],
        ..Default::default()
    };
    Some(Cte::new(
        path_scope_cte(),
        Query {
            union_all: vec![arm(end_cte, path_scope_end_alias())],
            ..arm(start_cte, path_scope_start_alias())
        },
    ))
}

fn endpoint_filter(np: &NodePlan, alias: &str, col: &str) -> Option<Expr> {
    if np.is_id_pinned_only() {
        return Expr::col_in(
            alias,
            col,
            SqlType::Int64,
            np.node_ids.iter().map(|id| Value::from(*id)).collect(),
        );
    }
    if !np.filters.is_empty() || np.id_range.is_some() {
        let cte_name = node_filter_cte(&np.alias);
        return Some(Expr::InSubquery {
            expr: Box::new(Expr::col(alias, col)),
            cte_name,
            columns: vec![DEFAULT_PRIMARY_KEY.into()],
        });
    }
    None
}

fn scope_filter(alias: &str, cte_name: &str) -> Expr {
    Expr::InSubquery {
        expr: Box::new(Expr::col(alias, TRAVERSAL_PATH_COLUMN)),
        cte_name: cte_name.to_string(),
        columns: vec![TRAVERSAL_PATH_COLUMN.to_string()],
    }
}

#[derive(Clone)]
struct FrontierOpts<'a> {
    rel_type_filter: &'a Option<Vec<String>>,
    anchor_entity: Option<&'a str>,
    edge_tables: &'a [String],
    scope_cte: Option<&'a str>,
    include_tp: bool,
    anchor_denorm_tags: Vec<Expr>,
}

fn build_frontier(
    anchor_cond: Option<Expr>,
    max_depth: u32,
    opts: &FrontierOpts<'_>,
    reach_ctes: &mut Vec<Cte>,
) -> Query {
    reach_ctes.extend((1..max_depth).map(|depth| {
        let arm = build_frontier_arm(anchor_cond.clone(), depth, opts);
        Cte {
            materialized: true,
            ..Cte::new(
                reach_cte(depth),
                Query {
                    select: vec![SelectExpr::col(reach_alias(), end_node_column())],
                    from: TableRef::subquery(arm, reach_alias()),
                    ..Default::default()
                },
            )
        }
    }));
    let arms: Vec<Query> = (1..=max_depth)
        .map(|depth| build_frontier_arm(anchor_cond.clone(), depth, opts))
        .collect();
    if arms.len() == 1 {
        arms.into_iter().next().unwrap()
    } else {
        let mut first = arms.into_iter();
        let base = first.next().unwrap();
        Query {
            union_all: first.collect(),
            ..base
        }
    }
}

fn build_frontier_arm(anchor_cond: Option<Expr>, depth: u32, opts: &FrontierOpts<'_>) -> Query {
    let first = hop_alias(1);
    let last = hop_alias(depth);

    let mut from = final_edge_table_scan(opts.edge_tables, &first);
    let mut first_type_cond = type_cond_for(&first, opts.rel_type_filter);
    for i in 2..=depth {
        let prev = hop_alias(i - 1);
        let curr = hop_alias(i);
        let edge_tbl = final_edge_table_scan(opts.edge_tables, &curr);
        let mut join_cond = Expr::and(
            Expr::eq(
                Expr::col(&prev, TARGET_ID_COLUMN),
                Expr::col(&curr, SOURCE_ID_COLUMN),
            ),
            Expr::eq(
                Expr::col(&prev, TARGET_KIND_COLUMN),
                Expr::col(&curr, SOURCE_KIND_COLUMN),
            ),
        );
        if opts.include_tp {
            join_cond = Expr::and(
                join_cond,
                Expr::eq(
                    Expr::col(&prev, TRAVERSAL_PATH_COLUMN),
                    Expr::col(&curr, TRAVERSAL_PATH_COLUMN),
                ),
            );
        }
        if let Some(tc) = type_cond_for(&curr, opts.rel_type_filter) {
            join_cond = Expr::and(join_cond, tc);
        }
        if let Some(sc) = opts.scope_cte {
            join_cond = Expr::and(join_cond, scope_filter(&curr, sc));
        }
        join_cond = Expr::and(
            join_cond,
            Expr::InSubquery {
                expr: Box::new(Expr::col(&curr, SOURCE_ID_COLUMN)),
                cte_name: reach_cte(i - 1),
                columns: vec![end_node_column().to_string()],
            },
        );
        from = TableRef::join(JoinType::Inner, from, edge_tbl, join_cond);
    }

    let path_nodes = Expr::func(
        Function::Array,
        (1..=depth)
            .map(|i| {
                let a = hop_alias(i);
                Expr::func(
                    Function::Tuple,
                    vec![
                        Expr::col(&a, TARGET_ID_COLUMN),
                        Expr::col(&a, TARGET_KIND_COLUMN),
                    ],
                )
            })
            .collect(),
    );

    let edge_kinds = Expr::func(
        Function::Array,
        (1..=depth)
            .map(|i| Expr::col(hop_alias(i), RELATIONSHIP_KIND_COLUMN))
            .collect(),
    );

    let anchor_kind_cond = opts
        .anchor_entity
        .map(|e| Expr::eq(Expr::col(&first, SOURCE_KIND_COLUMN), Expr::string(e)));
    let scope_cond = opts.scope_cte.map(|sc| scope_filter(&first, sc));

    let mut select = vec![
        SelectExpr::new(Expr::col(&first, SOURCE_ID_COLUMN), anchor_node_column()),
        SelectExpr::new(Expr::col(&last, TARGET_ID_COLUMN), end_node_column()),
        SelectExpr::new(Expr::col(&last, TARGET_KIND_COLUMN), end_kind_column()),
        SelectExpr::new(path_nodes, path_nodes_column()),
        SelectExpr::new(edge_kinds, frontier_edge_kinds_column()),
        SelectExpr::new(Expr::int(depth as i64), depth_column()),
    ];
    if opts.include_tp {
        select.push(SelectExpr::col(&first, TRAVERSAL_PATH_COLUMN));
    }

    let mut all_conds = vec![
        anchor_cond,
        first_type_cond.take(),
        anchor_kind_cond,
        scope_cond,
    ];
    all_conds.extend(opts.anchor_denorm_tags.iter().cloned().map(Some));

    Query {
        select,
        from,
        where_clause: Expr::and_all(all_conds),
        ..Default::default()
    }
}

/// Node ids the frontier reached after `depth` hops; the next hop reads only edges from them.
fn reach_cte(depth: u32) -> String {
    format!("{}_reach_{depth}", forward_cte())
}

fn type_cond_for(alias: &str, type_filter: &Option<Vec<String>>) -> Option<Expr> {
    rel_kind_filter(alias, type_filter.as_deref().unwrap_or(&[]))
}

fn build_denorm_tags(
    direction: query_data_model::DenormalizedDirection,
    edge_alias: &str,
    filters: &[(String, crate::passes::plan::BoundFilter)],
    denormalized: &HashMap<
        query_data_model::DenormalizedKey,
        query_data_model::DenormalizedProperty,
    >,
) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (_, filter) in filters {
        let Some(property) = filter.property else {
            continue;
        };
        let key = query_data_model::DenormalizedKey {
            property,
            direction,
        };
        if let Some(facts) = denormalized.get(&key)
            && let Some(expr) = denorm_tag_expr(
                edge_alias,
                &facts.edge_column,
                &facts.tag_key,
                &filter.filter,
            )
        {
            exprs.push(expr);
        }
    }
    exprs
}
