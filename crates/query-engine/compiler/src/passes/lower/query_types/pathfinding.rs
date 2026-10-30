//! Generates forward + backward frontier CTEs (UNION ALL of depth arms),
//! then combines via direct (depth-1) + intersection (forward meets backward).
//! Dedup is baked into anchor CTEs.

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
    let end_denorm = build_denorm_tags(
        query_data_model::DenormalizedDirection::Target,
        &first_hop,
        &end_np.filters,
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
            pf.forward_depth,
            FDir::Forward,
            &frontier_opts,
            &mut reach_ctes,
        ),
    );
    let backward_frontier = if pf.backward_depth > 0 {
        Some(Cte::new(
            backward_cte(),
            build_frontier(
                end_anchor.edge_filter.clone(),
                pf.backward_depth,
                FDir::Backward,
                &FrontierOpts {
                    anchor_entity: Some(end_entity),
                    anchor_denorm_tags: end_denorm,
                    ..frontier_opts.clone()
                },
                &mut reach_ctes,
            ),
        ))
    } else {
        None
    };

    let start_tuple = |t: &str| {
        Expr::func(
            Function::Tuple,
            vec![
                Expr::col(t, anchor_node_column()),
                Expr::string(start_entity),
            ],
        )
    };
    let end_tuple = |t: &str| {
        Expr::func(
            Function::Tuple,
            vec![Expr::col(t, anchor_node_column()), Expr::string(end_entity)],
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
            Some(Expr::binary(
                Op::Eq,
                Expr::col(forward_cte(), depth_column()),
                Expr::int(1),
            )),
            Some(Expr::eq(
                Expr::col(forward_cte(), end_kind_column()),
                Expr::string(end_entity),
            )),
            endpoint_filter(end_np, forward_cte(), end_node_column()),
        ]),
        ..Default::default()
    };

    let intersection_query = Query {
        select: vec![
            SelectExpr::new(
                Expr::col(forward_cte(), anchor_node_column()),
                path_start_column(),
            ),
            SelectExpr::new(
                Expr::col(backward_cte(), anchor_node_column()),
                path_end_column(),
            ),
            SelectExpr::new(
                Expr::binary(
                    Op::Add,
                    Expr::col(forward_cte(), depth_column()),
                    Expr::col(backward_cte(), depth_column()),
                ),
                depth_column(),
            ),
            SelectExpr::new(
                Expr::func(
                    Function::ArrayConcat,
                    vec![
                        Expr::func(Function::Array, vec![start_tuple(forward_cte())]),
                        Expr::col(forward_cte(), path_nodes_column()),
                        Expr::func(
                            Function::ArrayReverse,
                            vec![Expr::col(backward_cte(), path_nodes_column())],
                        ),
                        Expr::func(Function::Array, vec![end_tuple(backward_cte())]),
                    ],
                ),
                path_column(),
            ),
            SelectExpr::new(
                Expr::func(
                    Function::ArrayConcat,
                    vec![
                        Expr::col(forward_cte(), frontier_edge_kinds_column()),
                        Expr::func(
                            Function::ArrayReverse,
                            vec![Expr::col(backward_cte(), frontier_edge_kinds_column())],
                        ),
                    ],
                ),
                edge_kinds_column(),
            ),
        ],
        from: {
            let mut join_cond = Expr::eq(
                Expr::col(forward_cte(), end_node_column()),
                Expr::col(backward_cte(), end_node_column()),
            );
            join_cond = Expr::and(
                join_cond,
                Expr::eq(
                    Expr::col(forward_cte(), end_kind_column()),
                    Expr::col(backward_cte(), end_kind_column()),
                ),
            );
            if pf.scoped_by_tp {
                join_cond = Expr::and(
                    join_cond,
                    Expr::eq(
                        Expr::col(forward_cte(), TRAVERSAL_PATH_COLUMN),
                        Expr::col(backward_cte(), TRAVERSAL_PATH_COLUMN),
                    ),
                );
            }
            TableRef::join(
                JoinType::Inner,
                TableRef::cte(forward_cte(), forward_cte()),
                TableRef::cte(backward_cte(), backward_cte()),
                join_cond,
            )
        },
        where_clause: Some(Expr::binary(
            Op::Le,
            Expr::binary(
                Op::Add,
                Expr::col(forward_cte(), depth_column()),
                Expr::col(backward_cte(), depth_column()),
            ),
            Expr::int(pf.max_depth as i64),
        )),
        ..Default::default()
    };

    let paths_union = if pf.backward_depth == 0 {
        TableRef::subquery(direct_query, paths_alias())
    } else {
        TableRef::union_all(vec![direct_query, intersection_query], paths_alias())
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
            if let Some(bc) = backward_frontier {
                ctes.push(bc);
            }
            ctes
        },
        select: vec![
            SelectExpr::col(paths_alias(), path_column()),
            SelectExpr::col(paths_alias(), edge_kinds_column()),
            SelectExpr::col(paths_alias(), depth_column()),
        ],
        from: paths_union,
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

#[derive(Clone, Copy)]
enum FDir {
    Forward,
    Backward,
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
    direction: FDir,
    opts: &FrontierOpts<'_>,
    reach_ctes: &mut Vec<Cte>,
) -> Query {
    reach_ctes
        .extend((1..max_depth).map(|depth| reach_cte(anchor_cond.clone(), depth, direction, opts)));
    let arms: Vec<Query> = (1..=max_depth)
        .map(|depth| build_frontier_arm(anchor_cond.clone(), depth, direction, opts))
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

fn build_frontier_arm(
    anchor_cond: Option<Expr>,
    depth: u32,
    direction: FDir,
    opts: &FrontierOpts<'_>,
) -> Query {
    let (anchor_col, next_col, next_kind_col) = match direction {
        FDir::Forward => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN),
        FDir::Backward => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN),
    };

    let first = hop_alias(1);
    let last = hop_alias(depth);

    let mut from = final_edge_table_scan(opts.edge_tables, &first);
    for i in 2..=depth {
        let prev = hop_alias(i - 1);
        let curr = hop_alias(i);
        let edge_tbl = final_edge_table_scan(opts.edge_tables, &curr);
        let mut join_cond = Expr::eq(Expr::col(&prev, next_col), Expr::col(&curr, anchor_col));
        join_cond = Expr::and(
            join_cond,
            Expr::eq(
                Expr::col(&prev, next_kind_col),
                Expr::col(
                    &curr,
                    match direction {
                        FDir::Forward => SOURCE_KIND_COLUMN,
                        FDir::Backward => TARGET_KIND_COLUMN,
                    },
                ),
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
        join_cond = Expr::and(join_cond, reached_by(&curr, i - 1, direction));
        from = TableRef::join(JoinType::Inner, from, edge_tbl, join_cond);
    }

    let path_range = match direction {
        FDir::Forward => 1..=depth,
        FDir::Backward => 1..=depth.saturating_sub(1),
    };
    let tuples: Vec<Expr> = path_range
        .map(|i| {
            let a = hop_alias(i);
            Expr::func(
                Function::Tuple,
                vec![Expr::col(&a, next_col), Expr::col(&a, next_kind_col)],
            )
        })
        .collect();
    let path_nodes = if tuples.is_empty() {
        Expr::EmptyTupleArray(vec![SqlType::Int64, SqlType::String])
    } else {
        Expr::func(Function::Array, tuples)
    };

    let edge_kinds = Expr::func(
        Function::Array,
        (1..=depth)
            .map(|i| Expr::col(hop_alias(i), RELATIONSHIP_KIND_COLUMN))
            .collect(),
    );

    let mut select = vec![
        SelectExpr::new(Expr::col(&first, anchor_col), anchor_node_column()),
        SelectExpr::new(Expr::col(&last, next_col), end_node_column()),
        SelectExpr::new(Expr::col(&last, next_kind_col), end_kind_column()),
        SelectExpr::new(path_nodes, path_nodes_column()),
        SelectExpr::new(edge_kinds, frontier_edge_kinds_column()),
        SelectExpr::new(Expr::int(depth as i64), depth_column()),
    ];
    if opts.include_tp {
        select.push(SelectExpr::col(&first, TRAVERSAL_PATH_COLUMN));
    }

    Query {
        select,
        from,
        where_clause: Expr::and_all(first_hop_conds(anchor_cond, direction, opts)),
        ..Default::default()
    }
}

fn first_hop_conds(
    anchor_cond: Option<Expr>,
    direction: FDir,
    opts: &FrontierOpts<'_>,
) -> Vec<Option<Expr>> {
    let first = hop_alias(1);
    let anchor_kind_col = match direction {
        FDir::Forward => SOURCE_KIND_COLUMN,
        FDir::Backward => TARGET_KIND_COLUMN,
    };
    let mut conds = vec![
        anchor_cond,
        type_cond_for(&first, opts.rel_type_filter),
        opts.anchor_entity
            .map(|e| Expr::eq(Expr::col(&first, anchor_kind_col), Expr::string(e))),
        opts.scope_cte.map(|sc| scope_filter(&first, sc)),
    ];
    conds.extend(opts.anchor_denorm_tags.iter().cloned().map(Some));
    conds
}

fn direction_name(direction: FDir) -> &'static str {
    match direction {
        FDir::Forward => "forward",
        FDir::Backward => "backward",
    }
}

/// Lets the edge primary key prune a later hop to the nodes the previous hop reached.
fn reached_by(alias: &str, depth: u32, direction: FDir) -> Expr {
    let anchor_col = match direction {
        FDir::Forward => SOURCE_ID_COLUMN,
        FDir::Backward => TARGET_ID_COLUMN,
    };
    Expr::InSubquery {
        expr: Box::new(Expr::col(alias, anchor_col)),
        cte_name: frontier_reach_cte(direction_name(direction), depth),
        columns: vec![end_node_column().to_string()],
    }
}

fn reach_cte(
    anchor_cond: Option<Expr>,
    depth: u32,
    direction: FDir,
    opts: &FrontierOpts<'_>,
) -> Cte {
    let alias = hop_alias(depth);
    let next_col = match direction {
        FDir::Forward => TARGET_ID_COLUMN,
        FDir::Backward => SOURCE_ID_COLUMN,
    };
    let conds = if depth == 1 {
        first_hop_conds(anchor_cond, direction, opts)
    } else {
        vec![
            Some(reached_by(&alias, depth - 1, direction)),
            type_cond_for(&alias, opts.rel_type_filter),
            opts.scope_cte.map(|sc| scope_filter(&alias, sc)),
        ]
    };
    Cte {
        materialized: true,
        ..Cte::new(
            frontier_reach_cte(direction_name(direction), depth),
            Query {
                distinct: true,
                select: vec![SelectExpr::new(
                    Expr::col(&alias, next_col),
                    end_node_column(),
                )],
                from: final_edge_table_scan(opts.edge_tables, &alias),
                where_clause: Expr::and_all(conds),
                ..Default::default()
            },
        )
    }
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
