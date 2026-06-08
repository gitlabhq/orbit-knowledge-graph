//! PathFinding emit: bidirectional frontier expansion SQL generation.
//!
//! Reads Plan + PathFindingBody, produces SQL AST.
//!
//! Generates forward + backward frontier CTEs (UNION ALL of depth arms),
//! then combines via direct (depth-1) + intersection (forward meets backward).
//! Dedup is baked into anchor CTEs.

use std::collections::HashMap;

use ontology::constants::*;
use serde_json::Value;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use crate::passes::plan::{NodePlan, PathFindingBody, Plan};
use crate::passes::shared::{
    dedup_query, deleted_false, denorm_tag_expr, edge_table_scan, filter_to_expr,
    id_list_predicate, id_range_predicate, rel_kind_filter,
};

// ─────────────────────────────────────────────────────────────────────────────
// Emit
// ─────────────────────────────────────────────────────────────────────────────

pub fn emit_pathfinding(plan: &Plan, pf: &PathFindingBody) -> Result<Node> {
    let start_np = &plan.nodes[&pf.start];
    let end_np = &plan.nodes[&pf.end];

    let mut anchor_ctes: Vec<Cte> = Vec::new();
    let start_anchor = build_anchor(
        start_np,
        SOURCE_ID_COLUMN,
        &mut anchor_ctes,
        pf.scoped_by_tp,
        &plan.table_sort_keys,
    )?;
    let end_anchor = build_anchor(
        end_np,
        TARGET_ID_COLUMN,
        &mut anchor_ctes,
        pf.scoped_by_tp,
        &plan.table_sort_keys,
    )?;
    let path_scope_cte = build_scope_cte(&start_anchor, &end_anchor);

    let start_entity = start_np.entity.as_deref().unwrap_or("");
    let end_entity = end_np.entity.as_deref().unwrap_or("");

    let start_denorm = build_denorm_tags(
        start_entity,
        "source",
        "e1",
        &start_np.filters,
        &plan.denorm_columns,
    );
    let end_denorm = build_denorm_tags(
        end_entity,
        "target",
        "e1",
        &end_np.filters,
        &plan.denorm_columns,
    );

    let frontier_opts = FrontierOpts {
        rel_type_filter: &pf.edge.rel_type_filter,
        first_hop_filter: &pf.forward_first_hop_filter,
        anchor_entity: Some(start_entity),
        edge_tables: &pf.edge.tables,
        scope_cte: path_scope_cte.as_ref().map(|c| c.name.as_str()),
        include_tp: pf.scoped_by_tp,
        anchor_denorm_tags: start_denorm,
    };

    let forward_cte = Cte::new(
        FORWARD_CTE,
        build_frontier(
            start_anchor.edge_filter,
            pf.forward_depth,
            FDir::Forward,
            &frontier_opts,
        ),
    );
    let backward_cte = if pf.backward_depth > 0 {
        Some(Cte::new(
            BACKWARD_CTE,
            build_frontier(
                end_anchor.edge_filter.clone(),
                pf.backward_depth,
                FDir::Backward,
                &FrontierOpts {
                    first_hop_filter: &pf.backward_first_hop_filter,
                    anchor_entity: Some(end_entity),
                    anchor_denorm_tags: end_denorm,
                    ..frontier_opts.clone()
                },
            ),
        ))
    } else {
        None
    };

    let start_tuple = |t: &str| {
        Expr::func(
            "tuple",
            vec![Expr::col(t, ANCHOR_ID_COLUMN), Expr::string(start_entity)],
        )
    };
    let end_tuple = |t: &str| {
        Expr::func(
            "tuple",
            vec![Expr::col(t, ANCHOR_ID_COLUMN), Expr::string(end_entity)],
        )
    };

    // Direct depth-1 paths.
    let direct_query = Query {
        select: vec![
            SelectExpr::col(FORWARD_ALIAS, DEPTH_COLUMN),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::func("array", vec![start_tuple(FORWARD_ALIAS)]),
                        Expr::col(FORWARD_ALIAS, PATH_NODES_COLUMN),
                    ],
                ),
                path_column(),
            ),
            SelectExpr::new(
                Expr::col(FORWARD_ALIAS, FRONTIER_EDGE_KINDS_COLUMN),
                edge_kinds_column(),
            ),
        ],
        from: TableRef::scan(FORWARD_CTE, FORWARD_ALIAS),
        where_clause: Expr::and_all([
            Some(Expr::binary(
                Op::Eq,
                Expr::col(FORWARD_ALIAS, DEPTH_COLUMN),
                Expr::int(1),
            )),
            Some(Expr::eq(
                Expr::col(FORWARD_ALIAS, END_KIND_COLUMN),
                Expr::string(end_entity),
            )),
            endpoint_filter(end_np, FORWARD_ALIAS, END_ID_COLUMN),
        ]),
        ..Default::default()
    };

    // Intersection paths: forward meets backward.
    let intersection_query = Query {
        select: vec![
            SelectExpr::new(
                Expr::binary(
                    Op::Add,
                    Expr::col(FORWARD_ALIAS, DEPTH_COLUMN),
                    Expr::col(BACKWARD_ALIAS, DEPTH_COLUMN),
                ),
                DEPTH_COLUMN,
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::func("array", vec![start_tuple(FORWARD_ALIAS)]),
                        Expr::col(FORWARD_ALIAS, PATH_NODES_COLUMN),
                        Expr::func(
                            "arrayReverse",
                            vec![Expr::col(BACKWARD_ALIAS, PATH_NODES_COLUMN)],
                        ),
                        Expr::func("array", vec![end_tuple(BACKWARD_ALIAS)]),
                    ],
                ),
                path_column(),
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col(FORWARD_ALIAS, FRONTIER_EDGE_KINDS_COLUMN),
                        Expr::func(
                            "arrayReverse",
                            vec![Expr::col(BACKWARD_ALIAS, FRONTIER_EDGE_KINDS_COLUMN)],
                        ),
                    ],
                ),
                edge_kinds_column(),
            ),
        ],
        from: {
            let mut join_cond = Expr::eq(
                Expr::col(FORWARD_ALIAS, END_ID_COLUMN),
                Expr::col(BACKWARD_ALIAS, END_ID_COLUMN),
            );
            if pf.scoped_by_tp {
                join_cond = Expr::and(
                    join_cond,
                    Expr::eq(
                        Expr::col(FORWARD_ALIAS, TRAVERSAL_PATH_COLUMN),
                        Expr::col(BACKWARD_ALIAS, TRAVERSAL_PATH_COLUMN),
                    ),
                );
            }
            TableRef::join(
                JoinType::Inner,
                TableRef::scan(FORWARD_CTE, FORWARD_ALIAS),
                TableRef::scan(BACKWARD_CTE, BACKWARD_ALIAS),
                join_cond,
            )
        },
        where_clause: Some(Expr::binary(
            Op::Le,
            Expr::binary(
                Op::Add,
                Expr::col(FORWARD_ALIAS, DEPTH_COLUMN),
                Expr::col(BACKWARD_ALIAS, DEPTH_COLUMN),
            ),
            Expr::int(pf.max_depth as i64),
        )),
        ..Default::default()
    };

    let paths_union = if pf.backward_depth == 0 {
        TableRef::subquery(direct_query, PATHS_ALIAS)
    } else {
        TableRef::union_all(vec![direct_query, intersection_query], PATHS_ALIAS)
    };

    let mut order_by = vec![OrderExpr::asc(Expr::col(PATHS_ALIAS, DEPTH_COLUMN))];
    if plan.cursor.is_some() {
        order_by.extend([
            OrderExpr::asc(Expr::func(
                "toString",
                vec![Expr::col(PATHS_ALIAS, path_column())],
            )),
            OrderExpr::asc(Expr::func(
                "toString",
                vec![Expr::col(PATHS_ALIAS, edge_kinds_column())],
            )),
        ]);
    }

    Ok(Node::Query(Box::new(Query {
        ctes: {
            let mut ctes = anchor_ctes;
            if let Some(scope) = path_scope_cte {
                ctes.push(scope);
            }
            ctes.push(forward_cte);
            if let Some(bc) = backward_cte {
                ctes.push(bc);
            }
            ctes
        },
        select: vec![
            SelectExpr::col(PATHS_ALIAS, path_column()),
            SelectExpr::col(PATHS_ALIAS, edge_kinds_column()),
            SelectExpr::col(PATHS_ALIAS, DEPTH_COLUMN),
        ],
        from: paths_union,
        order_by,
        limit: Some(plan.limit),
        ..Default::default()
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Anchor {
    edge_filter: Option<Expr>,
    cte_name: Option<String>,
    has_tp: bool,
}

fn build_anchor(
    np: &NodePlan,
    edge_col: &str,
    ctes: &mut Vec<Cte>,
    force_cte: bool,
    table_sort_keys: &HashMap<String, Vec<String>>,
) -> Result<Anchor> {
    let alias = &np.alias;
    let table = np.table.as_deref().unwrap_or("");
    let has_tp = np.has_traversal_path;

    // Literal IN for concrete node_ids (no CTE needed).
    if !force_cte && !np.node_ids.is_empty() {
        return Ok(Anchor {
            edge_filter: Expr::col_in(
                "e1",
                edge_col,
                ChType::Int64,
                np.node_ids.iter().map(|id| Value::from(*id)).collect(),
            ),
            cte_name: None,
            has_tp: false,
        });
    }

    let has_conds = !np.node_ids.is_empty() || !np.filters.is_empty() || np.id_range.is_some();
    if !has_conds {
        return Ok(Anchor {
            edge_filter: None,
            cte_name: None,
            has_tp: false,
        });
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

    let sort_key = table_sort_keys
        .get(table)
        .ok_or_else(|| QueryError::Lowering(format!("no sort key for node table '{table}'")))?
        .as_slice();
    let dedup_scan = dedup_query(alias, table, select, scan_where, sort_key);

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
        limit: Some(crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32),
        ..Default::default()
    };
    ctes.push(Cte::new(&cte_name, cte_query));

    Ok(Anchor {
        edge_filter: Some(Expr::InSubquery {
            expr: Box::new(Expr::col("e1", edge_col)),
            cte_name: cte_name.clone(),
            column: DEFAULT_PRIMARY_KEY.into(),
        }),
        cte_name: Some(cte_name),
        has_tp,
    })
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
        from: TableRef::scan(cte, alias),
        group_by: vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN)],
        ..Default::default()
    };
    Some(Cte::new(
        PATH_SCOPE_CTE,
        Query {
            union_all: vec![arm(end_cte, PATH_SCOPE_END_ALIAS)],
            ..arm(start_cte, PATH_SCOPE_START_ALIAS)
        },
    ))
}

const PATH_SCOPE_CTE: &str = "_path_scope_traversal_paths";
const PATH_SCOPE_START_ALIAS: &str = "_path_scope_start";
const PATH_SCOPE_END_ALIAS: &str = "_path_scope_end";

fn endpoint_filter(np: &NodePlan, alias: &str, col: &str) -> Option<Expr> {
    if !np.node_ids.is_empty() {
        return Expr::col_in(
            alias,
            col,
            ChType::Int64,
            np.node_ids.iter().map(|id| Value::from(*id)).collect(),
        );
    }
    if !np.filters.is_empty() || np.id_range.is_some() {
        let cte_name = node_filter_cte(&np.alias);
        return Some(Expr::InSubquery {
            expr: Box::new(Expr::col(alias, col)),
            cte_name,
            column: DEFAULT_PRIMARY_KEY.into(),
        });
    }
    None
}

fn scope_filter(alias: &str, cte_name: &str) -> Expr {
    Expr::InSubquery {
        expr: Box::new(Expr::col(alias, TRAVERSAL_PATH_COLUMN)),
        cte_name: cte_name.to_string(),
        column: TRAVERSAL_PATH_COLUMN.to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Frontier building
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum FDir {
    Forward,
    Backward,
}

#[derive(Clone)]
struct FrontierOpts<'a> {
    rel_type_filter: &'a Option<Vec<String>>,
    first_hop_filter: &'a Option<Vec<String>>,
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
) -> Query {
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

    let last = format!("e{depth}");

    let mut from = edge_table_scan(opts.edge_tables, "e1");
    // Use specific first-hop filter if provided, otherwise fall back to
    // the general rel_type_filter so e1 isn't left unfiltered.
    let effective_first = if opts.first_hop_filter.is_some() {
        opts.first_hop_filter
    } else {
        opts.rel_type_filter
    };
    let mut first_type_cond = type_cond_for("e1", effective_first);
    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let edge_tbl = edge_table_scan(opts.edge_tables, &curr);
        let mut join_cond = Expr::eq(Expr::col(&prev, next_col), Expr::col(&curr, anchor_col));
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
        join_cond = Expr::and(join_cond, deleted_false(&curr));
        from = TableRef::join(JoinType::Inner, from, edge_tbl, join_cond);
    }

    let path_range = match direction {
        FDir::Forward => 1..=depth,
        FDir::Backward => 1..=depth.saturating_sub(1),
    };
    let tuples: Vec<Expr> = path_range
        .map(|i| {
            let a = format!("e{i}");
            Expr::func(
                "tuple",
                vec![Expr::col(&a, next_col), Expr::col(&a, next_kind_col)],
            )
        })
        .collect();
    let path_nodes = if tuples.is_empty() {
        Expr::func(
            "arrayResize",
            vec![
                Expr::func(
                    "array",
                    vec![Expr::func("tuple", vec![Expr::int(0), Expr::string("")])],
                ),
                Expr::int(0),
            ],
        )
    } else {
        Expr::func("array", tuples)
    };

    let edge_kinds = Expr::func(
        "array",
        (1..=depth)
            .map(|i| Expr::col(format!("e{i}"), RELATIONSHIP_KIND_COLUMN))
            .collect(),
    );

    let anchor_kind_col = match direction {
        FDir::Forward => SOURCE_KIND_COLUMN,
        FDir::Backward => TARGET_KIND_COLUMN,
    };
    let anchor_kind_cond = opts
        .anchor_entity
        .map(|e| Expr::eq(Expr::col("e1", anchor_kind_col), Expr::string(e)));
    let scope_cond = opts.scope_cte.map(|sc| scope_filter("e1", sc));

    let mut select = vec![
        SelectExpr::new(Expr::col("e1", anchor_col), ANCHOR_ID_COLUMN),
        SelectExpr::new(Expr::col(&last, next_col), END_ID_COLUMN),
        SelectExpr::new(Expr::col(&last, next_kind_col), END_KIND_COLUMN),
        SelectExpr::new(path_nodes, PATH_NODES_COLUMN),
        SelectExpr::new(edge_kinds, FRONTIER_EDGE_KINDS_COLUMN),
        SelectExpr::new(Expr::int(depth as i64), DEPTH_COLUMN),
    ];
    if opts.include_tp {
        select.push(SelectExpr::col("e1", TRAVERSAL_PATH_COLUMN));
    }

    let deleted_cond = Some(deleted_false("e1"));

    let mut all_conds = vec![
        anchor_cond,
        first_type_cond.take(),
        anchor_kind_cond,
        scope_cond,
        deleted_cond,
    ];
    all_conds.extend(opts.anchor_denorm_tags.iter().cloned().map(Some));

    Query {
        select,
        from,
        where_clause: Expr::and_all(all_conds),
        ..Default::default()
    }
}

fn type_cond_for(alias: &str, type_filter: &Option<Vec<String>>) -> Option<Expr> {
    rel_kind_filter(alias, type_filter.as_deref().unwrap_or(&[]))
}

fn build_denorm_tags(
    entity: &str,
    dir_prefix: &str,
    edge_alias: &str,
    filters: &[(String, InputFilter)],
    denorm_map: &HashMap<(String, String, String), (String, String)>,
) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (prop, filter) in filters {
        let key = (entity.to_string(), prop.clone(), dir_prefix.to_string());
        if let Some((tag_col, tag_key)) = denorm_map.get(&key)
            && let Some(expr) = denorm_tag_expr(edge_alias, tag_col, tag_key, filter)
        {
            exprs.push(expr);
        }
    }
    exprs
}
