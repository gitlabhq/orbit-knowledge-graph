//! V2 PathFinding: bidirectional frontier expansion.
//!
//! Generates forward + backward frontier CTEs (UNION ALL of depth arms),
//! then combines via direct (depth-1) + intersection (forward meets backward).
//! Dedup is baked into anchor CTEs (unlike v1 where DeduplicatePass adds it).

use ontology::constants::*;
use serde_json::Value;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

pub fn lower_pathfinding(input: &mut Input) -> Result<Node> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?
        .clone();

    let start = input
        .nodes
        .iter()
        .find(|n| n.id == path.from)
        .ok_or_else(|| QueryError::Lowering(format!("start node '{}' not found", path.from)))?;
    let end = input
        .nodes
        .iter()
        .find(|n| n.id == path.to)
        .ok_or_else(|| QueryError::Lowering(format!("end node '{}' not found", path.to)))?;

    let start_entity = start
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("start node has no entity".into()))?
        .to_string();
    let end_entity = end
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("end node has no entity".into()))?
        .to_string();

    let wildcard_path = path.rel_types.is_empty();
    let rel_type_filter = make_type_filter(&path.rel_types);
    let forward_first_hop_types = if wildcard_path {
        path.forward_first_hop_rel_types.clone()
    } else {
        path.rel_types.clone()
    };
    let backward_first_hop_types = if wildcard_path {
        path.backward_first_hop_rel_types.clone()
    } else {
        path.rel_types.clone()
    };
    let forward_first_hop_filter = make_type_filter(&forward_first_hop_types);
    let backward_first_hop_filter = make_type_filter(&backward_first_hop_types);

    let max_depth = path.max_depth;
    let forward_depth = max_depth.div_ceil(2);
    let backward_depth = max_depth / 2;

    let et = input.compiler.resolve_edge_tables(&path.rel_types);
    let scoped_by_tp = can_scope_by_tp(input, start, end, &et);

    // Build anchor CTEs with dedup baked in.
    let mut anchor_ctes: Vec<Cte> = Vec::new();
    let start_anchor = build_anchor(
        input,
        start,
        SOURCE_ID_COLUMN,
        &mut anchor_ctes,
        scoped_by_tp,
    )?;
    let end_anchor = build_anchor(input, end, TARGET_ID_COLUMN, &mut anchor_ctes, scoped_by_tp)?;
    let path_scope_cte = build_scope_cte(&start_anchor, &end_anchor);

    // Build denorm tags for start/end entities to push onto anchor edges.
    let start_denorm = build_denorm_tags(
        &start_entity,
        "source",
        "e1",
        &start.filters,
        &input.compiler.denormalized_columns,
    );
    let end_denorm = build_denorm_tags(
        &end_entity,
        "target",
        "e1",
        &end.filters,
        &input.compiler.denormalized_columns,
    );

    let frontier_opts = FrontierOpts {
        rel_type_filter: &rel_type_filter,
        first_hop_filter: &forward_first_hop_filter,
        anchor_entity: Some(&start_entity),
        edge_tables: &et,
        scope_cte: path_scope_cte.as_ref().map(|c| c.name.as_str()),
        include_tp: scoped_by_tp,
        anchor_denorm_tags: start_denorm,
    };

    let forward_cte = Cte::new(
        FORWARD_CTE,
        build_frontier(
            start_anchor.edge_filter,
            forward_depth,
            FDir::Forward,
            &frontier_opts,
        ),
    );
    let backward_cte = if backward_depth > 0 {
        Some(Cte::new(
            BACKWARD_CTE,
            build_frontier(
                end_anchor.edge_filter.clone(),
                backward_depth,
                FDir::Backward,
                &FrontierOpts {
                    first_hop_filter: &backward_first_hop_filter,
                    anchor_entity: Some(&end_entity),
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
            vec![Expr::col(t, ANCHOR_ID_COLUMN), Expr::string(&start_entity)],
        )
    };
    let end_tuple = |t: &str| {
        Expr::func(
            "tuple",
            vec![Expr::col(t, ANCHOR_ID_COLUMN), Expr::string(&end_entity)],
        )
    };

    // Direct depth-1 paths.
    let direct_query = Query {
        select: vec![
            SelectExpr::new(Expr::col(FORWARD_ALIAS, DEPTH_COLUMN), DEPTH_COLUMN),
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
                Expr::string(&end_entity),
            )),
            endpoint_filter(end, FORWARD_ALIAS, END_ID_COLUMN),
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
            if scoped_by_tp {
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
            Expr::int(max_depth as i64),
        )),
        ..Default::default()
    };

    let paths_union = if backward_depth == 0 {
        TableRef::subquery(direct_query, PATHS_ALIAS)
    } else {
        TableRef::union_all(vec![direct_query, intersection_query], PATHS_ALIAS)
    };

    let mut order_by = vec![OrderExpr {
        expr: Expr::col(PATHS_ALIAS, DEPTH_COLUMN),
        desc: false,
    }];
    if input.cursor.is_some() {
        order_by.extend([
            OrderExpr {
                expr: Expr::func("toString", vec![Expr::col(PATHS_ALIAS, path_column())]),
                desc: false,
            },
            OrderExpr {
                expr: Expr::func(
                    "toString",
                    vec![Expr::col(PATHS_ALIAS, edge_kinds_column())],
                ),
                desc: false,
            },
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
            SelectExpr::new(Expr::col(PATHS_ALIAS, path_column()), path_column()),
            SelectExpr::new(
                Expr::col(PATHS_ALIAS, edge_kinds_column()),
                edge_kinds_column(),
            ),
            SelectExpr::new(Expr::col(PATHS_ALIAS, DEPTH_COLUMN), DEPTH_COLUMN),
        ],
        from: paths_union,
        order_by,
        limit: Some(input.limit),
        ..Default::default()
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_type_filter(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() {
        None
    } else {
        Some(types.to_vec())
    }
}

fn can_scope_by_tp(
    input: &Input,
    start: &InputNode,
    end: &InputNode,
    edge_tables: &[String],
) -> bool {
    if edge_tables.is_empty()
        || edge_tables
            .iter()
            .any(|t| !input.compiler.table_has_column(t, TRAVERSAL_PATH_COLUMN))
    {
        return false;
    }
    [start, end].iter().all(|node| {
        node.table
            .as_ref()
            .is_some_and(|t| input.compiler.table_has_column(t, TRAVERSAL_PATH_COLUMN))
    })
}

#[derive(Clone)]
struct Anchor {
    edge_filter: Option<Expr>,
    cte_name: Option<String>,
    has_tp: bool,
}

fn build_anchor(
    input: &Input,
    node: &InputNode,
    edge_col: &str,
    ctes: &mut Vec<Cte>,
    force_cte: bool,
) -> Result<Anchor> {
    // Literal IN for concrete node_ids (no CTE needed).
    if !force_cte && !node.node_ids.is_empty() {
        return Ok(Anchor {
            edge_filter: Expr::col_in(
                "e1",
                edge_col,
                ChType::Int64,
                node.node_ids.iter().map(|id| Value::from(*id)).collect(),
            ),
            cte_name: None,
            has_tp: false,
        });
    }

    // Check if the node has conditions worth resolving.
    let has_conds =
        !node.node_ids.is_empty() || !node.filters.is_empty() || node.id_range.is_some();
    if !has_conds {
        return Ok(Anchor {
            edge_filter: None,
            cte_name: None,
            has_tp: false,
        });
    }

    let table = node
        .table
        .as_ref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.id)))?;
    let cte_name = node_filter_cte(&node.id);
    let has_tp = input
        .compiler
        .table_has_column(table, TRAVERSAL_PATH_COLUMN);

    // Build dedup scan with filters baked in (no DeduplicatePass needed).
    let alias = &node.id;
    let mut scan_where = Vec::new();
    for (prop, filter) in &node.filters {
        scan_where.push(super::shared::filter_to_expr(alias, prop, filter));
    }
    if !node.node_ids.is_empty() {
        scan_where.push(super::shared::id_list_predicate(
            alias,
            DEFAULT_PRIMARY_KEY,
            &node.node_ids,
        ));
    }
    if let Some(ref range) = node.id_range {
        scan_where.push(super::shared::id_range_predicate(alias, range));
    }

    let mut select = vec![SelectExpr::new(
        Expr::col(alias, DEFAULT_PRIMARY_KEY),
        DEFAULT_PRIMARY_KEY,
    )];
    if has_tp {
        select.push(SelectExpr::new(
            Expr::col(alias, TRAVERSAL_PATH_COLUMN),
            TRAVERSAL_PATH_COLUMN,
        ));
    }
    // _deleted for outer filter.
    select.push(SelectExpr::new(
        Expr::col(alias, DELETED_COLUMN),
        DELETED_COLUMN,
    ));

    let dedup_scan = Query {
        select,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(scan_where),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    // Wrap with _deleted=false + LIMIT cap.
    let mut outer_select = vec![SelectExpr::new(
        Expr::col(alias, DEFAULT_PRIMARY_KEY),
        DEFAULT_PRIMARY_KEY,
    )];
    if has_tp {
        outer_select.push(SelectExpr::new(
            Expr::col(alias, TRAVERSAL_PATH_COLUMN),
            TRAVERSAL_PATH_COLUMN,
        ));
    }

    let cte_query = Query {
        select: outer_select,
        from: TableRef::Subquery {
            query: Box::new(dedup_scan),
            alias: alias.to_string(),
        },
        where_clause: Some(Expr::eq(
            Expr::col(alias, DELETED_COLUMN),
            Expr::param(ChType::Bool, false),
        )),
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
    Some(Cte::new(
        PATH_SCOPE_CTE,
        Query {
            select: vec![SelectExpr::new(
                Expr::col(PATH_SCOPE_START_ALIAS, TRAVERSAL_PATH_COLUMN),
                TRAVERSAL_PATH_COLUMN,
            )],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan(start_cte, PATH_SCOPE_START_ALIAS),
                TableRef::scan(end_cte, PATH_SCOPE_END_ALIAS),
                Expr::eq(
                    Expr::col(PATH_SCOPE_START_ALIAS, TRAVERSAL_PATH_COLUMN),
                    Expr::col(PATH_SCOPE_END_ALIAS, TRAVERSAL_PATH_COLUMN),
                ),
            ),
            group_by: vec![Expr::col(PATH_SCOPE_START_ALIAS, TRAVERSAL_PATH_COLUMN)],
            ..Default::default()
        },
    ))
}

const PATH_SCOPE_CTE: &str = "_path_scope_traversal_paths";
const PATH_SCOPE_START_ALIAS: &str = "_path_scope_start";
const PATH_SCOPE_END_ALIAS: &str = "_path_scope_end";

fn endpoint_filter(node: &InputNode, alias: &str, col: &str) -> Option<Expr> {
    if !node.node_ids.is_empty() {
        return Expr::col_in(
            alias,
            col,
            ChType::Int64,
            node.node_ids.iter().map(|id| Value::from(*id)).collect(),
        );
    }
    if !node.filters.is_empty() || node.id_range.is_some() || !node.node_ids.is_empty() {
        let cte_name = node_filter_cte(&node.id);
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
    /// Denorm tag predicates to push onto the anchor edge (e1).
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

    // Build edge join chain: e1 JOIN e2 ON e1.next = e2.anchor ...
    let mut from = edge_scan(opts.edge_tables, "e1", opts.first_hop_filter);
    let mut first_type_cond = type_cond_for("e1", opts.first_hop_filter);
    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let edge_tbl = edge_scan(opts.edge_tables, &curr, opts.rel_type_filter);
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
        // _deleted=false on chained edges.
        join_cond = Expr::and(
            join_cond,
            Expr::eq(
                Expr::col(&curr, DELETED_COLUMN),
                Expr::param(ChType::Bool, false),
            ),
        );
        from = TableRef::join(JoinType::Inner, from, edge_tbl, join_cond);
    }

    // path_nodes: array of (id, kind) tuples.
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
        select.push(SelectExpr::new(
            Expr::col("e1", TRAVERSAL_PATH_COLUMN),
            TRAVERSAL_PATH_COLUMN,
        ));
    }

    // _deleted=false on the anchor edge (e1).
    let deleted_cond = Some(Expr::eq(
        Expr::col("e1", DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    let mut all_conds = vec![
        anchor_cond,
        first_type_cond.take(),
        anchor_kind_cond,
        scope_cond,
        deleted_cond,
    ];
    // Denorm tags on the anchor edge for prewhere granule skipping.
    all_conds.extend(opts.anchor_denorm_tags.iter().cloned().map(Some));

    Query {
        select,
        from,
        where_clause: Expr::and_all(all_conds),
        ..Default::default()
    }
}

/// Build a FROM for a single or multi-edge-table scan.
fn edge_scan(tables: &[String], alias: &str, _type_filter: &Option<Vec<String>>) -> TableRef {
    if tables.len() == 1 {
        TableRef::scan(&tables[0], alias)
    } else {
        let arms: Vec<Query> = tables
            .iter()
            .map(|table| Query {
                select: vec![SelectExpr::star()],
                from: TableRef::scan(table, format!("_{alias}")),
                ..Default::default()
            })
            .collect();
        TableRef::union_all(arms, alias)
    }
}

/// Build a type condition for an edge alias (separate from the FROM).
fn type_cond_for(alias: &str, type_filter: &Option<Vec<String>>) -> Option<Expr> {
    let types = type_filter.as_ref()?;
    Expr::col_in(
        alias,
        RELATIONSHIP_KIND_COLUMN,
        ChType::String,
        types.iter().map(|t| Value::String(t.clone())).collect(),
    )
}

/// Build denorm tag predicates for an anchor entity's filters.
fn build_denorm_tags(
    entity: &str,
    dir_prefix: &str,
    edge_alias: &str,
    filters: &std::collections::HashMap<String, InputFilter>,
    denorm_map: &std::collections::HashMap<(String, String, String), (String, String)>,
) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (prop, filter) in filters {
        let key = (entity.to_string(), prop.clone(), dir_prefix.to_string());
        let Some((tag_col, tag_key)) = denorm_map.get(&key) else {
            continue;
        };
        match filter.op {
            None | Some(FilterOp::Eq) => {
                let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                exprs.push(Expr::func(
                    "has",
                    vec![
                        Expr::col(edge_alias, tag_col.as_str()),
                        Expr::string(format!("{tag_key}:{val}")),
                    ],
                ));
            }
            Some(FilterOp::In) => {
                if let Some(values) = filter.value.as_ref().and_then(|v| v.as_array()) {
                    let tags: Vec<String> = values
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| format!("{tag_key}:{s}")))
                        .collect();
                    if tags.len() == 1 {
                        exprs.push(Expr::func(
                            "has",
                            vec![
                                Expr::col(edge_alias, tag_col.as_str()),
                                Expr::string(&tags[0]),
                            ],
                        ));
                    } else if !tags.is_empty() {
                        exprs.push(Expr::func(
                            "hasAny",
                            vec![
                                Expr::col(edge_alias, tag_col.as_str()),
                                Expr::func("array", tags.iter().map(Expr::string).collect()),
                            ],
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    exprs
}
