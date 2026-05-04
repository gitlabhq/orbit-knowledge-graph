//! Neighbors: single-hop edge scan for adjacent entities.
//!
//! For Direction::Both, produces outgoing UNION ALL incoming.
//! The enforce pass handles _gkg_* column injection (Neighbors emits
//! its own redaction columns directly since the center edge column
//! differs per direction arm).

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::plan::NeighborsPlan;

// ─── Plan ────────────────────────────────────────────────────────────────────

pub fn plan_neighbors(input: &Input) -> Result<NeighborsPlan> {
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = input
        .nodes
        .iter()
        .find(|n| n.id == config.node)
        .ok_or_else(|| QueryError::Lowering(format!("center node '{}' not found", config.node)))?;
    let center_entity = center_node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node entity missing".into()))?
        .clone();
    let center_table = center_node
        .table
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node table missing".into()))?
        .clone();

    let center_filters: Vec<(String, InputFilter)> =
        center_node.filters.clone().into_iter().collect();

    let has_non_denorm = center_filters.iter().any(|(prop, _)| {
        let src = input.compiler.denormalized_columns.contains_key(&(
            center_entity.clone(),
            prop.clone(),
            "source".to_string(),
        ));
        let tgt = input.compiler.denormalized_columns.contains_key(&(
            center_entity.clone(),
            prop.clone(),
            "target".to_string(),
        ));
        !src && !tgt
    }) || center_node.id_range.is_some();

    Ok(NeighborsPlan {
        center_id: center_node.id.clone(),
        center_entity,
        center_table,
        center_uses_default_pk: center_node.redaction_id_column == DEFAULT_PRIMARY_KEY,
        center_redaction_col: center_node.redaction_id_column.clone(),
        center_node_ids: center_node.node_ids.clone(),
        center_filters,
        center_id_range: center_node.id_range.clone(),
        has_non_denorm,
        direction: config.direction,
        edge_tables: input.compiler.resolve_edge_tables(&config.rel_types),
        rel_type_filter: if config.rel_types.is_empty() {
            None
        } else {
            Some(config.rel_types.clone())
        },
        denorm_columns: input.compiler.denormalized_columns.clone(),
        order_by: input.order_by.clone(),
        cursor: input.cursor,
        limit: input.limit,
    })
}

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_neighbors(p: NeighborsPlan, input: &mut Input) -> Result<Node> {
    let center_id = p.center_id.clone();
    let center_entity = p.center_entity.clone();
    let center_table = p.center_table.clone();
    let center_uses_default_pk = p.center_uses_default_pk;
    let center_redaction_col = p.center_redaction_col.clone();
    let center_node_ids = p.center_node_ids.clone();
    let center_filters = p.center_filters.clone();
    let center_id_range = p.center_id_range.clone();
    let has_non_denorm = p.has_non_denorm;
    let edge_table = p.edge_tables.clone();
    let edge_alias = "e";

    /// Build an inline dedup subquery for the center node (no CTE).
    /// Returns a TableRef::Subquery that can be JOINed to the edge.
    fn build_center_dedup(
        alias: &str,
        table: &str,
        filters: &[(String, InputFilter)],
        node_ids: &[i64],
        id_range: Option<&InputIdRange>,
    ) -> (TableRef, Expr) {
        let mut scan_where = Vec::new();
        for (prop, filter) in filters {
            scan_where.push(super::shared::filter_to_expr(alias, prop, filter));
        }
        if !node_ids.is_empty() {
            scan_where.push(super::shared::id_list_predicate(
                alias,
                DEFAULT_PRIMARY_KEY,
                node_ids,
            ));
        }
        if let Some(range) = id_range {
            scan_where.push(super::shared::id_range_predicate(alias, range));
        }
        let dedup_scan = Query {
            select: vec![
                SelectExpr::new(Expr::col(alias, DEFAULT_PRIMARY_KEY), DEFAULT_PRIMARY_KEY),
                SelectExpr::new(Expr::col(alias, DELETED_COLUMN), DELETED_COLUMN),
            ],
            from: TableRef::scan(table, alias),
            where_clause: Expr::conjoin(scan_where),
            order_by: vec![OrderExpr {
                expr: Expr::col(alias, VERSION_COLUMN),
                desc: true,
            }],
            limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
            ..Default::default()
        };
        let from = TableRef::Subquery {
            query: Box::new(dedup_scan),
            alias: alias.to_string(),
        };
        let deleted_filter = Expr::eq(
            Expr::col(alias, DELETED_COLUMN),
            Expr::param(ChType::Bool, false),
        );
        (from, deleted_filter)
    }

    let edge_tiebreakers = || -> Vec<OrderExpr> {
        vec![
            OrderExpr {
                expr: Expr::col(edge_alias, SOURCE_ID_COLUMN),
                desc: false,
            },
            OrderExpr {
                expr: Expr::col(edge_alias, TARGET_ID_COLUMN),
                desc: false,
            },
            OrderExpr {
                expr: Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN),
                desc: false,
            },
        ]
    };
    let order_by = match &p.order_by {
        Some(ob) => {
            let mut exprs = vec![OrderExpr {
                expr: Expr::col(&ob.node, &ob.property),
                desc: ob.direction == OrderDirection::Desc,
            }];
            if p.cursor.is_some() {
                exprs.extend(edge_tiebreakers());
            }
            exprs
        }
        None if p.cursor.is_some() => edge_tiebreakers(),
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
            where_parts.push(super::shared::id_list_predicate(
                edge_alias,
                center_edge_col,
                &center_node_ids,
            ));
        }

        // Push denorm filters as tags directly on edge.
        let denorm_dir = if dir == Direction::Outgoing {
            "source"
        } else {
            "target"
        };
        for (prop, filter) in &center_filters {
            let key = (center_entity.clone(), prop.clone(), denorm_dir.to_string());
            if let Some((tag_col, tag_key)) = p.denorm_columns.get(&key) {
                push_denorm_tag(&mut where_parts, edge_alias, tag_col, tag_key, filter);
            }
        }

        // Relationship type filter.
        if let Some(ref types) = p.rel_type_filter
            && let Some(f) = Expr::col_in(
                edge_alias,
                RELATIONSHIP_KIND_COLUMN,
                ChType::String,
                types
                    .iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            )
        {
            where_parts.push(f);
        }

        // _deleted on edge.
        where_parts.push(Expr::eq(
            Expr::col(edge_alias, DELETED_COLUMN),
            Expr::param(ChType::Bool, false),
        ));

        let mut select = vec![
            SelectExpr::new(Expr::col(edge_alias, neighbor_id), neighbor_id_column()),
            SelectExpr::new(Expr::col(edge_alias, neighbor_type), neighbor_type_column()),
            SelectExpr::new(
                Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN),
                relationship_type_column(),
            ),
            SelectExpr::new(Expr::int(is_outgoing), neighbor_is_outgoing_column()),
        ];

        let mut from: TableRef = if edge_table.len() == 1 {
            TableRef::scan(&edge_table[0], edge_alias)
        } else {
            let arms: Vec<Query> = edge_table
                .iter()
                .map(|table| Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(table, format!("_{edge_alias}")),
                    ..Default::default()
                })
                .collect();
            TableRef::union_all(arms, edge_alias)
        };

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

    // Populate node_edge_col so the enforce pass can find the center node.
    // Neighbors emits _gkg_* columns itself (center edge col differs per arm),
    // so the enforce pass skips redaction injection for Neighbors but still
    // needs the mapping for security context.
    input.compiler.node_edge_col.insert(
        center_id.clone(),
        (edge_alias.to_string(), SOURCE_ID_COLUMN.to_string()),
    );

    if p.direction == Direction::Both {
        let mut outgoing = build_arm(Direction::Outgoing);
        outgoing.union_all = vec![build_arm(Direction::Incoming)];
        outgoing.order_by = order_by;
        outgoing.limit = Some(p.limit);
        Ok(Node::Query(Box::new(outgoing)))
    } else {
        let mut arm = build_arm(p.direction);
        arm.order_by = order_by;
        arm.limit = Some(p.limit);
        Ok(Node::Query(Box::new(arm)))
    }
}

/// Push a denormalized tag predicate onto the edge WHERE.
fn push_denorm_tag(
    where_parts: &mut Vec<Expr>,
    edge_alias: &str,
    tag_col: &str,
    tag_key: &str,
    filter: &InputFilter,
) {
    match filter.op {
        None | Some(FilterOp::Eq) => {
            let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
            where_parts.push(Expr::func(
                "has",
                vec![
                    Expr::col(edge_alias, tag_col),
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
                    where_parts.push(Expr::func(
                        "has",
                        vec![Expr::col(edge_alias, tag_col), Expr::string(&tags[0])],
                    ));
                } else if !tags.is_empty() {
                    where_parts.push(Expr::func(
                        "hasAny",
                        vec![
                            Expr::col(edge_alias, tag_col),
                            Expr::func("array", tags.iter().map(Expr::string).collect()),
                        ],
                    ));
                }
            }
        }
        _ => {}
    }
}
