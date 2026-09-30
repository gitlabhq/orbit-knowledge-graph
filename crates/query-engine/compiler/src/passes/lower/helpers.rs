use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::error::Result;

use crate::passes::plan::*;
use crate::passes::shared::latest_row_dedup;
use crate::passes::shared::{
    deleted_false, denorm_tag_expr, filter_to_expr, id_list_predicate, rel_kind_filter,
};
pub(super) use crate::passes::shared::{latest_node_predicates, node_select_columns};

/// Narrowing source for a node's latest-row scan: a `_narrow_*` CTE referenced
/// by the node scan's WHERE clause.
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

pub(super) fn push_edge_predicates(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    table_columns: &HashMap<String, HashSet<String>>,
    skip_deleted: bool,
) {
    let (start_col, end_col) = hop.direction.edge_columns();

    if let Some(f) = rel_kind_filter(alias, &hop.rel_types) {
        where_parts.push(f);
    }
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        if let Some(np) = nodes.get(node_alias)
            && let Some(ref entity) = np.entity
        {
            let kind_col = if id_col == SOURCE_ID_COLUMN {
                SOURCE_KIND_COLUMN
            } else {
                TARGET_KIND_COLUMN
            };
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }
    if !skip_deleted {
        where_parts.push(deleted_false(alias));
    }

    // Push node-level filters down to the edge scan when the edge table
    // carries those columns (e.g. project_id, branch on gl_code_edge).
    // This lets ClickHouse use the primary key prefix for scoping.
    // Deduplicate by property name so we don't emit the same predicate
    // twice when both nodes share the filter (common case: both are
    // code entities in the same project).
    if let Some(edge_cols) = table_columns.get(&hop.edge_table) {
        let reserved: HashSet<&str> = ontology::constants::EDGE_RESERVED_COLUMNS
            .iter()
            .copied()
            .collect();
        let mut seen_props: HashSet<&str> = HashSet::new();
        for node_alias in [&hop.from_node, &hop.to_node] {
            if let Some(np) = nodes.get(node_alias) {
                for (prop, filter) in &np.filters {
                    if edge_cols.contains(prop)
                        && !reserved.contains(prop.as_str())
                        && seen_props.insert(prop.as_str())
                    {
                        where_parts.push(filter_to_expr(alias, prop, filter));
                    }
                }
            }
        }
    }
}

/// Build a `LIMIT 1 BY <sort_key> ORDER BY <sort_key>, _version DESC` subquery
/// over a plain (non-`FINAL`) scan, with WHERE predicates injected for PK
/// pruning. Reproduces `ReplacingMergeTree` latest-row semantics while keeping
/// column pruning and projections eligible. `select` is the projection (e.g.
/// `*` for edge aggregations, or the requested columns for hydration);
/// `sort_key` is the dedup identity (the table's full ORDER BY key).
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

pub(super) fn emit_denorm_tags(
    where_parts: &mut Vec<Expr>,
    plan: &Plan,
    hop: &Hop,
    edge_alias: &str,
    start_col: &str,
    end_col: &str,
    tagged: &mut HashSet<(String, String)>,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = plan.nodes.get(node_alias) else {
            continue;
        };
        let direction = if id_col == SOURCE_ID_COLUMN {
            query_data_model::DenormalizedDirection::Source
        } else {
            query_data_model::DenormalizedDirection::Target
        };
        for (prop, filter) in &np.filters {
            let tag_id = (node_alias.clone(), prop.clone());
            if tagged.contains(&tag_id) {
                continue;
            }
            let Some(property) = filter.property else {
                continue;
            };
            let key = query_data_model::DenormalizedKey {
                property,
                direction,
            };
            // Skip hops that don't write this tag; pushing it there matches an
            // empty edge and silently drops the row.
            if !hop_carries_denorm(plan, hop, &key) {
                continue;
            }
            if let Some(facts) = plan.denormalized.get(&key)
                && let Some(expr) = denorm_tag_expr(
                    edge_alias,
                    &facts.edge_column,
                    &facts.tag_key,
                    &filter.filter,
                )
            {
                where_parts.push(expr);
                tagged.insert(tag_id);
            }
        }
    }
}

fn hop_carries_denorm(plan: &Plan, hop: &Hop, key: &query_data_model::DenormalizedKey) -> bool {
    // A wildcard hop's relationship is unknown at runtime, so no tag is safe.
    if crate::passes::normalize::is_wildcard(&hop.rel_types) {
        return false;
    }
    plan.denormalized.get(key).is_some_and(|facts| {
        hop.relationships
            .iter()
            .any(|relationship| facts.relationships.contains(relationship))
    })
}

pub(super) fn node_id_pin_predicates(
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
) -> Vec<Expr> {
    let (start_col, end_col) = hop.direction.edge_columns();
    let mut out = Vec::new();
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        if let Some(ref range) = np.id_range {
            out.push(Expr::and(
                Expr::binary(Op::Ge, Expr::col(alias, id_col), Expr::int(range.start)),
                Expr::binary(Op::Le, Expr::col(alias, id_col), Expr::int(range.end)),
            ));
        }
        if !np.node_ids.is_empty() {
            out.push(id_list_predicate(alias, id_col, &np.node_ids));
        }
    }
    out
}

pub(super) fn emit_node_ids_on_edge(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    start_col: &str,
    end_col: &str,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        if let Some(ref range) = np.id_range {
            where_parts.push(Expr::and(
                Expr::binary(Op::Ge, Expr::col(alias, id_col), Expr::int(range.start)),
                Expr::binary(Op::Le, Expr::col(alias, id_col), Expr::int(range.end)),
            ));
        }
        if !np.node_ids.is_empty() {
            where_parts.push(id_list_predicate(alias, id_col, &np.node_ids));
        }
    }
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
