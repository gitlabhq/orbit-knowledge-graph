//! Hydration emit: fetch node properties for a set of IDs.
//!
//! Produces a UNION ALL of per-entity dedup scans with inline
//! `LIMIT 1 BY id` dedup and `_deleted=false` filtering.
//!
//! When the base query provided traversal paths, each arm injects a
//! `startsWith(traversal_path, tp)` predicate so ClickHouse can prune
//! granules via the primary key (sort key starts with `traversal_path`).

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use crate::passes::plan::HydrationNodePlan;
use crate::passes::shared::dedup_subquery;

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_hydration(nodes: &[HydrationNodePlan], limit: u32) -> Result<Node> {
    let mut arms = nodes.iter().map(emit_arm);
    let mut first = arms
        .next()
        .ok_or_else(|| QueryError::Lowering("hydration requires at least one node".into()))??;
    for arm in arms {
        first.union_all.push(arm?);
    }
    first.limit = Some(limit);
    Ok(Node::Query(Box::new(first)))
}

fn emit_arm(node: &HydrationNodePlan) -> Result<Query> {
    let alias = &node.alias;
    let pk = &node.id_property;

    let json_expr = if node.columns.is_empty() {
        Expr::string("{}")
    } else {
        let map_args: Vec<Expr> = node
            .columns
            .iter()
            .flat_map(|col| {
                [
                    Expr::string(col),
                    Expr::func("toString", vec![Expr::col(alias, col)]),
                ]
            })
            .collect();
        Expr::func("toJSONString", vec![Expr::func("map", map_args)])
    };

    let mut scan_where = Vec::new();

    // Narrow scan via traversal_path when base query provided TPs.
    if let Some(tp_filter) = traversal_path_filter(alias, &node.traversal_paths) {
        scan_where.push(tp_filter);
    }

    if let Some(id_filter) = Expr::col_in(
        alias,
        pk,
        ChType::Int64,
        node.node_ids
            .iter()
            .map(|id| serde_json::Value::Number((*id).into()))
            .collect(),
    ) {
        scan_where.push(id_filter);
    }

    let (from, deleted) = dedup_subquery(
        alias,
        &node.table,
        vec![
            SelectExpr::col(alias, pk),
            SelectExpr::col(alias, DELETED_COLUMN),
            SelectExpr::star(),
        ],
        scan_where,
        pk,
    );

    Ok(Query {
        select: vec![
            SelectExpr::new(Expr::col(alias, pk), format!("{alias}_{pk}")),
            SelectExpr::new(Expr::string(&node.entity), format!("{alias}_entity_type")),
            SelectExpr::new(json_expr, format!("{alias}_props")),
        ],
        from,
        where_clause: Some(deleted),
        ..Default::default()
    })
}

/// Build a `startsWith` predicate from collected traversal paths.
///
/// 1. **Leaf pruning:** drop any path that is a strict prefix of another
///    in the set. Keeps the most specific (deepest) paths for maximum
///    granule selectivity. Safe because `id IN (...)` is the correctness
///    guarantee — TP is purely a scan optimizer.
/// 2. **Single path:** `startsWith(alias.traversal_path, 'tp')`
/// 3. **Multiple paths:** `startsWith(tp, LCP) AND arrayExists(p -> startsWith(tp, p), [tps])`
///    Same structure as the security pass — the LCP lets ClickHouse prune
///    at the primary key level, and arrayExists handles the remaining paths.
fn traversal_path_filter(alias: &str, paths: &[String]) -> Option<Expr> {
    if paths.is_empty() {
        return None;
    }
    let leaves = prune_to_leaves(paths);
    match leaves.len() {
        0 => None,
        1 => Some(starts_with(alias, &leaves[0])),
        _ => {
            let lcp = lowest_common_prefix(&leaves);
            let lcp_filter = starts_with(alias, &lcp);
            let array_filter = array_exists_filter(alias, &leaves);
            Some(Expr::and(lcp_filter, array_filter))
        }
    }
}

fn starts_with(alias: &str, path: &str) -> Expr {
    Expr::func(
        "startsWith",
        vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string(path)],
    )
}

/// `arrayExists(p -> startsWith(alias.traversal_path, p), [paths])`
fn array_exists_filter(alias: &str, paths: &[String]) -> Expr {
    let lambda_param = "_gkg_tp";
    Expr::func(
        "arrayExists",
        vec![
            Expr::lambda(
                lambda_param,
                Expr::func(
                    "startsWith",
                    vec![
                        Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                        Expr::ident(lambda_param),
                    ],
                ),
            ),
            Expr::param(
                ChType::String.to_array(),
                serde_json::Value::Array(
                    paths
                        .iter()
                        .map(|p| serde_json::Value::String(p.clone()))
                        .collect(),
                ),
            ),
        ],
    )
}

/// Drop any path that is a strict prefix of another path in the set.
///
/// Given sorted paths, a path is an "ancestor" if another (longer) path
/// starts with it. Keeping only leaves maximizes primary-key selectivity
/// in the hydration scan.
fn prune_to_leaves(paths: &[String]) -> Vec<String> {
    if paths.len() <= 1 {
        return paths.to_vec();
    }
    let mut sorted: Vec<&str> = paths.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut leaves = Vec::with_capacity(sorted.len());
    for (i, path) in sorted.iter().enumerate() {
        let is_prefix_of_next = sorted.get(i + 1).is_some_and(|next| next.starts_with(path));
        if !is_prefix_of_next {
            leaves.push((*path).to_string());
        }
    }
    leaves
}

/// Longest common prefix across path segments.
fn lowest_common_prefix(paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let segments: Vec<Vec<&str>> = paths
        .iter()
        .map(|p| p.trim_end_matches('/').split('/').collect())
        .collect();
    let first = &segments[0];
    let common_len = (0..first.len())
        .take_while(|&i| segments.iter().all(|s| s.get(i) == first.get(i)))
        .count();
    if common_len == 0 {
        String::new()
    } else {
        format!("{}/", first[..common_len].join("/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::codegen::codegen;
    use crate::passes::enforce::ResultContext;
    use gkg_server_config::QueryConfig;

    fn render(node: &Node) -> String {
        codegen(node, ResultContext::new(), QueryConfig::empty())
            .unwrap()
            .sql
    }

    fn plan(
        columns: Vec<&str>,
        node_ids: Vec<i64>,
        traversal_paths: Vec<&str>,
    ) -> HydrationNodePlan {
        HydrationNodePlan {
            alias: "hydrate".into(),
            table: "gl_merge_request".into(),
            entity: "MergeRequest".into(),
            id_property: "id".into(),
            node_ids,
            columns: columns.into_iter().map(String::from).collect(),
            traversal_paths: traversal_paths.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn single_tp_emits_starts_with() {
        let node = emit_hydration(&[plan(vec!["title"], vec![1, 2], vec!["1/9970/"])], 10).unwrap();
        let sql = render(&node);
        assert!(
            sql.contains("startsWith"),
            "single TP should emit startsWith: {sql}"
        );
        assert!(
            sql.contains("traversal_path"),
            "should reference traversal_path column: {sql}"
        );
    }

    #[test]
    fn multiple_tps_emit_lcp_and_array_exists() {
        let node = emit_hydration(
            &[plan(
                vec!["title"],
                vec![1],
                vec!["1/9970/100/", "1/9970/200/"],
            )],
            10,
        )
        .unwrap();
        let sql = render(&node);
        assert!(
            sql.contains("arrayExists"),
            "multi-TP should use arrayExists: {sql}"
        );
        // LCP startsWith + arrayExists inner startsWith = 2
        let count = sql.matches("startsWith").count();
        assert_eq!(
            count, 2,
            "LCP + arrayExists should produce two startsWith: {sql}"
        );
    }

    #[test]
    fn no_tp_omits_starts_with() {
        let node = emit_hydration(&[plan(vec!["title"], vec![1, 2], vec![])], 10).unwrap();
        let sql = render(&node);
        assert!(
            !sql.contains("startsWith"),
            "empty TPs should not emit startsWith: {sql}"
        );
    }

    #[test]
    fn tp_filter_precedes_id_filter() {
        let node = emit_hydration(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10).unwrap();
        let sql = render(&node);
        let starts_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            starts_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }

    #[test]
    fn leaf_pruning_drops_broad_prefix() {
        // 1/9970/ is a prefix of 1/9970/100/ — should be dropped
        let node = emit_hydration(
            &[plan(vec!["title"], vec![1], vec!["1/9970/", "1/9970/100/"])],
            10,
        )
        .unwrap();
        let sql = render(&node);
        // Only the leaf path should survive — single startsWith, no arrayExists
        assert!(
            !sql.contains("arrayExists"),
            "ancestor should be pruned, leaving single TP: {sql}"
        );
        let count = sql.matches("startsWith").count();
        assert_eq!(count, 1, "only leaf path should remain: {sql}");
    }

    #[test]
    fn leaf_pruning_keeps_sibling_paths() {
        let leaves =
            prune_to_leaves(&["1/9970/".into(), "1/9970/100/".into(), "1/9970/200/".into()]);
        assert_eq!(leaves, vec!["1/9970/100/", "1/9970/200/"]);
    }

    #[test]
    fn leaf_pruning_noop_when_no_ancestors() {
        let leaves = prune_to_leaves(&["1/9970/100/".into(), "1/9970/200/".into()]);
        assert_eq!(leaves, vec!["1/9970/100/", "1/9970/200/"]);
    }
}
