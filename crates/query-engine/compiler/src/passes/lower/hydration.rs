//! Hydration emit: fetch node properties for a set of IDs.
//!
//! Produces a UNION ALL of per-entity latest-row scans with `FINAL` and
//! `_deleted=false` filtering.
//!
//! When the base query provided traversal paths, each arm injects a
//! `startsWith(traversal_path, tp)` predicate so ClickHouse can prune
//! granules via the primary key (sort key starts with `traversal_path`).

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use crate::passes::plan::HydrationNodePlan;
use crate::passes::shared::dedup_subquery;

const ARRAY_EXISTS_PATH_THRESHOLD: usize = 256;

#[derive(Clone, Copy)]
struct HydrationPathFilterContext {
    array_exists_path_threshold: usize,
}

impl Default for HydrationPathFilterContext {
    fn default() -> Self {
        Self {
            array_exists_path_threshold: ARRAY_EXISTS_PATH_THRESHOLD,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HydrationPathFilterShape {
    OrStartsWith,
    ArrayExists,
}

impl HydrationPathFilterContext {
    fn shape_for(self, is_dynamic: bool, path_count: usize) -> HydrationPathFilterShape {
        if is_dynamic && path_count > self.array_exists_path_threshold {
            HydrationPathFilterShape::ArrayExists
        } else {
            HydrationPathFilterShape::OrStartsWith
        }
    }
}

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_hydration(nodes: &[HydrationNodePlan], limit: u32, is_dynamic: bool) -> Result<Node> {
    let mut arms = nodes.iter().map(|n| emit_arm(n, is_dynamic));
    let mut first = arms
        .next()
        .ok_or_else(|| QueryError::Lowering("hydration requires at least one node".into()))??;
    for arm in arms {
        first.union_all.push(arm?);
    }
    first.limit = Some(limit);
    Ok(Node::Query(Box::new(first)))
}

fn emit_arm(node: &HydrationNodePlan, is_dynamic: bool) -> Result<Query> {
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
    if let Some(tp_filter) = traversal_path_filter(alias, &node.traversal_paths, is_dynamic) {
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

/// Build a traversal-path predicate from collected paths.
///
/// 1. **Leaf pruning:** drop any path that is a strict prefix of another
///    in the set. Keeps the most specific (deepest) paths for maximum
///    granule selectivity. Safe because `id IN (...)` is the correctness
///    guarantee — TP is purely a scan optimizer.
/// 2. **Small path sets:** balanced OR of `startsWith` calls. This keeps the
///    primary-key pruning that makes low-fanout hydration fast.
/// 3. **Large dynamic path sets:** `arrayExists(path -> startsWith(...))`.
///    This avoids ClickHouse parser-depth failures when dynamic hydration
///    discovers hundreds of traversal paths.
fn traversal_path_filter(alias: &str, paths: &[String], is_dynamic: bool) -> Option<Expr> {
    if paths.is_empty() {
        return None;
    }
    let leaves = prune_to_leaves(paths);
    if leaves.is_empty() {
        return None;
    }
    let ctx = HydrationPathFilterContext::default();
    match ctx.shape_for(is_dynamic, leaves.len()) {
        HydrationPathFilterShape::OrStartsWith => or_starts_with(alias, &leaves),
        HydrationPathFilterShape::ArrayExists => Some(array_exists_starts_with(alias, &leaves)),
    }
}

fn or_starts_with(alias: &str, paths: &[String]) -> Option<Expr> {
    or_balanced(paths.iter().map(|tp| starts_with_path(alias, tp)).collect())
}

fn starts_with_path(alias: &str, tp: &str) -> Expr {
    Expr::func(
        "startsWith",
        vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string(tp)],
    )
}

fn array_exists_starts_with(alias: &str, paths: &[String]) -> Expr {
    let lambda_param = "_gkg_path";
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

fn or_balanced(mut exprs: Vec<Expr>) -> Option<Expr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => {
            let right = exprs.split_off(exprs.len() / 2);
            let left = exprs;
            Some(Expr::binary(
                Op::Or,
                or_balanced(left)?,
                or_balanced(right)?,
            ))
        }
    }
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

    fn render_with_params(
        node: &Node,
    ) -> (
        String,
        std::collections::HashMap<String, crate::passes::codegen::ParamValue>,
    ) {
        let q = codegen(node, ResultContext::new(), QueryConfig::empty()).unwrap();
        (q.sql, q.params)
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

    fn emit_dynamic(plans: &[HydrationNodePlan], limit: u32) -> Node {
        emit_hydration(plans, limit, true).unwrap()
    }

    fn emit_static(plans: &[HydrationNodePlan], limit: u32) -> Node {
        emit_hydration(plans, limit, false).unwrap()
    }

    #[test]
    fn dynamic_single_tp_emits_starts_with() {
        let node = emit_dynamic(&[plan(vec!["title"], vec![1, 2], vec!["1/9970/"])], 10);
        let sql = render(&node);
        assert!(
            sql.contains("startsWith"),
            "dynamic single TP should emit startsWith: {sql}"
        );
        assert!(
            !sql.contains("arrayExists"),
            "small dynamic path set should not emit arrayExists: {sql}"
        );
        assert!(
            sql.contains("traversal_path"),
            "should reference traversal_path column: {sql}"
        );
    }

    #[test]
    fn dynamic_multiple_tps_emit_or_disjunction() {
        let node = emit_dynamic(
            &[plan(
                vec!["title"],
                vec![1],
                vec!["1/9970/100/", "1/9970/200/"],
            )],
            10,
        );
        let sql = render(&node);
        let starts_with_count = sql.matches("startsWith").count();
        assert_eq!(
            starts_with_count, 2,
            "two leaf TPs should produce two startsWith calls: {sql}"
        );
        assert!(sql.contains(" OR "), "two TPs should use OR: {sql}");
        assert!(
            !sql.contains("arrayExists"),
            "small dynamic path set should use OR not arrayExists: {sql}"
        );
        assert!(
            sql.contains("traversal_path"),
            "should reference traversal_path column: {sql}"
        );
    }

    #[test]
    fn static_single_tp_emits_starts_with() {
        let node = emit_static(&[plan(vec!["title"], vec![1, 2], vec!["1/9970/"])], 10);
        let sql = render(&node);
        assert!(
            sql.contains("startsWith"),
            "static single TP should emit startsWith: {sql}"
        );
        assert!(
            !sql.contains("arrayExists"),
            "static path should not emit arrayExists: {sql}"
        );
        assert!(
            sql.contains("traversal_path"),
            "should reference traversal_path column: {sql}"
        );
    }

    #[test]
    fn static_multiple_tps_emit_or_chain() {
        let node = emit_static(
            &[plan(
                vec!["title"],
                vec![1],
                vec!["1/9970/100/", "1/9970/200/", "1/9970/300/"],
            )],
            10,
        );
        let sql = render(&node);
        let starts_with_count = sql.matches("startsWith").count();
        assert_eq!(
            starts_with_count, 3,
            "static should emit one startsWith per leaf path: {sql}"
        );
        assert!(
            !sql.contains("arrayExists"),
            "static path must not emit arrayExists: {sql}"
        );
        assert!(
            sql.contains(" OR "),
            "static path should OR multiple startsWith calls: {sql}"
        );
    }

    #[test]
    fn large_dynamic_tp_sets_emit_array_exists() {
        let paths: Vec<String> = (0..=ARRAY_EXISTS_PATH_THRESHOLD)
            .map(|id| format!("1/9970/{id}/"))
            .collect();
        let plan = HydrationNodePlan {
            alias: "hydrate".into(),
            table: "gl_merge_request".into(),
            entity: "MergeRequest".into(),
            id_property: "id".into(),
            node_ids: vec![1],
            columns: vec!["title".into()],
            traversal_paths: paths.clone(),
        };

        let node = emit_hydration(&[plan], 10, true).unwrap();
        let (sql, params) = render_with_params(&node);

        assert!(
            sql.contains("arrayExists"),
            "large dynamic TP sets should use arrayExists: {sql}"
        );
        assert_eq!(
            sql.matches("startsWith").count(),
            1,
            "arrayExists should keep one startsWith in the lambda: {sql}"
        );
        let array_params: Vec<_> = params
            .values()
            .filter_map(|p| match &p.value {
                serde_json::Value::Array(items) => Some(items),
                _ => None,
            })
            .collect();
        assert_eq!(
            array_params.len(),
            1,
            "expected one traversal-path array param"
        );
        assert_eq!(array_params[0].len(), paths.len());
    }

    #[test]
    fn large_static_tp_sets_emit_or() {
        let paths: Vec<String> = (0..=ARRAY_EXISTS_PATH_THRESHOLD)
            .map(|id| format!("1/9970/{id}/"))
            .collect();
        let plan = HydrationNodePlan {
            alias: "hydrate".into(),
            table: "gl_merge_request".into(),
            entity: "MergeRequest".into(),
            id_property: "id".into(),
            node_ids: vec![1],
            columns: vec!["title".into()],
            traversal_paths: paths,
        };

        let node = emit_hydration(&[plan], 10, false).unwrap();
        let sql = render(&node);

        assert!(
            !sql.contains("arrayExists"),
            "static TP sets should keep OR startsWith: {sql}"
        );
        assert_eq!(
            sql.matches("startsWith").count(),
            ARRAY_EXISTS_PATH_THRESHOLD + 1
        );
    }

    #[test]
    fn dynamic_no_tp_omits_path_filter() {
        let node = emit_dynamic(&[plan(vec!["title"], vec![1, 2], vec![])], 10);
        let sql = render(&node);
        assert!(
            !sql.contains("startsWith"),
            "empty TPs should not emit startsWith: {sql}"
        );
        assert!(
            !sql.contains("arrayExists"),
            "empty TPs should not emit arrayExists: {sql}"
        );
    }

    #[test]
    fn static_no_tp_omits_path_filter() {
        let node = emit_static(&[plan(vec!["title"], vec![1, 2], vec![])], 10);
        let sql = render(&node);
        assert!(
            !sql.contains("startsWith"),
            "empty TPs should not emit startsWith: {sql}"
        );
    }

    #[test]
    fn dynamic_tp_filter_precedes_id_filter() {
        let node = emit_dynamic(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }

    #[test]
    fn static_tp_filter_precedes_id_filter() {
        let node = emit_static(&[plan(vec!["title"], vec![1], vec!["1/9970/"])], 10);
        let sql = render(&node);
        let tp_pos = sql.find("startsWith").unwrap();
        let in_pos = sql.find(" IN ").or_else(|| sql.find(" = ")).unwrap();
        assert!(
            tp_pos < in_pos,
            "TP filter should precede ID filter for primary key pruning: {sql}"
        );
    }

    #[test]
    fn dynamic_leaf_pruning_drops_broad_prefix() {
        // 1/9970/ is a prefix of 1/9970/100/, should be dropped from the OR
        let node = emit_dynamic(
            &[plan(vec!["title"], vec![1], vec!["1/9970/", "1/9970/100/"])],
            10,
        );
        let sql = render(&node);
        assert_eq!(
            sql.matches("startsWith").count(),
            1,
            "ancestor should be pruned, only one startsWith for the leaf: {sql}"
        );
        assert!(
            !sql.contains("arrayExists"),
            "small dynamic leaf set should not emit arrayExists: {sql}"
        );
    }

    #[test]
    fn static_leaf_pruning_drops_broad_prefix() {
        // 1/9970/ is a prefix of 1/9970/100/, should be dropped from the OR
        let node = emit_static(
            &[plan(vec!["title"], vec![1], vec!["1/9970/", "1/9970/100/"])],
            10,
        );
        let sql = render(&node);
        let starts_with_count = sql.matches("startsWith").count();
        assert_eq!(
            starts_with_count, 1,
            "ancestor should be pruned, only one startsWith for the leaf: {sql}"
        );
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
