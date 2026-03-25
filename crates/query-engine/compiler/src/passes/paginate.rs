//! Pagination pass.
//!
//! Runs after optimize — injects keyset cursor predicates into the query
//! and any SIP CTEs that optimize may have created. Removes OFFSET when
//! cursor-based or node_id-based pagination makes it redundant.
//!
//! ## Why this is a separate pass
//!
//! Keyset pagination is a semantic transformation, not an optimization.
//! Separating it lets optimize build SIP CTEs without knowing about cursors,
//! and lets this pass inject the keyset predicate everywhere it's needed
//! in one place — the main WHERE and the `_root_ids` CTE.

use crate::ast::{ChType, Expr, Node, Op, Query};
use crate::passes::optimize::ROOT_SIP_CTE;
use crate::passes::security::SecurityContext;
use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};

use crate::input::Input;

/// Apply pagination to the AST.
///
/// When a cursor is present, builds a decomposed keyset predicate and
/// injects it into:
///   1. The main query's WHERE clause
///   2. The `_root_ids` SIP CTE (if optimize created one)
///
/// Also strips OFFSET when cursor or `node_ids` make positional skipping
/// redundant.
pub fn paginate(node: &mut Node, input: &Input, ctx: &SecurityContext) {
    let Node::Query(q) = node;

    let root_node = match input.nodes.first() {
        Some(n) => n,
        None => return,
    };

    let has_node_ids = !root_node.node_ids.is_empty();

    if let Some(cursor) = &input.cursor {
        let root_alias = &root_node.id;
        let predicate = build_keyset_predicate(root_alias, &ctx.traversal_paths, cursor.id);

        // Inject into the main query WHERE.
        q.where_clause = Expr::and_all([q.where_clause.take(), Some(predicate.clone())]);
        q.offset = None;

        // Inject into the _root_ids SIP CTE if it exists.
        inject_into_sip_cte(q, predicate);
    } else if has_node_ids {
        q.offset = None;
    }
}

/// Build the full keyset predicate from traversal paths.
///
/// Single path → one exact predicate.
/// Multiple paths → OR over each path's predicate.
fn build_keyset_predicate(alias: &str, traversal_paths: &[String], cursor_id: i64) -> Expr {
    if traversal_paths.len() == 1 {
        build_keyset_expr(alias, &traversal_paths[0], cursor_id)
    } else {
        Expr::or_all(
            traversal_paths
                .iter()
                .map(|tp| Some(build_keyset_expr(alias, tp, cursor_id))),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
}

/// Build a decomposed keyset predicate for one traversal path:
///   `(traversal_path > :tp) OR (traversal_path = :tp AND id > :cursor_id)`
fn build_keyset_expr(alias: &str, tp: &str, cursor_id: i64) -> Expr {
    let tp_gt = Expr::binary(
        Op::Gt,
        Expr::col(alias, TRAVERSAL_PATH_COLUMN),
        Expr::param(ChType::String, tp.to_string()),
    );
    let tp_eq_and_id_gt = Expr::and(
        Expr::eq(
            Expr::col(alias, TRAVERSAL_PATH_COLUMN),
            Expr::param(ChType::String, tp.to_string()),
        ),
        Expr::binary(
            Op::Gt,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::param(ChType::Int64, cursor_id),
        ),
    );
    Expr::or(tp_gt, tp_eq_and_id_gt)
}

/// Find the `_root_ids` SIP CTE and AND the keyset predicate into its WHERE.
fn inject_into_sip_cte(q: &mut Query, predicate: Expr) {
    for cte in &mut q.ctes {
        if cte.name == ROOT_SIP_CTE {
            cte.query.where_clause =
                Expr::and_all([cte.query.where_clause.take(), Some(predicate)]);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cte, SelectExpr, TableRef};
    use crate::input::{InputCursor, InputNode};

    fn single_path_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    fn multi_path_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into(), "1/2/".into()]).unwrap()
    }

    fn search_input_with_cursor(cursor_id: i64) -> Input {
        Input {
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                ..Default::default()
            }],
            cursor: Some(InputCursor { id: cursor_id }),
            ..Default::default()
        }
    }

    fn search_input_with_node_ids() -> Input {
        Input {
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                node_ids: vec![1, 2, 3],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn base_query() -> Query {
        Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::scan("gl_user", "u"),
            offset: Some(100),
            ..Default::default()
        }
    }

    fn query_with_sip_cte() -> Query {
        let sip = Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::scan("gl_user", "u"),
            where_clause: Some(Expr::eq(
                Expr::col("u", "active"),
                Expr::param(ChType::Bool, true),
            )),
            ..Default::default()
        };
        Query {
            select: vec![SelectExpr::new(Expr::col("u", "id"), "id")],
            from: TableRef::scan("gl_user", "u"),
            ctes: vec![Cte::new(ROOT_SIP_CTE, sip)],
            offset: Some(100),
            ..Default::default()
        }
    }

    #[test]
    fn cursor_injects_keyset_predicate_single_path() {
        let input = search_input_with_cursor(42);
        let ctx = single_path_ctx();
        let mut node = Node::Query(Box::new(base_query()));

        paginate(&mut node, &input, &ctx);

        let Node::Query(q) = &node;
        assert!(q.where_clause.is_some(), "keyset predicate should be set");
        assert_eq!(q.offset, None, "offset should be removed");
    }

    #[test]
    fn cursor_injects_keyset_predicate_multi_path() {
        let input = search_input_with_cursor(42);
        let ctx = multi_path_ctx();
        let mut node = Node::Query(Box::new(base_query()));

        paginate(&mut node, &input, &ctx);

        let Node::Query(q) = &node;
        assert!(q.where_clause.is_some());
        assert_eq!(q.offset, None);
    }

    #[test]
    fn cursor_injects_into_sip_cte() {
        let input = search_input_with_cursor(42);
        let ctx = single_path_ctx();
        let mut node = Node::Query(Box::new(query_with_sip_cte()));

        paginate(&mut node, &input, &ctx);

        let Node::Query(q) = &node;
        // Main query gets keyset predicate
        assert!(q.where_clause.is_some());
        // SIP CTE WHERE should now have both the original filter AND keyset
        let sip_where = q.ctes[0].query.where_clause.as_ref().unwrap();
        let where_str = format!("{sip_where:?}");
        assert!(
            where_str.contains("Gt"),
            "SIP CTE should contain keyset Gt predicate: {where_str}"
        );
    }

    #[test]
    fn node_ids_removes_offset_without_cursor() {
        let input = search_input_with_node_ids();
        let ctx = single_path_ctx();
        let mut node = Node::Query(Box::new(base_query()));

        paginate(&mut node, &input, &ctx);

        let Node::Query(q) = &node;
        assert_eq!(q.offset, None, "offset should be removed for node_ids");
        assert!(
            q.where_clause.is_none(),
            "no keyset predicate without cursor"
        );
    }

    #[test]
    fn no_cursor_no_node_ids_preserves_offset() {
        let input = Input {
            nodes: vec![InputNode {
                id: "u".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let ctx = single_path_ctx();
        let mut node = Node::Query(Box::new(base_query()));

        paginate(&mut node, &input, &ctx);

        let Node::Query(q) = &node;
        assert_eq!(q.offset, Some(100), "offset should be preserved");
    }

    #[test]
    fn no_sip_cte_still_injects_main_where() {
        let input = search_input_with_cursor(99);
        let ctx = single_path_ctx();
        let mut node = Node::Query(Box::new(base_query()));

        paginate(&mut node, &input, &ctx);

        let Node::Query(q) = &node;
        assert!(q.ctes.is_empty(), "no CTEs should be created");
        assert!(q.where_clause.is_some(), "main WHERE should have keyset");
        assert_eq!(q.offset, None);
    }
}
