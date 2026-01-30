//! Enforce return columns for query results.
//!
//! Ensures all query results include ID and type columns for entities, enabling
//! the gkg-server to extract entity IDs and types for redaction validation.
//!
//! For aggregation queries, only nodes that appear in GROUP BY clauses can have
//! their ID columns selected (aggregated nodes don't have individual IDs).
//!
//! For path finding queries, the start node's ID is added to the base query and
//! the end node's ID is added to the final query.

use crate::ast::{Expr, Node, Query, SelectExpr};
use crate::error::Result;
use crate::input::{Input, QueryType};
use crate::result_context::{id_column, type_column, ResultContext};
use std::collections::HashSet;

pub fn enforce_return(node: &mut Node, input: &Input) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);

    // For aggregation queries, only nodes in GROUP BY can be selected.
    let selectable_nodes = selectable_node_ids(input);

    match node {
        Node::Query(q) => enforce_return_columns(q, input, &selectable_nodes, &mut ctx)?,
        Node::RecursiveCte(_) => {
            // Path finding queries use _gkg_path column (Array of (id, type) tuples)
            // which contains all nodes. No additional columns needed here.
        }
    }

    Ok(ctx)
}

/// Determine which node IDs can have their columns selected.
///
/// For traversal/pattern queries: all nodes are selectable.
/// For aggregation queries: only nodes appearing in group_by are selectable.
/// For path finding: handled separately in enforce_return.
fn selectable_node_ids(input: &Input) -> HashSet<String> {
    match input.query_type {
        QueryType::Aggregation => input
            .aggregations
            .iter()
            .filter_map(|agg| agg.group_by.clone())
            .collect(),
        QueryType::Traversal | QueryType::Search => {
            input.nodes.iter().map(|n| n.id.clone()).collect()
        }
        QueryType::PathFinding => HashSet::new(),
    }
}

fn enforce_return_columns(
    q: &mut Query,
    input: &Input,
    selectable_nodes: &HashSet<String>,
    ctx: &mut ResultContext,
) -> Result<()> {
    for node in &input.nodes {
        let Some(entity) = &node.entity else { continue };

        // Only add columns for nodes that are valid to select in this query type.
        if !selectable_nodes.contains(&node.id) {
            continue;
        }

        ctx.add_node(&node.id, entity);

        let id_col = id_column(&node.id);
        let type_col = type_column(&node.id);

        let has_id = q.select.iter().any(|s| s.alias.as_ref() == Some(&id_col));
        let has_type = q.select.iter().any(|s| s.alias.as_ref() == Some(&type_col));

        if !has_id {
            q.select.push(SelectExpr {
                expr: Expr::col(&node.id, "id"),
                alias: Some(id_col.clone()),
            });
        }

        if !has_type {
            let insert_pos = q
                .select
                .iter()
                .position(|s| s.alias.as_ref() == Some(&id_col))
                .map(|i| i + 1)
                .unwrap_or(q.select.len());

            q.select.insert(
                insert_pos,
                SelectExpr {
                    expr: Expr::lit(entity.as_str()),
                    alias: Some(type_col),
                },
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::TableRef;
    use crate::input::{InputNode, QueryType};

    fn test_input() -> Input {
        Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".to_string(),
                    entity: Some("User".to_string()),
                    columns: None,
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![],
                    id_range: None,
                    id_property: "id".to_string(),
                },
                InputNode {
                    id: "p".to_string(),
                    entity: Some("Project".to_string()),
                    columns: None,
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![],
                    id_range: None,
                    id_property: "id".to_string(),
                },
            ],
            relationships: vec![],
            aggregations: vec![],
            path: None,
            limit: 30,
            order_by: None,
            aggregation_sort: None,
        }
    }

    #[test]
    fn adds_type_columns_after_id_columns() {
        let query = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("u", "id"),
                    alias: Some("_gkg_u_id".into()),
                },
                SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: Some("_gkg_p_id".into()),
                },
            ],
            from: TableRef::scan("kg_user", "u"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(30),
        };

        let input = test_input();
        let mut node = Node::Query(Box::new(query));

        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 4);
        assert_eq!(q.select[0].alias, Some("_gkg_u_id".into()));
        assert_eq!(q.select[1].alias, Some("_gkg_u_type".into()));
        assert_eq!(q.select[2].alias, Some("_gkg_p_id".into()));
        assert_eq!(q.select[3].alias, Some("_gkg_p_type".into()));

        if let Expr::Literal(v) = &q.select[1].expr {
            assert_eq!(v.as_str(), Some("User"));
        } else {
            panic!("expected literal");
        }
        if let Expr::Literal(v) = &q.select[3].expr {
            assert_eq!(v.as_str(), Some("Project"));
        } else {
            panic!("expected literal");
        }
    }

    #[test]
    fn skips_existing_type_columns() {
        let query = Query {
            select: vec![
                SelectExpr {
                    expr: Expr::col("u", "id"),
                    alias: Some("_gkg_u_id".into()),
                },
                SelectExpr {
                    expr: Expr::lit("User"),
                    alias: Some("_gkg_u_type".into()),
                },
                SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: Some("_gkg_p_id".into()),
                },
            ],
            from: TableRef::scan("kg_user", "u"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(30),
        };

        let input = test_input();
        let mut node = Node::Query(Box::new(query));

        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 4);
        assert_eq!(q.select[0].alias, Some("_gkg_u_id".into()));
        assert_eq!(q.select[1].alias, Some("_gkg_u_type".into()));
        assert_eq!(q.select[2].alias, Some("_gkg_p_id".into()));
        assert_eq!(q.select[3].alias, Some("_gkg_p_type".into()));
    }

    #[test]
    fn adds_id_and_type_columns_when_missing() {
        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "username"),
                alias: Some("name".into()),
            }],
            from: TableRef::scan("kg_user", "u"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(30),
        };

        let input = test_input();
        let mut node = Node::Query(Box::new(query));

        enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 5);
        assert_eq!(q.select[0].alias, Some("name".into()));
        assert_eq!(q.select[1].alias, Some("_gkg_u_id".into()));
        assert_eq!(q.select[2].alias, Some("_gkg_u_type".into()));
        assert_eq!(q.select[3].alias, Some("_gkg_p_id".into()));
        assert_eq!(q.select[4].alias, Some("_gkg_p_type".into()));

        if let Expr::Column { table, column } = &q.select[1].expr {
            assert_eq!(table, "u");
            assert_eq!(column, "id");
        } else {
            panic!("expected column expression for _gkg_u_id");
        }
    }

    #[test]
    fn skips_nodes_without_entity() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "n".to_string(),
                entity: None,
                columns: None,
                filters: std::collections::HashMap::new(),
                node_ids: vec![],
                id_range: None,
                id_property: "id".to_string(),
            }],
            relationships: vec![],
            aggregations: vec![],
            path: None,
            limit: 30,
            order_by: None,
            aggregation_sort: None,
        };

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: Some("n_id".into()),
            }],
            from: TableRef::scan("kg_node", "n"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(30),
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        assert_eq!(q.select.len(), 1);
        assert!(ctx.is_empty());
    }

    #[test]
    fn builds_result_context() {
        let input = test_input();
        let query = Query {
            select: vec![],
            from: TableRef::scan("kg_user", "u"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(30),
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

        assert_eq!(ctx.len(), 2);

        let user = ctx.get("u").unwrap();
        assert_eq!(user.entity_type, "User");
        assert_eq!(user.id_column, "_gkg_u_id");
        assert_eq!(user.type_column, "_gkg_u_type");

        let project = ctx.get("p").unwrap();
        assert_eq!(project.entity_type, "Project");
    }

    #[test]
    fn aggregation_only_adds_columns_for_group_by_nodes() {
        use crate::input::{AggFunction, InputAggregation};

        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "u".to_string(),
                    entity: Some("User".to_string()),
                    columns: None,
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![],
                    id_range: None,
                    id_property: "id".to_string(),
                },
                InputNode {
                    id: "n".to_string(),
                    entity: Some("Note".to_string()),
                    columns: None,
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![],
                    id_range: None,
                    id_property: "id".to_string(),
                },
            ],
            relationships: vec![],
            aggregations: vec![InputAggregation {
                function: AggFunction::Count,
                target: Some("n".to_string()),
                group_by: Some("u".to_string()),
                property: None,
                alias: Some("note_count".to_string()),
            }],
            path: None,
            limit: 10,
            order_by: None,
            aggregation_sort: None,
        };

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: Some("u_id".into()),
            }],
            from: TableRef::scan("kg_user", "u"),
            where_clause: None,
            group_by: vec![Expr::col("u", "id")],
            order_by: vec![],
            limit: Some(10),
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        // Should only have columns for 'u' (group_by node), not 'n' (target node)
        assert_eq!(q.select.len(), 3); // u_id, _gkg_u_id, _gkg_u_type
        assert!(q
            .select
            .iter()
            .any(|s| s.alias.as_ref() == Some(&"_gkg_u_id".to_string())));
        assert!(q
            .select
            .iter()
            .any(|s| s.alias.as_ref() == Some(&"_gkg_u_type".to_string())));
        assert!(!q
            .select
            .iter()
            .any(|s| s.alias.as_ref() == Some(&"_gkg_n_id".to_string())));
        assert!(!q
            .select
            .iter()
            .any(|s| s.alias.as_ref() == Some(&"_gkg_n_type".to_string())));

        // Context should only have the group_by node
        assert_eq!(ctx.len(), 1);
        assert!(ctx.get("u").is_some());
        assert!(ctx.get("n").is_none());
    }

    #[test]
    fn path_finding_uses_gkg_path_column() {
        use crate::ast::RecursiveCte;
        use crate::input::InputPath;

        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    id: "start".to_string(),
                    entity: Some("Project".to_string()),
                    columns: None,
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![100],
                    id_range: None,
                    id_property: "id".to_string(),
                },
                InputNode {
                    id: "end".to_string(),
                    entity: Some("Project".to_string()),
                    columns: None,
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![200],
                    id_range: None,
                    id_property: "id".to_string(),
                },
            ],
            relationships: vec![],
            aggregations: vec![],
            path: Some(InputPath {
                path_type: crate::input::PathType::Shortest,
                from: "start".to_string(),
                to: "end".to_string(),
                max_depth: 3,
                rel_types: vec![],
            }),
            limit: 30,
            order_by: None,
            aggregation_sort: None,
        };

        // Path finding generates a RecursiveCte
        let mut cte = Node::RecursiveCte(Box::new(RecursiveCte {
            name: "path_cte".into(),
            base: Query {
                select: vec![SelectExpr {
                    expr: Expr::col("start", "id"),
                    alias: Some("node_id".into()),
                }],
                from: TableRef::scan("gl_projects", "start"),
                where_clause: None,
                group_by: vec![],
                order_by: vec![],
                limit: None,
            },
            recursive: Query {
                select: vec![],
                from: TableRef::scan("path_cte", "p"),
                where_clause: None,
                group_by: vec![],
                order_by: vec![],
                limit: None,
            },
            max_depth: 3,
            final_query: Query {
                select: vec![SelectExpr {
                    expr: Expr::col("path_cte", "path"),
                    alias: Some("_gkg_path".into()),
                }],
                from: TableRef::scan("gl_projects", "end"),
                where_clause: None,
                group_by: vec![],
                order_by: vec![],
                limit: Some(30),
            },
        }));

        let ctx = enforce_return(&mut cte, &input).unwrap();

        // Path finding queries use _gkg_path column for redaction data.
        // No additional _gkg_* columns are added by enforce_return.
        // The ResultContext is empty but has query_type set for path extraction.
        assert!(ctx.is_empty());
        assert_eq!(ctx.query_type, Some(QueryType::PathFinding));
    }
}
