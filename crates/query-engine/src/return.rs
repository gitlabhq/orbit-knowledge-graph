//! Enforce return columns for query results.
//!
//! Ensures all query results include ID and type columns for entities, enabling
//! the gkg-server to extract entity IDs and types for redaction validation.

use crate::ast::{Expr, Node, Query, SelectExpr};
use crate::error::Result;
use crate::input::Input;
use crate::result_context::{id_column, type_column, ResultContext};

pub fn enforce_return(node: &mut Node, input: &Input) -> Result<ResultContext> {
    let mut ctx = ResultContext::new();

    match node {
        Node::Query(q) => enforce_return_columns(q, input, &mut ctx)?,
        Node::RecursiveCte(cte) => {
            enforce_return_columns(&mut cte.base, input, &mut ctx)?;
            enforce_return_columns(&mut cte.recursive, input, &mut ctx)?;
            enforce_return_columns(&mut cte.final_query, input, &mut ctx)?;
        }
    }

    Ok(ctx)
}

fn enforce_return_columns(q: &mut Query, input: &Input, ctx: &mut ResultContext) -> Result<()> {
    for node in &input.nodes {
        let Some(entity) = &node.entity else { continue };

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
                    filters: std::collections::HashMap::new(),
                    node_ids: vec![],
                    id_range: None,
                    id_property: "id".to_string(),
                },
                InputNode {
                    id: "p".to_string(),
                    entity: Some("Project".to_string()),
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
}
