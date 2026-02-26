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
use crate::constants::{redaction_id_column, redaction_type_column};
use crate::error::Result;
use crate::input::{EntityAuthConfig, Input, QueryType};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionNode {
    pub alias: String,
    pub entity_type: String,
    pub id_column: String,
    pub type_column: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
    /// Auth config for every entity type that requires redaction.
    /// Covers all entities in the ontology, not just those in the current query,
    /// so dynamic nodes (path/neighbors) can be resolved without re-consulting the ontology.
    entity_auth: HashMap<String, EntityAuthConfig>,
}

impl ResultContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query_type = Some(query_type);
        self
    }

    pub fn add_node(&mut self, alias: &str, entity_type: &str) {
        self.nodes.insert(
            alias.to_string(),
            RedactionNode {
                alias: alias.to_string(),
                entity_type: entity_type.to_string(),
                id_column: redaction_id_column(alias),
                type_column: redaction_type_column(alias),
            },
        );
    }

    pub fn add_entity_auth(&mut self, entity_type: impl Into<String>, config: EntityAuthConfig) {
        self.entity_auth.insert(entity_type.into(), config);
    }

    pub fn get_entity_auth(&self, entity_type: &str) -> Option<&EntityAuthConfig> {
        self.entity_auth.get(entity_type)
    }

    pub fn entity_auth(&self) -> impl Iterator<Item = (&str, &EntityAuthConfig)> {
        self.entity_auth.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn nodes(&self) -> impl Iterator<Item = &RedactionNode> {
        self.nodes.values()
    }

    pub fn get(&self, alias: &str) -> Option<&RedactionNode> {
        self.nodes.get(alias)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

pub fn enforce_return(node: &mut Node, input: &Input) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth = input.entity_auth.clone();

    let selectable_nodes = match input.query_type {
        QueryType::Aggregation => input
            .aggregations
            .iter()
            .filter_map(|agg| agg.group_by.clone())
            .collect(),
        QueryType::Traversal | QueryType::Search | QueryType::Neighbors => {
            input.nodes.iter().map(|n| n.id.clone()).collect()
        }
        QueryType::PathFinding => HashSet::new(),
    };

    match node {
        Node::Query(q) => enforce_return_columns(q, input, &selectable_nodes, &mut ctx)?,
    }

    Ok(ctx)
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

        if let Some(redaction_node) = ctx.get(&node.id) {
            let id_col = redaction_node.id_column.clone();
            let type_col = redaction_node.type_column.clone();

            let has_id = q.select.iter().any(|s| s.alias.as_ref() == Some(&id_col));
            let has_type = q.select.iter().any(|s| s.alias.as_ref() == Some(&type_col));

            if !has_id {
                q.select.push(SelectExpr {
                    expr: Expr::col(&node.id, &node.redaction_id_column),
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
    }
    Ok(())
}

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
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
                    table: Some("gl_user".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "p".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    ..Default::default()
                },
            ],
            relationships: vec![],
            aggregations: vec![],
            path: None,
            neighbors: None,
            limit: 30,
            range: None,
            order_by: None,
            aggregation_sort: None,
            entity_auth: Default::default(),
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
            limit: Some(30),
            ..Default::default()
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
            limit: Some(30),
            ..Default::default()
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
            limit: Some(30),
            ..Default::default()
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
                ..Default::default()
            }],
            relationships: vec![],
            aggregations: vec![],
            path: None,
            neighbors: None,
            limit: 30,
            range: None,
            order_by: None,
            aggregation_sort: None,
            entity_auth: Default::default(),
        };

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: Some("n_id".into()),
            }],
            from: TableRef::scan("kg_node", "n"),
            limit: Some(30),
            ..Default::default()
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
            limit: Some(30),
            ..Default::default()
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
                    table: Some("gl_user".to_string()),
                    ..Default::default()
                },
                InputNode {
                    id: "n".to_string(),
                    entity: Some("Note".to_string()),
                    table: Some("gl_note".to_string()),
                    ..Default::default()
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
            neighbors: None,
            limit: 10,
            range: None,
            order_by: None,
            aggregation_sort: None,
            entity_auth: Default::default(),
        };

        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: Some("u_id".into()),
            }],
            from: TableRef::scan("kg_user", "u"),
            group_by: vec![Expr::col("u", "id")],
            limit: Some(10),
            ..Default::default()
        };

        let mut node = Node::Query(Box::new(query));
        let ctx = enforce_return(&mut node, &input).unwrap();

        let Node::Query(q) = node else {
            panic!("expected Query")
        };

        // Should only have columns for 'u' (group_by node), not 'n' (target node)
        assert_eq!(q.select.len(), 3); // u_id, _gkg_u_id, _gkg_u_type
        assert!(
            q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_u_id".to_string()))
        );
        assert!(
            q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_u_type".to_string()))
        );
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_n_id".to_string()))
        );
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_ref() == Some(&"_gkg_n_type".to_string()))
        );

        // Context should only have the group_by node
        assert_eq!(ctx.len(), 1);
        assert!(ctx.get("u").is_some());
        assert!(ctx.get("n").is_none());
    }

    #[test]
    fn path_finding_uses_gkg_path_column() {
        use crate::ast::Cte;
        use crate::input::InputPath;

        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    id: "start".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    node_ids: vec![100],
                    ..Default::default()
                },
                InputNode {
                    id: "end".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    node_ids: vec![200],
                    ..Default::default()
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
            neighbors: None,
            limit: 30,
            range: None,
            order_by: None,
            aggregation_sort: None,
            entity_auth: Default::default(),
        };

        // Path finding generates a Query with unrolled CTEs
        let mut query = Node::Query(Box::new(Query {
            ctes: vec![
                Cte::new(
                    "d0",
                    Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("start", "id"),
                            alias: Some("node_id".into()),
                        }],
                        from: TableRef::scan("gl_project", "start"),
                        ..Default::default()
                    },
                ),
                Cte::new(
                    "d1",
                    Query {
                        from: TableRef::scan("d0", "p"),
                        ..Default::default()
                    },
                ),
            ],
            select: vec![SelectExpr {
                expr: Expr::col("all_paths", "path"),
                alias: Some("_gkg_path".into()),
            }],
            from: TableRef::scan("gl_project", "end"),
            limit: Some(30),
            ..Default::default()
        }));

        let ctx = enforce_return(&mut query, &input).unwrap();

        // Path finding queries use _gkg_path column for redaction data.
        // No additional _gkg_* columns are added by enforce_return.
        // The ResultContext is empty but has query_type set for path extraction.
        assert!(ctx.is_empty());
        assert_eq!(ctx.query_type, Some(QueryType::PathFinding));
    }
}
