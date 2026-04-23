use query_engine::compiler::{Expr, Node, Query, SelectExpr, TableRef};

use super::input::{GraphStatsInput, NodeTable, ProjectTables};

pub fn lower_entity_counts(input: &GraphStatsInput) -> Node {
    let mut queries = input
        .nodes
        .iter()
        .map(|node| build_node_query(node, &input.traversal_path));

    let mut first = queries.next().expect("lower() requires at least one node");
    first.union_all = queries.collect();

    Node::Query(Box::new(first))
}

pub fn lower_projects(tables: &ProjectTables, traversal_path: &str) -> Node {
    let alias = "t";

    let total_known = build_projects_query(
        "total_known",
        &tables.project,
        "id",
        alias,
        traversal_path,
        true,
    );

    let mut indexed = build_projects_query(
        "indexed",
        &tables.code_checkpoint,
        "project_id",
        alias,
        traversal_path,
        true,
    );

    indexed.union_all = vec![total_known];

    Node::Query(Box::new(indexed))
}

fn build_projects_query(
    label: &str,
    table: &str,
    count_column: &str,
    alias: &str,
    traversal_path: &str,
    filter_deleted: bool,
) -> Query {
    let select = vec![
        SelectExpr::new(Expr::string(label), "metric"),
        SelectExpr::new(
            Expr::func("uniq", vec![Expr::col(alias, count_column)]),
            "cnt",
        ),
    ];

    let from = TableRef::scan(table, alias);

    let traversal_filter = Expr::func(
        "startsWith",
        vec![
            Expr::col(alias, "traversal_path"),
            Expr::string(traversal_path),
        ],
    );

    let where_clause = if filter_deleted {
        Expr::and(
            Expr::eq(Expr::col(alias, "_deleted"), Expr::int(0)),
            traversal_filter,
        )
    } else {
        traversal_filter
    };

    Query {
        select,
        from,
        where_clause: Some(where_clause),
        ..Default::default()
    }
}

fn build_node_query(node: &NodeTable, traversal_path: &str) -> Query {
    let alias = "t";

    let select = vec![
        SelectExpr::new(Expr::string(&node.name), "entity"),
        SelectExpr::new(Expr::func("uniq", vec![Expr::col(alias, "id")]), "cnt"),
    ];

    let from = TableRef::scan(&node.table, alias);

    let deleted_filter = Expr::eq(Expr::col(alias, "_deleted"), Expr::int(0));
    let traversal_filter = Expr::func(
        "startsWith",
        vec![
            Expr::col(alias, "traversal_path"),
            Expr::string(traversal_path),
        ],
    );

    Query {
        select,
        from,
        where_clause: Some(Expr::and(deleted_filter, traversal_filter)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use gkg_server_config::QueryConfig;
    use query_engine::compiler::{ResultContext, codegen};

    use super::*;

    fn test_tables() -> ProjectTables {
        ProjectTables {
            project: "v1_gl_project".to_string(),
            code_checkpoint: "v1_code_indexing_checkpoint".to_string(),
        }
    }

    fn test_input() -> GraphStatsInput {
        GraphStatsInput {
            traversal_path: "1/2/".to_string(),
            nodes: vec![
                NodeTable {
                    name: "Project".to_string(),
                    table: "v1_gl_project".to_string(),
                },
                NodeTable {
                    name: "Group".to_string(),
                    table: "v1_gl_group".to_string(),
                },
                NodeTable {
                    name: "MergeRequest".to_string(),
                    table: "v1_gl_merge_request".to_string(),
                },
            ],
            project_tables: test_tables(),
        }
    }

    #[test]
    fn entity_counts_produces_union_all() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(result.sql.contains("UNION ALL"), "SQL: {}", result.sql);
        assert!(result.sql.contains("v1_gl_project"), "SQL: {}", result.sql);
        assert!(result.sql.contains("v1_gl_group"), "SQL: {}", result.sql);
        assert!(
            result.sql.contains("v1_gl_merge_request"),
            "SQL: {}",
            result.sql
        );
    }

    #[test]
    fn entity_counts_has_starts_with_filter() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        let starts_with_count = result.sql.matches("startsWith").count();
        assert_eq!(
            starts_with_count,
            input.nodes.len(),
            "Each subquery should have startsWith filter. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn entity_counts_uses_uniq_id() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        let uniq_count = result.sql.matches("uniq(").count();
        assert_eq!(
            uniq_count,
            input.nodes.len(),
            "Each subquery should use uniq(id). SQL: {}",
            result.sql
        );
        assert!(
            !result.sql.contains("count()"),
            "Should not use count(). SQL: {}",
            result.sql
        );
    }

    #[test]
    fn entity_counts_has_deleted_filter() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        let deleted_count = result.sql.matches("_deleted").count();
        assert_eq!(
            deleted_count,
            input.nodes.len(),
            "Each subquery should have _deleted filter. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn projects_query_includes_both_tables() {
        let tables = test_tables();
        let ast = lower_projects(&tables, "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(result.sql.contains(&tables.project), "SQL: {}", result.sql);
        assert!(
            result.sql.contains(&tables.code_checkpoint),
            "SQL: {}",
            result.sql
        );
    }

    #[test]
    fn projects_query_uses_uniq() {
        let ast = lower_projects(&test_tables(), "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert_eq!(
            result.sql.matches("uniq(").count(),
            2,
            "Should have two uniq() calls. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn projects_query_filters_deleted_on_both_tables() {
        let ast = lower_projects(&test_tables(), "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert_eq!(
            result.sql.matches("_deleted").count(),
            2,
            "Both subqueries should have _deleted filter. SQL: {}",
            result.sql
        );
    }
}
