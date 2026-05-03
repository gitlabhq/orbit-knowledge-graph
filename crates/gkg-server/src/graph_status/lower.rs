use query_engine::compiler::{Expr, JoinType, Node, Query, SelectExpr, TableRef};

use super::input::{GraphStatusInput, NodeTable, ProjectTables};

pub fn lower_entity_counts(input: &GraphStatusInput) -> Node {
    let mut queries = input
        .nodes
        .iter()
        .map(|node| build_node_query(node, &input.traversal_path));

    let mut first = queries.next().expect("lower() requires at least one node");
    first.union_all = queries.collect();

    Node::Query(Box::new(first))
}

pub fn lower_projects(tables: &ProjectTables, traversal_path: &str) -> Node {
    let total_known = build_total_known_projects_query(&tables.project, traversal_path);
    let mut indexed =
        build_indexed_projects_query(&tables.project, &tables.code_checkpoint, traversal_path);

    indexed.union_all = vec![total_known];

    Node::Query(Box::new(indexed))
}

fn build_total_known_projects_query(project_table: &str, traversal_path: &str) -> Query {
    let alias = "p";

    let select = vec![
        SelectExpr::new(Expr::string("total_known"), "metric"),
        SelectExpr::new(Expr::func("uniq", vec![Expr::col(alias, "id")]), "cnt"),
    ];

    let from = TableRef::scan_final(project_table, alias);

    let where_clause = live_project_scope_filter(alias, traversal_path);

    Query {
        select,
        from,
        where_clause: Some(where_clause),
        ..Default::default()
    }
}

fn build_indexed_projects_query(
    project_table: &str,
    code_checkpoint_table: &str,
    traversal_path: &str,
) -> Query {
    let checkpoint_alias = "c";
    let project_alias = "p";

    let select = vec![
        SelectExpr::new(Expr::string("indexed"), "metric"),
        SelectExpr::new(
            Expr::func("uniq", vec![Expr::col(checkpoint_alias, "project_id")]),
            "cnt",
        ),
    ];

    let from = TableRef::join(
        JoinType::Inner,
        TableRef::scan_final(code_checkpoint_table, checkpoint_alias),
        TableRef::scan_final(project_table, project_alias),
        Expr::eq(
            Expr::col(checkpoint_alias, "project_id"),
            Expr::col(project_alias, "id"),
        ),
    );

    let where_clause = Expr::and(
        live_project_scope_filter(project_alias, traversal_path),
        Expr::and(
            Expr::eq(Expr::col(checkpoint_alias, "_deleted"), Expr::int(0)),
            Expr::func(
                "startsWith",
                vec![
                    Expr::col(checkpoint_alias, "traversal_path"),
                    Expr::string(traversal_path),
                ],
            ),
        ),
    );

    Query {
        select,
        from,
        where_clause: Some(where_clause),
        ..Default::default()
    }
}

fn live_project_scope_filter(alias: &str, traversal_path: &str) -> Expr {
    Expr::and(
        Expr::eq(Expr::col(alias, "_deleted"), Expr::int(0)),
        Expr::func(
            "startsWith",
            vec![
                Expr::col(alias, "traversal_path"),
                Expr::string(traversal_path),
            ],
        ),
    )
}

fn build_node_query(node: &NodeTable, traversal_path: &str) -> Query {
    let alias = "d";

    let select = vec![
        SelectExpr::new(Expr::string(&node.name), "entity"),
        SelectExpr::new(Expr::func("count", vec![]), "cnt"),
    ];

    let from = TableRef::subquery(build_deduplicated_node_query(node, traversal_path), alias);

    Query {
        select,
        from,
        where_clause: Some(Expr::eq(Expr::col(alias, "_deleted"), Expr::int(0))),
        ..Default::default()
    }
}

fn build_deduplicated_node_query(node: &NodeTable, traversal_path: &str) -> Query {
    let alias = "t";

    let select = vec![
        SelectExpr::new(Expr::col(alias, "id"), "id"),
        SelectExpr::new(
            Expr::func(
                "argMax",
                vec![Expr::col(alias, "_deleted"), Expr::col(alias, "_version")],
            ),
            "_deleted",
        ),
    ];

    let traversal_filter = Expr::func(
        "startsWith",
        vec![
            Expr::col(alias, "traversal_path"),
            Expr::string(traversal_path),
        ],
    );

    Query {
        select,
        from: TableRef::scan(&node.table, alias),
        where_clause: Some(traversal_filter),
        group_by: vec![Expr::col(alias, "id")],
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

    fn test_input() -> GraphStatusInput {
        GraphStatusInput {
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
    fn entity_counts_deduplicates_by_id() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        let argmax_count = result.sql.matches("argMax(").count();
        assert_eq!(
            argmax_count,
            input.nodes.len(),
            "Each subquery should use argMax to select the latest row per id. SQL: {}",
            result.sql
        );
        assert!(
            result.sql.contains("GROUP BY t.id"),
            "Each subquery should group by id. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn entity_counts_avoids_final_table_scans() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        let final_count = result.sql.matches(" FINAL").count();
        assert_eq!(
            final_count, 0,
            "Entity counts should deduplicate after traversal filtering instead of using FINAL. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn entity_counts_has_deleted_filter() {
        let input = test_input();
        let ast = lower_entity_counts(&input);
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(result.sql.contains("_deleted"), "SQL: {}", result.sql);
        assert!(result.sql.contains("d._deleted"), "SQL: {}", result.sql);
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
    fn projects_query_joins_checkpoints_to_live_projects() {
        let ast = lower_projects(&test_tables(), "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(result.sql.contains("INNER JOIN"), "SQL: {}", result.sql);
        assert!(
            result.sql.contains("c.project_id = p.id"),
            "SQL: {}",
            result.sql
        );
        assert!(
            result.sql.contains("startsWith(p.traversal_path"),
            "SQL: {}",
            result.sql
        );
        assert!(
            result.sql.contains("startsWith(c.traversal_path"),
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
            3,
            "Project coverage should filter deleted checkpoint and project rows. SQL: {}",
            result.sql
        );
    }
}
