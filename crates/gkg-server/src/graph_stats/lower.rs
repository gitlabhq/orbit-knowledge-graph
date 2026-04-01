use query_engine::compiler::{Expr, Node, Query, SelectExpr, TableRef};

use super::input::{GraphStatsInput, NodeStatsTarget};

pub fn lower(input: &GraphStatsInput) -> Node {
    let mut queries = input
        .nodes
        .iter()
        .map(|node| build_node_query(node, &input.traversal_path));

    let mut first = queries.next().expect("lower() requires at least one node");
    first.union_all = queries.collect();

    Node::Query(Box::new(first))
}

fn build_node_query(node: &NodeStatsTarget, traversal_path: &str) -> Query {
    let alias = "t";

    let select = vec![
        SelectExpr::new(Expr::string(&node.name), "entity"),
        SelectExpr::new(Expr::func("count", vec![]), "cnt"),
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
    use query_engine::compiler::{ResultContext, codegen};

    use super::*;

    fn test_input() -> GraphStatsInput {
        GraphStatsInput {
            traversal_path: "1/2/".to_string(),
            nodes: vec![
                NodeStatsTarget {
                    name: "Project".to_string(),
                    table: "gl_project".to_string(),
                },
                NodeStatsTarget {
                    name: "Group".to_string(),
                    table: "gl_group".to_string(),
                },
                NodeStatsTarget {
                    name: "MergeRequest".to_string(),
                    table: "gl_merge_request".to_string(),
                },
            ],
        }
    }

    #[test]
    fn lower_produces_union_all() {
        let input = test_input();
        let ast = lower(&input);
        let result = codegen(
            &ast,
            ResultContext::new(),
            gkg_config::global::DEFAULT_QUERY_CONFIG,
        )
        .unwrap();

        assert!(result.sql.contains("UNION ALL"), "SQL: {}", result.sql);
        assert!(result.sql.contains("gl_project"), "SQL: {}", result.sql);
        assert!(result.sql.contains("gl_group"), "SQL: {}", result.sql);
        assert!(
            result.sql.contains("gl_merge_request"),
            "SQL: {}",
            result.sql
        );
    }

    #[test]
    fn every_subquery_has_starts_with_filter() {
        let input = test_input();
        let ast = lower(&input);
        let result = codegen(
            &ast,
            ResultContext::new(),
            gkg_config::global::DEFAULT_QUERY_CONFIG,
        )
        .unwrap();

        let starts_with_count = result.sql.matches("startsWith").count();
        assert_eq!(
            starts_with_count,
            input.nodes.len(),
            "Each subquery should have startsWith filter. SQL: {}",
            result.sql
        );
    }

    #[test]
    fn every_subquery_has_deleted_filter() {
        let input = test_input();
        let ast = lower(&input);
        let result = codegen(
            &ast,
            ResultContext::new(),
            gkg_config::global::DEFAULT_QUERY_CONFIG,
        )
        .unwrap();

        let deleted_count = result.sql.matches("_deleted").count();
        assert_eq!(
            deleted_count,
            input.nodes.len(),
            "Each subquery should have _deleted filter. SQL: {}",
            result.sql
        );
    }
}
