use ontology::Ontology;
use query_engine::compiler::{Expr, Node, Query, SelectExpr, TableRef};

pub struct NodeCountTarget {
    pub name: String,
    pub table: String,
}

pub fn node_count_targets(ontology: &Ontology) -> Vec<NodeCountTarget> {
    ontology
        .nodes()
        .filter(|node| node.has_traversal_path)
        .map(|node| NodeCountTarget {
            name: node.name.clone(),
            table: node.destination_table.clone(),
        })
        .collect()
}

pub fn build_node_count_query(targets: &[NodeCountTarget], traversal_path: &str) -> Node {
    let mut queries = targets
        .iter()
        .map(|target| build_single_node_query(target, traversal_path));

    let mut first = queries.next().expect("at least one node target required");
    first.union_all = queries.collect();

    Node::Query(Box::new(first))
}

fn build_single_node_query(target: &NodeCountTarget, traversal_path: &str) -> Query {
    let alias = "t";

    let select = vec![
        SelectExpr::new(Expr::string(&target.name), "entity"),
        SelectExpr::new(Expr::func("uniq", vec![Expr::col(alias, "id")]), "cnt"),
        SelectExpr::new(Expr::col(alias, "traversal_path"), "traversal_path"),
    ];

    let from = TableRef::scan(&target.table, alias);

    let tp_filter = Expr::func(
        "startsWith",
        vec![
            Expr::col(alias, "traversal_path"),
            Expr::string(traversal_path),
        ],
    );

    Query {
        select,
        from,
        where_clause: Some(tp_filter),
        group_by: vec![Expr::col(alias, "traversal_path")],
        ..Default::default()
    }
}

pub fn build_edge_count_query(traversal_path: &str) -> Node {
    let alias = "e";

    let select = vec![
        SelectExpr::new(Expr::col(alias, "traversal_path"), "traversal_path"),
        SelectExpr::new(Expr::col(alias, "relationship_kind"), "relationship_kind"),
        SelectExpr::new(
            Expr::func(
                "uniq",
                vec![Expr::col(alias, "source_id"), Expr::col(alias, "target_id")],
            ),
            "cnt",
        ),
    ];

    let from = TableRef::scan("gl_edge", alias);

    let tp_filter = Expr::func(
        "startsWith",
        vec![
            Expr::col(alias, "traversal_path"),
            Expr::string(traversal_path),
        ],
    );

    let query = Query {
        select,
        from,
        where_clause: Some(tp_filter),
        group_by: vec![
            Expr::col(alias, "traversal_path"),
            Expr::col(alias, "relationship_kind"),
        ],
        ..Default::default()
    };

    Node::Query(Box::new(query))
}

#[cfg(test)]
mod tests {
    use gkg_server_config::QueryConfig;
    use query_engine::compiler::{ResultContext, codegen};

    use super::*;

    fn test_targets() -> Vec<NodeCountTarget> {
        vec![
            NodeCountTarget {
                name: "Project".to_string(),
                table: "gl_project".to_string(),
            },
            NodeCountTarget {
                name: "MergeRequest".to_string(),
                table: "gl_merge_request".to_string(),
            },
        ]
    }

    #[test]
    fn node_count_query_uses_uniq() {
        let targets = test_targets();
        let ast = build_node_count_query(&targets, "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(
            result.sql.contains("uniq"),
            "should use uniq: {}",
            result.sql
        );
        assert!(
            result.sql.contains("UNION ALL"),
            "should UNION ALL: {}",
            result.sql
        );
        assert!(result.sql.contains("gl_project"), "SQL: {}", result.sql);
        assert!(
            result.sql.contains("gl_merge_request"),
            "SQL: {}",
            result.sql
        );
    }

    #[test]
    fn node_count_query_groups_by_traversal_path() {
        let targets = test_targets();
        let ast = build_node_count_query(&targets, "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(
            result.sql.contains("GROUP BY"),
            "should group by traversal_path: {}",
            result.sql
        );
    }

    #[test]
    fn edge_count_query_uses_uniq_pair() {
        let ast = build_edge_count_query("1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(
            result.sql.contains("uniq"),
            "should use uniq: {}",
            result.sql
        );
        assert!(
            result.sql.contains("relationship_kind"),
            "should group by relationship_kind: {}",
            result.sql
        );
    }
}
