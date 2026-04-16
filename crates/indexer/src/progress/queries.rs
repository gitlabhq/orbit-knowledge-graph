use ontology::Ontology;
use query_engine::compiler::{Expr, JoinType, Node, Op, Query, SelectExpr, TableRef};

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

pub struct CrossNamespaceEdgeTarget {
    pub relationship_kinds: Vec<&'static str>,
    pub target_table: &'static str,
    pub target_alias: &'static str,
}

pub fn cross_namespace_edge_targets() -> Vec<CrossNamespaceEdgeTarget> {
    vec![
        CrossNamespaceEdgeTarget {
            relationship_kinds: vec!["CLOSES", "RELATED_TO"],
            target_table: "gl_work_item",
            target_alias: "w",
        },
        CrossNamespaceEdgeTarget {
            relationship_kinds: vec!["FIXES"],
            target_table: "gl_vulnerability",
            target_alias: "v",
        },
    ]
}

pub fn build_cross_namespace_edge_query(
    target: &CrossNamespaceEdgeTarget,
    traversal_path: &str,
) -> Node {
    let edge_alias = "e";
    let t = target.target_alias;

    let select = vec![
        SelectExpr::new(Expr::col(t, "traversal_path"), "traversal_path"),
        SelectExpr::new(
            Expr::col(edge_alias, "relationship_kind"),
            "relationship_kind",
        ),
        SelectExpr::new(Expr::func("count", vec![]), "cnt"),
    ];

    let edge_table = TableRef::scan("gl_edge", edge_alias);
    let target_table = TableRef::scan(target.target_table, t);

    let join_on = Expr::eq(Expr::col(edge_alias, "target_id"), Expr::col(t, "id"));
    let from = TableRef::join(JoinType::Inner, edge_table, target_table, join_on);

    let tp_filter = Expr::func(
        "startsWith",
        vec![Expr::col(t, "traversal_path"), Expr::string(traversal_path)],
    );

    let not_target_deleted = Expr::unary(Op::Not, Expr::col(t, "_deleted"));
    let not_edge_deleted = Expr::unary(Op::Not, Expr::col(edge_alias, "_deleted"));

    let not_same_namespace = Expr::unary(
        Op::Not,
        Expr::func(
            "startsWith",
            vec![
                Expr::col(edge_alias, "traversal_path"),
                Expr::string(traversal_path),
            ],
        ),
    );

    let rel_values: Vec<serde_json::Value> = target
        .relationship_kinds
        .iter()
        .map(|k| serde_json::Value::String(k.to_string()))
        .collect();

    let rel_filter = Expr::col_in(
        edge_alias,
        "relationship_kind",
        gkg_utils::clickhouse::ChType::String,
        rel_values,
    );

    let mut conditions = vec![
        Some(tp_filter),
        Some(not_target_deleted),
        Some(not_edge_deleted),
        Some(not_same_namespace),
        rel_filter,
    ];

    let where_clause = Expr::and_all(conditions.drain(..));

    let query = Query {
        select,
        from,
        where_clause,
        group_by: vec![
            Expr::col(t, "traversal_path"),
            Expr::col(edge_alias, "relationship_kind"),
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

    #[test]
    fn cross_namespace_work_item_query_joins_and_filters() {
        let targets = cross_namespace_edge_targets();
        let wi_target = &targets[0];
        let ast = build_cross_namespace_edge_query(wi_target, "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(
            result.sql.contains("INNER JOIN"),
            "should use INNER JOIN: {}",
            result.sql
        );
        assert!(
            result.sql.contains("gl_work_item"),
            "should join gl_work_item: {}",
            result.sql
        );
        assert!(
            result.sql.contains("gl_edge"),
            "should scan gl_edge: {}",
            result.sql
        );
        assert!(
            result.sql.contains("count"),
            "should use count(): {}",
            result.sql
        );
    }

    #[test]
    fn cross_namespace_vulnerability_query_joins_and_filters() {
        let targets = cross_namespace_edge_targets();
        let vuln_target = &targets[1];
        let ast = build_cross_namespace_edge_query(vuln_target, "1/2/");
        let result = codegen(&ast, ResultContext::new(), QueryConfig::default()).unwrap();

        assert!(
            result.sql.contains("INNER JOIN"),
            "should use INNER JOIN: {}",
            result.sql
        );
        assert!(
            result.sql.contains("gl_vulnerability"),
            "should join gl_vulnerability: {}",
            result.sql
        );
    }
}
