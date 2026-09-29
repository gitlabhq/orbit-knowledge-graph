use std::sync::Arc;

use compiler::ast::{Expr, Query, SelectExpr, TableRef};
use compiler::passes::{check, codegen, enforce::ResultContext, security};
use compiler::{AuthorizedPath, Node, Ontology, SecurityContext};
use query_data_model::{ClickHouseDataModel, QueryDataModel};

#[test]
fn a_view_requires_every_entity_path_at_its_own_role_floor() {
    let ontology = Ontology::load_embedded()
        .unwrap()
        .with_redaction_role("MergeRequest", ontology::RequiredRole::SecurityManager)
        .with_denormalized_join(
            "reviewer_project",
            &[
                ("REVIEWER", "User", "MergeRequest", false),
                ("IN_PROJECT", "MergeRequest", "Project", true),
            ],
        );
    let model = ClickHouseDataModel::derive(Arc::new(ontology)).unwrap();
    let view = model
        .backend()
        .views()
        .iter()
        .find(|view| view.table == "gl_denorm_reviewer_project")
        .unwrap();
    let property = model.property("MergeRequest", "id").unwrap().id;
    assert_eq!(view.properties.get(&(2, property)).unwrap(), "t2_id");
    assert_eq!(view.hops.len(), 2);
    assert_eq!(view.path_columns.len(), 3);

    let context = SecurityContext::new_with_roles(
        1,
        vec![
            AuthorizedPath::new("1/100/", 20),
            AuthorizedPath::new("1/200/", 30),
        ],
    )
    .unwrap();
    let mut node = Node::Query(Box::new(Query {
        select: vec![SelectExpr::col("v", "id")],
        from: TableRef::scan(&view.table, "v"),
        where_clause: Some(Expr::func(
            "startsWith",
            vec![Expr::col("v", "traversal_path"), Expr::string("1/100/")],
        )),
        ..Default::default()
    }));
    assert!(check::check_ast(&node, &context, &model).is_err());
    security::apply_security_context(&mut node, &context, &model).unwrap();
    check::check_ast(&node, &context, &model).unwrap();

    let query = codegen::duckdb::codegen(&node, ResultContext::new()).unwrap();
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_denorm_reviewer_project(id BIGINT, traversal_path VARCHAR, t2_traversal_path VARCHAR, t3_traversal_path VARCHAR);
         INSERT INTO gl_denorm_reviewer_project VALUES
             (1, '1/100/', '1/200/', '1/100/'),
             (2, '1/100/', '1/100/', '1/100/'),
             (3, '1/100/', '1/200/', '1/999/');"
    ).unwrap();
    let rows = connection
        .prepare(&query.render())
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows, vec![1]);
}
