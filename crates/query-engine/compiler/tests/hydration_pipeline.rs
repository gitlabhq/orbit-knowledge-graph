use std::sync::Arc;

#[path = "support/clickhouse.rs"]
mod database;

use compiler::{
    ColumnSelection, Frontend, HydrationCompileOptions, HydrationPlan, Input, InputNode, Ontology,
    QueryType, SecurityContext, compile, compile_input,
};

#[test]
fn virtual_requests_keep_resolver_dependencies_out_of_base_sql() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let compiled = compile(
        r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"columns":["path","content"],"filters":{"content":{"contains":"hello"}}}]}"#,
        Frontend::JsonDsl, &ontology, &context,
    ).unwrap();

    assert!(!compiled.base.render().contains(".content"));
    let HydrationPlan::Static(templates) = compiled.hydration else {
        panic!("missing virtual hydration")
    };
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].virtual_columns[0].column_name, "content");
    assert_eq!(templates[0].virtual_filters.len(), 1);
    assert!(templates[0].columns.contains(&"project_id".into()));
    assert!(templates[0].columns.contains(&"path".into()));
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn hydration_reads_latest_live_rows_and_serializes_multiple_entity_types() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let context = SecurityContext::new(1, vec![]).unwrap();
    let input = Input {
        query_type: QueryType::Hydration,
        nodes: vec![
            InputNode {
                id: "hydrate".into(),
                entity: Some("Project".into()),
                node_ids: vec![1, 2],
                columns: Some(ColumnSelection::List(vec!["name".into()])),
                ..Default::default()
            },
            InputNode {
                id: "hydrate".into(),
                entity: Some("User".into()),
                node_ids: vec![3],
                columns: Some(ColumnSelection::List(vec!["username".into()])),
                ..Default::default()
            },
        ],
        limit: 3,
        ..Default::default()
    };
    let compiled = compile_input(
        input,
        HydrationCompileOptions::default(),
        &ontology,
        &context,
    )
    .unwrap();
    let sql = compiled.base.render();
    let sql = sql.split(" SETTINGS ").next().unwrap();
    let output = database::execute(&format!(
        "CREATE TABLE gl_project(id Int64, name String, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         CREATE TABLE gl_user(id Int64, username String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY id;
         INSERT INTO gl_project VALUES (1, 'old', '1/100/', 1, false), (1, 'new', '1/100/', 2, false),
             (2, 'deleted', '1/100/', 1, false), (2, 'deleted', '1/100/', 2, true);
         INSERT INTO gl_user VALUES (3, 'alice', 1, false);
         SELECT hydrate_id, hydrate_entity_type, hydrate_props FROM ({sql}) ORDER BY hydrate_id FORMAT TSV;"
    ));

    assert_eq!(
        output.trim(),
        "1\tProject\t{\"name\":\"new\"}\n3\tUser\t{\"username\":\"alice\"}"
    );
}
