use std::sync::Arc;

use compiler::{Frontend, Ontology, compile_local};

#[test]
fn self_type_relationships_preserve_direction_multiplicity_and_edge_outputs() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_definition(id BIGINT, name VARCHAR);
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR, target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_definition VALUES (1, 'caller'), (2, 'callee'), (3, 'other');
         INSERT INTO gl_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (2, 1, 'Definition', 'Definition', 'CALLS'),
             (1, 3, 'Definition', 'Definition', 'DEFINES'),
             (1, 3, 'File', 'Definition', 'CALLS');"
    ).unwrap();

    for (query, expected) in [
        (
            "MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition) RETURN a.name, b.name",
            vec![(1, 2), (1, 2)],
        ),
        (
            "MATCH (a:Definition {id: 1})<-[:CALLS]-(b:Definition) RETURN a.name, b.name",
            vec![(2, 1)],
        ),
    ] {
        let compiled = compile_local(query, Frontend::Gql, &ontology).unwrap();
        let edge = &compiled.base.result_context.edges()[0];
        let mut statement = connection.prepare(&compiled.base.render()).unwrap();
        let mut rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(edge.src_column.as_str())?,
                    row.get::<_, i64>(edge.dst_column.as_str())?,
                ))
            })
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();

        rows.sort();
        assert_eq!(rows, expected);
    }
}

#[test]
fn shared_endpoint_joins_resolve_each_node_once() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE gl_definition(id BIGINT, name VARCHAR);
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR, target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_definition VALUES (1, 'a'), (2, 'b'), (3, 'c');
         INSERT INTO gl_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (2, 3, 'Definition', 'Definition', 'CALLS'),
             (1, 3, 'Definition', 'Definition', 'CALLS');"
    ).unwrap();

    let query = "MATCH (a:Definition {id: 1})-[:CALLS]->(b:Definition)-[:CALLS]->(c:Definition)
                 RETURN a.name, b.name, c.name";
    let compiled = compile_local(query, Frontend::Gql, &ontology).unwrap();
    let rows = connection
        .prepare(&compiled.base.render())
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>("a_name")?,
                row.get::<_, String>("b_name")?,
                row.get::<_, String>("c_name")?,
            ))
        })
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();

    assert_eq!(rows, vec![("a".into(), "b".into(), "c".into())]);
    assert_eq!(compiled.base.result_context.edges().len(), 2);
}
