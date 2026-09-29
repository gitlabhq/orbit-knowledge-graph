use std::sync::Arc;

use compiler::passes::cursor;
use compiler::{Frontend, Ontology, SecurityContext, compile, compile_local};

#[path = "support/clickhouse.rs"]
mod clickhouse;

fn query(direction: &str) -> serde_json::Value {
    serde_json::json!({
        "query_type": "neighbors",
        "nodes": [{
            "id": "center", "entity": "Definition", "columns": ["id"],
            "node_ids": [1], "filters": { "name": "keep" }
        }],
        "neighbors": { "direction": direction, "rel_types": ["CALLS"] },
        "cursor": { "page_size": 10 }
    })
}

#[test]
fn neighbors_preserve_direction_filters_self_edges_and_cursor_ties() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE gl_definition(id BIGINT, name VARCHAR);
         INSERT INTO gl_definition VALUES (1, 'keep'), (2, 'other');
         CREATE TABLE gl_edge(source_id BIGINT, target_id BIGINT, source_kind VARCHAR,
             target_kind VARCHAR, relationship_kind VARCHAR);
         INSERT INTO gl_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS'),
             (3, 1, 'Definition', 'Definition', 'CALLS'),
             (1, 1, 'Definition', 'Definition', 'CALLS'),
             (1, 4, 'File', 'Definition', 'CALLS'),
             (1, 5, 'Definition', 'Definition', 'CONTAINS');",
        )
        .unwrap();
    let execute = |input: &serde_json::Value| {
        let compiled = compile_local(&input.to_string(), Frontend::JsonDsl, &ontology).unwrap();
        let sql = format!(
            "SELECT _gkg_neighbor_id, _gkg_neighbor_is_outgoing FROM ({})",
            compiled.base.render(),
        );
        connection
            .prepare(&sql)
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(execute(&query("outgoing")), vec![(1, 1), (2, 1)]);
    assert_eq!(execute(&query("incoming")), vec![(1, 0), (3, 0)]);
    let mut both = query("both");
    assert_eq!(execute(&both), vec![(1, 0), (1, 1), (2, 1), (3, 0)]);
    let gql = compile_local(
        "MATCH (center:Definition {id: 1})-[:CALLS]-(neighbor)
         WHERE center.name = 'keep' RETURN center.id, neighbor",
        Frontend::Gql,
        &ontology,
    )
    .unwrap();
    let mut gql_rows = connection
        .prepare(&format!(
            "SELECT _gkg_neighbor_id, _gkg_neighbor_is_outgoing FROM ({})
         ORDER BY _gkg_neighbor_id, _gkg_neighbor_is_outgoing",
            gql.base.render()
        ))
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    gql_rows.sort();
    assert_eq!(gql_rows, execute(&both));
    both["cursor"]["after"] = cursor::encode(
        cursor::canonical_hash(&both),
        &[
            Some("1".into()),
            Some("1".into()),
            Some("Definition".into()),
            Some("CALLS".into()),
            Some("0".into()),
        ],
    )
    .into();
    assert_eq!(execute(&both), vec![(1, 1), (2, 1), (3, 0)]);
    let mut filtered = query("both");
    filtered["nodes"][0]["filters"]["name"] = "missing".into();
    assert!(execute(&filtered).is_empty());
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn neighbors_authorize_both_inputs_and_return_center_authorization_identity() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let compiled = compile(
        &query("both").to_string(),
        Frontend::JsonDsl,
        &ontology,
        &security,
    )
    .unwrap();
    let sql = compiled.base.render();
    let sql = sql.split(" SETTINGS ").next().unwrap();
    let identity = compiled.base.result_context.get("center").unwrap();
    assert_eq!(identity.pk_column, "_gkg_center_pk");
    assert!(matches!(
        compiled.hydration,
        compiler::passes::hydrate::HydrationPlan::Dynamic(_)
    ));
    let result = clickhouse::execute(&format!(
        "CREATE TABLE gl_definition(id Int64, name String, project_id Int64,
             traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_definition VALUES (1, 'keep', 100, '1/100/', 1, false);
         CREATE TABLE gl_code_edge(source_id Int64, target_id Int64, source_kind String,
             target_kind String, relationship_kind String, traversal_path String,
             _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version)
         ORDER BY (traversal_path, source_id, target_id, source_kind, target_kind, relationship_kind);
         INSERT INTO gl_code_edge VALUES
             (1, 2, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (3, 1, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (1, 4, 'Definition', 'Definition', 'CALLS', '1/200/', 1, false),
             (1, 5, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
             (1, 5, 'Definition', 'Definition', 'CALLS', '1/100/', 2, true);
         SELECT _gkg_center_pk, _gkg_center_id, _gkg_neighbor_id,
             _gkg_neighbor_is_outgoing FROM ({sql}) ORDER BY _gkg_neighbor_id FORMAT TSV;"
    ));
    assert_eq!(result.trim(), "1\t100\t2\t1\n1\t100\t3\t0");
}
