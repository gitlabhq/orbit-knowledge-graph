use std::sync::Arc;

#[path = "support/clickhouse.rs"]
mod clickhouse;

use compiler::{Frontend, Ontology, SecurityContext, compile};

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn remote_reads_filter_current_rows_and_authorize_before_aggregation() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let query = serde_json::json!({
        "query_type": "aggregation",
        "nodes": [{
            "id": "p", "entity": "Project", "columns": ["id"],
            "filters": { "name": "match" }
        }],
        "aggregations": [{ "count": "p", "as": "n" }]
    });
    let compiled = compile(&query.to_string(), Frontend::JsonDsl, &ontology, &security).unwrap();
    let sql = compiled.base.render();
    let sql = sql.split(" SETTINGS ").next().unwrap();

    let scoped = compile(
        "MATCH (p:Project {id: 4}) RETURN count(p) AS n",
        Frontend::Gql,
        &ontology,
        &security,
    )
    .unwrap();
    let scoped_sql = scoped.base.render();
    let scoped_sql = scoped_sql.split(" SETTINGS ").next().unwrap();

    let grouped = compile(
        "MATCH (d:Definition) WHERE d.id IN [10, 11, 12] RETURN d, count(d) AS n",
        Frontend::Gql,
        &ontology,
        &security,
    )
    .unwrap();
    let grouped_sql = grouped.base.render();
    let grouped_sql = grouped_sql.split(" SETTINGS ").next().unwrap();
    let identity = grouped.base.result_context.get("d").unwrap();
    let grouped_sql = format!(
        "SELECT {}, {}, n FROM ({grouped_sql}) ORDER BY {}",
        identity.pk_column, identity.id_column, identity.pk_column,
    );

    let ungrouped = compile(
        "MATCH (d:Definition) WHERE d.id IN [10, 11, 12] RETURN count(d) AS n",
        Frontend::Gql,
        &ontology,
        &security,
    )
    .unwrap();
    let ungrouped_sql = ungrouped.base.render();
    let ungrouped_sql = ungrouped_sql.split(" SETTINGS ").next().unwrap();

    let calls = compile(
        "MATCH (caller:Definition)-[:CALLS]->(callee:Definition {id: 10})
         RETURN count(caller) AS n",
        Frontend::Gql,
        &ontology,
        &security,
    )
    .unwrap();
    let calls_sql = calls.base.render();
    let calls_sql = calls_sql.split(" SETTINGS ").next().unwrap();

    let setup = "CREATE TABLE gl_project (
        id Int64, name String, traversal_path String, _version UInt64, _deleted Bool
    ) ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
    INSERT INTO gl_project VALUES
        (1, 'match', '1/100/', 1, false),
        (1, 'changed', '1/100/', 2, false),
        (2, 'match', '1/100/', 1, false),
        (2, 'match', '1/100/', 2, true),
        (3, 'match', '1/200/', 1, false),
        (4, 'match', '1/100/', 1, false);
    CREATE TABLE gl_definition (
        id Int64, project_id Int64, traversal_path String, _version UInt64, _deleted Bool
    ) ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
    INSERT INTO gl_definition VALUES
        (10, 1000, '1/100/', 1, false),
        (11, 1000, '1/100/', 1, false),
        (12, 2000, '1/200/', 1, false);
    CREATE TABLE gl_code_edge (
        source_id Int64, target_id Int64, source_kind String, target_kind String,
        relationship_kind String, traversal_path String, _version UInt64, _deleted Bool
    ) ENGINE = ReplacingMergeTree(_version)
      ORDER BY (traversal_path, source_id, target_id, source_kind, target_kind, relationship_kind);
    INSERT INTO gl_code_edge VALUES
        (11, 10, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false),
        (10, 11, 'Definition', 'Definition', 'CALLS', '1/100/', 1, false);";

    let output = clickhouse::execute(&format!(
        "{setup}\n{sql} FORMAT TSV;\n{scoped_sql} FORMAT TSV;\n{grouped_sql} FORMAT TSV;\n{ungrouped_sql} FORMAT TSV;\n{calls_sql} FORMAT TSV;"
    ));
    assert_eq!(output.trim(), "1\n1\n10\t1000\t1\n11\t1000\t1\n2\n1");
}
