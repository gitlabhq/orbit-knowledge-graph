use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

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
        (12, 2000, '1/200/', 1, false);";

    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "clickhouse/clickhouse-server:26.2",
            "clickhouse-local",
            "--multiquery",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    write!(
        child.stdin.take().unwrap(),
        "{setup}\n{sql} FORMAT TSV;\n{scoped_sql} FORMAT TSV;\n{grouped_sql} FORMAT TSV;\n{ungrouped_sql} FORMAT TSV;"
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "{}\n{sql}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "1\n1\n10\t1000\t1\n11\t1000\t1\n2"
    );
}
