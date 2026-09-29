use std::sync::Arc;

use compiler::{Frontend, Ontology, SecurityContext, compile, compile_local};

#[path = "support/clickhouse.rs"]
mod clickhouse;

fn ontology() -> Arc<Ontology> {
    Arc::new(Ontology::new().with_nodes(["Event"]).with_fields(
        "Event",
        [
            ("id", ontology::DataType::Int),
            ("day", ontology::DataType::Date),
            ("created_at", ontology::DataType::DateTime),
        ],
    ))
}

fn query() -> String {
    serde_json::json!({
        "query_type": "traversal",
        "nodes": [{ "id": "e", "entity": "Event", "node_ids": [1, 2, 3, 4], "columns": ["id"],
            "filters": {
                "day": { "gte": "2026-01-01", "lt": "2026-02-01" },
                "created_at": { "gte": "2026-01-01T01:00:00.125+01:00", "lt": "2026-01-02T00:00:00Z" }
            }
        }],
        "order_by": "e.id"
    }).to_string()
}

const ROWS: &str = "(1, '2026-01-01', '2026-01-01 00:00:00.124'),
    (2, '2026-01-01', '2026-01-01 00:00:00.125'),
    (3, '2026-02-01', '2026-01-01 12:00:00.000'),
    (4, '2026-01-01', NULL)";

#[test]
fn local_date_filters_preserve_boundaries_offsets_and_nulls() {
    let compiled = compile_local(&query(), Frontend::JsonDsl, &ontology()).unwrap();
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "SET TimeZone = 'UTC';
         CREATE TABLE gl_event(id BIGINT, day DATE, created_at TIMESTAMPTZ);
         INSERT INTO gl_event VALUES {ROWS};"
        ))
        .unwrap();
    let rows = connection
        .prepare(&compiled.base.render())
        .unwrap()
        .query_map([], |row| row.get::<_, i64>("e_id"))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec![2]);
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn remote_date_filters_preserve_boundaries_offsets_and_nulls() {
    let security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    let compiled = compile(&query(), Frontend::JsonDsl, &ontology(), &security).unwrap();
    let rendered = compiled.base.render();
    let sql = rendered.split(" SETTINGS ").next().unwrap();
    let result = clickhouse::execute(&format!(
        "CREATE TABLE input(id Int64, day Date, created_at Nullable(DateTime64(3, 'UTC'))) ENGINE = Memory;
         INSERT INTO input VALUES {ROWS};
         CREATE TABLE gl_event(id Int64, day Date, created_at Nullable(DateTime64(3, 'UTC')),
             traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_event SELECT *, '1/', 1, false FROM input;
         SELECT e_id FROM ({sql}) FORMAT TSV;"
    ));
    assert_eq!(result.trim(), "2");
}
