use std::sync::Arc;

use compiler::{Frontend, Ontology, SecurityContext, compile, compile_local};

#[path = "support/clickhouse.rs"]
mod clickhouse;

const ROWS: &str = "(1, 'Alpha%_core.rs'), (2, 'alphaXXcore.rs'),
    (3, 'preAlpha%_core.rs'), (4, 'Alpha%_core.RS'), (5, NULL),
    (6, 'quote''core.rs'), (7, 'plain.rs')";

fn cases() -> Vec<(serde_json::Value, Vec<i64>)> {
    vec![
        (serde_json::json!({ "contains": "ALPHA%_" }), vec![1, 3, 4]),
        (serde_json::json!({ "starts_with": "Alpha%_" }), vec![1, 4]),
        (
            serde_json::json!({ "ends_with": "core.rs" }),
            vec![1, 2, 3, 6],
        ),
        (serde_json::json!({ "contains": "quote'" }), vec![6]),
        (serde_json::json!({ "contains": "missing" }), vec![]),
        (
            serde_json::json!({ "starts_with": "Alpha%_", "ends_with": "core.rs" }),
            vec![1],
        ),
    ]
}

fn query(filter: serde_json::Value) -> String {
    serde_json::json!({
        "query_type": "traversal",
        "nodes": [{ "id": "f", "entity": "File", "columns": ["id"], "filters": { "path": filter } }],
        "order_by": "f.id"
    }).to_string()
}

#[test]
fn text_filters_preserve_literal_characters_case_rules_and_nulls() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE gl_file(id BIGINT, path VARCHAR); INSERT INTO gl_file VALUES {ROWS};"
        ))
        .unwrap();
    for (filter, expected) in cases() {
        let compiled = compile_local(&query(filter.clone()), Frontend::JsonDsl, &ontology).unwrap();
        let rows = connection
            .prepare(&compiled.base.render())
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<duckdb::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, expected, "{filter}");
    }
    let compiled = compile_local(
        "MATCH (f:File) WHERE f.path CONTAINS 'ALPHA%_' RETURN f.id ORDER BY f.id",
        Frontend::Gql,
        &ontology,
    )
    .unwrap();
    let rows = connection
        .prepare(&compiled.base.render())
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<duckdb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, vec![1, 3, 4]);
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn remote_text_filters_match_local_results_after_current_row_and_security_filters() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut script = format!(
        "CREATE TABLE input(id Int64, path Nullable(String)) ENGINE = Memory;
         INSERT INTO input VALUES {ROWS};
         CREATE TABLE gl_file(id Int64, path Nullable(String), project_id Int64,
             traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_file SELECT id, path, 100, '1/100/', 1, false FROM input;
         INSERT INTO gl_file VALUES
             (8, 'Alpha%_core.rs', 200, '1/200/', 1, false),
             (9, 'Alpha%_core.rs', 100, '1/100/', 1, false),
             (9, 'removed', 100, '1/100/', 2, true);"
    );
    let mut expected = Vec::new();
    for (index, (filter, ids)) in cases().into_iter().enumerate() {
        let compiled = compile(&query(filter), Frontend::JsonDsl, &ontology, &security).unwrap();
        let rendered = compiled.base.render();
        let sql = rendered.split(" SETTINGS ").next().unwrap();
        script.push_str(&format!(
            "SELECT {index}, f_id FROM ({sql}) ORDER BY f_id FORMAT TSV;"
        ));
        expected.extend(ids.into_iter().map(|id| format!("{index}\t{id}")));
    }
    assert_eq!(clickhouse::execute(&script).trim(), expected.join("\n"));
}

#[test]
#[ignore = "requires Docker with ClickHouse"]
fn token_filters_match_words_instead_of_substrings() {
    let ontology = Arc::new(Ontology::load_embedded().unwrap());
    let model = query_data_model::ClickHouseDataModel::derive(ontology.clone()).unwrap();
    use query_data_model::QueryDataModel;
    let property = model.property("MergeRequest", "title").unwrap();
    assert!(model.has_text_index(property.id));
    let security = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
    let mut script = String::from(
        "CREATE TABLE gl_merge_request(id Int64, title String, traversal_path String, _version UInt64, _deleted Bool)
         ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id);
         INSERT INTO gl_merge_request VALUES
             (1, 'fix parser', '1/100/', 1, false),
             (2, 'prefix parser', '1/100/', 1, false),
             (3, 'fix renderer', '1/100/', 1, false);"
    );
    for (index, (operator, value)) in [
        ("token_match", "fix"),
        ("all_tokens", "fix parser"),
        ("any_tokens", "fix parser"),
    ]
    .into_iter()
    .enumerate()
    {
        let input = serde_json::json!({
            "query_type": "traversal",
            "nodes": [{ "id": "m", "entity": "MergeRequest", "columns": ["id"], "node_ids": [1, 2, 3],
                "filters": { "title": { (operator): value } } }]
        });
        let compiled =
            compile(&input.to_string(), Frontend::JsonDsl, &ontology, &security).unwrap();
        let rendered = compiled.base.render();
        let sql = rendered.split(" SETTINGS ").next().unwrap();
        script.push_str(&format!(
            "SELECT {index}, m_id FROM ({sql}) ORDER BY m_id FORMAT TSV;"
        ));
    }
    assert_eq!(
        clickhouse::execute(&script).trim(),
        "0\t1\n0\t3\n1\t1\n2\t1\n2\t2\n2\t3"
    );
}
