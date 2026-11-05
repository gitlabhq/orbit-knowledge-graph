use std::collections::BTreeSet;

use duckdb_client::{DuckDbClient, Limits, OpenMode, string_column};
use indexer::modules::sdlc::{SOURCE_DATA_TABLE, sql_transformations};
use serde_json::Value;

const ALLOWED_FUNCTIONS: &[&str] = &["cardinality", "list_value", "map", "unnest", "||"];

#[test]
fn generated_transforms_are_selects_over_the_block_with_allowed_functions() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let transforms = sql_transformations(&ontology).unwrap();
    assert!(!transforms.is_empty());
    let parser = DuckDbClient::open(OpenMode::InMemory).unwrap();

    for transform in transforms {
        let batches = parser
            .query_arrow_json(
                "SELECT json_serialize_sql(?) AS tree",
                &[transform.sql.clone().into()],
            )
            .unwrap();
        let tree: Value = serde_json::from_str(&string_column(&batches, "tree")[0]).unwrap();
        let table = &transform.destination_table;
        assert_eq!(tree["error"], false, "{table} is not one SELECT: {tree}");
        assert_eq!(
            tree["statements"].as_array().map(Vec::len),
            Some(1),
            "{table}"
        );

        let mut found = Tree::default();
        found.collect(&tree);
        let unexpected: Vec<_> = found
            .functions
            .iter()
            .filter(|name| !ALLOWED_FUNCTIONS.contains(&name.as_str()))
            .collect();
        assert!(unexpected.is_empty(), "{table} calls {unexpected:?}");
        let read: BTreeSet<_> = found.tables.difference(&found.ctes).collect();
        assert_eq!(
            read,
            BTreeSet::from([&format!("..{SOURCE_DATA_TABLE}")]),
            "{table}"
        );
    }
}

#[test]
fn server_database_rejects_extensions_and_crypto_settings() {
    let database = DuckDbClient::open(OpenMode::InMemoryArrow(Limits::default())).unwrap();
    for sql in [
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET temp_file_encryption = true",
        "SET force_mbedtls_unsafe = true",
    ] {
        let error = database.execute(sql, &[]).unwrap_err().to_string();
        assert!(
            error.contains("disabled by configuration")
                || error.contains("disabled through configuration")
                || error.contains("configuration has been locked"),
            "{sql}: {error}"
        );
    }
}

#[test]
fn server_database_applies_the_configured_limits() {
    let database = DuckDbClient::open(OpenMode::InMemoryArrow(Limits {
        threads: Some(2),
        memory_limit: Some("512MiB"),
    }))
    .unwrap();
    let setting = |name: &str| {
        string_column(
            &database
                .query_arrow_json(
                    "SELECT current_setting(?)::VARCHAR AS value",
                    &[name.into()],
                )
                .unwrap(),
            "value",
        )
    };
    assert_eq!(setting("threads"), ["2"]);
    assert_eq!(setting("memory_limit"), ["512.0 MiB"]);
}

#[derive(Default)]
struct Tree {
    functions: BTreeSet<String>,
    tables: BTreeSet<String>,
    ctes: BTreeSet<String>,
}

impl Tree {
    fn collect(&mut self, node: &Value) {
        match node {
            Value::Object(map) => {
                if let Some(name) = map.get("function_name").and_then(Value::as_str) {
                    self.functions.insert(name.to_string());
                }
                if let Some(name) = map.get("table_name").and_then(Value::as_str) {
                    let qualifier = ["catalog_name", "schema_name"]
                        .map(|key| map.get(key).and_then(Value::as_str).unwrap_or_default());
                    self.tables
                        .insert(format!("{}.{}.{name}", qualifier[0], qualifier[1]));
                }
                let ctes = map.get("cte_map").and_then(|ctes| ctes["map"].as_array());
                for cte in ctes.into_iter().flatten() {
                    if let Some(name) = cte["key"].as_str() {
                        self.ctes.insert(format!("..{name}"));
                    }
                }
                map.values().for_each(|value| self.collect(value));
            }
            Value::Array(values) => values.iter().for_each(|value| self.collect(value)),
            _ => {}
        }
    }
}
