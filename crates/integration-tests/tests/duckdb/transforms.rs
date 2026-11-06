use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb_client::{DuckDbClient, OpenMode, string_column};
use indexer::modules::sdlc::{SOURCE_DATA_TABLE, Transformation, sql_transformations};
use serde_json::Value;

const ALLOWED_FUNCTIONS: &[&str] = &["cardinality", "concat", "list_value", "unnest"];

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
        let (functions, tables) = (found.functions, found.tables);
        let unexpected: Vec<_> = functions
            .iter()
            .filter(|name| !ALLOWED_FUNCTIONS.contains(&name.as_str()))
            .collect();
        assert!(unexpected.is_empty(), "{table} calls {unexpected:?}");
        assert_eq!(
            tables,
            BTreeSet::from([format!("..{SOURCE_DATA_TABLE}")]),
            "{table}"
        );
    }
}

#[tokio::test]
async fn every_node_transform_runs_on_its_input_columns() {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let parser = DuckDbClient::open(OpenMode::InMemory).unwrap();
    let factory = indexer_duckdb::sql_transform().unwrap();
    let mut inputs_read = BTreeSet::new();

    for transform in sql_transformations(&ontology)
        .unwrap()
        .into_iter()
        .filter(|transform| !transform.destination_table.ends_with("_edge"))
    {
        let columns = input_columns(&parser, &transform);
        inputs_read.extend(columns.iter().cloned());
        let fields: Vec<Field> = columns
            .iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect();
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|_| Arc::new(Int64Array::from(vec![1])) as ArrayRef)
            .collect();
        let block = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();

        let table = transform.destination_table.clone();
        let outputs = factory(table.clone(), vec![transform])
            .transform(&block)
            .await
            .unwrap_or_else(|error| panic!("{table}: {error}"));
        let rows: usize = outputs.iter().map(|output| output.batch.num_rows()).sum();
        assert_eq!(rows, 1, "{table}");
    }
    assert!(
        inputs_read.contains("when"),
        "no node transform read a keyword column"
    );
}

fn input_columns(parser: &DuckDbClient, transform: &Transformation) -> BTreeSet<String> {
    let batches = parser
        .query_arrow_json(
            "SELECT json_serialize_sql(?) AS tree",
            &[transform.sql.clone().into()],
        )
        .unwrap();
    let tree: Value = serde_json::from_str(&string_column(&batches, "tree")[0]).unwrap();
    let mut found = Tree::default();
    found.collect(&tree);
    found.columns
}

#[test]
fn server_database_rejects_extensions_and_crypto_settings() {
    let database = DuckDbClient::open(OpenMode::InMemoryArrow).unwrap();
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

#[derive(Default)]
struct Tree {
    functions: BTreeSet<String>,
    tables: BTreeSet<String>,
    columns: BTreeSet<String>,
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
                let column = map
                    .get("column_names")
                    .and_then(Value::as_array)
                    .and_then(|names| names.last())
                    .and_then(Value::as_str);
                if let Some(column) = column {
                    self.columns.insert(column.to_string());
                }
                map.values().for_each(|value| self.collect(value));
            }
            Value::Array(values) => values.iter().for_each(|value| self.collect(value)),
            _ => {}
        }
    }
}
