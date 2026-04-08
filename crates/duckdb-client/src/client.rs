use std::path::Path;
use std::thread;

use arrow::record_batch::RecordBatch;
use duckdb::params;

use crate::converter::LocalGraphData;
use crate::error::{DuckDbError, Result};
use crate::schema::{CODE_GRAPH_TABLES, SCHEMA_DDL};

pub struct DuckDbClient {
    conn: duckdb::Connection,
}

impl DuckDbClient {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| DuckDbError::Schema(e.to_string()))?;
        }
        let conn = duckdb::Connection::open(path)?;
        Ok(Self { conn })
    }

    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Result<Self> {
        let conn = duckdb::Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    pub fn initialize_schema(&self) -> Result<()> {
        self.conn
            .execute_batch(SCHEMA_DDL)
            .map_err(|e| DuckDbError::Schema(e.to_string()))?;
        Ok(())
    }

    /// Bulk insert via DuckDB's Appender, which converts Arrow RecordBatch
    /// directly to DuckDB DataChunks — no SQL parsing, no vtab overhead.
    pub fn insert_arrow(&self, table: &str, batch: RecordBatch) -> Result<()> {
        if !CODE_GRAPH_TABLES.contains(&table) {
            return Err(DuckDbError::Schema(format!("unknown table: {table}")));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut appender = self.conn.appender(table)?;
        appender.append_record_batch(batch)?;
        appender.flush()?;
        Ok(())
    }

    pub fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql)?;
        let batches = stmt.query_arrow([])?.collect();
        Ok(batches)
    }

    pub fn query_arrow_params(
        &self,
        sql: &str,
        params: &[Box<dyn duckdb::ToSql>],
    ) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql)?;
        let batches = stmt
            .query_arrow(duckdb::params_from_iter(params.iter()))?
            .collect();
        Ok(batches)
    }

    /// Deletes all data across all tables. In local mode each DB file is
    /// one project, so a full truncate is the correct reset before re-indexing.
    pub fn delete_all_data(&self) -> Result<()> {
        for table in CODE_GRAPH_TABLES {
            self.conn
                .execute(&format!("DELETE FROM {table}"), params![])?;
        }
        Ok(())
    }

    /// Insert all graph data concurrently. Each table gets its own connection
    /// and Appender, running in parallel threads. DuckDB supports concurrent
    /// inserts from multiple connections to the same file.
    pub fn insert_graph(path: &Path, data: LocalGraphData) -> Result<()> {
        let inserts: Vec<(&str, RecordBatch)> = vec![
            ("gl_directory", data.directories),
            ("gl_file", data.files),
            ("gl_definition", data.definitions),
            ("gl_imported_symbol", data.imported_symbols),
            ("gl_edge", data.edges),
        ];

        thread::scope(|s| {
            let handles: Vec<_> = inserts
                .into_iter()
                .filter(|(_, batch)| batch.num_rows() > 0)
                .map(|(table, batch)| {
                    s.spawn(move || -> Result<()> {
                        let conn = duckdb::Connection::open(path)?;
                        let mut appender = conn.appender(table)?;
                        appender.append_record_batch(batch)?;
                        appender.flush()?;
                        Ok(())
                    })
                })
                .collect();

            for handle in handles {
                handle.join().expect("insert thread panicked")?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn file_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("extension", DataType::Utf8, true),
            Field::new("language", DataType::Utf8, true),
            Field::new("_version", DataType::Int64, false),
        ]))
    }

    fn make_file_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let n = ids.len();
        RecordBatch::try_new(
            file_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(vec![42; n])),
                Arc::new(StringArray::from(vec!["main"; n])),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(vec![Some("rs"); n])),
                Arc::new(StringArray::from(vec![Some("Rust"); n])),
                Arc::new(Int64Array::from(vec![0; n])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn schema_creation_and_sql_roundtrip() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, project_id, branch, path, name, extension, language, _version) \
                 VALUES (1, 42, 'main', 'src/lib.rs', 'lib.rs', 'rs', 'Rust', 0)",
                [],
            )
            .unwrap();

        let batches = client
            .query_arrow("SELECT id, project_id, name, language FROM gl_file")
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);

        let names = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "lib.rs");
    }

    #[test]
    fn appender_insert_and_query() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        let batch = make_file_batch(&[10, 11], &["a.rs", "b.rs"]);
        client.insert_arrow("gl_file", batch).unwrap();

        let result = client
            .query_arrow("SELECT id, name FROM gl_file ORDER BY id")
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);

        let ids = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 10);
        assert_eq!(ids.value(1), 11);
    }

    #[test]
    fn large_batch_appender() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        let n = 5000;
        let ids: Vec<i64> = (0..n).collect();
        let names: Vec<String> = (0..n).map(|i| format!("file_{i}.rs")).collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

        let batch = make_file_batch(&ids, &name_refs);
        client.insert_arrow("gl_file", batch).unwrap();

        let result = client
            .query_arrow("SELECT count(*) as cnt FROM gl_file")
            .unwrap();
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), n);
    }

    #[test]
    fn delete_all_data_truncates() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, project_id, branch, path, name, _version) VALUES (1, 42, 'main', 'a.rs', 'a.rs', 0)",
                [],
            )
            .unwrap();
        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, project_id, branch, path, name, _version) VALUES (2, 99, 'main', 'b.rs', 'b.rs', 0)",
                [],
            )
            .unwrap();

        client.delete_all_data().unwrap();

        let batches = client
            .query_arrow("SELECT count(*) as cnt FROM gl_file")
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 0);
    }

    #[test]
    fn file_backed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");

        let client = DuckDbClient::open(&db_path).unwrap();
        client.initialize_schema().unwrap();
        client
            .conn
            .execute(
                "INSERT INTO gl_directory (id, project_id, branch, path, name, _version) VALUES (1, 1, 'main', 'src', 'src', 0)",
                [],
            )
            .unwrap();
        drop(client);

        let client2 = DuckDbClient::open(&db_path).unwrap();
        let batches = client2
            .query_arrow("SELECT name FROM gl_directory")
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "src");
    }

    #[test]
    fn insert_arrow_rejects_unknown_table() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        let batch = make_file_batch(&[1], &["a.rs"]);
        let err = client.insert_arrow("evil_table", batch).unwrap_err();
        assert!(err.to_string().contains("unknown table"));
    }

    #[test]
    fn insert_empty_batch_is_noop() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        let batch = make_file_batch(&[], &[]);
        client.insert_arrow("gl_file", batch).unwrap();

        let result = client
            .query_arrow("SELECT count(*) as cnt FROM gl_file")
            .unwrap();
        let count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 0);
    }
}
