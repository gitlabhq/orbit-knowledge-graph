use std::path::Path;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use duckdb::params;

use crate::converter::LocalGraphData;
use crate::error::{DuckDbError, Result};

const MAX_OPEN_RETRIES: u32 = 10;
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

pub struct DuckDbClient {
    conn: duckdb::Connection,
}

/// Check whether a DuckDB error is a file-lock contention error.
///
/// DuckDB emits `IO Error: Could not set lock on file` when another
/// process holds the write lock. The Rust crate surfaces this as a
/// generic `duckdb::Error` with the message embedded.
fn is_lock_error(e: &duckdb::Error) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("could not set lock") || msg.contains("lock on file")
}

impl DuckDbClient {
    /// Open a DuckDB database for read-write access, retrying with
    /// exponential backoff (capped at 5s per attempt, ~26s total) if
    /// another process holds the write lock.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| DuckDbError::Schema(e.to_string()))?;
        }

        let mut backoff = INITIAL_BACKOFF;
        for attempt in 0..=MAX_OPEN_RETRIES {
            let config = duckdb::Config::default()
                .access_mode(duckdb::AccessMode::ReadWrite)
                .map_err(|e| DuckDbError::Schema(e.to_string()))?;
            match duckdb::Connection::open_with_flags(path, config) {
                Ok(conn) => return Ok(Self { conn }),
                Err(e) if attempt < MAX_OPEN_RETRIES && is_lock_error(&e) => {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
                Err(e) => return Err(e.into()),
            }
        }
        unreachable!()
    }

    /// Open a DuckDB database for read-only access. Retries briefly
    /// (50ms intervals, ~250ms total) if a writer holds the lock.
    pub fn open_read_only(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| DuckDbError::Schema(e.to_string()))?;
        }

        for attempt in 0..=5 {
            let config = duckdb::Config::default()
                .access_mode(duckdb::AccessMode::ReadOnly)
                .map_err(|e| DuckDbError::Schema(e.to_string()))?;
            match duckdb::Connection::open_with_flags(path, config) {
                Ok(conn) => return Ok(Self { conn }),
                Err(e) if attempt < 5 && is_lock_error(&e) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(e.into()),
            }
        }
        unreachable!()
    }

    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Result<Self> {
        let conn = duckdb::Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Create all graph tables and the manifest table from the given DDL.
    ///
    /// The DDL is typically generated from the ontology via
    /// `generate_local_tables` + `emit_duckdb_create_table`, with the
    /// manifest DDL (`MANIFEST_DDL`) appended.
    pub fn initialize_schema(&self, ddl: &str) -> Result<()> {
        self.conn
            .execute_batch(ddl)
            .map_err(|e| DuckDbError::Schema(e.to_string()))?;
        Ok(())
    }

    /// Bulk insert via DuckDB's Appender, which converts Arrow RecordBatch
    /// directly to DuckDB DataChunks — no SQL parsing, no vtab overhead.
    ///
    /// `allowed_tables` is an allowlist of valid table names. Pass the
    /// table names derived from the ontology's `local_db` config.
    pub fn insert_arrow(
        &self,
        table: &str,
        batch: RecordBatch,
        allowed_tables: &[&str],
    ) -> Result<()> {
        if !allowed_tables.contains(&table) {
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

    /// Deletes all data across all graph tables. In local mode each DB file
    /// is one project, so a full truncate is the correct reset before
    /// re-indexing. The manifest table is preserved.
    pub fn delete_all_data(&self, tables: &[&str]) -> Result<()> {
        for table in tables {
            self.conn
                .execute(&format!("DELETE FROM {table}"), params![])?;
        }
        Ok(())
    }

    /// Delete data for a specific project across all tables.
    /// Node tables are scoped by `project_id`; the edge table is scoped
    /// by source_id matching any node ID for this project.
    /// Edges are deleted first while node IDs are still queryable.
    pub fn delete_project(
        &self,
        project_id: i64,
        node_tables: &[String],
        edge_table: &str,
    ) -> Result<()> {
        // Delete edges first (while node IDs are still in the tables).
        let subqueries: Vec<String> = node_tables
            .iter()
            .map(|t| format!("SELECT id FROM {t} WHERE project_id = ?1"))
            .collect();
        if !subqueries.is_empty() {
            let union = subqueries.join(" UNION ");
            self.conn.execute(
                &format!("DELETE FROM {edge_table} WHERE source_id IN ({union})"),
                params![project_id],
            )?;
        }

        // Then delete node tables.
        for table in node_tables {
            self.conn.execute(
                &format!("DELETE FROM {table} WHERE project_id = ?1"),
                params![project_id],
            )?;
        }

        Ok(())
    }

    /// Insert all graph data into DuckDB sequentially.
    ///
    /// Table names come from `LocalGraphData.tables`, which are derived from
    /// the ontology during conversion.
    pub fn insert_graph(&self, data: LocalGraphData) -> Result<()> {
        for (table, batch) in data.tables {
            if batch.num_rows() == 0 {
                continue;
            }
            let mut appender = self.conn.appender(&table)?;
            appender.append_record_batch(batch)?;
            appender.flush()?;
        }
        Ok(())
    }

    /// Execute a SQL statement with positional parameters.
    ///
    /// Params are `serde_json::Value`s converted to DuckDB types:
    /// strings, integers, floats, bools, and nulls.
    pub fn execute(&self, sql: &str, params: &[serde_json::Value]) -> Result<usize> {
        let boxed = json_params_to_sql(params);
        Ok(self
            .conn
            .execute(sql, duckdb::params_from_iter(boxed.iter()))?)
    }
}

fn json_params_to_sql(params: &[serde_json::Value]) -> Vec<Box<dyn duckdb::ToSql>> {
    params
        .iter()
        .map(|v| -> Box<dyn duckdb::ToSql> {
            match v {
                serde_json::Value::String(s) => Box::new(s.clone()),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Box::new(i)
                    } else if let Some(f) = n.as_f64() {
                        Box::new(f)
                    } else {
                        Box::new(n.to_string())
                    }
                }
                serde_json::Value::Bool(b) => Box::new(*b),
                serde_json::Value::Null => Box::new(Option::<String>::None),
                other => Box::new(other.to_string()),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Test DDL covering only the tables these tests exercise.
    const TEST_DDL: &str = "\
CREATE TABLE IF NOT EXISTS gl_directory (
    id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    commit_sha VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS gl_file (
    id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    commit_sha VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    extension VARCHAR,
    language VARCHAR
);

CREATE TABLE IF NOT EXISTS gl_edge (
    source_id BIGINT NOT NULL,
    source_kind VARCHAR NOT NULL,
    relationship_kind VARCHAR NOT NULL,
    target_id BIGINT NOT NULL,
    target_kind VARCHAR NOT NULL
);";

    const TEST_TABLES: &[&str] = &["gl_directory", "gl_file", "gl_edge"];

    fn file_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("commit_sha", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("extension", DataType::Utf8, true),
            Field::new("language", DataType::Utf8, true),
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
                Arc::new(StringArray::from(vec!["abc123"; n])),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(vec![Some("rs"); n])),
                Arc::new(StringArray::from(vec![Some("Rust"); n])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn schema_creation_and_sql_roundtrip() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema(TEST_DDL).unwrap();

        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, project_id, branch, commit_sha, path, name, extension, language) \
                 VALUES (1, 42, 'main', 'abc123', 'src/lib.rs', 'lib.rs', 'rs', 'Rust')",
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
        client.initialize_schema(TEST_DDL).unwrap();

        let batch = make_file_batch(&[10, 11], &["a.rs", "b.rs"]);
        client.insert_arrow("gl_file", batch, TEST_TABLES).unwrap();

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
        client.initialize_schema(TEST_DDL).unwrap();

        let n = 5000;
        let ids: Vec<i64> = (0..n).collect();
        let names: Vec<String> = (0..n).map(|i| format!("file_{i}.rs")).collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

        let batch = make_file_batch(&ids, &name_refs);
        client.insert_arrow("gl_file", batch, TEST_TABLES).unwrap();

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
        client.initialize_schema(TEST_DDL).unwrap();

        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, project_id, branch, commit_sha, path, name) VALUES (1, 42, 'main', 'abc', 'a.rs', 'a.rs')",
                [],
            )
            .unwrap();
        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, project_id, branch, commit_sha, path, name) VALUES (2, 99, 'main', 'abc', 'b.rs', 'b.rs')",
                [],
            )
            .unwrap();

        client.delete_all_data(TEST_TABLES).unwrap();

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
        client.initialize_schema(TEST_DDL).unwrap();
        client
            .conn
            .execute(
                "INSERT INTO gl_directory (id, project_id, branch, commit_sha, path, name) VALUES (1, 1, 'main', 'abc', 'src', 'src')",
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
        client.initialize_schema(TEST_DDL).unwrap();

        let batch = make_file_batch(&[1], &["a.rs"]);
        let err = client
            .insert_arrow("evil_table", batch, TEST_TABLES)
            .unwrap_err();
        assert!(err.to_string().contains("unknown table"));
    }

    #[test]
    fn insert_empty_batch_is_noop() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema(TEST_DDL).unwrap();

        let batch = make_file_batch(&[], &[]);
        client.insert_arrow("gl_file", batch, TEST_TABLES).unwrap();

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
