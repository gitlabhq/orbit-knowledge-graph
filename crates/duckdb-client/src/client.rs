use std::path::Path;

use arrow::record_batch::RecordBatch;
use duckdb::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
use duckdb::{Connection, params};

use crate::error::{DuckDbError, Result};
use crate::schema::SCHEMA_DDL;

pub struct DuckDbClient {
    conn: Connection,
}

impl DuckDbClient {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| DuckDbError::Schema(e.to_string()))?;
        }
        let conn = Connection::open(path)?;
        let client = Self { conn };
        client.configure()?;
        Ok(client)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let client = Self { conn };
        client.configure()?;
        Ok(client)
    }

    fn configure(&self) -> Result<()> {
        self.conn
            .register_table_function::<ArrowVTab>("arrow")
            .map_err(|e| DuckDbError::Schema(format!("failed to register arrow vtab: {e}")))?;
        Ok(())
    }

    pub fn initialize_schema(&self) -> Result<()> {
        self.conn
            .execute_batch(SCHEMA_DDL)
            .map_err(|e| DuckDbError::Schema(e.to_string()))?;
        Ok(())
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    pub fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql)?;
        let arrow = stmt.query_arrow([])?;
        Ok(arrow.collect())
    }

    pub fn query_arrow_params(
        &self,
        sql: &str,
        params: &[Box<dyn duckdb::ToSql>],
    ) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql)?;
        let arrow = stmt.query_arrow(duckdb::params_from_iter(params.iter()))?;
        Ok(arrow.collect())
    }

    /// Bulk insert a RecordBatch using DuckDB's Arrow virtual table scanner.
    /// This is zero-copy: DuckDB reads directly from the Arrow memory.
    /// Large batches are chunked to stay within DuckDB's vector size limits.
    pub fn insert_arrow(&self, table: &str, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let sql = format!("INSERT INTO {table} SELECT * FROM arrow(?, ?)");
        let mut stmt = self.conn.prepare(&sql)?;

        const CHUNK_SIZE: usize = 2048;
        let total = batch.num_rows();
        let mut offset = 0;
        while offset < total {
            let len = (total - offset).min(CHUNK_SIZE);
            let chunk = batch.slice(offset, len);
            let params = arrow_recordbatch_to_query_params(chunk);
            stmt.execute(params)?;
            offset += len;
        }
        Ok(())
    }

    pub fn delete_project_data(&self, project_id: i64, branch: &str) -> Result<()> {
        for table in [
            "gl_directory",
            "gl_file",
            "gl_definition",
            "gl_imported_symbol",
        ] {
            self.conn.execute(
                &format!("DELETE FROM {table} WHERE project_id = ? AND branch = ?"),
                params![project_id, branch],
            )?;
        }
        self.conn.execute(
            "DELETE FROM gl_edge WHERE traversal_path = ?",
            params!["0/"],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_in_memory_schema_and_roundtrip() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        client
            .conn
            .execute(
                "INSERT INTO gl_file (id, traversal_path, project_id, branch, path, name, extension, language, _version) \
                 VALUES (1, '0/', 42, 'main', 'src/lib.rs', 'lib.rs', 'rs', 'Rust', 0)",
                [],
            )
            .unwrap();

        let batches = client
            .query_arrow("SELECT id, project_id, branch, name, language FROM gl_file")
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);

        let name_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "lib.rs");
    }

    #[test]
    fn test_insert_arrow_batch() {
        let client = DuckDbClient::open_in_memory().unwrap();
        client.initialize_schema().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("project_id", DataType::Int64, false),
            Field::new("branch", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("extension", DataType::Utf8, true),
            Field::new("language", DataType::Utf8, true),
            Field::new("_version", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 11])),
                Arc::new(StringArray::from(vec!["0/", "0/"])),
                Arc::new(Int64Array::from(vec![42, 42])),
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["a.rs", "b.rs"])),
                Arc::new(StringArray::from(vec!["a.rs", "b.rs"])),
                Arc::new(StringArray::from(vec![Some("rs"), Some("rs")])),
                Arc::new(StringArray::from(vec![Some("Rust"), Some("Rust")])),
                Arc::new(Int64Array::from(vec![0, 0])),
            ],
        )
        .unwrap();

        client.insert_arrow("gl_file", &batch).unwrap();

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
    fn test_delete_project_data() {
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

        client.delete_project_data(42, "main").unwrap();

        let batches = client.query_arrow("SELECT id FROM gl_file").unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[test]
    fn test_open_file_database() {
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
    }
}
