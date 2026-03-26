use std::path::Path;

use duckdb::{Connection, params};
use serde_json::Value;
use tracing::info;

use duckdb::types::ValueRef;

use crate::error::{Result, SessionGraphError};
use crate::schema::{SCHEMA_DDL, SESSION_VIEW_DDL};
use crate::types::{DbStats, Edge, KindStat, Node, NodeKind};

fn value_ref_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(n) => Value::Number(n.into()),
        ValueRef::SmallInt(n) => Value::Number(n.into()),
        ValueRef::Int(n) => Value::Number(n.into()),
        ValueRef::BigInt(n) => Value::Number(n.into()),
        ValueRef::UTinyInt(n) => Value::Number(n.into()),
        ValueRef::USmallInt(n) => Value::Number(n.into()),
        ValueRef::UInt(n) => Value::Number(n.into()),
        ValueRef::UBigInt(n) => Value::Number(n.into()),
        ValueRef::Float(f) => {
            serde_json::Number::from_f64(f as f64).map_or(Value::Null, Value::Number)
        }
        ValueRef::Double(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        ValueRef::Text(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            if let Ok(v) = serde_json::from_str::<Value>(&s)
                && !v.is_string()
            {
                return v;
            }
            Value::String(s.into_owned())
        }
        _ => {
            // Timestamps, blobs, lists, etc. -- convert to string representation
            Value::String(format!("{v:?}"))
        }
    }
}

pub struct SessionDb {
    conn: Connection,
}

impl SessionDb {
    pub fn open_default() -> Result<Self> {
        let orbit_dir = dirs::home_dir()
            .ok_or_else(|| SessionGraphError::Other("cannot determine home directory".into()))?
            .join(".orbit");
        Self::open(&orbit_dir.join("sessions.duckdb"))
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.ensure_schema()?;
        info!("Opened session graph at {}", path.display());
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self { conn };
        db.ensure_schema()?;
        Ok(db)
    }

    fn ensure_schema(&self) -> Result<()> {
        self.conn
            .execute_batch(SCHEMA_DDL)
            .map_err(|e| SessionGraphError::Other(format!("schema init failed: {e}")))?;
        self.conn
            .execute_batch(SESSION_VIEW_DDL)
            .map_err(|e| SessionGraphError::Other(format!("view creation failed: {e}")))?;
        Ok(())
    }

    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }

    // -- Node CRUD --

    pub fn create_node(&self, id: &str, kind: &str, properties: &Value) -> Result<()> {
        let props_str = serde_json::to_string(properties)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO gl_node (id, kind, properties, created_at, updated_at)
             VALUES (?, ?, ?, now(), now())",
            params![id, kind, props_str],
        )?;
        self.ensure_kind_registered(kind, &[])?;
        Ok(())
    }

    pub fn update_node(&self, id: &str, properties: &Value) -> Result<()> {
        let existing = self
            .get_node(id)?
            .ok_or_else(|| SessionGraphError::NotFound(format!("node '{id}' not found")))?;

        let mut merged = match existing.properties {
            Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        if let Value::Object(new_map) = properties {
            for (k, v) in new_map {
                merged.insert(k.clone(), v.clone());
            }
        }

        let merged_str = serde_json::to_string(&Value::Object(merged))?;
        let affected = self.conn.execute(
            "UPDATE gl_node SET properties = ?, updated_at = now() WHERE id = ?",
            params![merged_str, id],
        )?;
        if affected == 0 {
            return Err(SessionGraphError::NotFound(format!(
                "node '{id}' not found"
            )));
        }
        Ok(())
    }

    pub fn delete_node(&self, id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM gl_edge WHERE source_id = ? OR target_id = ?",
            params![id, id],
        )?;
        let affected = self
            .conn
            .execute("DELETE FROM gl_node WHERE id = ?", params![id])?;
        if affected == 0 {
            return Err(SessionGraphError::NotFound(format!(
                "node '{id}' not found"
            )));
        }
        Ok(())
    }

    pub fn get_node(&self, id_prefix: &str) -> Result<Option<Node>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, kind, properties, created_at::VARCHAR, updated_at::VARCHAR
             FROM gl_node
             WHERE id LIKE ? || '%'
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![id_prefix], |row| {
            Ok(Node {
                id: row.get(0)?,
                kind: row.get(1)?,
                properties: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;
        match rows.next() {
            Some(Ok(node)) => Ok(Some(node)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    pub fn list_nodes(&self, kind: Option<&str>, limit: usize) -> Result<Vec<Node>> {
        let (sql, use_kind) = match kind {
            Some(_) => (
                "SELECT id, kind, properties, created_at::VARCHAR, updated_at::VARCHAR
                 FROM gl_node WHERE kind = ?
                 ORDER BY created_at DESC LIMIT ?",
                true,
            ),
            None => (
                "SELECT id, kind, properties, created_at::VARCHAR, updated_at::VARCHAR
                 FROM gl_node
                 ORDER BY created_at DESC LIMIT ?",
                false,
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let map_row = |row: &duckdb::Row| -> duckdb::Result<Node> {
            Ok(Node {
                id: row.get(0)?,
                kind: row.get(1)?,
                properties: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        };

        let rows = if use_kind {
            stmt.query_map(params![kind.unwrap(), limit as i64], map_row)?
        } else {
            stmt.query_map(params![limit as i64], map_row)?
        };

        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    // -- Edge CRUD --

    pub fn create_edge(
        &self,
        source_id: &str,
        target_id: &str,
        rel: &str,
        props: &Value,
    ) -> Result<()> {
        let source_kind = self.get_node_kind(source_id)?;
        let target_kind = self.get_node_kind(target_id)?;
        let props_str = serde_json::to_string(props)?;

        self.conn.execute(
            "INSERT OR REPLACE INTO gl_edge
             (source_id, source_kind, relationship_kind, target_id, target_kind, properties, created_at)
             VALUES (?, ?, ?, ?, ?, ?, now())",
            params![source_id, source_kind, rel, target_id, target_kind, props_str],
        )?;
        Ok(())
    }

    pub fn delete_edge(&self, source_id: &str, target_id: &str, rel: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM gl_edge
             WHERE source_id = ? AND target_id = ? AND relationship_kind = ?",
            params![source_id, target_id, rel],
        )?;
        Ok(())
    }

    pub fn list_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let mut stmt = self.conn.prepare(
            "SELECT source_id, source_kind, relationship_kind, target_id, target_kind,
                    properties, created_at::VARCHAR
             FROM gl_edge
             WHERE source_id = ? OR target_id = ?
             ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![node_id, node_id], |row| {
            Ok(Edge {
                source_id: row.get(0)?,
                source_kind: row.get(1)?,
                relationship_kind: row.get(2)?,
                target_id: row.get(3)?,
                target_kind: row.get(4)?,
                properties: serde_json::from_str(&row.get::<_, String>(5)?).unwrap_or_default(),
                created_at: row.get(6)?,
            })
        })?;
        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    // -- Schema Registry --

    fn ensure_kind_registered(&self, kind: &str, property_keys: &[&str]) -> Result<()> {
        let keys_json = serde_json::to_string(property_keys)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO gl_schema_registry (kind, property_keys)
             VALUES (?, ?)",
            params![kind, keys_json],
        )?;
        Ok(())
    }

    pub fn list_kinds(&self) -> Result<Vec<NodeKind>> {
        let mut stmt = self.conn.prepare(
            "SELECT kind, description, property_keys, created_at::VARCHAR
             FROM gl_schema_registry
             ORDER BY kind",
        )?;
        let rows = stmt.query_map([], |row| {
            let keys_str: String = row.get(2)?;
            let property_keys: Vec<String> = serde_json::from_str(&keys_str).unwrap_or_default();
            Ok(NodeKind {
                kind: row.get(0)?,
                description: row.get(1)?,
                property_keys,
                created_at: row.get(3)?,
            })
        })?;
        rows.map(|r| r.map_err(|e| e.into())).collect()
    }

    // -- Stats --

    pub fn stats(&self) -> Result<DbStats> {
        let node_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM gl_node", [], |row| row.get(0))?;
        let edge_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM gl_edge", [], |row| row.get(0))?;
        let kind_count: i64 =
            self.conn
                .query_row("SELECT COUNT(DISTINCT kind) FROM gl_node", [], |row| {
                    row.get(0)
                })?;

        let mut stmt = self
            .conn
            .prepare("SELECT kind, COUNT(*) AS cnt FROM gl_node GROUP BY kind ORDER BY cnt DESC")?;
        let kinds = stmt.query_map([], |row| {
            Ok(KindStat {
                kind: row.get(0)?,
                count: row.get(1)?,
            })
        })?;
        let kinds: Vec<KindStat> = kinds
            .map(|r| r.map_err(|e| e.into()))
            .collect::<Result<Vec<_>>>()?;

        Ok(DbStats {
            node_count,
            edge_count,
            kind_count,
            kinds,
        })
    }

    // -- Export / Import --

    /// Export the entire database to Parquet files in the given directory.
    pub fn export_to_parquet(&self, dir: &std::path::Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let dir_str = dir.display();
        self.conn
            .execute_batch(&format!(
                "EXPORT DATABASE '{dir_str}' (FORMAT parquet, COMPRESSION zstd);"
            ))
            .map_err(|e| SessionGraphError::Other(format!("export failed: {e}")))?;
        Ok(())
    }

    /// Import a previously exported database from Parquet files.
    /// Drops existing tables and replaces them with the imported data.
    pub fn import_from_parquet(&self, dir: &std::path::Path) -> Result<()> {
        // Drop existing tables/views to allow clean import
        self.conn
            .execute_batch(
                "DROP VIEW IF EXISTS gl_session;
                 DROP VIEW IF EXISTS gl_topic;
                 DROP TABLE IF EXISTS gl_edge;
                 DROP TABLE IF EXISTS gl_node;
                 DROP TABLE IF EXISTS gl_schema_registry;",
            )
            .map_err(|e| SessionGraphError::Other(format!("drop failed: {e}")))?;

        let dir_str = dir.display();
        self.conn
            .execute_batch(&format!("IMPORT DATABASE '{dir_str}';"))
            .map_err(|e| SessionGraphError::Other(format!("import failed: {e}")))?;

        // Recreate views after import
        self.conn
            .execute_batch(SESSION_VIEW_DDL)
            .map_err(|e| SessionGraphError::Other(format!("view recreation failed: {e}")))?;
        Ok(())
    }

    /// Export a single table to a Parquet file.
    pub fn export_table_to_parquet(&self, table: &str, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let path_str = path.display();
        // Validate table name to prevent injection
        if !["gl_node", "gl_edge", "gl_schema_registry"].contains(&table) {
            return Err(SessionGraphError::Other(format!("unknown table '{table}'")));
        }
        self.conn
            .execute_batch(&format!(
                "COPY {table} TO '{path_str}' (FORMAT parquet, COMPRESSION zstd);"
            ))
            .map_err(|e| SessionGraphError::Other(format!("table export failed: {e}")))?;
        Ok(())
    }

    // -- Helpers --

    fn get_node_kind(&self, id: &str) -> Result<String> {
        self.conn
            .query_row(
                "SELECT kind FROM gl_node WHERE id = ?",
                params![id],
                |row| row.get(0),
            )
            .map_err(|_| SessionGraphError::NotFound(format!("node '{id}' not found")))
    }

    pub fn execute_query_raw(&self, sql: &str) -> Result<Vec<Value>> {
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            let stmt_ref: &duckdb::Statement = row.as_ref();
            let column_count = stmt_ref.column_count();
            let column_names: Vec<String> = (0..column_count)
                .map(|i| {
                    stmt_ref
                        .column_name(i)
                        .map_or("?".to_string(), |v| v.to_string())
                })
                .collect();

            let mut map = serde_json::Map::new();
            for (i, name) in column_names.iter().enumerate() {
                let value_ref = row.get_ref(i)?;
                let json_val = value_ref_to_json(value_ref);
                map.insert(name.clone(), json_val);
            }
            Ok(Value::Object(map))
        })?;

        rows.map(|r| r.map_err(|e| e.into())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_and_get_node() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({"title": "Test session", "tool": "claude"}),
        )
        .unwrap();

        let node = db.get_node("s1").unwrap().unwrap();
        assert_eq!(node.id, "s1");
        assert_eq!(node.kind, "Session");
        assert_eq!(node.properties["title"], "Test session");
    }

    #[test]
    fn update_node_merges_properties() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({"title": "Original", "tool": "claude"}),
        )
        .unwrap();
        db.update_node("s1", &json!({"summary": "Updated summary"}))
            .unwrap();

        let node = db.get_node("s1").unwrap().unwrap();
        assert_eq!(node.properties["title"], "Original");
        assert_eq!(node.properties["summary"], "Updated summary");
    }

    #[test]
    fn create_and_list_edges() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({"title": "Session 1"}))
            .unwrap();
        db.create_node("s2", "Session", &json!({"title": "Session 2"}))
            .unwrap();
        db.create_edge("s1", "s2", "LINKED_TO", &json!({"reason": "related work"}))
            .unwrap();

        let edges = db.list_edges("s1").unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].relationship_kind, "LINKED_TO");
        assert_eq!(edges[0].source_kind, "Session");
        assert_eq!(edges[0].target_kind, "Session");
    }

    #[test]
    fn delete_node_cascades_edges() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("s2", "Session", &json!({})).unwrap();
        db.create_edge("s1", "s2", "LINKED_TO", &json!({})).unwrap();

        db.delete_node("s1").unwrap();
        let edges = db.list_edges("s2").unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn get_node_by_prefix() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("abc-123-def", "Session", &json!({"title": "Test"}))
            .unwrap();

        let node = db.get_node("abc-123").unwrap().unwrap();
        assert_eq!(node.id, "abc-123-def");
    }

    #[test]
    fn list_nodes_by_kind() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("t1", "Topic", &json!({"name": "test"}))
            .unwrap();
        db.create_node("s2", "Session", &json!({})).unwrap();

        let sessions = db.list_nodes(Some("Session"), 10).unwrap();
        assert_eq!(sessions.len(), 2);

        let all = db.list_nodes(None, 10).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn schema_registry_auto_populated() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("c1", "Concept", &json!({})).unwrap();

        let kinds = db.list_kinds().unwrap();
        assert!(kinds.iter().any(|k| k.kind == "Session"));
        assert!(kinds.iter().any(|k| k.kind == "Concept"));
    }

    #[test]
    fn stats_counts() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node("s1", "Session", &json!({})).unwrap();
        db.create_node("t1", "Topic", &json!({})).unwrap();
        db.create_edge("s1", "t1", "HAS_TOPIC", &json!({})).unwrap();

        let stats = db.stats().unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
        assert_eq!(stats.kind_count, 2);
    }

    #[test]
    fn session_view_works() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({
                "tool": "claude",
                "project": "/tmp/test",
                "title": "Test session",
                "model": "opus",
                "message_count": 5
            }),
        )
        .unwrap();

        let results = db
            .execute_query_raw(
                "SELECT id, tool, project, title, model, message_count FROM gl_session",
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["tool"], "claude");
        assert_eq!(results[0]["title"], "Test session");
    }
}
