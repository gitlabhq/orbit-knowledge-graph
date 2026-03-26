use ontology::Ontology;
use serde_json::Value;

use crate::db::SessionDb;
use crate::error::{Result, SessionGraphError};

pub struct QueryExecutor<'a> {
    db: &'a SessionDb,
    ontology: &'a Ontology,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(db: &'a SessionDb, ontology: &'a Ontology) -> Self {
        Self { db, ontology }
    }

    /// Compile a JSON graph query via the DSL compiler, then execute against DuckDB.
    pub fn execute(&self, json_query: &str) -> Result<Vec<Value>> {
        let compiled = compiler::compile_local(json_query, self.ontology)
            .map_err(|e| SessionGraphError::Compilation(e.to_string()))?;

        let rendered_sql = compiled.base.render();
        self.db.execute_query_raw(&rendered_sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::DataType;
    use serde_json::json;

    fn session_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["Session", "Topic"])
            .with_fields(
                "Session",
                [
                    ("id", DataType::String),
                    ("tool", DataType::String),
                    ("project", DataType::String),
                    ("title", DataType::String),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_fields(
                "Topic",
                [("id", DataType::String), ("name", DataType::String)],
            )
            .with_edges(["LINKED_TO", "HAS_TOPIC"])
    }

    #[test]
    fn execute_search_query() {
        let db = SessionDb::open_in_memory().unwrap();
        db.create_node(
            "s1",
            "Session",
            &json!({"tool": "claude", "project": "/tmp", "title": "Test"}),
        )
        .unwrap();

        let ontology = session_ontology();
        let executor = QueryExecutor::new(&db, &ontology);

        let query = r#"{
            "query_type": "search",
            "node": {"id": "s", "entity": "Session", "columns": ["id", "tool", "title"]},
            "limit": 10
        }"#;

        let results = executor.execute(query).unwrap();
        assert_eq!(results.len(), 1);
    }
}
