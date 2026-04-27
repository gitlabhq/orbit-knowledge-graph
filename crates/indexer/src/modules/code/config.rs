//! Constants for the code indexing module.

use std::collections::HashMap;

use ontology::{Ontology, OntologyError};

use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

pub mod subjects {
    pub const CODE_INDEXING_TASKS: &str = "p_knowledge_graph_code_indexing_tasks";
    pub const KNOWLEDGE_GRAPH_ENABLED_NAMESPACES: &str = "knowledge_graph_enabled_namespaces";
}

/// ClickHouse table names for code graph entities, derived from the ontology
/// and prefixed according to the embedded `SCHEMA_VERSION`.
pub struct CodeTableNames {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    /// Maps relationship kind to prefixed edge table name.
    /// The code indexer produces multiple edge types (IN_PROJECT, CONTAINS,
    /// DEFINES, IMPORTS, ON_BRANCH) that may route to different tables.
    pub edge_tables: HashMap<String, String>,
    /// Default edge table for relationship kinds not in the map.
    pub default_edge_table: String,
}

impl CodeTableNames {
    pub fn from_ontology(ontology: &Ontology) -> Result<Self, OntologyError> {
        let mut edge_tables = HashMap::new();
        for edge_name in ontology.edge_names() {
            let table = ontology.edge_table_for_relationship(edge_name);
            edge_tables.insert(
                edge_name.to_string(),
                prefixed_table_name(table, *SCHEMA_VERSION),
            );
        }
        let default_edge_table = prefixed_table_name(ontology.edge_table(), *SCHEMA_VERSION);

        Ok(Self {
            branch: prefixed_table_name(ontology.table_name("Branch")?, *SCHEMA_VERSION),
            directory: prefixed_table_name(ontology.table_name("Directory")?, *SCHEMA_VERSION),
            file: prefixed_table_name(ontology.table_name("File")?, *SCHEMA_VERSION),
            definition: prefixed_table_name(ontology.table_name("Definition")?, *SCHEMA_VERSION),
            imported_symbol: prefixed_table_name(
                ontology.table_name("ImportedSymbol")?,
                *SCHEMA_VERSION,
            ),
            edge_tables,
            default_edge_table,
        })
    }

    /// Resolve the prefixed table name for a given relationship kind.
    pub fn edge_table_for(&self, relationship_kind: &str) -> &str {
        self.edge_tables
            .get(relationship_kind)
            .map(|s| s.as_str())
            .unwrap_or(&self.default_edge_table)
    }

    /// All distinct edge table names (for stale data cleanup).
    pub fn edge_table_names(&self) -> Vec<&str> {
        let mut tables: Vec<&str> = self.edge_tables.values().map(|s| s.as_str()).collect();
        tables.sort();
        tables.dedup();
        tables
    }

    pub fn node_tables(&self) -> Vec<&str> {
        vec![
            &self.directory,
            &self.file,
            &self.definition,
            &self.imported_symbol,
        ]
    }
}
