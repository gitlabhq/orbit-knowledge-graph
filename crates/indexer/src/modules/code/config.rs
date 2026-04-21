//! Constants for the code indexing module.

use std::time::Duration;

use ontology::{Ontology, OntologyError};

use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

pub const CODE_LOCK_TTL: Duration = Duration::from_secs(60);

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
    pub edge: String,
}

impl CodeTableNames {
    pub fn from_ontology(ontology: &Ontology) -> Result<Self, OntologyError> {
        Ok(Self {
            branch: prefixed_table_name(ontology.table_name("Branch")?, *SCHEMA_VERSION),
            directory: prefixed_table_name(ontology.table_name("Directory")?, *SCHEMA_VERSION),
            file: prefixed_table_name(ontology.table_name("File")?, *SCHEMA_VERSION),
            definition: prefixed_table_name(ontology.table_name("Definition")?, *SCHEMA_VERSION),
            imported_symbol: prefixed_table_name(
                ontology.table_name("ImportedSymbol")?,
                *SCHEMA_VERSION,
            ),
            edge: prefixed_table_name(
                ontology.edge_table_for_relationship("DEFINES"),
                *SCHEMA_VERSION,
            ),
        })
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
