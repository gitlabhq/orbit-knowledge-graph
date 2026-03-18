//! Constants for the code indexing module.

use std::time::Duration;

use ontology::{Ontology, OntologyError};

pub const CODE_LOCK_TTL: Duration = Duration::from_secs(60);

pub mod subjects {
    pub const CODE_INDEXING_TASKS: &str = "p_knowledge_graph_code_indexing_tasks";
    pub const KNOWLEDGE_GRAPH_ENABLED_NAMESPACES: &str = "knowledge_graph_enabled_namespaces";
}

/// ClickHouse table names for code graph entities, derived from the ontology.
pub struct CodeTableNames {
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    pub edge: String,
}

impl CodeTableNames {
    pub fn from_ontology(ontology: &Ontology) -> Result<Self, OntologyError> {
        Ok(Self {
            directory: ontology.table_name("Directory")?.to_owned(),
            file: ontology.table_name("File")?.to_owned(),
            definition: ontology.table_name("Definition")?.to_owned(),
            imported_symbol: ontology.table_name("ImportedSymbol")?.to_owned(),
            edge: ontology.edge_table().to_string(),
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
