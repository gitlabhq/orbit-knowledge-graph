//! Constants for the code indexing module.

use std::collections::{HashMap, HashSet};

use ontology::{Ontology, OntologyError};

use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

pub mod subjects {
    pub const CODE_INDEXING_TASKS: &str = "p_knowledge_graph_code_indexing_tasks";
    pub const KNOWLEDGE_GRAPH_ENABLED_NAMESPACES: &str = "knowledge_graph_enabled_namespaces";
}

const CODE_DOMAIN: &str = "source_code";

pub struct CodeTableNames {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    edge_tables: HashMap<String, String>,
    default_edge_table: String,
}

impl CodeTableNames {
    pub fn from_ontology(ontology: &Ontology) -> Result<Self, OntologyError> {
        let code_node_types: HashSet<&str> = ontology
            .nodes()
            .filter(|node| node.domain == CODE_DOMAIN)
            .map(|node| node.name.as_str())
            .collect();

        let mut edge_tables = HashMap::new();
        for edge in ontology.edges() {
            let involves_code_node = code_node_types.contains(edge.source_kind.as_str())
                || code_node_types.contains(edge.target_kind.as_str());
            if involves_code_node {
                edge_tables
                    .entry(edge.relationship_kind.clone())
                    .or_insert_with(|| {
                        prefixed_table_name(&edge.destination_table, *SCHEMA_VERSION)
                    });
            }
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

    pub fn default_edge_table(&self) -> &str {
        &self.default_edge_table
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_tables_only_contain_code_relevant_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let names = CodeTableNames::from_ontology(&ontology).expect("code tables must resolve");

        let edge_tables: HashSet<&str> = names.edge_table_names().into_iter().collect();
        let gl_edge = prefixed_table_name("gl_edge", *SCHEMA_VERSION);
        let gl_code_edge = prefixed_table_name("gl_code_edge", *SCHEMA_VERSION);

        assert!(
            edge_tables.contains(gl_edge.as_str()),
            "should include gl_edge"
        );
        assert!(
            edge_tables.contains(gl_code_edge.as_str()),
            "should include gl_code_edge"
        );
        assert_eq!(
            edge_tables.len(),
            2,
            "should only contain gl_edge and gl_code_edge, got {edge_tables:?}"
        );
    }
}
