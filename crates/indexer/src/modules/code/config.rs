use std::collections::{HashMap, HashSet};

use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use ontology::{Ontology, OntologyError};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

const CODE_DOMAIN: &str = "source_code";

pub struct CodeTableNames {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    edge_tables: HashMap<String, Vec<(String, String, String)>>,
    edge_columns: HashMap<String, HashSet<String>>,
    node_columns: HashMap<String, Vec<String>>,
}

impl CodeTableNames {
    pub fn from_ontology(ontology: &Ontology) -> Result<Self, OntologyError> {
        let code_node_types: HashSet<&str> = ontology
            .nodes()
            .filter(|node| node.domain == CODE_DOMAIN)
            .map(|node| node.name.as_str())
            .collect();

        let node_columns = ontology
            .nodes()
            .filter(|node| node.domain == CODE_DOMAIN)
            .map(|node| {
                (
                    prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
                    node.immutable_columns()
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                )
            })
            .collect();
        let mut edge_tables: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
        for edge in ontology.edges() {
            let involves_code_node = code_node_types.contains(edge.source_kind.as_str())
                || code_node_types.contains(edge.target_kind.as_str());
            if involves_code_node {
                edge_tables
                    .entry(edge.relationship_kind.clone())
                    .or_default()
                    .push((
                        edge.source_kind.clone(),
                        edge.target_kind.clone(),
                        prefixed_table_name(&edge.destination_table, *SCHEMA_VERSION),
                    ));
            }
        }

        let edge_columns = ontology
            .edge_tables()
            .into_iter()
            .filter_map(|table| {
                let config = ontology.edge_table_config(table)?;
                let columns = config
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .chain(
                        config
                            .storage
                            .denormalized_columns
                            .iter()
                            .map(|column| column.name.clone()),
                    )
                    .chain([VERSION_COLUMN.to_string(), DELETED_COLUMN.to_string()])
                    .collect();
                Some((prefixed_table_name(table, *SCHEMA_VERSION), columns))
            })
            .collect();

        Ok(Self {
            branch: prefixed_table_name(ontology.table_name("Branch")?, *SCHEMA_VERSION),
            directory: prefixed_table_name(ontology.table_name("Directory")?, *SCHEMA_VERSION),
            file: prefixed_table_name(ontology.table_name("File")?, *SCHEMA_VERSION),
            definition: prefixed_table_name(ontology.table_name("Definition")?, *SCHEMA_VERSION),
            imported_symbol: prefixed_table_name(ontology.table_name("Import")?, *SCHEMA_VERSION),
            edge_tables,
            edge_columns,
            node_columns,
        })
    }

    pub fn edge_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.edge_columns.get(table)
    }

    /// Unchanging non-key columns a tombstone for `table` must carry.
    pub fn immutable_columns(&self, table: &str) -> &[String] {
        self.node_columns.get(table).map_or(&[], Vec::as_slice)
    }

    pub fn edge_table_for(
        &self,
        relationship_kind: &str,
        source_kind: &str,
        target_kind: &str,
    ) -> Result<&str, OntologyError> {
        self.edge_tables
            .get(relationship_kind)
            .and_then(|routes| {
                routes
                    .iter()
                    .find(|(source, target, _)| {
                        source == source_kind && target == target_kind
                    })
            })
            .map(|(_, _, table)| table.as_str())
            .ok_or_else(|| {
                OntologyError::Validation(format!(
                    "code indexer emitted undeclared {source_kind} -[{relationship_kind}]-> {target_kind} variant"
                ))
            })
    }

    /// For stale data cleanup.
    pub fn edge_table_names(&self) -> Vec<&str> {
        let mut tables: Vec<&str> = self
            .edge_tables
            .values()
            .flatten()
            .map(|(_, _, table)| table.as_str())
            .collect();
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

    /// Code-origin source kinds used to clean shared edge tables.
    pub const NODE_KINDS: [&'static str; 5] =
        ["Branch", "Directory", "File", "Definition", "Import"];

    /// `NODE_KINDS` as a quoted SQL list for a `source_kind IN (...)` predicate.
    pub fn node_kinds_sql_list() -> String {
        Self::NODE_KINDS.map(|kind| format!("'{kind}'")).join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_split_relationship_routes_by_variant() {
        let ontology = Ontology::load_embedded_with_overlay(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/seeds/overlays/variant_routes"
        ))
        .unwrap();
        let names = CodeTableNames::from_ontology(&ontology).unwrap();
        assert!(
            names
                .edge_table_for("CALLS", "Definition", "Definition")
                .unwrap()
                .ends_with("gl_code_edge")
        );
        assert!(
            names
                .edge_table_for("CALLS", "File", "Definition")
                .unwrap()
                .ends_with("gl_edge")
        );
    }

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
