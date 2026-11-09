use std::collections::{HashMap, HashSet};

use query_data_model::{Catalog, ClickHouse, DataModelError};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

const CODE_DOMAIN: &str = "source_code";

pub struct CodeTableNames {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    edge_tables: HashMap<String, Vec<(String, String, String)>>,
    node_columns: HashMap<String, Vec<String>>,
}

impl CodeTableNames {
    pub fn from_catalog(catalog: &Catalog<ClickHouse>) -> Result<Self, DataModelError> {
        let graph = catalog.graph();
        let schema = catalog.schema()?;
        let code_entities: Vec<_> = graph
            .entities()
            .filter(|entity| entity.domain.as_deref() == Some(CODE_DOMAIN))
            .collect();
        let code_ids: HashSet<_> = code_entities.iter().map(|entity| entity.id).collect();

        let mut node_columns = HashMap::new();
        for entity in &code_entities {
            let Some(table) = catalog.table_of(entity.id) else {
                continue;
            };
            let sort_key = schema.table(table).map_or(&[][..], |table| &table.order_by);
            let carried = entity
                .properties
                .iter()
                .map(|property| graph.property(*property))
                .filter(|property| !property.mutable && catalog.column(property.id).is_some())
                .map(|property| property.name.clone())
                .filter(|name| !sort_key.contains(name))
                .collect();
            node_columns.insert(prefixed_table_name(table, *SCHEMA_VERSION), carried);
        }

        let mut edge_tables: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
        for variant in graph.variants() {
            if !code_ids.contains(&variant.source) && !code_ids.contains(&variant.target) {
                continue;
            }
            let Some(table) = catalog.edge_table(variant.id) else {
                continue;
            };
            edge_tables
                .entry(graph.relationship(variant.relationship).name.clone())
                .or_default()
                .push((
                    graph.entity(variant.source).name.clone(),
                    graph.entity(variant.target).name.clone(),
                    prefixed_table_name(table, *SCHEMA_VERSION),
                ));
        }

        let table = |name: &str| -> Result<String, DataModelError> {
            graph
                .entity_id(name)
                .and_then(|entity| catalog.table_of(entity))
                .map(|table| prefixed_table_name(table, *SCHEMA_VERSION))
                .ok_or_else(|| DataModelError::Invalid(format!("{name} has no graph table")))
        };
        Ok(Self {
            branch: table("Branch")?,
            directory: table("Directory")?,
            file: table("File")?,
            definition: table("Definition")?,
            imported_symbol: table("ImportedSymbol")?,
            edge_tables,
            node_columns,
        })
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
    ) -> Result<&str, DataModelError> {
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
                DataModelError::Invalid(format!(
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
    pub const NODE_KINDS: [&'static str; 5] = [
        "Branch",
        "Directory",
        "File",
        "Definition",
        "ImportedSymbol",
    ];

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
        let ontology = ontology::Ontology::load_embedded_with_overlay(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/seeds/overlays/variant_routes"
        ))
        .unwrap();
        let catalog = query_data_model::from_ontology::clickhouse(&ontology).unwrap();
        let names = CodeTableNames::from_catalog(&catalog).unwrap();
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
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let names = CodeTableNames::from_catalog(&catalog).expect("code tables must resolve");

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
