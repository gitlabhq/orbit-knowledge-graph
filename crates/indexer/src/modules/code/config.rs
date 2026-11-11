use std::collections::{HashMap, HashSet};

use query_data_model::spec::CODE_INDEXER_SOURCE;
use query_data_model::{Catalog, ClickHouse, DataModelError};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

const PROJECT_COLUMN: &str = "project_id";
const BRANCH_COLUMN: &str = "branch";

pub struct CodeTableNames {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    edge_tables: HashMap<String, Vec<(String, String, String)>>,
    branch_scoped: HashSet<String>,
    node_columns: HashMap<String, Vec<String>>,
}

impl CodeTableNames {
    /// The tables of the entities and variants the model's code indexer source writes.
    pub fn from_catalog(catalog: &Catalog<ClickHouse>) -> Result<Self, DataModelError> {
        let graph = catalog.graph();
        let schema = catalog.schema()?;
        let writes = catalog
            .model()
            .sources()
            .direct_source(CODE_INDEXER_SOURCE)
            .ok_or_else(|| {
                DataModelError::Invalid(format!(
                    "the model declares no {CODE_INDEXER_SOURCE} source"
                ))
            })?;

        let mut node_columns = HashMap::new();
        for entity in writes.entities.iter().map(|entity| graph.entity(*entity)) {
            let table = catalog.table_of(entity.id).ok_or_else(|| {
                DataModelError::Invalid(format!("{} has no graph table", entity.name))
            })?;
            let sort_key = &schema
                .table(table)
                .ok_or_else(|| DataModelError::Invalid(format!("{table} has no schema")))?
                .sort_key;
            let carried = entity
                .properties
                .iter()
                .map(|property| graph.property(*property))
                .filter(|property| {
                    catalog.model().immutable(property.id) && catalog.column(property.id).is_some()
                })
                .map(|property| property.name.clone())
                .filter(|name| !sort_key.contains(name))
                .collect();
            node_columns.insert(prefixed_table_name(table, *SCHEMA_VERSION), carried);
        }

        let mut edge_tables: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
        let mut branch_scoped = HashSet::new();
        for variant in writes
            .variants
            .iter()
            .map(|variant| graph.variant(*variant))
        {
            let relationship = &graph.relationship(variant.relationship).name;
            let table = catalog.edge_table(variant.id).ok_or_else(|| {
                DataModelError::Invalid(format!(
                    "{relationship} written by the code indexer has no edge table"
                ))
            })?;
            let columns = &schema
                .table(table)
                .ok_or_else(|| DataModelError::Invalid(format!("{table} has no schema")))?
                .columns;
            let has = |name: &str| columns.iter().any(|column| column.name == name);
            if has(PROJECT_COLUMN) && has(BRANCH_COLUMN) {
                branch_scoped.insert(prefixed_table_name(table, *SCHEMA_VERSION));
            }
            edge_tables.entry(relationship.clone()).or_default().push((
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
            branch_scoped,
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

    /// Whether `table` carries the project and branch columns that scope one code push.
    pub fn branch_scoped(&self, table: &str) -> bool {
        self.branch_scoped.contains(table)
    }

    /// The predicate on `s` that keeps the rows of `table` whose variant the code indexer writes.
    pub fn written_variants_sql(&self, table: &str) -> String {
        let mut variants: Vec<String> = self
            .edge_tables
            .iter()
            .flat_map(|(relationship, routes)| {
                routes.iter().filter(|(_, _, routed)| routed == table).map(
                    move |(source, target, _)| {
                        format!("('{relationship}', '{source}', '{target}')")
                    },
                )
            })
            .collect();
        variants.sort();
        format!(
            "(s.relationship_kind, s.source_kind, s.target_kind) IN ({})",
            variants.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_split_relationship_routes_by_variant() {
        let catalog = crate::testkit::variant_routes_catalog();
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
    fn edge_tables_are_the_tables_of_the_variants_the_code_indexer_writes() {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let names = CodeTableNames::from_catalog(&catalog).expect("code tables must resolve");
        let gl_code_edge = prefixed_table_name("gl_code_edge", *SCHEMA_VERSION);
        assert_eq!(names.edge_table_names(), [gl_code_edge.as_str()]);
        assert!(names.branch_scoped(&gl_code_edge));
    }
}
