use std::collections::{HashMap, HashSet};

use query_data_model::catalog::sources::Role;
use query_data_model::spec::CODE_INDEXER_SOURCE;
use query_data_model::{Catalog, ClickHouse, DataModelError};

use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::lifetime::{Lifetime, Tombstone};

const PROJECT_COLUMN: &str = "project_id";
const BRANCH_COLUMN: &str = "branch";

/// The entity and relationship names the code indexer writes by role, from its source's roles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeKinds {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    pub project: String,
    pub in_project: String,
    pub contains: String,
    pub on_branch: String,
}

impl CodeKinds {
    pub fn from_catalog(catalog: &Catalog<ClickHouse>) -> Result<Self, DataModelError> {
        let graph = catalog.graph();
        let sources = catalog.model().sources();
        let name = |role: &str| match sources.role(CODE_INDEXER_SOURCE, role) {
            Some(Role::Entity(entity)) => Ok(graph.entity(*entity).name.clone()),
            Some(Role::Relationship(relationship)) => {
                Ok(graph.relationship(*relationship).name.clone())
            }
            _ => Err(DataModelError::Invalid(format!(
                "{CODE_INDEXER_SOURCE} declares no {role} role"
            ))),
        };
        Ok(Self {
            branch: name("branch")?,
            directory: name("directory")?,
            file: name("file")?,
            definition: name("definition")?,
            imported_symbol: name("imported_symbol")?,
            project: name("project")?,
            in_project: name("in_project")?,
            contains: name("contains")?,
            on_branch: name("on_branch")?,
        })
    }
}

pub struct CodeTableNames {
    pub branch: String,
    pub directory: String,
    pub file: String,
    pub definition: String,
    pub imported_symbol: String,
    edge_tables: HashMap<String, Vec<(String, String, String)>>,
    shared_edge_table: Option<String>,
    branch_scoped: HashSet<String>,
    node_columns: HashMap<String, Vec<String>>,
    foreign_edges: HashMap<String, Vec<Tombstone>>,
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

        let foreign = Lifetime::foreign_to(catalog, CODE_INDEXER_SOURCE)?;
        let mut node_columns = HashMap::new();
        let mut foreign_edges = HashMap::new();
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
            let table = prefixed_table_name(table, *SCHEMA_VERSION);
            foreign_edges.insert(table.clone(), foreign.edges_of(entity.id));
            node_columns.insert(table, carried);
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

        let kinds = CodeKinds::from_catalog(catalog)?;
        let table = |name: &str| -> Result<String, DataModelError> {
            graph
                .entity_id(name)
                .and_then(|entity| catalog.table_of(entity))
                .map(|table| prefixed_table_name(table, *SCHEMA_VERSION))
                .ok_or_else(|| DataModelError::Invalid(format!("{name} has no graph table")))
        };
        Ok(Self {
            branch: table(&kinds.branch)?,
            directory: table(&kinds.directory)?,
            file: table(&kinds.file)?,
            definition: table(&kinds.definition)?,
            imported_symbol: table(&kinds.imported_symbol)?,
            edge_tables,
            shared_edge_table: catalog
                .shared_edge_table()
                .map(|table| prefixed_table_name(table, *SCHEMA_VERSION)),
            branch_scoped,
            node_columns,
            foreign_edges,
        })
    }

    /// Tombstones, for `ids`, the edges other writers wrote of the nodes in `table`.
    pub fn foreign_edges(&self, table: &str) -> &[Tombstone] {
        self.foreign_edges.get(table).map_or(&[], Vec::as_slice)
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

    /// The tables the code indexer's edges live in, and the shared edge table a code migration
    /// clones with the edges an earlier version routed there.
    pub fn edge_table_names(&self) -> Vec<&str> {
        let mut tables: Vec<&str> = self
            .edge_tables
            .values()
            .flatten()
            .map(|(_, _, table)| table.as_str())
            .chain(self.shared_edge_table.as_deref())
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

    /// The predicate on `s` that keeps the edges of the variants the code indexer writes.
    pub fn written_variants_sql(&self) -> String {
        let mut variants: Vec<String> = self
            .edge_tables
            .iter()
            .flat_map(|(relationship, routes)| {
                routes.iter().map(move |(source, target, _)| {
                    format!("('{relationship}', '{source}', '{target}')")
                })
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
    fn edge_tables_are_the_code_variant_tables_and_the_shared_table() {
        let catalog = Catalog::<ClickHouse>::embedded().expect("embedded catalog");
        let names = CodeTableNames::from_catalog(&catalog).expect("code tables must resolve");
        let gl_code_edge = prefixed_table_name("gl_code_edge", *SCHEMA_VERSION);
        let gl_edge = prefixed_table_name("gl_edge", *SCHEMA_VERSION);
        assert_eq!(
            names.edge_table_names(),
            [gl_code_edge.as_str(), gl_edge.as_str()]
        );
        assert!(names.branch_scoped(&gl_code_edge));
    }
}
