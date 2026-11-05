mod refreshable;

pub use query_data_model::implementations::clickhouse::schema::{
    Dictionary, DictionaryCredentials, Engine, MaterializedView, RefreshableView, Schema, Table,
};

pub use refreshable::render_refreshable_view_select;

#[derive(Debug)]
pub struct GraphSchema {
    pub physical: Schema,
    pub tables: Vec<Table>,
    pub views: Vec<MaterializedView>,
    pub dictionaries: Vec<Dictionary>,
    pub refreshable_views: Vec<RefreshableView>,
    pub unversioned_definitions: Vec<UnversionedDefinition>,
}

#[derive(Debug, Clone)]
pub struct UnversionedDefinition {
    pub entity_type: String,
    pub name: String,
    pub create_statement: String,
}

impl GraphSchema {
    /// `replicated` renders every MergeTree engine as its `Replicated*` variant for a
    /// self-managed cluster; a `Replicated` database replicates DDL only.
    pub fn new(physical: Schema, replicated: bool) -> Self {
        let engine = |engine: Engine| {
            if replicated {
                engine.replicated()
            } else {
                engine
            }
        };
        let table = |table: &Table| {
            let mut table = table.clone();
            table.engine = engine(table.engine);
            table
        };
        let view = |view: &MaterializedView| {
            let mut view = view.clone();
            view.engine = view.engine.map(engine);
            view
        };
        let all_table_names: Vec<String> = physical.table_names().map(String::from).collect();
        let unversioned_tables = physical
            .auxiliary_tables()
            .iter()
            .filter(|auxiliary| !auxiliary.versioned)
            .map(|auxiliary| UnversionedDefinition {
                entity_type: "TABLE".into(),
                name: auxiliary.table.name.clone(),
                create_statement: table(&auxiliary.table).to_create_sql(""),
            });
        let unversioned_views = physical
            .views()
            .iter()
            .filter(|definition| !definition.versioned)
            .map(|definition| {
                let prefixed = view(definition).with_schema_version_prefix("", &all_table_names);
                UnversionedDefinition {
                    entity_type: "MATERIALIZED VIEW".into(),
                    name: prefixed.name.clone(),
                    create_statement: prefixed.to_create_sql(),
                }
            });
        Self {
            tables: physical.versioned_tables().map(table).collect(),
            views: physical.views().iter().map(view).collect(),
            dictionaries: physical.dictionaries().to_vec(),
            refreshable_views: physical.refreshable_views().to_vec(),
            unversioned_definitions: unversioned_tables.chain(unversioned_views).collect(),
            physical,
        }
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables
            .iter()
            .map(|table| table.name.as_str())
            .collect()
    }

    pub fn prefixed_table_names(&self, prefix: &str) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| format!("{prefix}{}", table.name))
            .collect()
    }

    pub fn entity_base_names(&self) -> std::collections::HashSet<String> {
        let mut names = std::collections::HashSet::new();
        for table in &self.tables {
            names.insert(table.name.clone());
        }
        for view in &self.views {
            names.insert(view.name.clone());
        }
        for dictionary in &self.dictionaries {
            names.insert(dictionary.name.clone());
        }
        names
    }
}

pub fn clone_table_sql(source_table: &str, target_table: &str) -> String {
    format!("CREATE TABLE IF NOT EXISTS {target_table} AS {source_table}")
}

pub fn attach_partitions_sql(source_table: &str, target_table: &str) -> String {
    format!("ALTER TABLE {target_table} ATTACH PARTITION ALL FROM {source_table}")
}

pub fn drop_entity_sql(entity_name: &str, entity_type: &str) -> String {
    format!("DROP {entity_type} IF EXISTS {entity_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge_tree_engines(schema: &GraphSchema) -> Vec<String> {
        schema
            .tables
            .iter()
            .map(|table| table.engine.name.clone())
            .chain(
                schema
                    .views
                    .iter()
                    .filter_map(|view| view.engine.as_ref().map(|engine| engine.name.clone())),
            )
            .filter(|name| name.ends_with("MergeTree"))
            .collect()
    }

    #[test]
    fn replicated_schema_renders_replicated_engines_everywhere() {
        let physical =
            Schema::from_model(&query_data_model::Packages::embedded().load().unwrap()).unwrap();
        let plain = GraphSchema::new(physical.clone(), false);
        let replicated = GraphSchema::new(physical, true);

        assert!(!merge_tree_engines(&plain).is_empty());
        assert!(
            merge_tree_engines(&plain)
                .iter()
                .all(|name| !name.starts_with("Replicated"))
        );
        assert!(
            merge_tree_engines(&replicated)
                .iter()
                .all(|name| name.starts_with("Replicated"))
        );
        for table in &replicated.tables {
            assert!(table.to_create_sql("v1_").contains("ENGINE = Replicated"));
        }
        for definition in &replicated.unversioned_definitions {
            assert!(
                !definition.create_statement.contains("ENGINE = ")
                    || !definition.create_statement.contains("MergeTree")
                    || definition.create_statement.contains("ENGINE = Replicated"),
                "{}",
                definition.name
            );
        }
    }
}
