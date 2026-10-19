pub(crate) mod translate;

use ontology::Ontology;
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use orbit_utils::clickhouse::quote_sql_literal;
use query_data_model::DataModelError;

pub use translate::render_refreshable_view_select;

#[derive(Debug)]
pub struct GraphSchema {
    pub storage: query_data_model::Relational<
        query_data_model::implementations::clickhouse::layout::ClickHouse,
    >,
    pub tables: Vec<Table>,
    pub views: Vec<View>,
    pub dictionaries: Vec<Dictionary>,
    pub refreshable_views: Vec<RefreshableView>,
    pub unversioned_definitions: Vec<UnversionedDefinition>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub projections: Vec<Projection>,
    pub engine: Engine,
    pub partition_by: Vec<String>,
    pub order_by: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub settings: Vec<(String, String)>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub default: Option<String>,
    pub codec: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub column: String,
    pub lowercase: bool,
    pub index_type: String,
    pub granularity: u32,
}

impl Index {
    pub fn expression(&self) -> String {
        let column = quote_identifier(&self.column);
        if self.lowercase {
            format!("lower({column})")
        } else {
            column
        }
    }
}

#[derive(Debug, Clone)]
pub enum Projection {
    Reorder {
        name: String,
        order_by: Vec<String>,
    },
    Lightweight {
        name: String,
        order_by: Vec<String>,
    },
    Aggregate {
        name: String,
        select: Vec<String>,
        group_by: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Engine {
    pub name: String,
    pub args: Vec<String>,
}

impl Engine {
    pub fn replacing_merge_tree() -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![VERSION_COLUMN.into(), DELETED_COLUMN.into()],
        }
    }

    pub fn replacing_merge_tree_version_only() -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![VERSION_COLUMN.into()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct View {
    pub name: String,
    pub to_table: Option<String>,
    pub select_query: String,
    pub engine: Option<Engine>,
    pub order_by: Vec<String>,
    pub populate: bool,
    pub versioned: bool,
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    pub name: String,
    pub source_table: String,
    pub key: String,
    pub attributes: Vec<Column>,
    pub layout_kind: String,
    pub layout_size_in_cells: Option<u64>,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
}

impl Dictionary {
    pub fn with_schema_version_prefix(mut self, prefix: &str) -> Self {
        self.name = format!("{prefix}{}", self.name);
        self.source_table = format!("{prefix}{}", self.source_table);
        self
    }
}

#[derive(Debug)]
pub struct DictionaryCredentials {
    pub database: String,
    pub user: String,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RefreshableView {
    pub name: String,
    pub select_query: String,
    pub append_to: String,
    pub refresh: String,
    pub versioned: bool,
}

#[derive(Debug, Clone)]
pub struct UnversionedDefinition {
    pub entity_type: String,
    pub name: String,
    pub create_statement: String,
}

impl GraphSchema {
    pub fn from_ontology(ontology: &Ontology) -> Result<Self, DataModelError> {
        Self::from_ontology_replicated(ontology, false)
    }

    /// `replicated` renders every MergeTree engine as its `Replicated*` variant for a
    /// self-managed cluster; a `Replicated` database replicates DDL only.
    pub fn from_ontology_replicated(
        ontology: &Ontology,
        replicated: bool,
    ) -> Result<Self, DataModelError> {
        let storage = query_data_model::Relational::<
            query_data_model::implementations::clickhouse::layout::ClickHouse,
        >::derive(ontology)?;
        let mut tables = translate::build_all_tables(&storage);
        let mut views = translate::build_views(&storage)?;
        if replicated {
            for table in &mut tables {
                table.engine = table.engine.clone().replicated();
            }
            for view in &mut views {
                view.engine = view.engine.take().map(Engine::replicated);
            }
        }
        let all_table_names = storage.table_names().map(String::from).collect::<Vec<_>>();

        Ok(Self {
            views,
            dictionaries: translate::build_dictionaries(&storage),
            refreshable_views: translate::build_refreshable_views(&storage),
            unversioned_definitions: translate::build_unversioned_definitions(
                &storage,
                &all_table_names,
                replicated,
            )?,
            tables,
            storage,
        })
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

const RESERVED_WORDS: &[&str] = &[
    "when", "order", "table", "database", "select", "from", "where", "group", "having", "limit",
];

impl Table {
    pub fn to_create_sql(&self, table_name_prefix: &str) -> String {
        let prefixed_name = format!("{table_name_prefix}{}", self.name);
        let mut clauses = Vec::new();

        let mut body: Vec<String> = self
            .columns
            .iter()
            .map(|column| column.to_definition_sql())
            .collect();

        for index in &self.indexes {
            body.push(format!(
                "    INDEX {} {} TYPE {} GRANULARITY {}",
                quote_identifier(&index.name),
                index.expression(),
                index.index_type,
                index.granularity,
            ));
        }

        for projection in &self.projections {
            body.push(projection.to_definition_sql());
        }

        clauses.push(format!(
            "CREATE TABLE IF NOT EXISTS {prefixed_name} (\n{}\n) ENGINE = {}",
            body.join(",\n"),
            self.engine.to_engine_sql(),
        ));

        if !self.partition_by.is_empty() {
            clauses.push(format!("PARTITION BY ({})", self.partition_by.join(", ")));
        }

        if self.order_by.is_empty() {
            if self.engine.name.contains("MergeTree") {
                clauses.push("ORDER BY tuple()".into());
            }
        } else {
            let order_by = format!("ORDER BY ({})", self.order_by.join(", "));
            match &self.primary_key {
                Some(primary_key) if primary_key == &self.order_by => {
                    clauses.push(format!(
                        "{order_by} PRIMARY KEY ({})",
                        primary_key.join(", ")
                    ));
                }
                Some(primary_key) => {
                    clauses.push(order_by);
                    clauses.push(format!("PRIMARY KEY ({})", primary_key.join(", ")));
                }
                None => clauses.push(order_by),
            }
        }

        if let Some(ttl) = &self.ttl {
            clauses.push(format!("TTL {ttl}"));
        }

        if !self.settings.is_empty() {
            let rendered: Vec<String> = self
                .settings
                .iter()
                .map(|(key, value)| format!("{key} = {value}"))
                .collect();
            clauses.push(format!("SETTINGS {}", rendered.join(", ")));
        }

        clauses.join("\n")
    }
}

impl Column {
    pub(crate) fn to_definition_sql(&self) -> String {
        let mut fragments = vec![format!(
            "    {} {}",
            quote_identifier(&self.name),
            self.column_type,
        )];
        if let Some(default) = &self.default {
            fragments.push(format!("DEFAULT {default}"));
        }
        if let Some(codecs) = &self.codec {
            fragments.push(format!("CODEC({})", codecs.join(", ")));
        }
        fragments.join(" ")
    }
}

impl Projection {
    fn to_definition_sql(&self) -> String {
        match self {
            Self::Reorder { name, order_by } | Self::Lightweight { name, order_by } => {
                let select_expr = if matches!(self, Self::Reorder { .. }) {
                    "*"
                } else {
                    "_part_offset"
                };
                let order = format_order_by(order_by);
                format!("    PROJECTION {name} (SELECT {select_expr} ORDER BY {order})")
            }
            Self::Aggregate {
                name,
                select,
                group_by,
            } => format!(
                "    PROJECTION {name} (\n      SELECT {}\n      GROUP BY {}\n    )",
                select.join(", "),
                group_by.join(", ")
            ),
        }
    }
}

impl Engine {
    pub fn replicated(mut self) -> Self {
        if self.name.ends_with("MergeTree")
            && !self.name.starts_with("Replicated")
            && !self.name.starts_with("Shared")
        {
            self.name.insert_str(0, "Replicated");
        }
        self
    }

    pub(crate) fn to_engine_sql(&self) -> String {
        if self.args.is_empty() {
            self.name.clone()
        } else {
            format!("{}({})", self.name, self.args.join(", "))
        }
    }
}

impl View {
    pub fn with_schema_version_prefix(mut self, prefix: &str, known_tables: &[String]) -> Self {
        self.name = format!("{prefix}{}", self.name);
        if let Some(ref mut to) = self.to_table {
            *to = format!("{prefix}{to}");
        }
        for table in known_tables {
            let placeholder = format!("{{{table}}}");
            let replacement = format!("{prefix}{table}");
            self.select_query = self.select_query.replace(&placeholder, &replacement);
        }
        self
    }

    pub fn to_create_sql(&self) -> Result<String, DataModelError> {
        let mut header = format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {}", self.name);

        if let Some(ref to_table) = self.to_table {
            header.push_str(&format!("\nTO {to_table}"));
        } else {
            let engine = self.engine.as_ref().ok_or_else(|| {
                DataModelError::Invalid(format!(
                    "materialized view '{}' uses implicit storage but has no engine",
                    self.name
                ))
            })?;
            header.push_str(&format!("\nENGINE = {}", engine.to_engine_sql()));
            if !self.order_by.is_empty() {
                header.push_str(&format!("\nORDER BY ({})", self.order_by.join(", ")));
            }
        }

        if self.populate {
            header.push_str("\nPOPULATE");
        }

        Ok(format!("{header}\nAS {}", self.select_query))
    }
}

impl Dictionary {
    pub fn to_create_sql(&self, credentials: &DictionaryCredentials) -> String {
        let key_type = self
            .attributes
            .iter()
            .find(|attribute| attribute.name == self.key)
            .map(|attribute| attribute.column_type.as_str())
            .unwrap_or("Int64");

        let mut column_definitions: Vec<String> =
            vec![format!("    {} {}", quote_identifier(&self.key), key_type)];
        for attribute in &self.attributes {
            if attribute.name == self.key {
                continue;
            }
            column_definitions.push(format!(
                "    {} {}",
                quote_identifier(&attribute.name),
                attribute.column_type,
            ));
        }

        let non_key_names: Vec<&str> = self
            .attributes
            .iter()
            .map(|attribute| attribute.name.as_str())
            .filter(|name| *name != self.key)
            .collect();

        let dedup_selects: Vec<String> = non_key_names
            .iter()
            .map(|name| format!("argMax({name}, {VERSION_COLUMN}) AS {name}"))
            .collect();
        let outer_selects: Vec<String> = std::iter::once(self.key.clone())
            .chain(non_key_names.iter().map(|name| (*name).to_string()))
            .collect();
        let inner_selects: Vec<String> = std::iter::once(self.key.clone())
            .chain(dedup_selects)
            .collect();

        let source_query = format!(
            "SELECT {outer} FROM (SELECT {inner} FROM `{db}`.{table} GROUP BY {key} \
             HAVING argMax({DELETED_COLUMN}, {VERSION_COLUMN}) = false)",
            outer = outer_selects.join(", "),
            inner = inner_selects.join(", "),
            db = credentials.database,
            table = self.source_table,
            key = self.key,
        );

        let credentials_clause = match &credentials.password {
            Some(password) => format!(
                "USER {} PASSWORD {} ",
                quote_sql_literal(&credentials.user),
                quote_sql_literal(password)
            ),
            None => format!("USER {} ", quote_sql_literal(&credentials.user)),
        };

        let layout = match self.layout_size_in_cells {
            Some(size) => format!("{}(SIZE_IN_CELLS {size})", self.layout_kind.to_uppercase()),
            None => format!("{}()", self.layout_kind.to_uppercase()),
        };

        format!(
            "CREATE DICTIONARY IF NOT EXISTS {name} (\n{columns}\n)\n\
             PRIMARY KEY {key}\n\
             SOURCE(CLICKHOUSE({credentials_clause}QUERY $q${source_query}$q$))\n\
             LIFETIME(MIN {min} MAX {max})\n\
             LAYOUT({layout})",
            name = self.name,
            columns = column_definitions.join(",\n"),
            key = self.key,
            min = self.lifetime_min,
            max = self.lifetime_max,
        )
    }
}

impl RefreshableView {
    pub fn to_create_sql(&self, rendered_select: &str) -> String {
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {}\nREFRESH {} APPEND TO {}\nAS {}",
            self.name, self.refresh, self.append_to, rendered_select
        )
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

fn format_order_by(columns: &[String]) -> String {
    if columns.len() == 1 {
        columns[0].clone()
    } else {
        format!("({})", columns.join(", "))
    }
}

fn quote_identifier(name: &str) -> String {
    let bare = name.trim_matches('`').replace('`', "``");
    if RESERVED_WORDS.contains(&bare.to_lowercase().as_str()) {
        format!("`{bare}`")
    } else {
        bare
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn materialized_view_requires_a_destination_or_engine() {
        let view = super::View {
            name: "invalid_view".into(),
            to_table: None,
            select_query: "SELECT 1".into(),
            engine: None,
            order_by: vec![],
            populate: false,
            versioned: true,
        };
        let error = view.to_create_sql().unwrap_err();
        assert!(error.to_string().contains("invalid_view"));
        assert!(error.to_string().contains("no engine"));
    }

    #[test]
    fn joined_view_generation_rejects_missing_target_tables() {
        let ontology = ontology::Ontology::load_embedded_with_overlay(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/seeds/overlays/denorm_approved"
        ))
        .unwrap();
        let mut schema = super::GraphSchema::from_ontology(&ontology).unwrap();
        let join_table = schema.storage.joins().first().unwrap().table.clone();
        schema
            .storage
            .tables
            .retain(|table| table.name != join_table);
        assert!(
            matches!(super::translate::build_views(&schema.storage), Err(DataModelError::UnknownReference { kind: "join table", name }) if name == join_table)
        );
    }
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
    fn replicated_prefixes_merge_tree_engines_only() {
        let engine = Engine::replacing_merge_tree().replicated();
        assert_eq!(engine.name, "ReplicatedReplacingMergeTree");
        assert_eq!(engine.replicated().name, "ReplicatedReplacingMergeTree");
        for name in ["Null", "SharedMergeTree", "Dictionary"] {
            let engine = Engine {
                name: name.into(),
                args: vec![],
            };
            assert_eq!(engine.replicated().name, name);
        }
    }

    #[test]
    fn replicated_schema_renders_replicated_engines_everywhere() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let plain = GraphSchema::from_ontology(&ontology).unwrap();
        let replicated = GraphSchema::from_ontology_replicated(&ontology, true).unwrap();

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
