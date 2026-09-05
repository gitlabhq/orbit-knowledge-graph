use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};

use super::types::{
    Column, Dictionary, DictionaryCredentials, Projection, RefreshableView, Table, View,
};

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
                quote_identifier(&index.expression),
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

impl super::types::Engine {
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

    pub fn to_create_sql(&self) -> String {
        let mut header = format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {}", self.name);

        if let Some(ref to_table) = self.to_table {
            header.push_str(&format!("\nTO {to_table}"));
        } else {
            let engine = self.engine.as_ref().unwrap_or_else(|| {
                panic!(
                    "materialized view '{}' uses implicit storage but has no engine",
                    self.name
                )
            });
            header.push_str(&format!("\nENGINE = {}", engine.to_engine_sql()));
            if !self.order_by.is_empty() {
                header.push_str(&format!("\nORDER BY ({})", self.order_by.join(", ")));
            }
        }

        if self.populate {
            header.push_str("\nPOPULATE");
        }

        format!("{header}\nAS {}", self.select_query)
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

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}
