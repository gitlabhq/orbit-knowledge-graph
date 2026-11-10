use orbit_utils::clickhouse::quote_sql_literal;

use super::{
    Column, Dictionary, Engine, Index, MaterializedView, Projection, RefreshableView, Table,
};

const RESERVED_WORDS: &[&str] = &[
    "when", "order", "table", "database", "select", "from", "where", "group", "having", "limit",
];

#[derive(Debug)]
pub struct DictionaryCredentials {
    pub database: String,
    pub user: String,
    pub password: Option<String>,
}

impl Table {
    pub fn to_create_sql(&self, prefix: &str) -> String {
        let mut body: Vec<String> = self.columns.iter().map(Column::definition_sql).collect();
        body.extend(self.indexes.iter().map(Index::definition_sql));
        body.extend(self.projections.iter().map(Projection::definition_sql));
        let mut clauses = vec![format!(
            "CREATE TABLE IF NOT EXISTS {prefix}{} (\n{}\n) ENGINE = {}",
            self.name,
            body.join(",\n"),
            self.engine.to_sql(),
        )];
        clauses.extend(self.order_by_clauses());
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

    fn order_by_clauses(&self) -> Vec<String> {
        if self.order_by.is_empty() {
            return if self.engine.name.contains("MergeTree") {
                vec!["ORDER BY tuple()".into()]
            } else {
                Vec::new()
            };
        }
        let order_by = format!("ORDER BY ({})", self.order_by.join(", "));
        match &self.primary_key {
            Some(primary_key) if primary_key == &self.order_by => {
                vec![format!(
                    "{order_by} PRIMARY KEY ({})",
                    primary_key.join(", ")
                )]
            }
            Some(primary_key) => vec![
                order_by,
                format!("PRIMARY KEY ({})", primary_key.join(", ")),
            ],
            None => vec![order_by],
        }
    }
}

impl Column {
    fn definition_sql(&self) -> String {
        let mut fragments = vec![format!(
            "    {} {}",
            quote_identifier(&self.name),
            self.storage_type,
        )];
        if let Some(default) = &self.default {
            fragments.push(format!("DEFAULT {default}"));
        }
        if !self.codecs.is_empty() {
            fragments.push(format!("CODEC({})", self.codecs.join(", ")));
        }
        fragments.join(" ")
    }
}

impl Index {
    fn definition_sql(&self) -> String {
        let column = quote_identifier(&self.column);
        let expression = if self.lowercase {
            format!("lower({column})")
        } else {
            column
        };
        format!(
            "    INDEX {} {expression} TYPE {} GRANULARITY {}",
            quote_identifier(&self.name),
            self.kind,
            self.granularity,
        )
    }
}

impl Projection {
    fn definition_sql(&self) -> String {
        match self {
            Self::Reorder { name, order_by } => {
                format!(
                    "    PROJECTION {name} (SELECT * ORDER BY {})",
                    order_list(order_by)
                )
            }
            Self::Lightweight { name, order_by } => format!(
                "    PROJECTION {name} (SELECT _part_offset ORDER BY {})",
                order_list(order_by)
            ),
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
    #[must_use]
    pub fn replicated(mut self) -> Self {
        if self.name.ends_with("MergeTree")
            && !self.name.starts_with("Replicated")
            && !self.name.starts_with("Shared")
        {
            self.name.insert_str(0, "Replicated");
        }
        self
    }

    pub fn to_sql(&self) -> String {
        if self.arguments.is_empty() {
            self.name.clone()
        } else {
            format!("{}({})", self.name, self.arguments.join(", "))
        }
    }
}

impl MaterializedView {
    #[must_use]
    pub fn with_schema_version_prefix(mut self, prefix: &str, known_tables: &[String]) -> Self {
        self.name = format!("{prefix}{}", self.name);
        if let Some(to) = &mut self.to_table {
            *to = format!("{prefix}{to}");
        }
        for table in known_tables {
            self.select_query = self
                .select_query
                .replace(&format!("{{{table}}}"), &format!("{prefix}{table}"));
        }
        self
    }

    pub fn to_create_sql(&self) -> String {
        let mut header = format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {}", self.name);
        match (&self.to_table, &self.engine) {
            (Some(to_table), _) => header.push_str(&format!("\nTO {to_table}")),
            (None, Some(engine)) => {
                header.push_str(&format!("\nENGINE = {}", engine.to_sql()));
                if !self.order_by.is_empty() {
                    header.push_str(&format!("\nORDER BY ({})", self.order_by.join(", ")));
                }
            }
            (None, None) => {}
        }
        if self.populate {
            header.push_str("\nPOPULATE");
        }
        format!("{header}\nAS {}", self.select_query)
    }
}

impl Dictionary {
    #[must_use]
    pub fn with_schema_version_prefix(mut self, prefix: &str) -> Self {
        self.name = format!("{prefix}{}", self.name);
        self.source_table = format!("{prefix}{}", self.source_table);
        self
    }

    pub fn to_create_sql(&self, credentials: &DictionaryCredentials) -> String {
        let key_type = self
            .attributes
            .iter()
            .find(|attribute| attribute.name == self.key)
            .map_or("Int64", |attribute| attribute.storage_type.as_str());
        let values: Vec<&Column> = self
            .attributes
            .iter()
            .filter(|attribute| attribute.name != self.key)
            .collect();
        let columns: Vec<String> =
            std::iter::once(format!("    {} {key_type}", quote_identifier(&self.key)))
                .chain(values.iter().map(|attribute| {
                    format!(
                        "    {} {}",
                        quote_identifier(&attribute.name),
                        attribute.storage_type
                    )
                }))
                .collect();
        let (version, deleted) = (&self.version_column, &self.deleted_column);
        let outer: Vec<&str> = std::iter::once(self.key.as_str())
            .chain(values.iter().map(|attribute| attribute.name.as_str()))
            .collect();
        let inner: Vec<String> = std::iter::once(self.key.clone())
            .chain(
                values
                    .iter()
                    .map(|attribute| format!("argMax({0}, {version}) AS {0}", attribute.name)),
            )
            .collect();
        let source_query = format!(
            "SELECT {outer} FROM (SELECT {inner} FROM `{database}`.{table} GROUP BY {key} \
             HAVING argMax({deleted}, {version}) = false)",
            outer = outer.join(", "),
            inner = inner.join(", "),
            database = credentials.database,
            table = self.source_table,
            key = self.key,
        );
        let user = match &credentials.password {
            Some(password) => format!(
                "USER {} PASSWORD {} ",
                quote_sql_literal(&credentials.user),
                quote_sql_literal(password)
            ),
            None => format!("USER {} ", quote_sql_literal(&credentials.user)),
        };
        let layout = match self.size_in_cells {
            Some(size) => format!("{}(SIZE_IN_CELLS {size})", self.layout.to_uppercase()),
            None => format!("{}()", self.layout.to_uppercase()),
        };
        format!(
            "CREATE DICTIONARY IF NOT EXISTS {name} (\n{columns}\n)\n\
             PRIMARY KEY {key}\n\
             SOURCE(CLICKHOUSE({user}QUERY $q${source_query}$q$))\n\
             LIFETIME(MIN {min} MAX {max})\n\
             LAYOUT({layout})",
            name = self.name,
            columns = columns.join(",\n"),
            key = self.key,
            min = self.lifetime_min,
            max = self.lifetime_max,
        )
    }
}

impl RefreshableView {
    pub fn to_create_sql(&self, name: &str, rendered_select: &str) -> String {
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {name}\nREFRESH {} APPEND TO {}\nAS {rendered_select}",
            self.refresh, self.append_to
        )
    }
}

fn order_list(columns: &[String]) -> String {
    match columns {
        [column] => column.clone(),
        _ => format!("({})", columns.join(", ")),
    }
}

fn is_reserved_word(name: &str) -> bool {
    RESERVED_WORDS.contains(&name.to_lowercase().as_str())
}

fn quote_identifier(name: &str) -> String {
    let bare = name.trim_matches('`').replace('`', "``");
    if is_reserved_word(&bare) {
        format!("`{bare}`")
    } else {
        bare
    }
}
