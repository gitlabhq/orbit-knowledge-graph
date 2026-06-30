use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

use super::format::{Row, Seed, SeedSettings};
use crate::context::TestContext;
use crate::t;

/// Fixed replication timestamp injected when a seeded row omits
/// `_siphon_replicated_at`. Must stay below the default test watermark
/// (2024-01-21) or the indexer window would exclude the row.
pub const DEFAULT_REPLICATED_AT: &str = "2024-01-20 12:00:00";

const PREFIXED_AUX_TABLES: [&str; 3] = [
    "checkpoint",
    "namespace_deletion_schedule",
    "code_indexing_checkpoint",
];

pub type TableColumns = HashMap<String, HashSet<String>>;

pub async fn fetch_table_columns(ctx: &TestContext) -> TableColumns {
    let batches = ctx
        .query("SELECT table, name FROM system.columns WHERE database = currentDatabase()")
        .await;
    let mut columns: TableColumns = HashMap::new();
    for batch in &batches {
        let tables =
            ArrowUtils::get_column_by_name::<StringArray>(batch, "table").expect("table column");
        let names =
            ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
        for i in 0..batch.num_rows() {
            columns
                .entry(tables.value(i).to_string())
                .or_default()
                .insert(names.value(i).to_string());
        }
    }
    columns
}

pub async fn apply_seed(
    ctx: &TestContext,
    seed: &Seed,
    settings: &SeedSettings,
    columns: &TableColumns,
    location: &str,
) {
    for (table, rows) in seed {
        for (physical_table, physical_rows) in expand_table(table, rows, location) {
            insert_rows(
                ctx,
                &physical_table,
                physical_rows,
                settings,
                columns,
                location,
            )
            .await;
        }
    }
}

fn expand_table(table: &str, rows: &[Row], location: &str) -> Vec<(String, Vec<Row>)> {
    match table {
        "namespaces" => expand_pseudo_rows(rows, location, expand_namespace),
        "projects" => expand_pseudo_rows(rows, location, expand_project),
        _ => vec![(prefix_graph_table(table), rows.to_vec())],
    }
}

pub(crate) fn prefix_graph_table(table: &str) -> String {
    if table.starts_with("gl_") || PREFIXED_AUX_TABLES.contains(&table) {
        t(table)
    } else {
        table.to_string()
    }
}

fn expand_pseudo_rows(
    rows: &[Row],
    location: &str,
    expand: fn(&Row, &str) -> Vec<(String, Row)>,
) -> Vec<(String, Vec<Row>)> {
    let mut by_table: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for row in rows {
        for (table, expanded) in expand(row, location) {
            by_table.entry(table).or_default().push(expanded);
        }
    }
    by_table.into_iter().collect()
}

fn expand_namespace(row: &Row, location: &str) -> Vec<(String, Row)> {
    let mut fields = PseudoFields::new("namespaces", row, location);
    let id = fields.required("id");
    let traversal_path = fields.required_str("traversal_path");
    let parent_id = fields.optional("parent_id");
    let visibility_level = fields.optional_or("visibility_level", 0.into());
    let id_number = yaml_i64(&id, "namespaces.id", location);
    let slug = fields.optional_str_or("slug", format!("namespace-{id_number}"));
    fields.finish();

    vec![
        (
            "siphon_namespaces".to_string(),
            row_of([
                ("id", id.clone()),
                ("name", slug.clone().into()),
                ("path", slug.into()),
                ("type", "Group".into()),
                ("visibility_level", visibility_level),
                ("parent_id", parent_id),
                ("owner_id", 1.into()),
                ("traversal_ids", traversal_ids(&traversal_path)),
                ("created_at", "2023-01-01".into()),
                ("updated_at", "2024-01-15".into()),
            ]),
        ),
        (
            "siphon_namespace_details".to_string(),
            row_of([
                ("namespace_id", id.clone()),
                ("traversal_path", traversal_path.clone().into()),
            ]),
        ),
        (
            "namespace_traversal_paths".to_string(),
            row_of([("id", id), ("traversal_path", traversal_path.into())]),
        ),
    ]
}

fn expand_project(row: &Row, location: &str) -> Vec<(String, Row)> {
    let mut fields = PseudoFields::new("projects", row, location);
    let id = fields.required("id");
    let namespace_id = fields.required("namespace_id");
    let traversal_path = fields.required_str("traversal_path");
    let creator_id = fields.optional_or("creator_id", 1.into());
    let visibility_level = fields.optional_or("visibility_level", 0.into());
    let id_number = yaml_i64(&id, "projects.id", location);
    let slug = fields.optional_str_or("slug", format!("project-{id_number}"));
    fields.finish();

    vec![
        (
            "siphon_projects".to_string(),
            row_of([
                ("id", id.clone()),
                ("name", slug.clone().into()),
                ("visibility_level", visibility_level),
                ("path", slug.into()),
                ("namespace_id", namespace_id),
                ("creator_id", creator_id),
                ("created_at", "2023-01-01".into()),
                ("updated_at", "2024-01-15".into()),
                ("archived", false.into()),
                ("star_count", 0.into()),
                ("last_activity_at", "2024-01-15".into()),
            ]),
        ),
        (
            "project_namespace_traversal_paths".to_string(),
            row_of([("id", id), ("traversal_path", traversal_path.into())]),
        ),
    ]
}

fn row_of<const N: usize>(entries: [(&str, serde_yaml::Value); N]) -> Row {
    entries
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

fn traversal_ids(traversal_path: &str) -> serde_yaml::Value {
    serde_yaml::Value::Sequence(
        traversal_path
            .trim_end_matches('/')
            .split('/')
            .filter_map(|s| s.parse::<i64>().ok())
            .map(serde_yaml::Value::from)
            .collect(),
    )
}

fn yaml_i64(value: &serde_yaml::Value, field: &str, location: &str) -> i64 {
    value
        .as_i64()
        .unwrap_or_else(|| panic!("{location}: {field} must be an integer, got {value:?}"))
}

/// Field extractor for pseudo-table rows that rejects unknown keys, so a
/// typo'd field fails loudly instead of silently using the default.
struct PseudoFields<'a> {
    pseudo_table: &'a str,
    row: &'a Row,
    location: &'a str,
    consumed: HashSet<&'a str>,
}

impl<'a> PseudoFields<'a> {
    fn new(pseudo_table: &'a str, row: &'a Row, location: &'a str) -> Self {
        Self {
            pseudo_table,
            row,
            location,
            consumed: HashSet::new(),
        }
    }

    fn required(&mut self, key: &'a str) -> serde_yaml::Value {
        self.consumed.insert(key);
        self.row.get(key).cloned().unwrap_or_else(|| {
            panic!(
                "{}: pseudo-table '{}' row is missing required field '{key}'",
                self.location, self.pseudo_table
            )
        })
    }

    fn required_str(&mut self, key: &'a str) -> String {
        let value = self.required(key);
        value.as_str().map(str::to_string).unwrap_or_else(|| {
            panic!(
                "{}: pseudo-table '{}' field '{key}' must be a string, got {value:?}",
                self.location, self.pseudo_table
            )
        })
    }

    fn optional(&mut self, key: &'a str) -> serde_yaml::Value {
        self.consumed.insert(key);
        self.row
            .get(key)
            .cloned()
            .unwrap_or(serde_yaml::Value::Null)
    }

    fn optional_or(&mut self, key: &'a str, default: serde_yaml::Value) -> serde_yaml::Value {
        self.consumed.insert(key);
        self.row.get(key).cloned().unwrap_or(default)
    }

    fn optional_str_or(&mut self, key: &'a str, default: String) -> String {
        let value = self.optional_or(key, default.clone().into());
        value.as_str().map(str::to_string).unwrap_or_else(|| {
            panic!(
                "{}: pseudo-table '{}' field '{key}' must be a string, got {value:?}",
                self.location, self.pseudo_table
            )
        })
    }

    fn finish(self) {
        for key in self.row.keys() {
            assert!(
                self.consumed.contains(key.as_str()),
                "{}: pseudo-table '{}' does not support field '{key}'",
                self.location,
                self.pseudo_table
            );
        }
    }
}

async fn insert_rows(
    ctx: &TestContext,
    table: &str,
    rows: Vec<Row>,
    settings: &SeedSettings,
    columns: &TableColumns,
    location: &str,
) {
    let table_columns = columns.get(table).unwrap_or_else(|| {
        panic!("{location}: table '{table}' does not exist in the test database")
    });

    let mut groups: BTreeMap<Vec<String>, Vec<Row>> = BTreeMap::new();
    for mut row in rows {
        for column in row.keys() {
            assert!(
                table_columns.contains(column),
                "{location}: table '{table}' has no column '{column}'"
            );
        }
        if table_columns.contains("_siphon_replicated_at")
            && !row.contains_key("_siphon_replicated_at")
        {
            row.insert(
                "_siphon_replicated_at".to_string(),
                DEFAULT_REPLICATED_AT.into(),
            );
        }
        groups
            .entry(row.keys().cloned().collect())
            .or_default()
            .push(row);
    }

    for (column_names, group_rows) in groups {
        let quoted_columns: Vec<String> = column_names.iter().map(|c| format!("`{c}`")).collect();
        if settings.is_empty() {
            let values: Vec<String> = group_rows
                .iter()
                .map(|row| {
                    let rendered: Vec<String> = column_names
                        .iter()
                        .map(|c| render_value(&row[c], location))
                        .collect();
                    format!("({})", rendered.join(", "))
                })
                .collect();
            ctx.execute(&format!(
                "INSERT INTO {table} ({}) VALUES {}",
                quoted_columns.join(", "),
                values.join(", ")
            ))
            .await;
        } else {
            insert_json_rows(
                ctx,
                table,
                &quoted_columns,
                &column_names,
                &group_rows,
                settings,
                location,
            )
            .await;
        }
    }
}

/// `seed_settings` inserts go through JSONEachRow because some settings only
/// take effect there: a textual VALUES insert saturates an out-of-range
/// `Date32` to the type boundary even with `date_time_overflow_behavior` set,
/// while JSONEachRow honours it and keeps the raw day count.
async fn insert_json_rows(
    ctx: &TestContext,
    table: &str,
    quoted_columns: &[String],
    column_names: &[String],
    rows: &[Row],
    settings: &SeedSettings,
    location: &str,
) {
    let rendered_settings: Vec<String> = settings
        .iter()
        .map(|(name, value)| format!("{name}={}", render_setting_value(value, name, location)))
        .collect();
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let object: serde_json::Map<String, serde_json::Value> = column_names
                .iter()
                .map(|c| (c.clone(), json_value(&row[c], location)))
                .collect();
            serde_json::Value::Object(object).to_string()
        })
        .collect();
    let sql = format!(
        "INSERT INTO {table} ({}) SETTINGS {} FORMAT JSONEachRow\n{}",
        quoted_columns.join(", "),
        rendered_settings.join(", "),
        lines.join("\n")
    );

    // A FORMAT-bearing insert consumes the rest of the request body as data,
    // so it must go over the raw HTTP interface rather than the typed client.
    let client = reqwest::Client::new();
    let url = format!("{}/?database={}", ctx.config.url, ctx.config.database);
    let response = client
        .post(&url)
        .basic_auth(&ctx.config.username, ctx.config.password.as_deref())
        .body(sql)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{location}: seed insert request failed: {e}"));
    let status = response.status();
    assert!(
        status.is_success(),
        "{location}: seed insert into '{table}' failed: {status} {}",
        response.text().await.unwrap_or_default()
    );
}

fn render_setting_value(value: &serde_yaml::Value, name: &str, location: &str) -> String {
    match value {
        serde_yaml::Value::Bool(b) => b.to_string(),
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) => quote(s),
        _ => panic!("{location}: seed_settings '{name}' must be a scalar, got {value:?}"),
    }
}

fn json_value(value: &serde_yaml::Value, location: &str) -> serde_json::Value {
    match value {
        serde_yaml::Value::Mapping(_) | serde_yaml::Value::Tagged(_) => panic!(
            "{location}: the {{ sql: ... }} seed-value escape is not supported in steps \
             with seed_settings (JSONEachRow inserts), got {value:?}"
        ),
        _ => serde_json::to_value(value)
            .unwrap_or_else(|e| panic!("{location}: seed value is not valid JSON: {e}")),
    }
}

fn render_value(value: &serde_yaml::Value, location: &str) -> String {
    match value {
        serde_yaml::Value::Null => "NULL".to_string(),
        serde_yaml::Value::Bool(b) => b.to_string(),
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) => quote(s),
        serde_yaml::Value::Sequence(items) => {
            let rendered: Vec<String> = items.iter().map(|v| render_value(v, location)).collect();
            format!("[{}]", rendered.join(", "))
        }
        serde_yaml::Value::Mapping(m) => {
            if let (1, Some(serde_yaml::Value::String(sql))) =
                (m.len(), m.get(serde_yaml::Value::from("sql")))
            {
                return sql.clone();
            }
            panic!("{location}: seed value mappings must be {{ sql: \"...\" }}, got {value:?}")
        }
        serde_yaml::Value::Tagged(_) => {
            panic!("{location}: tagged YAML values are not supported in seeds")
        }
    }
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
