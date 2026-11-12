use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ontology::Ontology;

use crate::config::{Config, Entity, Type};
use crate::fetch::Record;

pub const EDGE_TABLE: &str = "gl_edge";

pub const DOMAIN: &str = "third_party";
pub const TABLE_PREFIX: &str = "gl_tp_";

pub fn table(entity: &str) -> String {
    format!("{TABLE_PREFIX}{}", snake(entity))
}

pub(crate) fn snake(name: &str) -> String {
    let mut out = String::new();
    for (index, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

fn storage_type(kind: Type) -> &'static str {
    match kind {
        Type::String => "String",
        Type::Int => "Int64",
        Type::Bool => "Bool",
        Type::Timestamp => "DateTime64(6, 'UTC')",
    }
}

fn node_yaml(name: &str, entity: &Entity) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut columns = Vec::new();
    let mut add = |field: &str, kind: Type, description: &str, hidden: bool| {
        let required = matches!(field, "id" | "key" | "source");
        properties.insert(
            field.to_string(),
            serde_json::json!({
                "type": kind.ontology(),
                "source": field,
                "nullable": !required,
                "hidden": hidden,
                "filterable": !hidden,
                "description": description,
            }),
        );
        let storage = match required {
            true => storage_type(kind).to_string(),
            false => format!("Nullable({})", storage_type(kind)),
        };
        columns.push(serde_json::json!({"name": field, "type": storage}));
    };
    add(
        "id",
        Type::Int,
        "Orbit ID, a hash of the source and key.",
        false,
    );
    add(
        "traversal_path",
        Type::String,
        "Unused for third-party data.",
        true,
    );
    add("key", Type::String, "The ID in the source system.", false);
    add(
        "source",
        Type::String,
        "The configured source it came from.",
        false,
    );
    for (field, property) in &entity.properties {
        add(field, property.kind, &property.description, false);
    }
    let mut defaults = vec!["id".to_string(), entity.label.clone()];
    defaults.extend(
        entity
            .search
            .iter()
            .filter(|f| **f != entity.label)
            .cloned(),
    );
    serde_json::json!({
        "node_type": name,
        "domain": DOMAIN,
        "description": entity.description,
        "label": entity.label,
        "destination_table": table(name),
        "default_columns": defaults,
        "sort_key": ["id"],
        "redaction": {"resource_type": "third_party", "id_column": "id", "ability": "read_third_party"},
        "properties": properties,
        "storage": {"columns": columns},
    })
}

pub fn build(config: &Config) -> Result<Arc<Ontology>> {
    let base = Ontology::load_embedded().context("loading the embedded ontology")?;
    let dir = tempfile::tempdir()?;
    write_overlay(dir.path(), config, &base)?;
    let ontology = Ontology::load_embedded_with_overlay(dir.path())
        .context("merging third-party entities into the ontology")?;
    Ok(Arc::new(ontology))
}

fn write_overlay(dir: &Path, config: &Config, base: &Ontology) -> Result<()> {
    let entities = config.unique_entities();
    let mut nodes = serde_json::Map::new();
    let mut local = Vec::new();
    for (name, entity) in &entities {
        anyhow::ensure!(
            base.get_node(name).is_none(),
            "{name} is already a GitLab entity; pick another name"
        );
        let path = format!("nodes/{DOMAIN}/{}.yaml", snake(name));
        write(dir, &path, &node_yaml(name, entity))?;
        nodes.insert(name.clone(), path.into());
        local.push(serde_json::json!({"name": name}));
    }
    let mut edges: indexmap::IndexMap<String, Vec<serde_json::Value>> = indexmap::IndexMap::new();
    for (name, entity) in &entities {
        for link in &entity.links {
            edges
                .entry(link.kind.clone())
                .or_default()
                .push(serde_json::json!({
                    "from_node": {"type": name, "id": "id"},
                    "to_node": {"type": link.to, "id": "id"},
                    "description": format!("{name}.{} names the {}.", link.via, link.to),
                }));
        }
    }
    let mut edge_files = serde_json::Map::new();
    for (kind, variants) in edges {
        let path = format!("edges/{}.yaml", kind.to_ascii_lowercase());
        let mut file = serde_json::json!({ "variants": variants });
        if !base.has_edge(&kind) {
            file["description"] = format!("Third-party {kind} link").into();
        }
        write(dir, &path, &file)?;
        edge_files.insert(kind, path.into());
    }
    let schema = serde_json::json!({
        "settings": {"local_db": {"entities": local}},
        "domains": {DOMAIN: {"description": "Data from configured third-party sources.", "nodes": nodes}},
        "edges": edge_files,
    });
    write(dir, "schema.yaml", &schema)
}

fn write(dir: &Path, path: &str, value: &serde_json::Value) -> Result<()> {
    let full = dir.join(path);
    std::fs::create_dir_all(full.parent().expect("overlay files sit in a directory"))?;
    std::fs::write(&full, orbit_utils::yaml::to_string(value)?)?;
    Ok(())
}

pub fn id(source: &str, entity: &str, key: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in source
        .bytes()
        .chain([0])
        .chain(entity.bytes())
        .chain([0])
        .chain(key.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

/// Writes fetched records and their links into an in-memory graph for one query.
pub fn load(
    client: &duckdb_client::DuckDbClient,
    source: &str,
    name: &str,
    entity: &Entity,
    records: &[Record],
) -> Result<()> {
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    if records.is_empty() {
        return Ok(());
    }
    let table = table(name);
    let mut columns: Vec<(String, Type)> = vec![
        ("id".into(), Type::Int),
        ("traversal_path".into(), Type::String),
        ("key".into(), Type::String),
        ("source".into(), Type::String),
    ];
    columns.extend(
        entity
            .properties
            .iter()
            .map(|(field, property)| (field.clone(), property.kind)),
    );
    let value = |record: &Record, field: &str| -> Option<serde_json::Value> {
        match field {
            "id" => Some(id(source, name, &record.key).into()),
            "traversal_path" | "source" => Some(source.into()),
            "key" => Some(record.key.clone().into()),
            _ => record.properties.get(field).cloned(),
        }
    };
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (field, kind) in &columns {
        let array: ArrayRef = match kind {
            Type::Int => Arc::new(Int64Array::from_iter(
                records
                    .iter()
                    .map(|record| value(record, field).and_then(|v| v.as_i64())),
            )),
            Type::Bool => Arc::new(BooleanArray::from_iter(
                records
                    .iter()
                    .map(|record| value(record, field).and_then(|v| v.as_bool())),
            )),
            Type::String | Type::Timestamp => {
                Arc::new(StringArray::from_iter(records.iter().map(|record| {
                    value(record, field).map(|v| crate::fetch::text(&v))
                })))
            }
        };
        fields.push(Field::new(field, array.data_type().clone(), true));
        arrays.push(array);
    }
    let staging = format!("_orbit_rows_{table}");
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    client.create_temp_table(&staging, batch)?;
    let names: Vec<&str> = columns.iter().map(|(field, _)| field.as_str()).collect();
    let selected: Vec<String> = columns
        .iter()
        .map(|(field, kind)| match kind {
            Type::Timestamp => format!("CAST({field} AS TIMESTAMP)"),
            _ => field.clone(),
        })
        .collect();
    client.execute(
        &format!(
            "INSERT INTO {table} ({}) SELECT {} FROM {staging}",
            names.join(", "),
            selected.join(", ")
        ),
        &[],
    )?;
    let mut links: Vec<(i64, &str, i64, &str)> = Vec::new();
    for record in records {
        for link in &entity.links {
            let Some(target) = record.properties.get(&link.via).map(crate::fetch::text) else {
                continue;
            };
            links.push((
                id(source, name, &record.key),
                link.kind.as_str(),
                id(source, &link.to, &target),
                link.to.as_str(),
            ));
        }
    }
    if links.is_empty() {
        return Ok(());
    }
    let strings = |values: Vec<&str>| -> ArrayRef { Arc::new(StringArray::from(values)) };
    let ids = |values: Vec<i64>| -> ArrayRef { Arc::new(Int64Array::from(values)) };
    let schema = Schema::new(vec![
        Field::new("source_id", DataType::Int64, true),
        Field::new("source_kind", DataType::Utf8, true),
        Field::new("relationship_kind", DataType::Utf8, true),
        Field::new("target_id", DataType::Int64, true),
        Field::new("target_kind", DataType::Utf8, true),
        Field::new("traversal_path", DataType::Utf8, true),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema),
        vec![
            ids(links.iter().map(|link| link.0).collect()),
            strings(links.iter().map(|_| name).collect()),
            strings(links.iter().map(|link| link.1).collect()),
            ids(links.iter().map(|link| link.2).collect()),
            strings(links.iter().map(|link| link.3).collect()),
            strings(links.iter().map(|_| source).collect()),
        ],
    )?;
    let staging = format!("_orbit_edges_{table}");
    client.create_temp_table(&staging, batch)?;
    client.execute(
        &format!(
            "INSERT INTO {EDGE_TABLE} (source_id, source_kind, relationship_kind, target_id, target_kind, traversal_path) SELECT * FROM {staging}"
        ),
        &[],
    )?;
    Ok(())
}
