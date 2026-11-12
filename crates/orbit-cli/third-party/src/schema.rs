use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ontology::Ontology;
use serde_json::{Value, json};

use crate::config::{Config, Entity, Type};
use crate::fetch::Record;

const DOMAIN: &str = "third_party";

fn table(entity: &str) -> String {
    format!("gl_tp_{}", snake(entity))
}

fn snake(name: &str) -> String {
    let mut out = String::new();
    for (index, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

fn columns(entity: &Entity) -> Vec<(&str, Type, &str)> {
    let mut columns = vec![
        ("id", Type::Int, "Orbit ID, a hash of the source and key."),
        ("traversal_path", Type::String, ""),
        ("key", Type::String, "The ID in the source system."),
        ("source", Type::String, "The source it came from."),
    ];
    columns.extend(
        entity.properties.iter().map(|(field, property)| {
            (field.as_str(), property.kind, property.description.as_str())
        }),
    );
    columns
}

fn node(name: &str, entity: &Entity) -> Value {
    let mut properties = serde_json::Map::new();
    let mut storage = Vec::new();
    for (field, kind, description) in columns(entity) {
        let required = matches!(field, "id" | "key" | "source");
        properties.insert(
            field.into(),
            json!({"type": kind.ontology(), "source": field, "nullable": !required, "hidden": field == "traversal_path", "description": description}),
        );
        let column = match kind {
            Type::String => "String",
            Type::Int => "Int64",
            Type::Timestamp | Type::UnixTime => "DateTime64(6, 'UTC')",
        };
        let column = match required {
            true => column.to_string(),
            false => format!("Nullable({column})"),
        };
        storage.push(json!({"name": field, "type": column}));
    }
    let mut defaults = vec!["id".to_string(), entity.label.clone()];
    defaults.extend(
        entity
            .search
            .iter()
            .filter(|f| **f != entity.label)
            .cloned(),
    );
    json!({
        "node_type": name, "domain": DOMAIN, "description": entity.description, "label": entity.label,
        "destination_table": table(name), "default_columns": defaults, "properties": properties, "storage": {"columns": storage},
        "redaction": {"resource_type": "third_party", "id_column": "id", "ability": "read_third_party"},
    })
}

pub fn build(config: &Config) -> Result<Arc<Ontology>> {
    let base = Ontology::load_embedded().context("loading the embedded ontology")?;
    let dir = tempfile::tempdir()?;
    let mut nodes = serde_json::Map::new();
    let mut local = Vec::new();
    let mut edges: indexmap::IndexMap<String, Vec<Value>> = indexmap::IndexMap::new();
    for (_, name, entity) in config.entities() {
        anyhow::ensure!(
            base.get_node(name).is_none(),
            "{name} is already a GitLab entity"
        );
        let path = format!("nodes/{DOMAIN}/{}.yaml", snake(name));
        write(dir.path(), &path, &node(name, entity))?;
        nodes.insert(name.into(), path.into());
        local.push(json!({"name": name}));
        for link in &entity.links {
            edges.entry(link.kind.clone()).or_default().push(json!({
                "from_node": {"type": name, "id": "id"},
                "to_node": {"type": link.to, "id": "id"},
                "description": format!("{name}.{} names the {}.", link.via, link.to),
            }));
        }
    }
    let mut edge_files = serde_json::Map::new();
    for (kind, variants) in edges {
        let path = format!("edges/{}.yaml", kind.to_ascii_lowercase());
        let mut file = json!({ "variants": variants });
        if !base.has_edge(&kind) {
            file["description"] = format!("Third-party {kind} link").into();
        }
        write(dir.path(), &path, &file)?;
        edge_files.insert(kind, path.into());
    }
    let schema = json!({
        "settings": {"local_db": {"entities": local}},
        "domains": {DOMAIN: {"description": "Data read live from third-party sources.", "nodes": nodes}},
        "edges": edge_files,
    });
    write(dir.path(), "schema.yaml", &schema)?;
    let ontology = Ontology::load_embedded_with_overlay(dir.path())
        .context("merging third-party entities into the ontology")?;
    Ok(Arc::new(ontology))
}

fn write(dir: &Path, path: &str, value: &Value) -> Result<()> {
    let full = dir.join(path);
    std::fs::create_dir_all(full.parent().expect("overlay files sit in a directory"))?;
    std::fs::write(&full, orbit_utils::yaml::to_string(value)?)?;
    Ok(())
}

fn id(source: &str, entity: &str, key: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in [source, entity, key].join("\0").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

pub fn load(
    client: &duckdb_client::DuckDbClient,
    source: &str,
    name: &str,
    entity: &Entity,
    records: &[Record],
) -> Result<()> {
    let columns = columns(entity);
    let names: Vec<&str> = columns.iter().map(|(field, ..)| *field).collect();
    let marks = vec!["?"; names.len()].join(", ");
    let insert = format!(
        "INSERT INTO {} ({}) VALUES ({marks})",
        table(name),
        names.join(", ")
    );
    for record in records {
        let own = id(source, name, &record.key);
        let row: Vec<Value> = names
            .iter()
            .map(|field| match *field {
                "id" => own.into(),
                "traversal_path" | "source" => source.into(),
                "key" => record.key.clone().into(),
                field => record.properties.get(field).cloned().unwrap_or(Value::Null),
            })
            .collect();
        client.execute(&insert, &row)?;
        for link in &entity.links {
            if let Some(target) = record.properties.get(&link.via).map(crate::fetch::text) {
                client.execute(
                    "INSERT INTO gl_edge (source_id, source_kind, relationship_kind, target_id, target_kind, traversal_path) VALUES (?, ?, ?, ?, ?, ?)",
                    &[
                        own.into(),
                        name.into(),
                        link.kind.clone().into(),
                        id(source, &link.to, &target).into(),
                        link.to.clone().into(),
                        source.into(),
                    ],
                )?;
            }
        }
    }
    Ok(())
}
