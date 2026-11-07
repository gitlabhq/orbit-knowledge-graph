use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ontology::Ontology;

use crate::config::{Config, Entity, Type};

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

/// The embedded ontology with every configured third-party entity and link merged in.
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
