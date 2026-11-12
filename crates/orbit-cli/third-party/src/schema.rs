use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use ontology::Ontology;
use serde_json::Value;

use crate::config::Entity;
use crate::fetch::Record;

const OVERLAY: &[(&str, &str)] = &[
    ("schema.yaml", include_str!("../ontology/schema.yaml")),
    (
        "nodes/third_party/slack_channel.yaml",
        include_str!("../ontology/nodes/third_party/slack_channel.yaml"),
    ),
    (
        "nodes/third_party/slack_user.yaml",
        include_str!("../ontology/nodes/third_party/slack_user.yaml"),
    ),
    (
        "nodes/third_party/slack_message.yaml",
        include_str!("../ontology/nodes/third_party/slack_message.yaml"),
    ),
    (
        "nodes/third_party/grafana_folder.yaml",
        include_str!("../ontology/nodes/third_party/grafana_folder.yaml"),
    ),
    (
        "nodes/third_party/grafana_dashboard.yaml",
        include_str!("../ontology/nodes/third_party/grafana_dashboard.yaml"),
    ),
    (
        "nodes/third_party/grafana_alert.yaml",
        include_str!("../ontology/nodes/third_party/grafana_alert.yaml"),
    ),
    (
        "edges/in_channel.yaml",
        include_str!("../ontology/edges/in_channel.yaml"),
    ),
    (
        "edges/authored_by.yaml",
        include_str!("../ontology/edges/authored_by.yaml"),
    ),
    (
        "edges/in_folder.yaml",
        include_str!("../ontology/edges/in_folder.yaml"),
    ),
    (
        "edges/on_dashboard.yaml",
        include_str!("../ontology/edges/on_dashboard.yaml"),
    ),
    (
        "edges/mentions.yaml",
        include_str!("../ontology/edges/mentions.yaml"),
    ),
];

pub fn build() -> Result<Arc<Ontology>> {
    let dir = tempfile::tempdir()?;
    for (path, text) in OVERLAY {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("overlay files sit in a directory"))?;
        std::fs::write(full, text)?;
    }
    let ontology = Ontology::load_embedded_with_overlay(dir.path())
        .context("merging third-party entities into the ontology")?;
    Ok(Arc::new(ontology))
}

fn id(source: &str, entity: &str, key: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in [source, entity, key].join("\0").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

pub struct Row {
    pub id: i64,
    pub values: BTreeMap<String, Value>,
}

pub fn rows(
    source: &str,
    name: &str,
    entity: &Entity,
    records: &[Record],
) -> (Vec<Row>, Vec<Edge>) {
    let mut rows = Vec::new();
    let mut edges = Vec::new();
    for record in records {
        let own = id(source, name, &record.key);
        let mut values = record.properties.clone();
        values.insert("key".into(), record.key.clone().into());
        values.insert("source".into(), source.into());
        rows.push(Row { id: own, values });
        for link in &entity.links {
            if let Some(target) = record.properties.get(&link.via).map(crate::fetch::text) {
                edges.push(Edge {
                    from: (own, name.into()),
                    kind: link.kind.clone(),
                    to: (id(source, &link.to, &target), link.to.clone()),
                });
            }
        }
    }
    (rows, edges)
}

pub struct Edge {
    pub from: (i64, String),
    pub kind: String,
    pub to: (i64, String),
}

pub fn load(
    client: &duckdb_client::DuckDbClient,
    ontology: &Ontology,
    name: &str,
    rows: &[Row],
) -> Result<()> {
    let node = ontology
        .get_node(name)
        .with_context(|| format!("{name} is not in the ontology"))?;
    let excluded = ontology.local_entity_excludes(name).unwrap_or_default();
    let names: Vec<&str> = node
        .storage
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .filter(|column| !excluded.iter().any(|excluded| excluded == column))
        .collect();
    for row in rows {
        let present: Vec<&str> = names
            .iter()
            .copied()
            .filter(|column| {
                matches!(*column, "id" | "traversal_path") || row.values.contains_key(*column)
            })
            .collect();
        let values: Vec<Value> = present
            .iter()
            .map(|column| match *column {
                "id" => row.id.into(),
                "traversal_path" => row
                    .values
                    .get(*column)
                    .cloned()
                    .unwrap_or_else(|| "0/".into()),
                column => row.values[column].clone(),
            })
            .collect();
        let marks = vec!["?"; present.len()].join(", ");
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({marks})",
            node.destination_table,
            present.join(", ")
        );
        client.execute(&insert, &values)?;
    }
    Ok(())
}

pub fn link(client: &duckdb_client::DuckDbClient, edges: &[Edge]) -> Result<()> {
    for edge in edges {
        client.execute(
            "INSERT INTO gl_edge (source_id, source_kind, relationship_kind, target_id, target_kind, traversal_path) VALUES (?, ?, ?, ?, ?, '0/')",
            &[edge.from.0.into(), edge.from.1.clone().into(), edge.kind.clone().into(), edge.to.0.into(), edge.to.1.clone().into()],
        )?;
    }
    Ok(())
}
