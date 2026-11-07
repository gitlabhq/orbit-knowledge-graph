use std::path::Path;

use anyhow::{Context, Result};
use duckdb_client::{DuckDbClient, OpenMode};
use ontology::Ontology;

use crate::config::Type;
use crate::fetch::Record;
use crate::schema::table;

pub const EDGE_TABLE: &str = "gl_edge";
const SYNCED_TABLE: &str = "_orbit_third_party_synced";

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

pub fn open(path: &Path, ontology: &Ontology) -> Result<DuckDbClient> {
    let client = DuckDbClient::open(path).with_context(|| format!("opening {}", path.display()))?;
    client.initialize_schema(&compiler::generate_local_ddl(ontology, ""))?;
    client.initialize_schema(&format!(
        "CREATE TABLE IF NOT EXISTS {SYNCED_TABLE} (source VARCHAR PRIMARY KEY, entities VARCHAR NOT NULL, synced_at BIGINT NOT NULL)"
    ))?;
    Ok(client)
}

pub fn open_read(path: &Path) -> Result<Option<DuckDbClient>> {
    match path.exists() {
        true => Ok(Some(DuckDbClient::open(OpenMode::ReadOnly(path))?)),
        false => Ok(None),
    }
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

/// When the source last synced with this exact set of entities, in unix seconds.
pub fn synced_at(client: &DuckDbClient, source: &str, entities: &str) -> Result<Option<i64>> {
    let batches = client.query_arrow_json(
        &format!("SELECT synced_at FROM {SYNCED_TABLE} WHERE source = ? AND entities = ?"),
        &[source.into(), entities.into()],
    )?;
    Ok(duckdb_client::i64_column(&batches, "synced_at")
        .first()
        .copied())
}

pub fn mark_synced(client: &DuckDbClient, source: &str, entities: &str) -> Result<()> {
    client.execute(
        &format!("INSERT OR REPLACE INTO {SYNCED_TABLE} VALUES (?, ?, ?)"),
        &[source.into(), entities.into(), now().into()],
    )?;
    Ok(())
}

/// Marks the source stale, and with `drop_data` deletes everything it synced.
/// Works on the store alone, so it also cleans up after a source left the config.
pub fn forget(path: &Path, source: &str, drop_data: bool) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let client = DuckDbClient::open(path).with_context(|| format!("opening {}", path.display()))?;
    let tables = duckdb_client::string_column(
        &client.query_arrow(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
        )?,
        "table_name",
    );
    if tables.iter().any(|table| table == SYNCED_TABLE) {
        client.execute(
            &format!("DELETE FROM {SYNCED_TABLE} WHERE source = ?"),
            &[source.into()],
        )?;
    }
    if !drop_data {
        return Ok(());
    }
    for table in tables
        .iter()
        .filter(|table| table.starts_with(crate::schema::TABLE_PREFIX))
    {
        client.execute(
            &format!("DELETE FROM {table} WHERE source = ?"),
            &[source.into()],
        )?;
    }
    if tables.iter().any(|table| table == EDGE_TABLE) {
        client.execute(
            &format!("DELETE FROM {EDGE_TABLE} WHERE traversal_path = ?"),
            &[source.into()],
        )?;
    }
    Ok(())
}

pub fn replace(
    client: &DuckDbClient,
    source: &str,
    entity: &str,
    kinds: &[(String, Type)],
    records: &[Record],
) -> Result<()> {
    let table = table(entity);
    client.execute(
        &format!("DELETE FROM {table} WHERE source = ?"),
        &[source.into()],
    )?;
    client.execute(
        &format!("DELETE FROM {EDGE_TABLE} WHERE source_kind = ? AND traversal_path = ?"),
        &[entity.into(), source.into()],
    )?;
    let mut columns = vec!["id", "traversal_path", "key", "source"];
    columns.extend(kinds.iter().map(|(name, _)| name.as_str()));
    let marks = vec!["?"; columns.len()].join(", ");
    let insert = format!(
        "INSERT INTO {table} ({}) VALUES ({marks})",
        columns.join(", ")
    );
    client.execute("BEGIN TRANSACTION", &[])?;
    for record in records {
        let mut values: Vec<serde_json::Value> = vec![
            record.id.into(),
            source.into(),
            record.key.clone().into(),
            source.into(),
        ];
        values.extend(kinds.iter().map(|(name, _)| {
            record
                .properties
                .get(name)
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }));
        client.execute(&insert, &values)?;
        for link in &record.links {
            client.execute(
                &format!(
                    "INSERT INTO {EDGE_TABLE} (source_id, source_kind, relationship_kind, target_id, target_kind, traversal_path) VALUES (?, ?, ?, ?, ?, ?)"
                ),
                &[
                    record.id.into(),
                    entity.into(),
                    link.kind.clone().into(),
                    link.target_id.into(),
                    link.target_kind.clone().into(),
                    source.into(),
                ],
            )?;
        }
    }
    client.execute("COMMIT", &[])?;
    Ok(())
}
