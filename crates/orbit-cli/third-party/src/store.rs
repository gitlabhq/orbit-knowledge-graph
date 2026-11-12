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
    drop_changed_tables(&client, ontology)?;
    client.initialize_schema(&compiler::generate_local_ddl(ontology, ""))?;
    client.initialize_schema(&format!(
        "CREATE TABLE IF NOT EXISTS {SYNCED_TABLE} (source VARCHAR PRIMARY KEY, entities VARCHAR NOT NULL, synced_at BIGINT NOT NULL)"
    ))?;
    Ok(client)
}

fn drop_changed_tables(client: &DuckDbClient, ontology: &Ontology) -> Result<()> {
    let existing = client.query_arrow(
        "SELECT table_name, string_agg(column_name, ',' ORDER BY column_name) AS columns \
         FROM information_schema.columns WHERE table_schema = 'main' GROUP BY table_name",
    )?;
    let names = duckdb_client::string_column(&existing, "table_name");
    let columns = duckdb_client::string_column(&existing, "columns");
    let mut dropped = false;
    for node in ontology.nodes() {
        if !node
            .destination_table
            .starts_with(crate::schema::TABLE_PREFIX)
        {
            continue;
        }
        let Some(have) = names
            .iter()
            .position(|name| name == &node.destination_table)
            .map(|index| columns[index].as_str())
        else {
            continue;
        };
        let mut want: Vec<&str> = node
            .storage
            .columns
            .iter()
            .map(|column| column.name.trim_matches('`'))
            .collect();
        want.sort_unstable();
        if have != want.join(",") {
            client.execute(&format!("DROP TABLE {}", node.destination_table), &[])?;
            dropped = true;
        }
    }
    if dropped && names.iter().any(|name| name == SYNCED_TABLE) {
        client.execute(&format!("DELETE FROM {SYNCED_TABLE}"), &[])?;
    }
    Ok(())
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
    client.execute("BEGIN TRANSACTION", &[])?;
    let written = clear_source(client, &table, source, entity)
        .and_then(|()| write(client, &table, source, entity, kinds, records));
    match written {
        Ok(()) => {
            client.execute("COMMIT", &[])?;
            Ok(())
        }
        Err(error) => {
            let _ = client.execute("ROLLBACK", &[]);
            Err(error)
        }
    }
}

fn clear_source(client: &DuckDbClient, table: &str, source: &str, entity: &str) -> Result<()> {
    client.execute(
        &format!("DELETE FROM {table} WHERE source = ?"),
        &[source.into()],
    )?;
    client.execute(
        &format!("DELETE FROM {EDGE_TABLE} WHERE source_kind = ? AND traversal_path = ?"),
        &[entity.into(), source.into()],
    )?;
    Ok(())
}

fn write(
    client: &DuckDbClient,
    table: &str,
    source: &str,
    entity: &str,
    kinds: &[(String, Type)],
    records: &[Record],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let staging = format!("_orbit_staging_{table}");
    let edge_staging = format!("_orbit_staging_{EDGE_TABLE}_{table}");
    let mut columns: Vec<(String, Type)> = vec![
        ("id".into(), Type::Int),
        ("traversal_path".into(), Type::String),
        ("key".into(), Type::String),
        ("source".into(), Type::String),
    ];
    columns.extend(kinds.iter().cloned());
    let staged: Vec<String> = columns
        .iter()
        .map(|(name, kind)| format!("{name} {}", staging_type(*kind)))
        .collect();
    client.execute(
        &format!(
            "CREATE OR REPLACE TEMP TABLE {staging} ({})",
            staged.join(", ")
        ),
        &[],
    )?;
    client.execute(
        &format!(
            "CREATE OR REPLACE TEMP TABLE {edge_staging} (source_id BIGINT, source_kind VARCHAR, relationship_kind VARCHAR, target_id BIGINT, target_kind VARCHAR, traversal_path VARCHAR)"
        ),
        &[],
    )?;
    client.insert_batch(&staging, &rows(&columns, source, records)?)?;
    client.insert_batch(&edge_staging, &edges(source, entity, records)?)?;
    let names: Vec<&str> = columns.iter().map(|(name, _)| name.as_str()).collect();
    let selected: Vec<String> = columns
        .iter()
        .map(|(name, kind)| match kind {
            Type::Timestamp => format!("CAST({name} AS TIMESTAMP)"),
            _ => name.clone(),
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
    client.execute(
        &format!(
            "INSERT INTO {EDGE_TABLE} (source_id, source_kind, relationship_kind, target_id, target_kind, traversal_path) SELECT * FROM {edge_staging}"
        ),
        &[],
    )?;
    client.execute(&format!("DROP TABLE {staging}"), &[])?;
    client.execute(&format!("DROP TABLE {edge_staging}"), &[])?;
    Ok(())
}

fn staging_type(kind: Type) -> &'static str {
    match kind {
        Type::Int => "BIGINT",
        Type::Bool => "BOOLEAN",
        Type::String | Type::Timestamp => "VARCHAR",
    }
}

fn rows(
    columns: &[(String, Type)],
    source: &str,
    records: &[Record],
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;
    let value = |record: &Record, name: &str| -> Option<serde_json::Value> {
        match name {
            "id" => Some(record.id.into()),
            "traversal_path" | "source" => Some(source.into()),
            "key" => Some(record.key.clone().into()),
            _ => record.properties.get(name).cloned(),
        }
    };
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, kind) in columns {
        let array: ArrayRef = match kind {
            Type::Int => Arc::new(Int64Array::from_iter(
                records
                    .iter()
                    .map(|record| value(record, name).and_then(|v| v.as_i64())),
            )),
            Type::Bool => Arc::new(BooleanArray::from_iter(
                records
                    .iter()
                    .map(|record| value(record, name).and_then(|v| v.as_bool())),
            )),
            Type::String | Type::Timestamp => {
                Arc::new(StringArray::from_iter(records.iter().map(|record| {
                    value(record, name).map(|v| match v {
                        serde_json::Value::String(text) => text,
                        other => other.to_string(),
                    })
                })))
            }
        };
        fields.push(Field::new(name, array.data_type().clone(), true));
        arrays.push(array);
    }
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        arrays,
    )?)
}

fn edges(
    source: &str,
    entity: &str,
    records: &[Record],
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let links: Vec<(&Record, &crate::fetch::Link)> = records
        .iter()
        .flat_map(|record| record.links.iter().map(move |link| (record, link)))
        .collect();
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
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema),
        vec![
            ids(links.iter().map(|(record, _)| record.id).collect()),
            strings(links.iter().map(|_| entity).collect()),
            strings(links.iter().map(|(_, link)| link.kind.as_str()).collect()),
            ids(links.iter().map(|(_, link)| link.target_id).collect()),
            strings(
                links
                    .iter()
                    .map(|(_, link)| link.target_kind.as_str())
                    .collect(),
            ),
            strings(links.iter().map(|_| source).collect()),
        ],
    )?)
}
