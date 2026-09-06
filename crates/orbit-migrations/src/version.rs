use std::collections::HashSet;
use std::sync::LazyLock;

use arrow::datatypes::UInt32Type;
use clickhouse_client::ArrowClickHouseClient;
use orbit_utils::arrow::ArrowUtils;
use thiserror::Error;
use uuid::Uuid;

pub static SCHEMA_VERSION: LazyLock<u32> = LazyLock::new(|| orbit_versions::VERSIONS.schema);

const CREATE_VERSION_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS gkg_schema_version (
    version UInt32,
    status Enum8('active' = 1, 'migrating' = 2, 'retired' = 3, 'dropped' = 4),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (version)";

const READ_ACTIVE_VERSION: &str = "\
SELECT version FROM gkg_schema_version FINAL \
WHERE status = 'active' ORDER BY created_at DESC LIMIT 1";

const READ_MIGRATING_VERSION: &str = "\
SELECT version FROM gkg_schema_version FINAL \
WHERE status = 'migrating' ORDER BY created_at DESC LIMIT 1";

const READ_ALL_VERSIONS: &str = "\
SELECT version, CAST(status AS String) AS status \
FROM gkg_schema_version FINAL ORDER BY version DESC";

const WRITE_VERSION: &str = "\
INSERT INTO gkg_schema_version (version, status) VALUES ({version:UInt32}, {status:String})";

const LIST_VERSION_ENTITIES: &str = "\
SELECT name, engine FROM system.tables \
WHERE database = {database:String} AND startsWith(name, {prefix:String})";

#[derive(Debug, Error)]
pub enum SchemaVersionError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
}

pub fn table_prefix(schema_version: u32) -> String {
    if schema_version == 0 {
        String::new()
    } else {
        format!("v{schema_version}_")
    }
}

pub fn prefixed_table_name(table: &str, schema_version: u32) -> String {
    format!("{}{table}", table_prefix(schema_version))
}

pub async fn ensure_version_table(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    graph.execute(CREATE_VERSION_TABLE).await?;
    Ok(())
}

pub async fn read_active_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u32>, SchemaVersionError> {
    read_single_version(graph, READ_ACTIVE_VERSION).await
}

pub async fn read_migrating_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u32>, SchemaVersionError> {
    read_single_version(graph, READ_MIGRATING_VERSION).await
}

pub async fn read_all_versions(
    graph: &ArrowClickHouseClient,
) -> Result<Vec<VersionEntry>, SchemaVersionError> {
    let batches = graph.query_arrow(READ_ALL_VERSIONS).await?;
    let mut entries = Vec::new();

    for batch in &batches {
        for row in 0..batch.num_rows() {
            let version = ArrowUtils::get_column::<UInt32Type>(batch, "version", row)
                .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing version".into()))?;
            let status = ArrowUtils::get_column_string(batch, "status", row)
                .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing status".into()))?;
            entries.push(VersionEntry { version, status });
        }
    }

    Ok(entries)
}

pub const STATUS_ACTIVE: &str = "active";
pub const STATUS_MIGRATING: &str = "migrating";
pub const STATUS_RETIRED: &str = "retired";
pub const STATUS_DROPPED: &str = "dropped";

pub async fn mark_version_active(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    set_version_status(graph, version, STATUS_ACTIVE).await
}

pub async fn mark_version_retired(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    set_version_status(graph, version, STATUS_RETIRED).await
}

pub async fn mark_version_migrating(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    set_version_status(graph, version, STATUS_MIGRATING).await
}

pub async fn mark_version_dropped(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    set_version_status(graph, version, STATUS_DROPPED).await
}

async fn set_version_status(
    graph: &ArrowClickHouseClient,
    version: u32,
    status: &str,
) -> Result<(), SchemaVersionError> {
    graph
        .query(WRITE_VERSION)
        .param("version", version)
        .param("status", status)
        .with_setting("insert_deduplication_token", Uuid::new_v4().to_string())
        .execute()
        .await?;
    Ok(())
}

pub async fn list_version_entities(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<Vec<VersionEntity>, SchemaVersionError> {
    if version == 0 {
        return Ok(Vec::new());
    }

    let batches = graph
        .query(LIST_VERSION_ENTITIES)
        .param("database", graph.database())
        .param("prefix", table_prefix(version))
        .fetch_arrow()
        .await?;

    let mut entities = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let name = ArrowUtils::get_column_string(batch, "name", row)
                .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing name".into()))?;
            let engine = ArrowUtils::get_column_string(batch, "engine", row)
                .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing engine".into()))?;
            entities.push(VersionEntity { name, engine });
        }
    }
    Ok(entities)
}

pub async fn version_tables_complete(
    graph: &ArrowClickHouseClient,
    version: u32,
    expected_table_names: &[String],
) -> Result<bool, SchemaVersionError> {
    if version == 0 {
        return Ok(true);
    }

    let entities = list_version_entities(graph, version).await?;
    let actual: HashSet<&str> = entities.iter().map(|entity| entity.name.as_str()).collect();

    let missing: Vec<&String> = expected_table_names
        .iter()
        .filter(|name| !actual.contains(name.as_str()))
        .collect();

    if !missing.is_empty() {
        tracing::warn!(version, ?missing, "version table set is incomplete");
        return Ok(false);
    }

    Ok(true)
}

pub fn entity_type_for_clickhouse_engine(engine: &str) -> &'static str {
    match engine {
        "MaterializedView" | "View" | "LiveView" | "WindowView" => "VIEW",
        "Dictionary" => "DICTIONARY",
        _ => "TABLE",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionEntry {
    pub version: u32,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct VersionEntity {
    pub name: String,
    pub engine: String,
}

async fn read_single_version(
    graph: &ArrowClickHouseClient,
    query: &str,
) -> Result<Option<u32>, SchemaVersionError> {
    let batches = graph.query_arrow(query).await?;
    for batch in &batches {
        if batch.num_rows() > 0 {
            return Ok(ArrowUtils::get_column::<UInt32Type>(batch, "version", 0));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_prefix_applies_version_number() {
        assert_eq!(table_prefix(0), "");
        assert_eq!(table_prefix(1), "v1_");
        assert_eq!(table_prefix(93), "v93_");
        assert_eq!(prefixed_table_name("gl_user", 0), "gl_user");
        assert_eq!(prefixed_table_name("gl_user", 2), "v2_gl_user");
    }

    #[test]
    fn engine_to_entity_type_mapping() {
        assert_eq!(
            entity_type_for_clickhouse_engine("MaterializedView"),
            "VIEW"
        );
        assert_eq!(
            entity_type_for_clickhouse_engine("Dictionary"),
            "DICTIONARY"
        );
        assert_eq!(
            entity_type_for_clickhouse_engine("ReplacingMergeTree"),
            "TABLE"
        );
    }
}
