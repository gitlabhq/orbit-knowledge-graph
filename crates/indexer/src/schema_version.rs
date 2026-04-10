//! Schema version tracking with table prefix support.
//!
//! Embeds the schema version at compile time from `config/SCHEMA_VERSION` and
//! persists it in the `gkg_schema_version` ClickHouse control table.
//!
//! Table prefix derivation maps a version number to the string prepended to
//! graph table names. Version 0 uses no prefix (backward compatible); version N
//! uses `vN_`.

use clickhouse_client::ArrowClickHouseClient;
use thiserror::Error;
use tracing::info;

/// Schema version embedded at compile time from `config/SCHEMA_VERSION`.
///
/// Bump this file whenever `config/graph.sql` or `config/ontology/` changes
/// in a way that requires a new table-set. The CI `schema-version-check` job
/// enforces this.
pub const SCHEMA_VERSION: u32 = {
    let bytes = include_bytes!("../../../config/SCHEMA_VERSION");
    parse_u32_from_bytes(bytes)
};

/// Parses a `u32` from a byte slice containing an ASCII decimal integer
/// optionally terminated by a newline. Panics at compile time on invalid input.
const fn parse_u32_from_bytes(bytes: &[u8]) -> u32 {
    let mut i = 0;
    let mut value: u32 = 0;
    let mut found_digit = false;
    while i < bytes.len() {
        let b = bytes[i];
        if b >= b'0' && b <= b'9' {
            value = value * 10 + (b - b'0') as u32;
            found_digit = true;
        } else if b == b'\n' || b == b'\r' || b == b' ' {
            // Trailing whitespace is allowed.
        } else {
            panic!("config/SCHEMA_VERSION contains non-digit characters");
        }
        i += 1;
    }
    if !found_digit {
        panic!("config/SCHEMA_VERSION is empty");
    }
    value
}

const CREATE_VERSION_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS gkg_schema_version (
    version UInt32,
    status Enum8('active' = 1, 'migrating' = 2, 'retired' = 3, 'dropped' = 4),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY version";

const READ_ACTIVE_VERSION: &str = "\
SELECT version FROM gkg_schema_version FINAL WHERE status = 'active' ORDER BY created_at DESC LIMIT 1";

const WRITE_VERSION: &str = "\
INSERT INTO gkg_schema_version (version, status) VALUES ({version:UInt32}, 'active')";

#[derive(Debug, Error)]
pub enum SchemaVersionError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
}

/// Returns the table prefix for a given schema version.
///
/// Version 0 → `""` (no prefix, backward compatible).
/// Version N → `"vN_"`.
pub fn table_prefix(schema_version: u32) -> String {
    if schema_version == 0 {
        String::new()
    } else {
        format!("v{schema_version}_")
    }
}

/// Returns the fully-qualified (prefixed) table name for the given schema version.
pub fn prefixed_table_name(table: &str, schema_version: u32) -> String {
    format!("{}{}", table_prefix(schema_version), table)
}

/// Creates the `gkg_schema_version` control table if it does not exist.
///
/// This table is never prefixed or dropped across schema versions — it is the
/// single source of truth for which version is active.
pub async fn ensure_version_table(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    graph.execute(CREATE_VERSION_TABLE).await?;
    Ok(())
}

/// Reads the active schema version from ClickHouse.
///
/// Returns `None` on a fresh install (no rows yet).
/// Uses `FINAL` to handle `ReplacingMergeTree` eventual consistency.
pub async fn read_active_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u32>, SchemaVersionError> {
    let batches = graph.query_arrow(READ_ACTIVE_VERSION).await?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch
            .column_by_name("version")
            .ok_or_else(|| SchemaVersionError::UnexpectedResult("missing version column".into()))?;
        let col = col
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .ok_or_else(|| {
                SchemaVersionError::UnexpectedResult("version column is not UInt32".into())
            })?;
        return Ok(Some(col.value(0)));
    }

    Ok(None)
}

/// Records the embedded `SCHEMA_VERSION` as the active version in ClickHouse.
pub async fn write_schema_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    graph
        .query(WRITE_VERSION)
        .param("version", version)
        .execute()
        .await?;
    Ok(())
}

/// Ensures the `gkg_schema_version` table exists and, on a fresh install,
/// records version 0 as active.
///
/// Called by all service modes (Indexer, Webserver, DispatchIndexing) at
/// startup so the control table is always present.
pub async fn init(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    ensure_version_table(graph).await?;

    let active = read_active_version(graph).await?;
    if active.is_none() {
        info!(
            version = SCHEMA_VERSION,
            "fresh install — recording initial schema version"
        );
        write_schema_version(graph, SCHEMA_VERSION).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_is_valid_u32() {
        let _ = SCHEMA_VERSION;
    }

    #[test]
    fn table_prefix_v0_is_empty() {
        assert_eq!(table_prefix(0), "");
    }

    #[test]
    fn table_prefix_v1() {
        assert_eq!(table_prefix(1), "v1_");
    }

    #[test]
    fn table_prefix_v2() {
        assert_eq!(table_prefix(2), "v2_");
    }

    #[test]
    fn prefixed_table_name_v0_no_change() {
        assert_eq!(prefixed_table_name("gl_user", 0), "gl_user");
    }

    #[test]
    fn prefixed_table_name_v1() {
        assert_eq!(prefixed_table_name("gl_user", 1), "v1_gl_user");
    }

    #[test]
    fn prefixed_table_name_v2() {
        assert_eq!(prefixed_table_name("gl_user", 2), "v2_gl_user");
    }

    #[test]
    fn create_table_ddl_uses_replacing_merge_tree() {
        assert!(CREATE_VERSION_TABLE.contains("ReplacingMergeTree"));
        assert!(CREATE_VERSION_TABLE.contains("IF NOT EXISTS"));
    }

    #[test]
    fn read_query_uses_final() {
        assert!(
            READ_ACTIVE_VERSION.contains("FINAL"),
            "version query must use FINAL for ReplacingMergeTree consistency"
        );
    }

    #[test]
    fn table_prefix_large_version() {
        assert_eq!(table_prefix(99), "v99_");
        assert_eq!(prefixed_table_name("checkpoint", 99), "v99_checkpoint");
    }
}
