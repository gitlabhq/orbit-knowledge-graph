use std::collections::HashSet;
use std::path::Path;

use clickhouse_client::ArrowClickHouseClient;
use tracing::{info, warn};

use crate::MigrationError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Migration {
    pub version: u64,
    pub description: String,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationDirection {
    Up,
    Down,
}

/// Expects `20260303143022_description.up.sql` or `20260303143022_description.down.sql`.
fn parse_migration_filename(name: &str) -> Option<(u64, String, MigrationDirection)> {
    let (without_ext, direction) = if let Some(stem) = name.strip_suffix(".up.sql") {
        (stem, MigrationDirection::Up)
    } else if let Some(stem) = name.strip_suffix(".down.sql") {
        (stem, MigrationDirection::Down)
    } else {
        return None;
    };

    let (version_str, rest) = without_ext.split_once('_')?;
    let version = version_str.parse::<u64>().ok()?;
    Some((version, rest.to_string(), direction))
}

fn load_migrations_with_suffix(
    migrations_dir: &Path,
    direction: MigrationDirection,
) -> Result<Vec<Migration>, MigrationError> {
    if !migrations_dir.exists() {
        return Ok(Vec::new());
    }

    let mut migrations = Vec::new();

    for entry in std::fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();

        if !name.ends_with(".sql") {
            continue;
        }

        let Some((version, description, file_direction)) = parse_migration_filename(&name) else {
            warn!(file = %name, "skipping file with unrecognized migration name format");
            continue;
        };

        if file_direction != direction {
            continue;
        }

        let sql = std::fs::read_to_string(entry.path())?;
        migrations.push(Migration {
            version,
            description,
            sql,
        });
    }

    migrations.sort_by_key(|m| m.version);
    Ok(migrations)
}

pub fn load_migrations(migrations_dir: &Path) -> Result<Vec<Migration>, MigrationError> {
    load_migrations_with_suffix(migrations_dir, MigrationDirection::Up)
}

pub fn load_rollback_migrations(migrations_dir: &Path) -> Result<Vec<Migration>, MigrationError> {
    load_migrations_with_suffix(migrations_dir, MigrationDirection::Down)
}

pub async fn ensure_tracking_table(client: &ArrowClickHouseClient) -> Result<(), MigrationError> {
    let sql = "CREATE TABLE IF NOT EXISTS schema_migrations (
    version UInt64,
    description String,
    applied_at DateTime64(6, 'UTC') DEFAULT now64(6),
    checksum String DEFAULT '',
    _version DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (version)";

    client
        .query(sql)
        .execute()
        .await
        .map_err(MigrationError::ClickHouse)?;

    Ok(())
}

pub async fn applied_versions(
    client: &ArrowClickHouseClient,
) -> Result<HashSet<u64>, MigrationError> {
    let result = client
        .query("SELECT version FROM schema_migrations FINAL")
        .fetch_arrow()
        .await
        .map_err(MigrationError::ClickHouse)?;

    let mut versions = HashSet::new();
    for batch in &result {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("version column should be UInt64");
        for i in 0..col.len() {
            versions.insert(col.value(i));
        }
    }
    Ok(versions)
}

pub async fn apply_migration(
    client: &ArrowClickHouseClient,
    migration: &Migration,
) -> Result<(), MigrationError> {
    let statements: Vec<&str> = migration
        .sql
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .collect();

    for statement in &statements {
        info!(version = migration.version, "executing migration statement");
        client
            .query(statement)
            .execute()
            .await
            .map_err(MigrationError::ClickHouse)?;
    }

    let checksum = simple_checksum(&migration.sql);
    let record_sql = format!(
        "INSERT INTO schema_migrations (version, description, checksum) VALUES ({}, '{}', '{}')",
        migration.version,
        migration.description.replace('\'', "\\'"),
        checksum,
    );

    client
        .query(&record_sql)
        .execute()
        .await
        .map_err(MigrationError::ClickHouse)?;

    Ok(())
}

pub async fn remove_migration_record(
    client: &ArrowClickHouseClient,
    version: u64,
) -> Result<(), MigrationError> {
    let sql = format!("DELETE FROM schema_migrations WHERE version = {version}");

    client
        .query(&sql)
        .execute()
        .await
        .map_err(MigrationError::ClickHouse)?;

    Ok(())
}

fn simple_checksum(content: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn parse_up_migration_filename() {
        let result = parse_migration_filename("20260303143022_initial_schema.up.sql");
        assert_eq!(
            result,
            Some((
                20260303143022,
                "initial_schema".to_string(),
                MigrationDirection::Up
            ))
        );
    }

    #[test]
    fn parse_down_migration_filename() {
        let result = parse_migration_filename("20260303143022_initial_schema.down.sql");
        assert_eq!(
            result,
            Some((
                20260303143022,
                "initial_schema".to_string(),
                MigrationDirection::Down
            ))
        );
    }

    #[test]
    fn parse_plain_sql_is_not_recognized() {
        assert!(parse_migration_filename("20260303143022_initial_schema.sql").is_none());
    }

    #[test]
    fn parse_invalid_filename_returns_none() {
        assert!(parse_migration_filename("not_a_migration.up.sql").is_none());
        assert!(parse_migration_filename("_no_version.up.sql").is_none());
    }

    #[test]
    fn load_up_migrations_skips_down_files() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("20260303150000_third.up.sql"),
            "CREATE TABLE c (id Int64) ENGINE = MergeTree ORDER BY id;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303150000_third.down.sql"),
            "DROP TABLE IF EXISTS c;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303140000_first.up.sql"),
            "CREATE TABLE a (id Int64) ENGINE = MergeTree ORDER BY id;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303140000_first.down.sql"),
            "DROP TABLE IF EXISTS a;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303143000_second.up.sql"),
            "CREATE TABLE b (id Int64) ENGINE = MergeTree ORDER BY id;",
        )
        .unwrap();

        let migrations = load_migrations(dir.path()).unwrap();
        assert_eq!(migrations.len(), 3);
        assert_eq!(migrations[0].version, 20260303140000);
        assert_eq!(migrations[1].version, 20260303143000);
        assert_eq!(migrations[2].version, 20260303150000);
    }

    #[test]
    fn load_rollback_migrations_only_loads_down_files() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("20260303140000_first.up.sql"),
            "CREATE TABLE a (id Int64) ENGINE = MergeTree ORDER BY id;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303140000_first.down.sql"),
            "DROP TABLE IF EXISTS a;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303150000_second.up.sql"),
            "CREATE TABLE b (id Int64) ENGINE = MergeTree ORDER BY id;",
        )
        .unwrap();
        fs::write(
            dir.path().join("20260303150000_second.down.sql"),
            "DROP TABLE IF EXISTS b;",
        )
        .unwrap();

        let rollbacks = load_rollback_migrations(dir.path()).unwrap();
        assert_eq!(rollbacks.len(), 2);
        assert_eq!(rollbacks[0].version, 20260303140000);
        assert!(rollbacks[0].sql.contains("DROP TABLE IF EXISTS a"));
        assert_eq!(rollbacks[1].version, 20260303150000);
        assert!(rollbacks[1].sql.contains("DROP TABLE IF EXISTS b"));
    }

    #[test]
    fn load_migrations_from_empty_dir() {
        let dir = TempDir::new().unwrap();
        let migrations = load_migrations(dir.path()).unwrap();
        assert!(migrations.is_empty());
    }
}
