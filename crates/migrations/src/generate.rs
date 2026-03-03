use std::path::{Path, PathBuf};

use chrono::Utc;
use clickhouse_client::ClickHouseConfiguration;
use ontology::Ontology;
use tracing::{info, warn};

use crate::MigrationError;
use crate::introspect;
use crate::ir::diff::{SchemaDiff, diff_schemas};
use crate::ir::from_ontology::all_table_schemas;
use crate::ir::sql::{render_diff, render_rollback};

#[derive(Debug)]
pub struct CreateReport {
    pub up_path: PathBuf,
    pub down_path: PathBuf,
}

#[derive(Debug)]
pub struct GenerateReport {
    pub migration_files: Vec<(PathBuf, PathBuf)>,
    pub tables_created: Vec<String>,
    pub tables_altered: Vec<String>,
    pub warnings: Vec<String>,
}

fn diff_description(diff: &SchemaDiff) -> String {
    match diff {
        SchemaDiff::CreateTable(schema) => format!("create_{}", schema.name),
        SchemaDiff::AlterTable { table_name, .. } => format!("alter_{}", table_name),
    }
}

/// `YYYYMMDDHHmmss` (e.g. `20260303143022`).
fn timestamp_version() -> u64 {
    Utc::now()
        .format("%Y%m%d%H%M%S")
        .to_string()
        .parse()
        .expect("timestamp format always produces a valid u64")
}

fn migration_file_paths(directory: &Path, version: u64, description: &str) -> (PathBuf, PathBuf) {
    let up = directory.join(format!("{version}_{description}.up.sql"));
    let down = directory.join(format!("{version}_{description}.down.sql"));
    (up, down)
}

pub fn create(migrations_dir: &Path, description: &str) -> Result<CreateReport, MigrationError> {
    std::fs::create_dir_all(migrations_dir)?;

    let version = timestamp_version();
    let sanitized_description = description.replace(' ', "_");

    let (up_path, down_path) =
        migration_file_paths(migrations_dir, version, &sanitized_description);

    std::fs::write(&up_path, "")?;
    std::fs::write(&down_path, "")?;

    Ok(CreateReport { up_path, down_path })
}

/// Diff the ontology against the current ClickHouse schema and write migration files.
pub async fn generate(
    config: &ClickHouseConfiguration,
    migrations_dir: &Path,
) -> Result<GenerateReport, MigrationError> {
    let ontology = Ontology::load_embedded()?;
    let desired_schemas = all_table_schemas(&ontology);

    let mut diffs = Vec::new();
    let mut tables_created = Vec::new();
    let mut tables_altered = Vec::new();
    let mut all_warnings = Vec::new();

    for desired in &desired_schemas {
        let current = introspect::introspect_table(config, &desired.name).await?;

        if let Some(diff) = diff_schemas(desired, current.as_ref()) {
            match &diff {
                SchemaDiff::CreateTable(schema) => {
                    info!(table = %schema.name, "table will be created");
                    tables_created.push(schema.name.clone());
                }
                SchemaDiff::AlterTable {
                    table_name,
                    add_columns,
                    warnings,
                } => {
                    if !add_columns.is_empty() {
                        info!(
                            table = %table_name,
                            columns = add_columns.len(),
                            "columns will be added"
                        );
                        tables_altered.push(table_name.clone());
                    }
                    for warning in warnings {
                        warn!(%warning);
                        all_warnings.push(warning.clone());
                    }
                }
            }
            diffs.push(diff);
        }
    }

    if diffs.is_empty() {
        return Ok(GenerateReport {
            migration_files: Vec::new(),
            tables_created,
            tables_altered,
            warnings: all_warnings,
        });
    }

    std::fs::create_dir_all(migrations_dir)?;
    let base_version = timestamp_version();
    let mut migration_files = Vec::new();

    for (index, diff) in diffs.iter().enumerate() {
        let version = base_version + index as u64;
        let description = diff_description(diff);
        let (up_path, down_path) = migration_file_paths(migrations_dir, version, &description);

        std::fs::write(&up_path, render_diff(diff))?;
        std::fs::write(&down_path, render_rollback(diff))?;

        migration_files.push((up_path, down_path));
    }

    Ok(GenerateReport {
        migration_files,
        tables_created,
        tables_altered,
        warnings: all_warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_writes_empty_paired_files() {
        let dir = TempDir::new().unwrap();
        let report = create(dir.path(), "add_users_table").unwrap();

        assert!(report.up_path.exists());
        assert!(report.down_path.exists());
        assert_eq!(std::fs::read_to_string(&report.up_path).unwrap(), "");
        assert_eq!(std::fs::read_to_string(&report.down_path).unwrap(), "");

        let up_name = report.up_path.file_name().unwrap().to_string_lossy();
        let down_name = report.down_path.file_name().unwrap().to_string_lossy();
        assert!(up_name.ends_with("_add_users_table.up.sql"));
        assert!(down_name.ends_with("_add_users_table.down.sql"));
    }

    #[test]
    fn create_sanitizes_spaces_in_description() {
        let dir = TempDir::new().unwrap();
        let report = create(dir.path(), "add users table").unwrap();

        let up_name = report.up_path.file_name().unwrap().to_string_lossy();
        assert!(up_name.contains("add_users_table"));
    }
}
