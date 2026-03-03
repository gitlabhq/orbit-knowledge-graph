use std::path::{Path, PathBuf};

use clickhouse_client::ClickHouseConfiguration;
use tracing::{info, warn};

use crate::MigrationError;
use crate::apply::{
    applied_versions, apply_migration, ensure_tracking_table, load_migrations,
    load_rollback_migrations, remove_migration_record,
};
use crate::generate::{CreateReport, GenerateReport};

#[derive(Debug)]
pub struct ApplyReport {
    pub applied_count: usize,
    pub already_applied: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct RollbackReport {
    pub rolled_back_count: usize,
    pub warnings: Vec<String>,
}

fn default_migrations_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

pub fn create(description: &str) -> Result<CreateReport, MigrationError> {
    let migrations_dir = default_migrations_dir();
    crate::generate::create(&migrations_dir, description)
}

pub async fn generate(config: &ClickHouseConfiguration) -> Result<GenerateReport, MigrationError> {
    let migrations_dir = default_migrations_dir();
    crate::generate::generate(config, &migrations_dir).await
}

pub async fn apply(config: &ClickHouseConfiguration) -> Result<ApplyReport, MigrationError> {
    apply_dir(config, &default_migrations_dir()).await
}

pub async fn apply_dir(
    config: &ClickHouseConfiguration,
    migrations_dir: &Path,
) -> Result<ApplyReport, MigrationError> {
    let client = config.build_client();

    ensure_tracking_table(&client).await?;

    let migrations = load_migrations(migrations_dir)?;
    if migrations.is_empty() {
        info!("no migration files found");
        return Ok(ApplyReport {
            applied_count: 0,
            already_applied: 0,
            warnings: Vec::new(),
        });
    }

    let applied = applied_versions(&client).await?;
    let mut applied_count = 0;
    let mut already_applied = 0;
    let mut warnings = Vec::new();

    for migration in &migrations {
        if applied.contains(&migration.version) {
            already_applied += 1;
            continue;
        }

        info!(
            version = migration.version,
            description = %migration.description,
            "applying migration"
        );

        match apply_migration(&client, migration).await {
            Ok(()) => {
                applied_count += 1;
                info!(
                    version = migration.version,
                    "migration applied successfully"
                );
            }
            Err(error) => {
                let message = format!(
                    "migration V{} failed: {error} (subsequent migrations skipped)",
                    migration.version
                );
                warn!(%message);
                warnings.push(message);
                break;
            }
        }
    }

    Ok(ApplyReport {
        applied_count,
        already_applied,
        warnings,
    })
}

pub async fn rollback(
    config: &ClickHouseConfiguration,
    target_version: u64,
) -> Result<RollbackReport, MigrationError> {
    rollback_dir(config, target_version, &default_migrations_dir()).await
}

pub async fn rollback_dir(
    config: &ClickHouseConfiguration,
    target_version: u64,
    migrations_dir: &Path,
) -> Result<RollbackReport, MigrationError> {
    let client = config.build_client();

    ensure_tracking_table(&client).await?;

    let mut down_migrations = load_rollback_migrations(migrations_dir)?;
    let applied = applied_versions(&client).await?;

    down_migrations.retain(|m| m.version > target_version && applied.contains(&m.version));
    down_migrations.reverse();

    let mut rolled_back_count = 0;
    let mut warnings = Vec::new();

    for migration in &down_migrations {
        info!(
            version = migration.version,
            description = %migration.description,
            "rolling back migration"
        );

        match apply_migration(&client, migration).await {
            Ok(()) => {
                remove_migration_record(&client, migration.version).await?;
                rolled_back_count += 1;
                info!(
                    version = migration.version,
                    "migration rolled back successfully"
                );
            }
            Err(error) => {
                let message = format!(
                    "rollback of V{} failed: {error} (subsequent rollbacks skipped)",
                    migration.version
                );
                warn!(%message);
                warnings.push(message);
                break;
            }
        }
    }

    Ok(RollbackReport {
        rolled_back_count,
        warnings,
    })
}
