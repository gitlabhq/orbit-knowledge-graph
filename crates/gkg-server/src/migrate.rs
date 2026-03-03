use tracing::{info, warn};

use crate::cli::MigrateCommand;
use crate::config::AppConfig;

pub async fn run(config: &AppConfig, command: MigrateCommand) -> anyhow::Result<()> {
    match command {
        MigrateCommand::Create { description } => {
            let report = migrations::create(&description)?;
            info!(up = %report.up_path.display(), down = %report.down_path.display(), "migration files created");
        }
        MigrateCommand::Generate => {
            let report = migrations::generate(&config.graph).await?;
            if report.migration_files.is_empty() {
                info!("schema is up to date, no migration files generated");
            }
            for (up_path, down_path) in &report.migration_files {
                info!(up = %up_path.display(), down = %down_path.display(), "migration file generated");
            }
            for table in &report.tables_created {
                info!(table = %table, "CREATE TABLE");
            }
            for table in &report.tables_altered {
                info!(table = %table, "ALTER TABLE");
            }
            log_warnings(&report.warnings);
        }
        MigrateCommand::Apply => {
            let report = migrations::apply(&config.graph).await?;
            info!(
                applied = report.applied_count,
                skipped = report.already_applied,
                "migrations applied"
            );
            log_warnings(&report.warnings);
        }
        MigrateCommand::Rollback { target_version } => {
            let report = migrations::rollback(&config.graph, target_version).await?;
            info!(
                rolled_back = report.rolled_back_count,
                "migrations rolled back"
            );
            log_warnings(&report.warnings);
        }
    }

    Ok(())
}

fn log_warnings(warnings: &[String]) {
    for warning in warnings {
        warn!(%warning);
    }
}
