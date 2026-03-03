use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Knowledge Graph server")]
pub struct Args {
    #[arg(long, value_enum, default_value = "webserver")]
    pub mode: Mode,

    #[command(subcommand)]
    pub migrate_command: Option<MigrateCommand>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    DispatchIndexing,
    HealthCheck,
    Indexer,
    Migrate,
    Webserver,
}

#[derive(Debug, Clone, Subcommand)]
pub enum MigrateCommand {
    /// Create empty up/down migration files for manual SQL.
    Create {
        /// Description for the migration (used in the filename).
        #[arg(long)]
        description: String,
    },
    /// Generate up/down migration files from ontology diffs.
    Generate,
    /// Apply all pending migrations.
    Apply,
    /// Roll back all migrations newer than the target version.
    Rollback {
        /// Target version. All migrations newer than this will be rolled back.
        #[arg(long)]
        target_version: u64,
    },
}
