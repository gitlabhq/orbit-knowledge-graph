use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Orbit server")]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Command>,
    #[arg(long, value_enum, default_value = "webserver")]
    pub mode: Mode,
    /// Config overlay applied over the embedded config/default.yaml; defaults to config/config.yaml when present.
    #[arg(long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Load the config as startup does, reject unknown keys, and exit without
    /// starting a server. Secret files are not read.
    CheckConfig,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    ClickhouseSetup,
    DispatchIndexing,
    HealthCheck,
    Indexer,
    Webserver,
}

impl Mode {
    pub fn service_name(self) -> &'static str {
        match self {
            Self::Webserver => "gkg-webserver",
            Self::Indexer => "gkg-indexer",
            Self::DispatchIndexing => "gkg-dispatcher",
            Self::HealthCheck => "gkg-healthcheck",
            Self::ClickhouseSetup => "gkg-clickhouse-setup",
        }
    }
}
