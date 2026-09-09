use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Orbit server")]
pub struct Args {
    #[arg(long, value_enum, default_value = "webserver")]
    pub mode: Mode,
    /// Config overlay applied over the embedded config/default.yaml, in order when repeated;
    /// defaults to config/config.yaml when present.
    #[arg(long, value_name = "PATH")]
    pub config: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
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
        }
    }
}
