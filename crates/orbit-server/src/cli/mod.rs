use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Knowledge Graph server")]
pub struct Args {
    #[arg(long, value_enum, default_value = "webserver")]
    pub mode: Mode,
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
