use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Knowledge Graph server")]
pub struct Args {
    #[arg(long, value_enum, default_value = "webserver")]
    pub mode: Mode,

    #[arg(long, env = "TRELLO_SYNC_CONFIG", default_value = "trello-sync.yaml")]
    pub trello_config: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    DispatchIndexing,
    HealthCheck,
    Indexer,
    TrelloSync,
    Webserver,
}
