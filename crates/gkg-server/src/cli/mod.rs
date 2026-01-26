use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "gkg-server", about = "GitLab Knowledge Graph server")]
pub struct Args {
    #[arg(long, value_enum, default_value = "webserver")]
    pub mode: Mode,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    Indexer,
    Webserver,
}
