mod download;

use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum BenchmarkCommand {
    /// Download GDK repositories for code indexing benchmarks.
    ///
    /// Downloads gitlab, gitlab-shell, and gitlab-development-kit at pinned
    /// versions, extracts them, and initializes minimal git repos so the
    /// code indexer can process them.
    DownloadRepos {
        /// Directory to store downloaded repositories.
        #[arg(long, default_value = "output/gdk")]
        output_dir: PathBuf,
    },
}

pub async fn run(command: BenchmarkCommand) -> Result<()> {
    match command {
        BenchmarkCommand::DownloadRepos { output_dir } => {
            download::download_repos(&output_dir).await
        }
    }
}
