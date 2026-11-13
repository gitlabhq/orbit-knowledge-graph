use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::DuckDbClient;

use crate::workspace;

pub(super) struct LocalBackend {
    client: crate::database::GraphReader,
    git: workspace::GitInfo,
    paths: Vec<String>,
}

impl LocalBackend {
    pub(super) fn open(
        repo: Option<PathBuf>,
        db: Option<PathBuf>,
        paths: &[String],
        no_index: bool,
    ) -> Result<Self> {
        let workspace::IndexedRepo { git, client } = match no_index {
            true => workspace::open_indexed_without_indexing(repo, db)?,
            false => workspace::open_indexed(repo, db)?,
        };
        let paths = workspace::repo_relative_paths(&git.repo_path, paths);
        Ok(Self { client, paths, git })
    }

    pub(super) fn paths(&self) -> &[String] {
        &self.paths
    }

    pub(super) fn git(&self) -> &workspace::GitInfo {
        &self.git
    }

    pub(super) fn header(&self) -> &str {
        self.git.short_sha()
    }

    pub(super) fn client(&self) -> &DuckDbClient {
        &self.client
    }
}
