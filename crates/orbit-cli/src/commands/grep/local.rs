use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::search::DuckDbSearch;

use crate::workspace;

pub(super) struct LocalBackend {
    search: DuckDbSearch,
    header: String,
    git: workspace::GitInfo,
    paths: Vec<String>,
}

impl LocalBackend {
    pub(super) fn open(
        repo: Option<PathBuf>,
        db: Option<PathBuf>,
        paths: &[String],
        touched: &[String],
    ) -> Result<Self> {
        let workspace::IndexedRepo {
            git,
            client,
            edited,
        } = workspace::open_indexed(repo, db, touched)?;
        let paths = workspace::repo_relative_paths(&git.repo_path, paths);
        Ok(Self {
            search: DuckDbSearch::scoped(client, git.project_id, &git.commit_sha, &paths)?,
            header: match edited {
                0 => git.short_sha().to_string(),
                n => format!("{} + {n} edited files", git.short_sha()),
            },
            paths,
            git,
        })
    }

    pub(super) fn paths(&self) -> &[String] {
        &self.paths
    }

    pub(super) fn git(&self) -> &workspace::GitInfo {
        &self.git
    }

    pub(super) fn header(&self) -> &str {
        &self.header
    }

    pub(super) fn search(&self) -> &DuckDbSearch {
        &self.search
    }
}
