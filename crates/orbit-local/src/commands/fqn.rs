use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::{DuckDbClient, i64_column, string_column};

use crate::commands::setup::spec;
use crate::{sql, workspace};

pub(crate) struct ResolvedFqn {
    pub git: workspace::GitInfo,
    pub client: DuckDbClient,
    pub ids: Vec<i64>,
    pub kinds: Vec<String>,
    pub files: Vec<String>,
    pub starts: Vec<i64>,
    pub ends: Vec<i64>,
}

pub(crate) fn resolve(
    fqn: &str,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
) -> Result<ResolvedFqn> {
    let top_level = workspace::git_toplevel(&repo.unwrap_or_else(|| PathBuf::from(".")))?;
    let git = workspace::git_info(&top_level)?;
    let client = sql::open_graph(db)?;
    let defs = client.query_arrow_json(
        "SELECT id, definition_type, file_path, start_line, end_line
         FROM gl_definition
         WHERE project_id = ?1 AND commit_sha = ?2 AND fqn = ?3
         ORDER BY file_path, start_line",
        &[
            git.project_id.into(),
            git.commit_sha.clone().into(),
            fqn.into(),
        ],
    )?;
    let ids = i64_column(&defs, "id");
    if ids.is_empty() {
        let launcher = spec::launcher();
        anyhow::bail!(
            "no definition {fqn:?} for commit {} — pass the exact fqn printed by \
             `{launcher} ask`, and make sure the commit is indexed (`{launcher} index <path>`)",
            git.commit_sha
        );
    }
    Ok(ResolvedFqn {
        kinds: string_column(&defs, "definition_type"),
        files: string_column(&defs, "file_path"),
        starts: i64_column(&defs, "start_line"),
        ends: i64_column(&defs, "end_line"),
        ids,
        git,
        client,
    })
}
