use std::path::PathBuf;

use anyhow::{Context, Result};
use duckdb_client::{i64_column, string_column};

use crate::{sql, workspace};

pub(crate) fn run(fqn: String, repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<()> {
    let top_level = workspace::git_toplevel(&repo.unwrap_or_else(|| PathBuf::from(".")))?;
    let git = workspace::git_info(&top_level)?;
    let batches = sql::open_graph(db)?.query_arrow_json(
        "SELECT definition_type, file_path, start_line, end_line
         FROM gl_definition
         WHERE project_id = ?1 AND commit_sha = ?2 AND fqn = ?3
         ORDER BY file_path, start_line",
        &[
            git.project_id.into(),
            git.commit_sha.clone().into(),
            fqn.clone().into(),
        ],
    )?;
    let kinds = string_column(&batches, "definition_type");
    let files = string_column(&batches, "file_path");
    let starts = i64_column(&batches, "start_line");
    let ends = i64_column(&batches, "end_line");
    if kinds.is_empty() {
        anyhow::bail!(
            "no definition {fqn:?} for commit {} — pass the exact fqn printed by \
             `orbit local ask`, and make sure the commit is indexed (`orbit index <path>`)",
            git.commit_sha
        );
    }
    for i in 0..kinds.len() {
        if i > 0 {
            println!();
        }
        println!(
            "{fqn}  [{}]  {}:{}-{}",
            kinds[i], files[i], starts[i], ends[i]
        );
        let content = std::fs::read_to_string(git.repo_path.join(&files[i]))
            .with_context(|| format!("failed to read {}", files[i]))?;
        for (n, line) in content
            .lines()
            .enumerate()
            .take(usize::try_from(ends[i]).unwrap_or(0))
            .skip(usize::try_from(starts[i]).unwrap_or(1).saturating_sub(1))
        {
            println!("{} | {}", n + 1, line);
        }
    }
    Ok(())
}
