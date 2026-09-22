use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Result, bail};
use arrow::array::{Array, StringArray};
use serde::Deserialize;

use super::spec;
use crate::workspace::{Workspace, git_info, git_toplevel};

pub(super) struct Indexed {
    pub(super) summary: String,
    pub(super) suggested_grep: Option<String>,
}

pub(super) fn current_repository_dir() -> Result<Option<PathBuf>> {
    let cwd = std::env::current_dir()?;
    let repos = Workspace::open_default()?.resolve_repos(&cwd)?;
    Ok((!repos.is_empty()).then_some(cwd))
}

pub(super) fn index_command_line() -> String {
    format!("{} index .", spec::launcher())
}

pub(super) fn index_repository(cwd: &Path) -> Result<Indexed> {
    let output = launcher_command()?
        .args(["index", "."])
        .current_dir(cwd)
        .stderr(Stdio::null())
        .output()?;
    if !output.status.success() {
        bail!("failed with {}", output.status);
    }

    let counts = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<IndexSummary>()
        .filter_map(Result::ok)
        .map(|summary| {
            format!(
                "{} files, {} definitions, {:.0}s",
                format_with_thousands(summary.graph.files),
                format_with_thousands(summary.graph.definitions),
                summary.time_seconds
            )
        })
        .collect::<Vec<_>>();
    let summary = match counts.is_empty() {
        true => "done".to_string(),
        false => counts.join("; "),
    };
    Ok(Indexed {
        summary,
        suggested_grep: most_referenced_definition(cwd),
    })
}

fn launcher_command() -> Result<Command> {
    Ok(match spec::launcher() {
        spec::GLAB_LAUNCHER => {
            let mut glab = Command::new("glab");
            glab.arg("orbit");
            glab
        }
        _ => Command::new(std::env::current_exe()?),
    })
}

#[derive(Deserialize)]
struct IndexSummary {
    time_seconds: f64,
    graph: IndexedGraph,
}

#[derive(Deserialize)]
struct IndexedGraph {
    files: usize,
    definitions: usize,
}

fn most_referenced_definition(repo: &Path) -> Option<String> {
    let git = git_info(&git_toplevel(repo).ok()?).ok()?;
    let batches = crate::sql::open_graph(None)
        .ok()?
        .query_arrow_json(
            "SELECT d.name FROM gl_definition d JOIN gl_edge e ON e.target_id = d.id \
             WHERE d.project_id = ?1 AND d.commit_sha = ?2 AND length(d.name) > 3 \
             GROUP BY d.name ORDER BY count(*) DESC, d.name LIMIT 1",
            &[git.project_id.into(), git.commit_sha.into()],
        )
        .ok()?;
    let names = batches
        .first()?
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()?;
    (!names.is_empty()).then(|| names.value(0).to_string())
}

fn format_with_thousands(count: usize) -> String {
    let digits = count.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}
