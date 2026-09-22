use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Result, bail};
use serde::Deserialize;

use super::spec;
use crate::commands::index::most_referenced_definition;
use crate::tui::format_with_thousands;
use crate::workspace::Workspace;

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
        suggested_grep: most_referenced_definition(cwd, None),
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
