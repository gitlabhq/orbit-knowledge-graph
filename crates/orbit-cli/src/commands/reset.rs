//! `orbit workspace reset`: removes the local graph, its manifest, and the index state, after
//! the running indexes and readers release them. Settings and DuckDB extensions stay.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::commands::setup::spec::launcher;
use crate::database::{lock_database_waiting, wait_timeout};
use crate::tui;
use crate::workspace::Workspace;

pub(crate) fn run(yes: bool, dry_run: bool) -> Result<()> {
    let interactive = tui::can_prompt(yes || dry_run)?;
    let graph_paths = Workspace::open_default()?.graph_paths();
    tui::intro("Orbit workspace reset")?;
    if graph_paths.is_empty() {
        return tui::outro("The local graph is already empty. Nothing to remove.");
    }

    let listed = graph_paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    tui::card("Local graph", listed)?;
    if dry_run {
        return tui::outro("Dry run: nothing removed.");
    }
    if interactive && !tui::confirm("Remove the local graph?", false)? {
        return tui::outro_cancel("Nothing removed.");
    }

    let _locks = locked_databases(&graph_paths)?
        .iter()
        .map(|database| lock_database_waiting(database, wait_timeout()))
        .collect::<Result<Vec<_>>>()?;
    for path in &graph_paths {
        remove(path).with_context(|| format!("failed to remove {}", path.display()))?;
    }
    tui::outro(format!(
        "Removed. `{} workspace index` builds the graph again.",
        launcher()
    ))
}

/// Shallow paths first, so index state and the manifest lock before the revisions.
fn locked_databases(graph_paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut databases = Vec::new();
    let mut pending: VecDeque<PathBuf> = graph_paths.iter().cloned().collect();
    while let Some(path) = pending.pop_front() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                pending.push_back(entry?.path());
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "lock")
        {
            databases.push(path.with_extension(""));
        }
    }
    Ok(databases)
}

fn remove(path: &Path) -> std::io::Result<()> {
    match path.is_dir() {
        true => std::fs::remove_dir_all(path),
        false => std::fs::remove_file(path),
    }
}
