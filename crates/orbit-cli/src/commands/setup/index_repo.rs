use std::path::PathBuf;

use anyhow::Result;

use crate::commands::index::{index_command_line, index_with_progress};
use crate::tui;
use crate::workspace::git_toplevel;

pub(super) enum IndexOutcome {
    OutsideRepository,
    NotIndexed,
    Indexed { suggested_grep: Option<String> },
}

pub(super) fn current_repository_root() -> Option<PathBuf> {
    git_toplevel(&std::env::current_dir().ok()?).ok()
}

pub(super) fn index_repository(repo_root: PathBuf) -> Result<IndexOutcome> {
    match index_with_progress(repo_root, None) {
        Ok(suggested_grep) => Ok(IndexOutcome::Indexed { suggested_grep }),
        Err(error) if tui::is_cancelled(&error) => Err(error),
        Err(error) => {
            tui::error(format!("{} failed: {error:#}", index_command_line(".")));
            Ok(IndexOutcome::NotIndexed)
        }
    }
}
