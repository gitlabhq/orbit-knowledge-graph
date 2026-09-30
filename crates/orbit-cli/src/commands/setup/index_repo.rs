use anyhow::Result;

use super::spec;
use crate::commands::index::index_with_progress;
use crate::tui;
use crate::workspace::git_toplevel;

pub(super) struct Indexed {
    pub(super) suggested_grep: Option<String>,
}

pub(super) fn index_command_line() -> String {
    format!("{} index .", spec::launcher())
}

pub(super) fn index_current_repository() -> Result<Option<Indexed>> {
    let Some(repo_root) = std::env::current_dir()
        .ok()
        .and_then(|cwd| git_toplevel(&cwd).ok())
    else {
        return Ok(None);
    };
    match index_with_progress(repo_root, None) {
        Ok(suggested_grep) => Ok(Some(Indexed { suggested_grep })),
        Err(error) if tui::is_cancelled(&error) => Err(error),
        Err(error) => {
            tui::error(format!("{} failed: {error:#}", index_command_line()));
            Ok(None)
        }
    }
}
