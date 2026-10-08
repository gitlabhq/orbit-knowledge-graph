use std::path::{Path, PathBuf};

use anyhow::Result;

use super::Target;
use crate::commands::index::{index_command_line, index_with_progress};
use crate::tui;
use crate::workspace::git_toplevel;

pub(super) enum IndexOutcome {
    OutsideRepository,
    /// `index_path` is what the user should pass to `orbit index`.
    NotIndexed {
        index_path: String,
    },
    Indexed {
        suggested_grep: Option<String>,
    },
}

/// A project setup is about its project root; a global setup is about the
/// folder the command runs in.
pub(super) fn repository_root(target: &Target) -> Option<PathBuf> {
    let folder = match target {
        Target::Project(root) => root.clone(),
        Target::Global => std::env::current_dir().ok()?,
    };
    git_toplevel(&folder).ok()
}

pub(super) fn index_path_for(target: &Target, repo_root: &Path) -> String {
    let working_folder = std::env::current_dir()
        .ok()
        .and_then(|folder| dunce::canonicalize(folder).ok());
    match target {
        Target::Project(_) if working_folder.as_deref() != Some(repo_root) => {
            repo_root.display().to_string()
        }
        _ => ".".to_string(),
    }
}

pub(super) fn index_repository(repo_root: PathBuf, index_path: String) -> Result<IndexOutcome> {
    match index_with_progress(repo_root, None) {
        Ok(suggested_grep) => Ok(IndexOutcome::Indexed { suggested_grep }),
        Err(error) if tui::is_cancelled(&error) => Err(error),
        Err(error) => {
            tui::error(format!(
                "{} failed: {error:#}",
                index_command_line(&index_path)
            ));
            Ok(IndexOutcome::NotIndexed { index_path })
        }
    }
}
