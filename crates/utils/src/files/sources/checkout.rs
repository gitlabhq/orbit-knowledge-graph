//! A checkout on disk. Listing is cheap and reading is not, so every file is
//! linked where it is; the store reads it now only if a pass asks to.

use std::io::ErrorKind;
use std::path::Path;
use std::sync::Mutex;

use ignore::{WalkBuilder, WalkState};
use rayon::prelude::*;
use tracing::warn;

use super::{Loading, Put, Source, SourceError, Tag};

/// Every file below the root with git's listing semantics: .gitignore,
/// .git/info/exclude and dotfiles honored; ripgrep .ignore and ancestor
/// ignores not; `.git` itself never. The walk is parallel and puts each
/// file in as it is found.
pub struct Checkout<'a>(pub &'a Path);

/// The named files below the root: a change set, where a walk is not wanted.
pub struct Changed<'a> {
    pub root: &'a Path,
    pub paths: Vec<String>,
}

impl Source for Checkout<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let root = self.0;
        let failed: Mutex<Option<SourceError>> = Mutex::new(None);
        let fail = |error: SourceError| {
            *failed.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
            WalkState::Quit
        };
        WalkBuilder::new(root)
            .hidden(false)
            .git_ignore(true)
            .git_exclude(true)
            .ignore(false)
            .git_global(false)
            .parents(false)
            .require_git(false)
            .filter_entry(|entry| entry.file_name() != ".git")
            .build_parallel()
            .run(|| {
                Box::new(|entry| {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(e) if vanished(&e) => {
                            warn!(error = %e, "skipping an entry that vanished during the walk");
                            return WalkState::Continue;
                        }
                        Err(e) => return fail(SourceError::Io(std::io::Error::other(e))),
                    };
                    let is_file_or_link = entry
                        .file_type()
                        .is_some_and(|kind| kind.is_file() || kind.is_symlink());
                    let Ok(path) = entry.path().strip_prefix(root) else {
                        return WalkState::Continue;
                    };
                    if !is_file_or_link {
                        return WalkState::Continue;
                    }
                    match put(root, &path.to_string_lossy(), into) {
                        Ok(()) => WalkState::Continue,
                        Err(e) => fail(e),
                    }
                })
            });
        failed
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .map_or(Ok(()), Err)
    }
}

impl Source for Changed<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        self.paths
            .into_par_iter()
            .try_for_each(|path| put(self.root, &path, into))
    }
}

/// A live checkout moves under us: something gone since it was listed is
/// skipped. Anything else (a directory we may not read, an I/O fault) would
/// make the graph claim a repository it did not see, so the run fails.
fn vanished(error: &ignore::Error) -> bool {
    error
        .io_error()
        .is_some_and(|io| io.kind() == ErrorKind::NotFound)
}

fn put<T: Tag>(root: &Path, path: &str, into: &Loading<T>) -> Result<(), SourceError> {
    let on_disk = root.join(path);
    let metadata = match on_disk.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(e) => {
            warn!(path, error = %e, "skipping a file that vanished during discovery");
            return Ok(());
        }
    };
    if !metadata.is_symlink() {
        return into.put(
            path,
            Put::OnDisk {
                path: on_disk,
                size: metadata.len(),
            },
        );
    }
    match std::fs::read_link(&on_disk) {
        Ok(target) => into.put(path, Put::Symlink(target.to_string_lossy().into_owned())),
        Err(e) => {
            warn!(path, error = %e, "skipping a symlink that vanished during discovery");
            Ok(())
        }
    }
}
