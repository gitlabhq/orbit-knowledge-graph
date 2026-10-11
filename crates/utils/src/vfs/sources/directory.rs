use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use ignore::{WalkBuilder, WalkState};
use tracing::warn;
use typed_path::Utf8UnixPathBuf;

use super::{Loading, Put, Source, SourceError, Tag};
use crate::safe_fs;

pub struct Directory<'a>(pub &'a Path);

impl Source for Directory<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let root = dunce::canonicalize(self.0)?;
        let failed: Mutex<Option<SourceError>> = Mutex::new(None);
        let fail = |error: SourceError| {
            failed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_or_insert(error);
            WalkState::Quit
        };
        WalkBuilder::new(&root)
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
                        Err(e)
                            if e.io_error()
                                .is_some_and(|io| io.kind() == ErrorKind::NotFound) =>
                        {
                            warn!(error = %e, "skipping an entry deleted during the walk");
                            return WalkState::Continue;
                        }
                        Err(e) => return fail(SourceError::Io(std::io::Error::other(e))),
                    };
                    let is_file_or_link = entry
                        .file_type()
                        .is_some_and(|kind| kind.is_file() || kind.is_symlink());
                    let Ok(path) = entry.path().strip_prefix(&root) else {
                        return WalkState::Continue;
                    };
                    if !is_file_or_link {
                        return WalkState::Continue;
                    }
                    let key = match virtual_path(path) {
                        Ok(key) => key,
                        Err(error) => return fail(error.into()),
                    };
                    match put(entry.into_path(), &key, into) {
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

pub(super) fn put<T: Tag>(
    on_disk: PathBuf,
    path: &str,
    into: &Loading<T>,
) -> Result<(), SourceError> {
    let entry = match safe_fs::inspect(&on_disk) {
        Ok(entry) => entry,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    match entry {
        Some(safe_fs::Entry::File(file)) => into.put(
            path,
            Put::ReadOnDemand {
                size: file.size(),
                read: Arc::new(move |max_bytes| file.read(max_bytes)),
            },
        ),
        Some(safe_fs::Entry::Symlink(target)) => {
            into.put(path, Put::Symlink(virtual_path(&target)?))
        }
        None => Ok(()),
    }
}

pub(super) fn virtual_path(path: &Path) -> std::io::Result<String> {
    let mut virtual_path = Utf8UnixPathBuf::new();
    for component in path.components() {
        let part = match component {
            std::path::Component::Prefix(_) => {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    "host prefix cannot name a virtual path",
                ));
            }
            std::path::Component::RootDir => "/",
            _ => component.as_os_str().to_str().ok_or_else(|| {
                std::io::Error::new(ErrorKind::InvalidData, "source path is not UTF-8")
            })?,
        };
        virtual_path.push(part);
    }
    Ok(virtual_path.into_string())
}
