//! Checkout walks with git ignore rules; Changed loads explicit relative paths without walking.
//! Metadata and link targets are read through pinned parent descriptors without following links.
//! Missing files are skipped; other I/O errors fail loading. Real host paths remain separate from
//! lossy inventory keys. Relative roots are canonicalized once so later reads do not depend on cwd.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ignore::{WalkBuilder, WalkState};
use rayon::prelude::*;
use rustix::fs::{AtFlags, FileType, readlinkat, statat};
use tracing::warn;

use super::super::disk;
use super::super::path::is_safe_relative_path;
use super::{Loading, Put, Source, SourceError, Tag};

pub struct Checkout<'a>(pub &'a Path);

pub struct Changed<'a> {
    pub root: &'a Path,
    pub paths: Vec<String>,
}

impl Source for Checkout<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let root = self.0.canonicalize()?;
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
                    let key = path.to_string_lossy().into_owned();
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

impl Source for Changed<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let root = self.root.canonicalize()?;
        self.paths.into_par_iter().try_for_each(|path| {
            if !is_safe_relative_path(Path::new(&path)) || path.is_empty() {
                return Err(
                    std::io::Error::new(ErrorKind::InvalidInput, "invalid changed path").into(),
                );
            }
            put(root.join(&path), &path, into)
        })
    }
}

fn put<T: Tag>(on_disk: PathBuf, path: &str, into: &Loading<T>) -> Result<(), SourceError> {
    let parent = match disk::open_parent(&on_disk) {
        Ok(parent) => parent,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let name = on_disk
        .file_name()
        .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "missing filename"))?;
    let metadata = match statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(rustix::io::Errno::NOENT) => return Ok(()),
        Err(e) => return Err(std::io::Error::from(e).into()),
    };
    let kind = FileType::from_raw_mode(metadata.st_mode);
    if kind == FileType::RegularFile {
        return into.put(
            path,
            Put::OnDisk {
                path: on_disk,
                size: metadata.st_size as u64,
            },
        );
    }
    if kind != FileType::Symlink {
        return Ok(());
    }
    match readlinkat(&parent, name, Vec::new()) {
        Ok(target) => into.put(path, Put::Symlink(target.to_string_lossy().into_owned())),
        Err(rustix::io::Errno::NOENT) => Ok(()),
        Err(e) => Err(std::io::Error::from(e).into()),
    }
}
