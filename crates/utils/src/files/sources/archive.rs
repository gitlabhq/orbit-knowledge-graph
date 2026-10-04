//! A Gitaly tar.gz: one sequential pass over the inflating stream. Each
//! entry is offered lazily, so a file the header already decided is never
//! inflated into memory.

use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::EntryType;
use tracing::warn;

use super::{Loading, Put, Source, SourceError, Tag};

pub struct Archive<R: Read>(pub R);

impl<R: Read> Source for Archive<R> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let mut archive = tar::Archive::new(GzDecoder::new(self.0));
        let mut root: Option<OsString> = None;
        let mut any_entry_seen = false;
        let entries = archive.entries().map_err(std::io::Error::other)?;

        for entry in entries {
            let mut entry = match entry {
                Ok(entry) => entry,
                Err(e) if !any_entry_seen && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!(error = %e, "archive stream truncated before first entry; treating as empty");
                    return Err(SourceError::Empty);
                }
                Err(e) => return Err(SourceError::Io(e)),
            };
            any_entry_seen = true;

            let kind = entry.header().entry_type();
            let is_link = matches!(kind, EntryType::Symlink | EntryType::Link);
            // Directories exist because files are in them; PAX headers,
            // devices and fifos have no place in a checkout.
            if kind != EntryType::Regular && !is_link {
                continue;
            }
            let Some(path) = relative_path(&entry, &mut root)? else {
                continue;
            };
            if is_link {
                let target = link_target(&entry, kind, &mut root)?;
                into.put(&path, Put::Symlink(target))?;
                continue;
            }
            let size = entry.size();
            let read = Box::new(move || {
                let mut bytes = Vec::with_capacity(size as usize);
                entry.read_to_end(&mut bytes)?;
                Ok(bytes)
            });
            into.put(&path, Put::Lazy { size, read })?;
        }
        Ok(())
    }
}

/// The entry's path below the archive root, or `None` for the root itself
/// and for an entry under a different root, which Gitaly never produces.
fn relative_path<R: Read>(
    entry: &tar::Entry<'_, R>,
    root: &mut Option<OsString>,
) -> Result<Option<String>, SourceError> {
    let entry_path = entry.path().map_err(std::io::Error::other)?;
    let shown = entry_path.to_string_lossy();
    if shown == "/" || shown == "." || shown.is_empty() {
        return Ok(None);
    }
    let below_root = entry_path.strip_prefix("/").unwrap_or(&entry_path);
    let relative = match strip_root(below_root, root) {
        Ok(path) => path,
        Err(e) => {
            warn!(entry = %shown, error = %e, "skipping archive entry outside the archive root");
            return Ok(None);
        }
    };
    if relative.as_os_str().is_empty() {
        return Ok(None);
    }
    if !crate::fs::is_safe_relative_path(&relative) {
        return Err(SourceError::Io(std::io::Error::other(format!(
            "path traversal detected: {}",
            relative.display()
        ))));
    }
    Ok(Some(relative.to_string_lossy().into_owned()))
}

/// A hard link names another archive entry under the root; a symlink's
/// target is already relative to the link.
fn link_target<R: Read>(
    entry: &tar::Entry<'_, R>,
    kind: EntryType,
    root: &mut Option<OsString>,
) -> Result<String, SourceError> {
    let target = entry
        .link_name()
        .map_err(std::io::Error::other)?
        .map(|t| t.to_string_lossy().into_owned())
        .unwrap_or_default();
    Ok(match kind {
        EntryType::Link => format!("/{}", strip_root(Path::new(&target), root)?.display()),
        _ => target,
    })
}

/// Strip the Gitaly archive root (`<slug>-<ref>/`). The first entry records
/// the root; later entries must share it.
fn strip_root(path: &Path, root: &mut Option<OsString>) -> Result<PathBuf, SourceError> {
    let mut components = path.components();
    let Some(first) = components.next() else {
        return Ok(PathBuf::new());
    };
    let first = first.as_os_str().to_os_string();
    match root {
        None => *root = Some(first),
        Some(expected) if first != *expected => {
            return Err(SourceError::Io(std::io::Error::other(format!(
                "archive entry '{}' is not under the expected root directory '{}'",
                path.display(),
                expected.to_string_lossy()
            ))));
        }
        _ => {}
    }
    Ok(components.as_path().to_path_buf())
}
