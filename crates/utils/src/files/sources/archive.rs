//! Gitaly tar.gz input is processed sequentially. Rejected bodies are streamed past, not retained.
//! Entry paths are validated before stripping the archive root. Hard links name archive entries;
//! symlink targets stay virtual. Malformed streams fail rather than returning partial contents.

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
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes)?;
                Ok(bytes)
            });
            into.put(&path, Put::Lazy { size, read })?;
        }
        if any_entry_seen {
            Ok(())
        } else {
            Err(SourceError::Empty)
        }
    }
}

fn relative_path<R: Read>(
    entry: &tar::Entry<'_, R>,
    root: &mut Option<OsString>,
) -> Result<Option<String>, SourceError> {
    let entry_path = entry.path().map_err(std::io::Error::other)?;
    if !crate::fs::is_safe_relative_path(&entry_path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path traversal detected in archive entry",
        )
        .into());
    }
    let shown = entry_path.to_string_lossy();
    if shown.is_empty() {
        return Ok(None);
    }
    let relative = match strip_root(&entry_path, root) {
        Ok(path) => path,
        Err(e) => {
            warn!(entry = %shown, error = %e, "skipping archive entry outside the archive root");
            return Ok(None);
        }
    };
    if relative.as_os_str().is_empty() {
        return Ok(None);
    }
    Ok(Some(relative.to_string_lossy().into_owned()))
}

fn link_target<R: Read>(
    entry: &tar::Entry<'_, R>,
    kind: EntryType,
    root: &mut Option<OsString>,
) -> Result<String, SourceError> {
    let target = entry
        .link_name()
        .map_err(std::io::Error::other)?
        .map(|t| t.to_string_lossy().into_owned())
        .filter(|target| !target.is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive link has no target",
            )
        })?;
    if kind == EntryType::Link && !crate::fs::is_safe_relative_path(Path::new(&target)) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid archive hard link",
        )
        .into());
    }
    Ok(match kind {
        EntryType::Link => format!("/{}", strip_root(Path::new(&target), root)?.display()),
        _ => target,
    })
}

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
