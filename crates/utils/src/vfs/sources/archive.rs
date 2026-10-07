//! Gitaly tar.gz input is processed sequentially. Rejected bodies are streamed past, not retained.
//! Entry paths are validated before stripping the archive root. Hard links name archive entries;
//! symlink targets stay virtual. Malformed streams fail rather than returning partial contents.

use std::io::Read;
use std::sync::Arc;

use flate2::read::GzDecoder;
use tar::EntryType;
use tracing::warn;
use typed_path::{Utf8Component, Utf8Components, Utf8UnixComponent, Utf8UnixPath};

use super::{Loading, Put, Source, SourceError, Tag};

pub struct Archive<R: Read>(pub R);

impl<R: Read> Source for Archive<R> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let mut archive = tar::Archive::new(GzDecoder::new(self.0));
        let mut root: Option<String> = None;
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
            let entry_path = entry.path_bytes().into_owned();
            let entry_path = std::str::from_utf8(&entry_path)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let Some(path) = relative_path(entry_path, &mut root)? else {
                continue;
            };
            if path.is_empty() {
                continue;
            }
            if is_link {
                let target = entry
                    .link_name_bytes()
                    .filter(|target| !target.is_empty())
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "archive link has no target",
                        )
                    })?;
                let target = std::str::from_utf8(&target)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let target = if kind == EntryType::Link {
                    let relative = relative_path(target, &mut root)?
                        .ok_or_else(|| std::io::Error::other("archive hard link outside root"))?;
                    format!("/{relative}")
                } else {
                    target.to_owned()
                };
                into.put(path, Put::Symlink(target))?;
                continue;
            }
            let size = entry.size();
            let mut stream_error = None;
            let read = Box::new(|| {
                let mut bytes = Vec::new();
                let result = entry.read_to_end(&mut bytes).and_then(|_| {
                    if bytes.len() as u64 == size {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "truncated archive entry",
                        ))
                    }
                });
                if let Err(error) = result {
                    let error = Arc::new(error);
                    stream_error = Some(error.clone());
                    return Err(std::io::Error::new(error.kind(), error));
                }
                Ok(bytes)
            });
            into.put(path, Put::ReadAndStore { size, read })?;
            if let Some(error) = stream_error {
                return Err(std::io::Error::new(error.kind(), error).into());
            }
        }
        if any_entry_seen {
            Ok(())
        } else {
            Err(SourceError::Empty)
        }
    }
}

fn relative_path<'a>(
    path: &'a str,
    root: &mut Option<String>,
) -> Result<Option<&'a str>, SourceError> {
    let path = Utf8UnixPath::new(path);
    if !path
        .components()
        .all(|part| matches!(part, Utf8UnixComponent::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path traversal detected in archive entry",
        )
        .into());
    }
    let mut components = path.components();
    let Some(first) = components.next() else {
        return Ok(None);
    };
    if first.as_str() != root.get_or_insert_with(|| first.as_str().to_owned()) {
        warn!(entry = %path, "skipping archive entry outside the archive root");
        return Ok(None);
    }
    Ok(Some(components.as_str()))
}
