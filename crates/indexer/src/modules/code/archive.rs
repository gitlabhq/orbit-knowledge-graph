use std::fs::File;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("archive error: {0}")]
    Archive(String),
}

fn normalize_path(path: &Path) -> PathBuf {
    path.components().fold(PathBuf::new(), |mut acc, c| {
        match c {
            Component::ParentDir => {
                acc.pop();
            }
            Component::CurDir => {}
            _ => acc.push(c),
        }
        acc
    })
}

pub fn unpack_archive(archive_path: &Path, target_dir: &Path) -> Result<(), ArchiveError> {
    let file = File::open(archive_path).map_err(|e| ArchiveError::Io(e.to_string()))?;
    let mut archive = tar::Archive::new(file);

    let target_canonical = target_dir
        .canonicalize()
        .map_err(|e| ArchiveError::Io(e.to_string()))?;

    for entry in archive
        .entries()
        .map_err(|e| ArchiveError::Archive(e.to_string()))?
    {
        let mut entry = entry.map_err(|e| ArchiveError::Archive(e.to_string()))?;
        let entry_path = entry
            .path()
            .map_err(|e| ArchiveError::Archive(e.to_string()))?;

        let entry_path_str = entry_path.to_string_lossy();
        if entry_path_str == "/" || entry_path_str == "." || entry_path_str.is_empty() {
            continue;
        }

        let relative_path = entry_path.strip_prefix("/").unwrap_or(&entry_path);
        let dest = target_canonical.join(relative_path);

        let dest_canonical = if dest.exists() {
            dest.canonicalize()
                .map_err(|e| ArchiveError::Io(e.to_string()))?
        } else if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| ArchiveError::Io(e.to_string()))?;
            parent
                .canonicalize()
                .map_err(|e| ArchiveError::Io(e.to_string()))?
                .join(dest.file_name().unwrap_or_default())
        } else {
            dest.clone()
        };

        if !dest_canonical.starts_with(&target_canonical) {
            return Err(ArchiveError::Archive(format!(
                "path traversal detected: {}",
                relative_path.display()
            )));
        }

        let entry_type = entry.header().entry_type();
        let is_link = entry_type == tar::EntryType::Symlink || entry_type == tar::EntryType::Link;
        if let (true, Ok(Some(link_name))) = (is_link, entry.link_name()) {
            let link_target = if link_name.is_absolute() {
                link_name.to_path_buf()
            } else {
                dest_canonical
                    .parent()
                    .unwrap_or(&target_canonical)
                    .join(&link_name)
            };

            let normalized = normalize_path(&link_target);
            if !normalized.starts_with(&target_canonical) {
                return Err(ArchiveError::Archive(format!(
                    "symlink target escapes target directory: {} -> {}",
                    relative_path.display(),
                    link_name.display()
                )));
            }
        }

        entry
            .unpack(&dest_canonical)
            .map_err(|e| ArchiveError::Archive(e.to_string()))?;
    }

    std::fs::remove_file(archive_path).map_err(|e| ArchiveError::Io(e.to_string()))?;
    Ok(())
}
