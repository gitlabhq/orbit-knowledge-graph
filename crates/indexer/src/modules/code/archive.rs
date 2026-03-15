use std::io::Read;
use std::path::{Component, Path, PathBuf};

use flate2::read::GzDecoder;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("archive error: {0}")]
    Archive(String),
}

impl From<std::io::Error> for ArchiveError {
    fn from(e: std::io::Error) -> Self {
        ArchiveError::Io(e.to_string())
    }
}

pub fn extract_tar_gz(data: &[u8], target_dir: &Path) -> Result<(), ArchiveError> {
    let decoder = GzDecoder::new(data);
    unpack_tar(decoder, target_dir)
}

fn unpack_tar<R: Read>(reader: R, target_dir: &Path) -> Result<(), ArchiveError> {
    std::fs::create_dir_all(target_dir)?;

    let mut archive = tar::Archive::new(reader);

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

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    fn build_tar_gz(entries: Vec<(&str, &[u8])>) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar_builder.append_data(&mut header, path, content).unwrap();
        }
        let tar_bytes = tar_builder.into_inner().unwrap();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn build_tar_gz_with_symlink(file_path: &str, link_path: &str, link_target: &str) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());

        let content = b"hello";
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, file_path, &content[..])
            .unwrap();

        let mut link_header = tar::Header::new_gnu();
        link_header.set_entry_type(tar::EntryType::Symlink);
        link_header.set_size(0);
        link_header.set_mode(0o777);
        link_header.set_cksum();
        tar_builder
            .append_link(&mut link_header, link_path, link_target)
            .unwrap();

        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn extracts_valid_archive() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_tar_gz(vec![("src/main.rs", b"fn main() {}")]);

        extract_tar_gz(&data, dir.path()).unwrap();

        let content = std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    fn build_tar_gz_with_raw_path(path: &str, content: &[u8]) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        // Write the path directly into the header bytes to bypass tar crate validation
        let path_bytes = path.as_bytes();
        let raw = header.as_mut_bytes();
        raw[..path_bytes.len()].copy_from_slice(path_bytes);
        header.set_cksum();
        tar_builder
            .append(&header, std::io::Cursor::new(content))
            .unwrap();
        let tar_bytes = tar_builder.into_inner().unwrap();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_tar_gz_with_raw_path("../escape.txt", b"malicious");

        let result = extract_tar_gz(&data, dir.path());

        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("path traversal"), "got: {error}");
    }

    #[test]
    fn rejects_symlink_escaping_target_directory() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_tar_gz_with_symlink("legit.txt", "escape", "../../etc/passwd");

        let result = extract_tar_gz(&data, dir.path());

        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("symlink target escapes"), "got: {error}");
    }
}
