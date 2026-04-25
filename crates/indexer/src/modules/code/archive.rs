use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};

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

#[cfg(test)]
fn extract_tar_gz(data: &[u8], target_dir: &Path) -> Result<(), ArchiveError> {
    let decoder = GzDecoder::new(data);
    unpack_tar(decoder, target_dir)
}

pub fn extract_tar_gz_from_reader<R: Read>(
    reader: R,
    target_dir: &Path,
) -> Result<(), ArchiveError> {
    let decoder = GzDecoder::new(reader);
    unpack_tar(decoder, target_dir)
}

fn unpack_tar<R: Read>(reader: R, target_dir: &Path) -> Result<(), ArchiveError> {
    std::fs::create_dir_all(target_dir)?;

    let mut archive = tar::Archive::new(reader);

    let target_canonical = target_dir
        .canonicalize()
        .map_err(|e| ArchiveError::Io(e.to_string()))?;

    // Tracks the archive root directory. The first entry sets it; all
    // subsequent entries must share the same root or extraction fails.
    let mut archive_root: Option<OsString> = None;

    // Symlinks are deferred until all regular files and directories are
    // extracted. This guarantees no symlink exists on disk during the main
    // extraction loop, so create_dir_all and dest.exists() can never follow
    // a symlink that resolves outside the target directory.
    let mut deferred_symlinks: Vec<(PathBuf, PathBuf)> = Vec::new();

    for entry in archive
        .entries()
        .map_err(|e| ArchiveError::Archive(e.to_string()))?
    {
        let mut entry = entry.map_err(|e| ArchiveError::Archive(e.to_string()))?;

        // Skip PAX metadata entries that aren't real files. XGlobalHeader
        // appears in Gitaly archives and would otherwise be treated as the
        // archive root by strip_archive_root. XHeader is included defensively
        // (the tar crate already consumes it internally, but future versions
        // may not). GNULongName/GNULongLink are consumed by the tar crate's
        // iterator and never reach this loop.
        let entry_type = entry.header().entry_type();
        if entry_type == tar::EntryType::XGlobalHeader || entry_type == tar::EntryType::XHeader {
            continue;
        }

        let entry_path = entry
            .path()
            .map_err(|e| ArchiveError::Archive(e.to_string()))?;

        let entry_path_str = entry_path.to_string_lossy();
        if entry_path_str == "/" || entry_path_str == "." || entry_path_str.is_empty() {
            continue;
        }

        let relative_path = entry_path.strip_prefix("/").unwrap_or(&entry_path);

        // Strip the Gitaly archive root (`<slug>-<ref>/`). Validates all
        // entries share the same root. The root directory entry itself
        // becomes empty after stripping and is skipped.
        let relative_path = strip_archive_root(relative_path, &mut archive_root)?;
        if relative_path.as_os_str().is_empty() {
            continue;
        }

        let dest = target_canonical.join(&relative_path);

        if entry_type == tar::EntryType::Symlink || entry_type == tar::EntryType::Link {
            let link_target = entry
                .link_name()
                .map_err(|e| ArchiveError::Archive(e.to_string()))?
                .map(|cow| cow.into_owned())
                .unwrap_or_default();
            deferred_symlinks.push((dest, link_target));
            continue;
        }

        let dest_canonical = if dest.exists() {
            dest.canonicalize()
                .map_err(|e| ArchiveError::Io(e.to_string()))?
        } else {
            gkg_utils::fs::safe_create_dir_all(&dest, &target_canonical)
                .map_err(|e| ArchiveError::Archive(e.to_string()))?
        };

        if !dest_canonical.starts_with(&target_canonical) {
            return Err(ArchiveError::Archive(format!(
                "path traversal detected: {}",
                relative_path.display()
            )));
        }

        entry
            .unpack(&dest_canonical)
            .map_err(|e| ArchiveError::Archive(e.to_string()))?;
    }

    // Create symlinks now that all regular files and directories are in place.
    // Earlier symlinks in the list can redirect later create_dir_all calls,
    // so each iteration validates the path via safe_create_dir_all.
    for (link_path, target) in deferred_symlinks {
        gkg_utils::fs::safe_create_dir_all(&link_path, &target_canonical)
            .map_err(|e| ArchiveError::Archive(e.to_string()))?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link_path)
            .map_err(|e| ArchiveError::Io(e.to_string()))?;
    }

    gkg_utils::fs::validate_symlinks(&target_canonical)
        .map_err(|e| ArchiveError::Archive(e.to_string()))?;

    Ok(())
}

/// Strip the Gitaly archive root prefix from a path during extraction.
///
/// On the first entry, detects the root directory name and validates it
/// matches the `<slug>-<ref>` pattern. Subsequent entries must share the
/// same root or extraction fails. Returns the path with the root stripped,
/// or an empty path for the root directory entry itself (which callers skip).
fn strip_archive_root(
    path: &Path,
    detected_root: &mut Option<OsString>,
) -> Result<PathBuf, ArchiveError> {
    let mut components = path.components();
    let first = match components.next() {
        Some(c) => c.as_os_str().to_os_string(),
        None => return Ok(PathBuf::new()),
    };

    match detected_root {
        None => {
            // First entry -- record the root directory name.
            *detected_root = Some(first);
        }
        Some(expected) if first != *expected => {
            return Err(ArchiveError::Archive(format!(
                "archive entry '{}' is not under the expected root directory '{}'",
                path.display(),
                expected.to_string_lossy()
            )));
        }
        _ => {}
    }

    Ok(components.as_path().to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    enum Entry<'a> {
        File(&'a str, &'a [u8]),
        Symlink(&'a str, &'a str),
    }

    fn build_archive(entries: &[Entry]) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        for entry in entries {
            match entry {
                Entry::File(path, content) => {
                    let mut h = tar::Header::new_gnu();
                    h.set_size(content.len() as u64);
                    h.set_mode(0o644);
                    h.set_cksum();
                    tar_builder.append_data(&mut h, path, *content).unwrap();
                }
                Entry::Symlink(path, target) => {
                    let mut h = tar::Header::new_gnu();
                    h.set_entry_type(tar::EntryType::Symlink);
                    h.set_size(0);
                    h.set_mode(0o777);
                    h.set_cksum();
                    tar_builder.append_link(&mut h, *path, *target).unwrap();
                }
            }
        }
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn extracts_and_strips_archive_root() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/src/lib.rs", b"pub mod lib;"),
        ]);

        extract_tar_gz(&data, dir.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap(),
            "fn main() {}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/lib.rs")).unwrap(),
            "pub mod lib;"
        );
        assert!(!dir.path().join("project-main").exists());
    }

    #[test]
    fn skips_pax_global_header() {
        let dir = tempfile::tempdir().unwrap();

        let mut tar_builder = tar::Builder::new(Vec::new());

        // Add a PAX global header entry (like Gitaly produces)
        let pax_content = b"comment=some metadata\n";
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::XGlobalHeader);
        header.set_size(pax_content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "pax_global_header", &pax_content[..])
            .unwrap();

        // Add actual files under the archive root
        let content = b"fn main() {}";
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "project-main/src/main.rs", &content[..])
            .unwrap();

        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        let data = encoder.finish().unwrap();

        extract_tar_gz(&data, dir.path()).unwrap();

        let content = std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[test]
    fn skips_pax_per_file_header() {
        let dir = tempfile::tempdir().unwrap();

        let mut tar_builder = tar::Builder::new(Vec::new());

        // Add a PAX per-file extended header (XHeader) before the root entry.
        // The tar crate consumes these internally, but we skip them defensively.
        let pax_content = b"path=project-main/src/main.rs\n";
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::XHeader);
        header.set_size(pax_content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "PaxHeader/main.rs", &pax_content[..])
            .unwrap();

        let content = b"fn main() {}";
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "project-main/src/main.rs", &content[..])
            .unwrap();

        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        let data = encoder.finish().unwrap();

        extract_tar_gz(&data, dir.path()).unwrap();

        let content = std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[test]
    fn rejects_inconsistent_archive_root() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root-a/file1.rs", b"a"),
            Entry::File("root-b/file2.rs", b"b"),
        ]);

        let result = extract_tar_gz(&data, dir.path());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not under the expected root"),
        );
    }

    fn build_tar_gz_with_raw_path(path: &str, content: &[u8]) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
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
    fn handles_root_dir_only_archive() {
        let dir = tempfile::tempdir().unwrap();
        // Archive with only the root directory entry (no files)
        let mut tar_builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_size(0);
        header.set_mode(0o755);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "project-main/", &[] as &[u8])
            .unwrap();
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        let data = encoder.finish().unwrap();

        // Should succeed but extract nothing
        extract_tar_gz(&data, dir.path()).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        // After stripping root, the remaining path still attempts traversal
        let data = build_tar_gz_with_raw_path("root/../../escape.txt", b"malicious");

        let result = extract_tar_gz(&data, dir.path());

        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("path traversal"), "got: {error}");
    }

    #[test]
    fn rejects_symlink_escaping_target_directory() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"hello"),
            Entry::Symlink("root/escape", outside.path().to_str().unwrap()),
        ]);

        let result = extract_tar_gz(&data, dir.path());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("symlink target escapes")
        );
    }

    #[test]
    fn rejects_chained_symlink_attack() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"legit"),
            Entry::Symlink("root/a", "."),
            Entry::Symlink("root/b", "a/.."),
        ]);

        let result = extract_tar_gz(&data, dir.path());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("symlink target escapes")
        );
    }

    #[test]
    fn prevents_create_dir_all_through_escaping_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::Symlink("root/escape", outside.path().to_str().unwrap()),
            Entry::File("root/escape/sub/pwned.txt", b"pwned"),
        ]);

        let result = extract_tar_gz(&data, dir.path());
        assert!(result.is_err());
        assert!(!outside.path().join("sub").exists());
    }

    #[test]
    fn prevents_deferred_symlink_redirect_of_create_dir_all() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"legit"),
            Entry::Symlink("root/a", outside.path().to_str().unwrap()),
            Entry::Symlink("root/a/b/link", "foo"),
        ]);

        let result = extract_tar_gz(&data, dir.path());
        assert!(result.is_err());
        assert!(
            !outside.path().join("b").exists(),
            "create_dir_all must not follow symlink outside target"
        );
    }

    #[test]
    fn allows_valid_internal_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/src/lib.rs", b"real content"),
            Entry::Symlink("root/bin/run", "../src/lib.rs"),
        ]);

        extract_tar_gz(&data, dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("bin/run")).unwrap(),
            "real content"
        );
    }

    #[test]
    fn tolerates_dangling_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"hello"),
            Entry::Symlink("root/dangling", "nonexistent/file.rs"),
        ]);

        extract_tar_gz(&data, dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("legit.txt")).unwrap(),
            "hello"
        );
        assert!(!dir.path().join("dangling").exists());
    }
}
