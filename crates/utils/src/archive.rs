//! Tar.gz extraction as a [`FileStreamHooks`] source: untar, safety-check each
//! path, hand the entry to the hooks, and materialize only the files they
//! [`Decision::Keep`]. No filtering of its own.

use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tracing::warn;

use crate::fs_stream::{Decision, FileInventoryEntry, FileStreamHooks, StreamError, step};

/// Extract a gzipped tar from `reader` into `target_dir`, running every regular
/// file through `hooks`. `Keep` files are written to disk; every non-dropped
/// file (and symlink) is returned in the inventory.
pub fn extract_tar_gz<R: Read, H: FileStreamHooks>(
    reader: R,
    target_dir: &Path,
    hooks: &mut H,
) -> Result<Vec<FileInventoryEntry>, StreamError> {
    std::fs::create_dir_all(target_dir)?;

    let mut archive = tar::Archive::new(GzDecoder::new(reader));
    let target_canonical = target_dir.canonicalize()?;

    // The first entry sets the Gitaly archive root (`<slug>-<ref>/`); all others
    // must share it.
    let mut archive_root: Option<OsString> = None;

    // Symlinks are deferred until all regular files and directories exist, so no
    // symlink is on disk during the main loop to redirect a create_dir_all or
    // dest.exists() outside the target.
    let mut deferred_symlinks: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut inventory = Vec::new();
    let mut content = Vec::new();

    // False + a truncation-shaped error on the first `next()` means the body
    // ended before any tar header could be read (empty/truncated repo).
    let mut any_entry_seen = false;
    let entries = archive
        .entries()
        .map_err(|e| StreamError::Io(std::io::Error::other(e)))?;

    for entry in entries {
        let mut entry = match entry {
            Ok(e) => {
                any_entry_seen = true;
                e
            }
            Err(e) if !any_entry_seen && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                warn!(error = %e, "archive stream truncated before first entry; treating as empty");
                return Err(StreamError::Empty);
            }
            Err(e) => return Err(StreamError::Io(e)),
        };

        let entry_type = entry.header().entry_type();
        // PAX metadata entries are not real files; XGlobalHeader would otherwise
        // be mistaken for the archive root.
        if entry_type == tar::EntryType::XGlobalHeader || entry_type == tar::EntryType::XHeader {
            continue;
        }

        let entry_path = entry.path().map_err(std::io::Error::other)?;
        let entry_path_str = entry_path.to_string_lossy();
        if entry_path_str == "/" || entry_path_str == "." || entry_path_str.is_empty() {
            continue;
        }
        let relative_path = entry_path.strip_prefix("/").unwrap_or(&entry_path);
        let relative_path = strip_archive_root(relative_path, &mut archive_root)?;
        if relative_path.as_os_str().is_empty() {
            continue;
        }
        if !crate::fs::is_safe_relative_path(&relative_path) {
            return Err(StreamError::Io(std::io::Error::other(format!(
                "path traversal detected: {}",
                relative_path.display()
            ))));
        }
        let dest = target_canonical.join(&relative_path);

        if entry_type == tar::EntryType::Symlink || entry_type == tar::EntryType::Link {
            // A symlink is a node, never a parse candidate — we'd be parsing the
            // link, not source. (The dir walk likewise never yields symlinks.)
            inventory.push(FileInventoryEntry {
                path: relative_path.to_string_lossy().into_owned(),
                size: entry.header().size().unwrap_or(0),
                decision: Decision::ListOnly,
            });
            let link_target = entry
                .link_name()
                .map_err(std::io::Error::other)?
                .map(|cow| cow.into_owned())
                .unwrap_or_default();
            deferred_symlinks.push((dest, link_target));
            continue;
        }

        if entry_type == tar::EntryType::Regular {
            let mut meta = FileInventoryEntry {
                path: relative_path.to_string_lossy().into_owned(),
                size: entry.header().size().unwrap_or(0),
                decision: Decision::Keep,
            };
            // The tar Entry Read stops at the declared entry size, so this is
            // bounded by it; oversize files are settled in `on_header` and never
            // reach the read.
            meta.decision = step(hooks, &meta, &mut content, |buf| {
                entry.read_to_end(buf).map(|_| ())
            })?;
            match meta.decision {
                Decision::Drop => continue,
                Decision::ListOnly => inventory.push(meta),
                Decision::Keep => {
                    let dest_canonical = crate::fs::resolve_dest_within(&target_canonical, &dest)?;
                    let mut file = std::fs::File::create(&dest_canonical)?;
                    file.write_all(&content)?;
                    inventory.push(meta);
                }
            }
            continue;
        }

        let dest_canonical = crate::fs::resolve_dest_within(&target_canonical, &dest)?;
        entry
            .unpack(&dest_canonical)
            .map_err(std::io::Error::other)?;
    }

    for (link_path, target) in deferred_symlinks {
        crate::fs::safe_create_dir_all(&link_path, &target_canonical)
            .map_err(std::io::Error::other)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link_path)?;
    }

    let removed_symlinks =
        crate::fs::validate_symlinks(&target_canonical).map_err(std::io::Error::other)?;
    if !removed_symlinks.is_empty() {
        let removed: std::collections::HashSet<String> = removed_symlinks
            .iter()
            .map(|r| r.relative_path.to_string_lossy().into_owned())
            .collect();
        inventory.retain(|entry| !removed.contains(&entry.path));
    }

    Ok(crate::fs_stream::canonicalize_inventory(inventory))
}

/// Strip the Gitaly archive root (`<slug>-<ref>/`). The first entry records the
/// root; later entries must share it. Returns an empty path for the root entry.
fn strip_archive_root(
    path: &Path,
    detected_root: &mut Option<OsString>,
) -> Result<PathBuf, StreamError> {
    let mut components = path.components();
    let first = match components.next() {
        Some(c) => c.as_os_str().to_os_string(),
        None => return Ok(PathBuf::new()),
    };
    match detected_root {
        None => *detected_root = Some(first),
        Some(expected) if first != *expected => {
            return Err(StreamError::Io(std::io::Error::other(format!(
                "archive entry '{}' is not under the expected root directory '{}'",
                path.display(),
                expected.to_string_lossy()
            ))));
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

    /// Records every file as Keep — the archive's mechanism with no filtering.
    struct KeepAll;
    impl FileStreamHooks for KeepAll {}

    /// Drops files by extension (header) and by a NUL in content; mirrors the
    /// shape of the production `CodeFilter` without depending on code-graph.
    struct TestFilter;
    impl FileStreamHooks for TestFilter {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Decision {
            if Path::new(&f.path).extension().and_then(|e| e.to_str()) == Some("png") {
                Decision::ListOnly
            } else {
                Decision::Keep
            }
        }
        fn on_content(&mut self, _f: &FileInventoryEntry, content: &[u8]) -> Decision {
            if content.contains(&0) {
                Decision::ListOnly
            } else {
                Decision::Keep
            }
        }
    }

    enum Entry<'a> {
        File(&'a str, &'a [u8]),
        Symlink(&'a str, &'a str),
    }

    fn build_archive(entries: &[Entry]) -> Vec<u8> {
        let mut tb = tar::Builder::new(Vec::new());
        for entry in entries {
            match entry {
                Entry::File(path, content) => {
                    let mut h = tar::Header::new_gnu();
                    h.set_size(content.len() as u64);
                    h.set_mode(0o644);
                    h.set_cksum();
                    tb.append_data(&mut h, path, *content).unwrap();
                }
                Entry::Symlink(path, target) => {
                    let mut h = tar::Header::new_gnu();
                    h.set_entry_type(tar::EntryType::Symlink);
                    h.set_size(0);
                    h.set_mode(0o777);
                    h.set_cksum();
                    tb.append_link(&mut h, *path, *target).unwrap();
                }
            }
        }
        let tar_bytes = tb.into_inner().unwrap();
        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(&tar_bytes).unwrap();
        enc.finish().unwrap()
    }

    fn paths(inv: &[FileInventoryEntry]) -> Vec<&str> {
        inv.iter().map(|e| e.path.as_str()).collect()
    }

    #[test]
    fn extracts_and_strips_archive_root() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/src/lib.rs", b"pub mod lib;"),
        ]);
        extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap(),
            "fn main() {}"
        );
        assert!(!dir.path().join("project-main").exists());
    }

    #[test]
    fn skips_pax_global_and_per_file_headers() {
        let dir = tempfile::tempdir().unwrap();
        let mut tb = tar::Builder::new(Vec::new());
        for (ty, name, body) in [
            (
                tar::EntryType::XGlobalHeader,
                "pax_global_header",
                b"comment=x\n".as_slice(),
            ),
            (
                tar::EntryType::XHeader,
                "PaxHeader/main.rs",
                b"path=project-main/src/main.rs\n",
            ),
        ] {
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(ty);
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tb.append_data(&mut h, name, body).unwrap();
        }
        let content = b"fn main() {}";
        let mut h = tar::Header::new_gnu();
        h.set_size(content.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tb.append_data(&mut h, "project-main/src/main.rs", &content[..])
            .unwrap();
        let tar_bytes = tb.into_inner().unwrap();
        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(&tar_bytes).unwrap();
        let data = enc.finish().unwrap();

        extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap(),
            "fn main() {}"
        );
    }

    #[test]
    fn rejects_inconsistent_archive_root() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root-a/file1.rs", b"a"),
            Entry::File("root-b/file2.rs", b"b"),
        ]);
        let err = extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap_err();
        assert!(err.to_string().contains("not under the expected root"));
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let mut tb = tar::Builder::new(Vec::new());
        let content = b"malicious";
        let mut h = tar::Header::new_gnu();
        h.set_size(content.len() as u64);
        h.set_mode(0o644);
        h.set_entry_type(tar::EntryType::Regular);
        let path = "root/../../escape.txt";
        let raw = h.as_mut_bytes();
        raw[..path.len()].copy_from_slice(path.as_bytes());
        h.set_cksum();
        tb.append(&h, std::io::Cursor::new(content)).unwrap();
        let tar_bytes = tb.into_inner().unwrap();
        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(&tar_bytes).unwrap();
        let data = enc.finish().unwrap();

        let err = extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap_err();
        assert!(err.to_string().contains("path traversal"), "got: {err}");
    }

    #[test]
    fn skips_symlink_escaping_target_directory() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"hello"),
            Entry::Symlink("root/escape", outside.path().to_str().unwrap()),
        ]);
        extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("legit.txt")).unwrap(),
            "hello"
        );
        assert!(!dir.path().join("escape").exists());
    }

    #[test]
    fn removes_skipped_symlinks_from_inventory() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"hello"),
            Entry::Symlink("root/escape", outside.path().to_str().unwrap()),
        ]);
        let inv = extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap();
        assert_eq!(paths(&inv), vec!["legit.txt"]);
    }

    #[test]
    fn allows_valid_internal_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/src/lib.rs", b"real content"),
            Entry::Symlink("root/bin/run", "../src/lib.rs"),
        ]);
        extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("bin/run")).unwrap(),
            "real content"
        );
    }

    #[test]
    fn empty_and_truncated_bodies_are_classified_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            extract_tar_gz(&[][..], dir.path(), &mut KeepAll),
            Err(StreamError::Empty)
        ));
        let full = build_archive(&[Entry::File("project-main/src/main.rs", b"fn main() {}")]);
        let truncated = &full[..full.len() / 2];
        assert!(matches!(
            extract_tar_gz(truncated, dir.path(), &mut KeepAll),
            Err(StreamError::Empty)
        ));
    }

    #[test]
    fn list_only_files_are_recorded_but_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/assets/logo.png", b"\x89PNGdata"),
            Entry::File("project-main/model/weights.onnx", b"\x00\x01\x02blob"),
        ]);
        let inv = extract_tar_gz(&data[..], dir.path(), &mut TestFilter).unwrap();

        assert_eq!(
            paths(&inv),
            vec!["assets/logo.png", "model/weights.onnx", "src/main.rs"]
        );
        assert_eq!(
            inv.iter()
                .find(|e| e.path == "src/main.rs")
                .unwrap()
                .decision,
            Decision::Keep
        );
        assert_eq!(
            inv.iter()
                .find(|e| e.path == "assets/logo.png")
                .unwrap()
                .decision,
            Decision::ListOnly
        );
        assert_eq!(
            inv.iter()
                .find(|e| e.path == "model/weights.onnx")
                .unwrap()
                .decision,
            Decision::ListOnly
        );
        assert!(dir.path().join("src/main.rs").exists());
        assert!(!dir.path().join("assets/logo.png").exists());
        assert!(!dir.path().join("model/weights.onnx").exists());
    }

    #[test]
    fn text_file_larger_than_sniff_window_is_written_in_full() {
        let dir = tempfile::tempdir().unwrap();
        let body: Vec<u8> = (0..12_000).map(|i| ((i % 254) + 1) as u8).collect();
        let data = build_archive(&[Entry::File("project-main/big.txt", &body)]);
        extract_tar_gz(&data[..], dir.path(), &mut KeepAll).unwrap();
        assert_eq!(std::fs::read(dir.path().join("big.txt")).unwrap(), body);
    }
}
