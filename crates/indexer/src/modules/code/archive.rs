use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tracing::{trace, warn};

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("archive error: {0}")]
    Archive(String),
    /// The stream ended before any tar entry could be read. This happens when
    /// the GitLab archive endpoint returns 200 OK with an empty or truncated
    /// body for a project whose repository has no content. Callers classify
    /// this as an empty-repository outcome rather than a retryable failure.
    #[error("archive contained no entries (empty or truncated stream)")]
    EmptyArchive,
}

impl From<std::io::Error> for ArchiveError {
    fn from(e: std::io::Error) -> Self {
        ArchiveError::Io(e.to_string())
    }
}

/// True when the tar crate's iterator failed because the archive body ended
/// before a tar header could be read.
///
/// Verified empirically against `tar = "0.4.45"` + `flate2 = "1.x"`:
/// - 200 OK with a zero-byte body produces `ErrorKind::UnexpectedEof` from
///   `GzDecoder` (gzip header parser hits EOF immediately).
/// - A body truncated mid-header also produces `ErrorKind::UnexpectedEof`.
///
/// The `truncation_io_error_kinds_are_stable` test pins this shape so future
/// tar/flate2 upgrades that change wrapping fail loudly instead of silently
/// leaking real EOFs through as generic `Archive` errors.
fn looks_like_truncated_stream(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::UnexpectedEof
}

#[cfg(test)]
fn extract_tar_gz(data: &[u8], target_dir: &Path) -> Result<(), ArchiveError> {
    extract_tar_gz_from_reader(data, target_dir, accept_all_filter)
}

#[cfg(test)]
fn accept_all_filter(_path: &Path, _size: u64) -> bool {
    true
}

/// `filter` receives the archive-root-stripped path and the tar header size,
/// and is consulted only for regular files; symlinks and directories pass through.
pub fn extract_tar_gz_from_reader<R: Read, F>(
    reader: R,
    target_dir: &Path,
    filter: F,
) -> Result<(), ArchiveError>
where
    F: Fn(&Path, u64) -> bool,
{
    let decoder = GzDecoder::new(reader);
    unpack_tar(decoder, target_dir, filter)
}

fn unpack_tar<R: Read, F>(reader: R, target_dir: &Path, filter: F) -> Result<(), ArchiveError>
where
    F: Fn(&Path, u64) -> bool,
{
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

    // Tracks whether `entries.next()` has yielded an Ok item (including
    // skipped PAX headers). False + a truncation-shaped error means the body
    // ended before any tar header could be read.
    let mut any_entry_seen = false;

    // tar::Archive::entries() does no I/O for fresh archives; truncation-
    // shaped errors surface from the iterator below, not here.
    let entries = archive
        .entries()
        .map_err(|e| ArchiveError::Archive(e.to_string()))?;

    for entry in entries {
        let mut entry = match entry {
            Ok(e) => {
                any_entry_seen = true;
                e
            }
            Err(e) if !any_entry_seen && looks_like_truncated_stream(&e) => {
                warn!(
                    error = %e,
                    kind = ?e.kind(),
                    stage = "first_entry",
                    "archive stream truncated before first tar entry; classifying as empty archive"
                );
                return Err(ArchiveError::EmptyArchive);
            }
            Err(e) => return Err(ArchiveError::Archive(e.to_string())),
        };

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

        if entry_type == tar::EntryType::Regular {
            let declared_size = entry.header().size().unwrap_or(0);
            if !filter(&relative_path, declared_size) {
                trace!(
                    path = %relative_path.display(),
                    size = declared_size,
                    "skipping archive entry filtered out before extraction"
                );
                continue;
            }
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
    fn empty_body_is_classified_as_empty_archive() {
        let dir = tempfile::tempdir().unwrap();
        let result = extract_tar_gz(&[], dir.path());
        assert!(
            matches!(result, Err(ArchiveError::EmptyArchive)),
            "got {result:?}"
        );
    }

    #[test]
    fn truncated_body_is_classified_as_empty_archive() {
        // Build a real archive then truncate it so the gzip stream ends
        // mid-header. The tar iterator must surface an UnexpectedEof error,
        // which we classify as EmptyArchive.
        let full = build_archive(&[Entry::File("project-main/src/main.rs", b"fn main() {}")]);
        let truncated = &full[..full.len() / 2];
        let dir = tempfile::tempdir().unwrap();
        let result = extract_tar_gz(truncated, dir.path());
        assert!(
            matches!(result, Err(ArchiveError::EmptyArchive)),
            "got {result:?}"
        );
    }

    /// Pins the io::Error shape produced by the tar/flate2 stack for the two
    /// truncation cases we classify as `EmptyArchive`. A future tar or flate2
    /// upgrade that rewraps these as `ErrorKind::Other` (or anything else)
    /// will fail this test loudly instead of silently turning real EOFs into
    /// generic `Archive` errors.
    #[test]
    fn truncation_io_error_kinds_are_stable() {
        // Empty body: gzip header parser hits EOF before any byte.
        // tar::Archive::entries() does no I/O; the error surfaces on next().
        let mut archive = tar::Archive::new(GzDecoder::new(&[][..]));
        let mut iter = archive.entries().expect("entries() does no I/O");
        let kind = match iter.next() {
            Some(Err(e)) => e.kind(),
            other => panic!(
                "expected empty body to surface as Some(Err), got {}",
                match other {
                    Some(Ok(_)) => "Some(Ok(_))",
                    None => "None",
                    _ => unreachable!(),
                }
            ),
        };
        assert_eq!(
            kind,
            std::io::ErrorKind::UnexpectedEof,
            "empty body must surface as UnexpectedEof"
        );

        // Mid-stream truncation: real archive cut in half.
        let full = build_archive(&[Entry::File("project-main/x.rs", b"x")]);
        let truncated = &full[..full.len() / 2];
        let mut archive = tar::Archive::new(GzDecoder::new(truncated));
        let mut iter = archive.entries().expect("entries() does no I/O");
        let kind = match iter.next() {
            Some(Err(e)) => e.kind(),
            Some(Ok(_)) => panic!("expected truncated archive to error, got Ok entry"),
            None => panic!("expected truncated archive to error, got None"),
        };
        assert_eq!(
            kind,
            std::io::ErrorKind::UnexpectedEof,
            "mid-stream truncation must surface as UnexpectedEof"
        );
    }

    #[test]
    fn filter_skips_unwanted_files_before_extraction() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/assets/logo.png", b"\x89PNG\r\n\x1a\nbinary"),
            Entry::File("project-main/Cargo.lock", b"# lockfile"),
        ]);

        extract_tar_gz_from_reader(&data[..], dir.path(), |path, _size| {
            path.extension().and_then(|e| e.to_str()) == Some("rs")
        })
        .unwrap();

        assert!(dir.path().join("src/main.rs").exists());
        assert!(!dir.path().join("assets/logo.png").exists());
        assert!(!dir.path().join("Cargo.lock").exists());
    }

    #[test]
    fn filter_receives_size_from_tar_header() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/small.rs", b"fn s() {}"),
            Entry::File("project-main/big.rs", &vec![b'x'; 4096]),
        ]);

        extract_tar_gz_from_reader(&data[..], dir.path(), |_path, size| size <= 100).unwrap();

        assert!(dir.path().join("small.rs").exists());
        assert!(
            !dir.path().join("big.rs").exists(),
            "oversize file must be skipped before unpack"
        );
    }

    #[test]
    fn filter_does_not_apply_to_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/src/lib.rs", b"real"),
            Entry::Symlink("project-main/bin/run", "../src/lib.rs"),
        ]);

        extract_tar_gz_from_reader(&data[..], dir.path(), |path, _| {
            path.extension().and_then(|e| e.to_str()) == Some("rs")
        })
        .unwrap();

        assert!(dir.path().join("src/lib.rs").exists());
        assert!(dir.path().join("bin/run").exists());
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

    // ── Archive-endpoint pipeline tests ─────────────────────────────
    //
    // Stand up an axum server emulating the GitLab `/repository/archive`
    // endpoint, fetch via reqwest, stream the response through the real
    // `extract_tar_gz_from_reader` (with the production exclusion
    // filter), and run `Pipeline::run`. These tests verify that the
    // resolver inputs the indexer needs (Cargo.toml, package.json,
    // tsconfig.json, .gitignore) survive the round trip and that
    // resolution actually works against the extracted tree. They mirror
    // the cargo-workspace and JS module-resolution YAML suites in
    // `integration-tests-codegraph/fixtures/...` but go through the
    // real archival path.

    use code_graph::v2::config::is_excluded_from_indexing;
    use code_graph::v2::linker::CodeGraph;
    use code_graph::v2::linker::graph::GraphNode;
    use code_graph::v2::types::EdgeKind;
    use code_graph::v2::{
        BatchSink, GraphConverter, NullSink, Pipeline, PipelineConfig, SinkError,
    };
    use std::sync::{Arc, Mutex};

    struct CapturingConverter {
        graphs: Mutex<Vec<CodeGraph>>,
    }

    impl GraphConverter for CapturingConverter {
        fn convert(
            &self,
            graph: CodeGraph,
        ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, SinkError> {
            self.graphs.lock().unwrap().push(graph);
            Ok(Vec::new())
        }
    }

    /// Stand up the archive endpoint, fetch via reqwest, stream into
    /// `extract_tar_gz_from_reader` with the production exclusion
    /// filter. Mirrors `LocalRepositoryCache::store_archive` end-to-end.
    async fn extract_via_archive_endpoint(entries: &[Entry<'_>], target: &Path) {
        use axum::Router;
        use axum::body::Body;
        use axum::http::header;
        use axum::response::IntoResponse;
        use axum::routing::get;
        use futures::StreamExt;
        use tokio_util::io::SyncIoBridge;

        let archive_bytes = build_archive(entries);
        let app = Router::new().route(
            "/api/v4/internal/orbit/project/{project_id}/repository/archive",
            get(move || {
                let body = archive_bytes.clone();
                async move {
                    (
                        [(header::CONTENT_TYPE, "application/x-gzip")],
                        Body::from(body),
                    )
                        .into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!(
            "http://{}/api/v4/internal/orbit/project/42/repository/archive?ref=main",
            addr
        );
        let response = reqwest::get(&url).await.unwrap();
        assert!(response.status().is_success(), "fetch failed: {url}");
        let async_reader = tokio_util::io::StreamReader::new(
            response
                .bytes_stream()
                .map(|r| r.map_err(std::io::Error::other)),
        );
        let target = target.to_path_buf();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(async_reader, handle);
            extract_tar_gz_from_reader(bridge, &target, |rel, _size| {
                !is_excluded_from_indexing(rel)
            })
            .unwrap();
        })
        .await
        .unwrap();
        server.abort();
    }

    async fn run_pipeline(root: &Path) -> Vec<CodeGraph> {
        let capturer = Arc::new(CapturingConverter {
            graphs: Mutex::new(Vec::new()),
        });
        let capturer_for_pipeline = capturer.clone();
        let root = root.to_path_buf();
        let result = tokio::task::spawn_blocking(move || {
            let sink: Arc<dyn BatchSink> = Arc::new(NullSink);
            Pipeline::run(
                &root,
                PipelineConfig::default(),
                capturer_for_pipeline as Arc<dyn GraphConverter>,
                sink,
            )
        })
        .await
        .unwrap();
        assert!(
            result.errors.is_empty(),
            "pipeline errors: {:#?}",
            result.errors
        );
        Arc::try_unwrap(capturer)
            .ok()
            .expect("capturer still has outstanding refs")
            .graphs
            .into_inner()
            .unwrap()
    }

    fn has_def(graphs: &[CodeGraph], file: &str, name: &str) -> bool {
        graphs.iter().any(|g| {
            g.graph.node_indices().any(|idx| {
                if let GraphNode::Definition { file_path, id } = &g.graph[idx] {
                    if !file_path.ends_with(file) {
                        return false;
                    }
                    let def = &g.defs[id.0 as usize];
                    g.str(def.name) == name
                } else {
                    false
                }
            })
        })
    }

    fn edge_count(graphs: &[CodeGraph], kind: EdgeKind) -> usize {
        graphs
            .iter()
            .map(|g| {
                g.graph
                    .raw_edges()
                    .iter()
                    .filter(|e| e.weight.relationship.edge_kind == kind)
                    .count()
            })
            .sum()
    }

    /// Mirrors the cargo workspace YAML suites: cross-crate `lib::greet`
    /// callable from `app::main` only resolves if both `Cargo.toml`s
    /// survive the round-trip — they aren't parsable source, so an
    /// inclusion filter would have dropped them.
    #[tokio::test]
    async fn cargo_workspace_resolves_through_archive_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let entries = [
            Entry::File(
                "root/Cargo.toml",
                b"[workspace]\nmembers = [\"crates/lib\", \"crates/app\"]\nresolver = \"2\"\n",
            ),
            Entry::File(
                "root/crates/lib/Cargo.toml",
                b"[package]\nname = \"lib\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            Entry::File(
                "root/crates/lib/src/lib.rs",
                b"pub fn greet() -> &'static str { \"hi\" }\n",
            ),
            Entry::File(
                "root/crates/app/Cargo.toml",
                b"[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nlib = { path = \"../lib\" }\n",
            ),
            Entry::File(
                "root/crates/app/src/main.rs",
                b"fn main() { lib::greet(); }\n",
            ),
            // Noise the exclusion filter must drop.
            Entry::File("root/assets/logo.png", b"\x89PNG"),
            Entry::File("root/dist/build.zip", b"PK"),
        ];
        extract_via_archive_endpoint(&entries, dir.path()).await;

        // Resolver inputs survived.
        assert!(dir.path().join("Cargo.toml").exists());
        assert!(dir.path().join("crates/lib/Cargo.toml").exists());
        assert!(dir.path().join("crates/app/Cargo.toml").exists());
        // Excluded extensions dropped.
        assert!(!dir.path().join("assets/logo.png").exists());
        assert!(!dir.path().join("dist/build.zip").exists());

        let graphs = run_pipeline(dir.path()).await;
        assert!(
            has_def(&graphs, "crates/lib/src/lib.rs", "greet"),
            "Rust workspace resolver missed lib::greet"
        );
        assert!(
            has_def(&graphs, "crates/app/src/main.rs", "main"),
            "Rust workspace resolver missed app::main"
        );
        assert!(
            edge_count(&graphs, EdgeKind::Calls) > 0,
            "no Calls edges emitted; cross-crate resolution likely failed"
        );
    }

    /// Mirrors the JS module-resolution YAML suites: a tsconfig
    /// `paths` alias only resolves to `src/utils.ts` if both
    /// `tsconfig.json` and `package.json` survive the round-trip.
    #[tokio::test]
    async fn js_tsconfig_alias_resolves_through_archive_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let entries = [
            Entry::File(
                "root/package.json",
                b"{\"name\":\"frontend\",\"version\":\"0.0.0\"}\n",
            ),
            Entry::File(
                "root/tsconfig.json",
                b"{\"compilerOptions\":{\"baseUrl\":\".\",\"paths\":{\"@/*\":[\"src/*\"]}}}\n",
            ),
            Entry::File(
                "root/src/utils.ts",
                b"export function helper() { return 42; }\n",
            ),
            Entry::File(
                "root/src/main.ts",
                b"import { helper } from '@/utils';\nexport function run() { return helper(); }\n",
            ),
            // Noise the exclusion filter must drop.
            Entry::File("root/static/banner.gif", b"GIF89a"),
            Entry::File("root/fonts/Inter.woff2", b""),
        ];
        extract_via_archive_endpoint(&entries, dir.path()).await;

        // Resolver inputs survived.
        assert!(dir.path().join("package.json").exists());
        assert!(dir.path().join("tsconfig.json").exists());
        // Excluded extensions dropped.
        assert!(!dir.path().join("static/banner.gif").exists());
        assert!(!dir.path().join("fonts/Inter.woff2").exists());

        let graphs = run_pipeline(dir.path()).await;
        assert!(
            has_def(&graphs, "src/utils.ts", "helper"),
            "JS resolver missed utils::helper"
        );
        assert!(
            has_def(&graphs, "src/main.ts", "run"),
            "JS resolver missed main::run"
        );
        assert!(
            edge_count(&graphs, EdgeKind::Imports) > 0,
            "no Imports edges emitted; tsconfig alias likely failed to resolve"
        );
    }
}
