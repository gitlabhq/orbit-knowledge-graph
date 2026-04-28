use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tracing::warn;

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

    /// End-to-end contract for !1110: real archive → real extraction →
    /// post-extraction filter (`is_parsable || is_required_by_indexer`)
    /// → real `Pipeline::run`. The pipeline must produce graph output;
    /// if the filter dropped any indexer input (Cargo.toml, package.json,
    /// tsconfig.json, .gitignore, etc.) the resolvers would silently
    /// degrade and we'd see zero defs / edges instead.
    #[test]
    fn pipeline_runs_against_filtered_extracted_archive() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use code_graph::v2::config::{detect_language_from_path, is_required_by_indexer};
        use code_graph::v2::{
            BatchSink, GraphConverter, NullSink, Pipeline, PipelineConfig, SinkError,
        };

        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            // Rust workspace with cross-crate dep so resolution is exercised.
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
                b"fn main() { println!(\"{}\", lib::greet()); }\n",
            ),
            // JS workspace with a tsconfig path alias so the alias must resolve.
            Entry::File(
                "root/frontend/package.json",
                b"{\"name\":\"frontend\",\"version\":\"0.0.0\"}\n",
            ),
            Entry::File(
                "root/frontend/tsconfig.json",
                b"{\"compilerOptions\":{\"baseUrl\":\".\",\"paths\":{\"@/*\":[\"src/*\"]}}}\n",
            ),
            Entry::File(
                "root/frontend/src/utils.ts",
                b"export function helper() { return 42; }\n",
            ),
            Entry::File(
                "root/frontend/src/main.ts",
                b"import { helper } from '@/utils';\nexport function run() { return helper(); }\n",
            ),
            Entry::File("root/.gitignore", b"node_modules/\ntarget/\n"),
            // Noise the filter must drop.
            Entry::File("root/README.md", b"# project\n"),
            Entry::File("root/Cargo.lock", b"# generated\n"),
            Entry::File("root/frontend/yarn.lock", b"# generated\n"),
            Entry::File("root/assets/logo.png", b"\x89PNG..."),
            Entry::File("root/vendor/jquery.min.js", b"/* min */\n"),
            Entry::File("root/pkg/server_test.go", b"package pkg\n"),
        ]);

        // 1. Real archival extraction endpoint.
        extract_tar_gz(&data, dir.path()).unwrap();

        // 2. Apply the filter !1110 will compose, by deleting files the
        //    filter would have refused to write to disk.
        let keep = |rel: &Path| -> bool {
            let s = rel.to_string_lossy();
            let parsable = detect_language_from_path(&s)
                .map(|l| !l.exclude_extensions().iter().any(|e| s.ends_with(e)))
                .unwrap_or(false);
            parsable || is_required_by_indexer(rel)
        };
        let extracted = [
            "Cargo.toml",
            "crates/lib/Cargo.toml",
            "crates/lib/src/lib.rs",
            "crates/app/Cargo.toml",
            "crates/app/src/main.rs",
            "frontend/package.json",
            "frontend/tsconfig.json",
            "frontend/src/utils.ts",
            "frontend/src/main.ts",
            ".gitignore",
            "README.md",
            "Cargo.lock",
            "frontend/yarn.lock",
            "assets/logo.png",
            "vendor/jquery.min.js",
            "pkg/server_test.go",
        ];
        for rel in extracted {
            let abs = dir.path().join(rel);
            assert!(abs.exists(), "extraction missed: {rel}");
            if !keep(Path::new(rel)) {
                std::fs::remove_file(&abs).unwrap();
            }
        }
        for required in [
            "Cargo.toml",
            "crates/lib/Cargo.toml",
            "crates/app/Cargo.toml",
            "frontend/package.json",
            "frontend/tsconfig.json",
            ".gitignore",
            "crates/lib/src/lib.rs",
            "crates/app/src/main.rs",
            "frontend/src/utils.ts",
            "frontend/src/main.ts",
        ] {
            assert!(
                dir.path().join(required).exists(),
                "filter dropped indexer input: {required}"
            );
        }
        for noise in [
            "README.md",
            "Cargo.lock",
            "frontend/yarn.lock",
            "assets/logo.png",
            "vendor/jquery.min.js",
            "pkg/server_test.go",
        ] {
            assert!(
                !dir.path().join(noise).exists(),
                "filter kept noise: {noise}"
            );
        }

        // 3. Run the real code-indexing pipeline against the filtered tree
        //    and verify it produced graph content. Counting via a custom
        //    converter: a Pipeline that finds no manifests would produce
        //    zero defs because Rust falls back to standalone parsing and
        //    JS bare-specifier resolution collapses.
        struct CountingConverter {
            defs: AtomicUsize,
            edges: AtomicUsize,
        }
        impl GraphConverter for CountingConverter {
            fn convert(
                &self,
                graph: code_graph::v2::linker::CodeGraph,
            ) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, SinkError> {
                self.defs.fetch_add(graph.defs.len(), Ordering::Relaxed);
                self.edges
                    .fetch_add(graph.graph.edge_count(), Ordering::Relaxed);
                Ok(Vec::new())
            }
        }

        let counter = Arc::new(CountingConverter {
            defs: AtomicUsize::new(0),
            edges: AtomicUsize::new(0),
        });
        let sink: Arc<dyn BatchSink> = Arc::new(NullSink);
        let result = Pipeline::run(
            dir.path(),
            PipelineConfig::default(),
            counter.clone() as Arc<dyn GraphConverter>,
            sink,
        );
        assert!(
            result.errors.is_empty(),
            "pipeline errors: {:#?}",
            result.errors
        );
        let defs = counter.defs.load(Ordering::Relaxed);
        let edges = counter.edges.load(Ordering::Relaxed);
        assert!(
            defs > 0,
            "pipeline produced zero defs against filtered tree (resolvers likely degraded)"
        );
        assert!(
            edges > 0,
            "pipeline produced zero graph edges against filtered tree"
        );
    }
}
