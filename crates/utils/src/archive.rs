//! Tar.gz extraction as a [`FileStreamHooks`] source: untar, safety-check each
//! path, hand the entry to the hooks, and write the bytes of the files they
//! load ([`Decision::Parse`] or [`Decision::Load`]). No filtering of its own.

use std::ffi::OsString;
use std::io::{Read, Write};
use std::ops::AddAssign;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use rayon::prelude::*;
use tracing::warn;

use crate::fs_walk::{
    Decision, FileInventory, FileInventoryEntry, FileStreamHooks, StreamError,
    classify_in_parallel, settle_header,
};

/// Extract a gzipped tar from `reader` into `target_dir`, running every regular
/// file through `hooks`. Loaded files are written to disk; every non-dropped
/// file (and symlink) is returned in the inventory.
///
/// Inflating is one sequential stream, so that thread only reads: it settles
/// what the hooks can decide from a header, and sends every other file's bytes
/// ahead through a bounded channel to workers that classify them, write them
/// and tally with their own clone of `hooks`, added back at the end.
pub fn extract_tar_gz<R: Read, H>(
    reader: R,
    target_dir: &Path,
    hooks: &mut H,
) -> Result<FileInventory, StreamError>
where
    H: FileStreamHooks + Clone + AddAssign + Send + Sync,
{
    std::fs::create_dir_all(target_dir)?;
    let target = target_dir.canonicalize()?;
    let (sender, receiver) = std::sync::mpsc::sync_channel::<Pending>(LOOKAHEAD);

    let mut inflating_hooks = hooks.clone();
    let (inflated, classified) = std::thread::scope(|scope| {
        let classifying = scope.spawn(|| {
            classify_in_parallel(
                receiver.into_iter().par_bridge(),
                hooks,
                |hooks, _, pending| pending.settle(hooks, &target),
            )
        });
        let inflated = inflate(reader, &target, &mut inflating_hooks, sender);
        let classified = classifying.join().expect("classifying thread panicked");
        (inflated, classified)
    });
    // A failure on either side closes the channel and ends the other; the
    // side that failed on its own has the error worth reporting.
    let (mut inventory, deferred_symlinks) = match (inflated, classified) {
        (Err(inflate_error), Err(_)) => return Err(inflate_error),
        (inflated, classified) => {
            let Inflated { entries, symlinks } = inflated?;
            let mut inventory = classified?;
            inventory.extend(entries);
            (inventory, symlinks)
        }
    };
    *hooks += inflating_hooks;

    for (link_path, link_target) in deferred_symlinks {
        crate::fs::safe_create_dir_all(&link_path, &target).map_err(std::io::Error::other)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&link_target, &link_path)?;
    }
    let removed_symlinks = crate::fs::validate_symlinks(&target).map_err(std::io::Error::other)?;
    if !removed_symlinks.is_empty() {
        let removed: std::collections::HashSet<String> = removed_symlinks
            .iter()
            .map(|r| r.relative_path.to_string_lossy().into_owned())
            .collect();
        inventory.retain(|entry| !removed.contains(&entry.path));
    }
    Ok(FileInventory::new(inventory))
}

/// How many files' bytes may wait between the inflating thread and the
/// workers; with the per-file size cap this bounds the memory in flight.
const LOOKAHEAD: usize = 64;

/// A regular file the header did not settle: its bytes, read off the stream,
/// waiting for a worker.
struct Pending {
    meta: FileInventoryEntry,
    content: Vec<u8>,
    dest: PathBuf,
}

impl Pending {
    fn settle<H: FileStreamHooks>(
        self,
        hooks: &mut H,
        target: &Path,
    ) -> Result<Option<FileInventoryEntry>, StreamError> {
        let Pending {
            mut meta,
            content,
            dest,
        } = self;
        let (decision, label) = hooks.on_content(&meta, &content);
        meta.decision = decision;
        meta.label = label;
        match decision {
            Decision::Drop => Ok(None),
            Decision::ListOnly => Ok(Some(meta)),
            // Both loaded states materialize the bytes; only the parse axis
            // differs, which the pipeline acts on, not the extractor.
            Decision::Parse | Decision::Load => {
                // A containment escape (PermissionDenied from resolve_dest_within)
                // is fatal; any other error, a path the filesystem rejects such
                // as an over-long file or directory component, skips the entry.
                let written = crate::fs::resolve_dest_within(target, &dest)
                    .and_then(std::fs::File::create)
                    .and_then(|mut file| file.write_all(&content));
                match written {
                    Ok(()) => Ok(Some(meta)),
                    Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                        Err(StreamError::Io(e))
                    }
                    Err(e) => {
                        warn!(entry = %meta.path, error = %e, "skipping archive entry that could not be written");
                        Ok(None)
                    }
                }
            }
        }
    }
}

/// What the inflating thread settled itself: header-decided files and
/// symlinks, the latter created only once every regular file exists.
#[derive(Default)]
struct Inflated {
    entries: Vec<FileInventoryEntry>,
    symlinks: Vec<(PathBuf, PathBuf)>,
}

fn inflate<R: Read, H: FileStreamHooks>(
    reader: R,
    target: &Path,
    hooks: &mut H,
    sender: std::sync::mpsc::SyncSender<Pending>,
) -> Result<Inflated, StreamError> {
    let mut archive = tar::Archive::new(GzDecoder::new(reader));
    // The first entry sets the Gitaly archive root (`<slug>-<ref>/`); all others
    // must share it.
    let mut archive_root: Option<OsString> = None;
    // Symlinks are deferred until all regular files and directories exist, so no
    // symlink is on disk during the main loop to redirect a create_dir_all or
    // dest.exists() outside the target.
    let mut inflated = Inflated::default();

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
        let relative_path = match strip_archive_root(relative_path, &mut archive_root) {
            Ok(path) => path,
            Err(e) => {
                warn!(entry = %entry_path_str, error = %e, "skipping archive entry outside the archive root");
                continue;
            }
        };
        if relative_path.as_os_str().is_empty() {
            continue;
        }
        if !crate::fs::is_safe_relative_path(&relative_path) {
            return Err(StreamError::Io(std::io::Error::other(format!(
                "path traversal detected: {}",
                relative_path.display()
            ))));
        }
        let dest = target.join(&relative_path);
        let mut meta = FileInventoryEntry {
            path: relative_path.to_string_lossy().into_owned(),
            size: entry.size(),
            decision: Decision::ListOnly,
            label: Default::default(),
        };

        if entry_type == tar::EntryType::Symlink || entry_type == tar::EntryType::Link {
            // A symlink is never a parse candidate, we'd be parsing the link, not
            // source, so the hooks settle it (and record why); we keep only the
            // within-root deferral, which is the source's security mechanism.
            let (decision, label) = hooks.on_non_regular(&meta);
            meta.decision = decision;
            meta.label = label;
            if meta.decision != Decision::Drop {
                let link_target = entry
                    .link_name()
                    .map_err(std::io::Error::other)?
                    .map(|cow| cow.into_owned())
                    .unwrap_or_default();
                inflated.symlinks.push((dest, link_target));
                inflated.entries.push(meta);
            }
            continue;
        }
        if entry_type == tar::EntryType::Regular {
            meta.decision = Decision::Parse;
            if let Some((decision, label)) = settle_header(hooks, &meta)? {
                meta.decision = decision;
                meta.label = label;
                if meta.decision != Decision::Drop {
                    inflated.entries.push(meta);
                }
                continue;
            }
            let mut content = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut content)?;
            let pending = Pending {
                meta,
                content,
                dest,
            };
            if sender.send(pending).is_err() {
                // The workers stopped; their error is the one to report.
                return Ok(inflated);
            }
            continue;
        }
        let unpacked = crate::fs::resolve_dest_within(target, &dest)
            .and_then(|dest_canonical| entry.unpack(&dest_canonical).map(|_| ()));
        match unpacked {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(StreamError::Io(e));
            }
            Err(e) => {
                warn!(entry = %relative_path.display(), error = %e, "skipping archive entry that could not be unpacked");
                continue;
            }
        }
    }
    Ok(inflated)
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
    use crate::fs_walk::FileLabel;
    use flate2::Compression;
    use flate2::write::GzEncoder;

    #[derive(Clone)]
    struct ParseAll;
    impl AddAssign for ParseAll {
        fn add_assign(&mut self, _: Self) {}
    }
    impl FileStreamHooks for ParseAll {}

    /// Drops files by extension (header) and by a NUL in content; mirrors the
    /// shape of the production `CodeFilter` without depending on code-graph.
    #[derive(Clone)]
    struct TestFilter;
    impl AddAssign for TestFilter {
        fn add_assign(&mut self, _: Self) {}
    }
    impl FileStreamHooks for TestFilter {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
            (Path::new(&f.path).extension().and_then(|e| e.to_str()) == Some("png"))
                .then_some((Decision::ListOnly, FileLabel::default()))
        }
        fn on_content(&mut self, _f: &FileInventoryEntry, content: &[u8]) -> (Decision, FileLabel) {
            if content.contains(&0) {
                (Decision::ListOnly, FileLabel::default())
            } else {
                (Decision::Parse, FileLabel::default())
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
        extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
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

        extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap(),
            "fn main() {}"
        );
    }

    #[test]
    fn skips_entry_outside_archive_root_and_keeps_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root-a/file1.rs", b"a"),
            Entry::File("root-b/file2.rs", b"b"),
        ]);
        let inv = extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(paths(&inv), vec!["file1.rs"]);
        assert!(dir.path().join("file1.rs").exists());
    }

    #[test]
    fn skips_entry_whose_name_is_too_long_and_keeps_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let long_name = "z".repeat(500);
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File(&format!("project-main/{long_name}.rs"), b"unwritable"),
        ]);
        let inv = extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(paths(&inv), vec!["src/main.rs"]);
        assert!(dir.path().join("src/main.rs").exists());
    }

    #[test]
    fn skips_entry_whose_directory_name_is_too_long_and_keeps_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let long_dir = "z".repeat(500);
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File(&format!("project-main/{long_dir}/f.rs"), b"unwritable"),
        ]);
        let inv = extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(paths(&inv), vec!["src/main.rs"]);
        assert!(dir.path().join("src/main.rs").exists());
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

        let err = extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap_err();
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
        extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
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
        let inv = extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(paths(&inv), vec!["legit.txt"]);
    }

    #[test]
    fn allows_valid_internal_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/src/lib.rs", b"real content"),
            Entry::Symlink("root/bin/run", "../src/lib.rs"),
        ]);
        extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("bin/run")).unwrap(),
            "real content"
        );
    }

    #[test]
    fn empty_and_truncated_bodies_are_classified_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            extract_tar_gz(&[][..], dir.path(), &mut ParseAll),
            Err(StreamError::Empty)
        ));
        let full = build_archive(&[Entry::File("project-main/src/main.rs", b"fn main() {}")]);
        let truncated = &full[..full.len() / 2];
        assert!(matches!(
            extract_tar_gz(truncated, dir.path(), &mut ParseAll),
            Err(StreamError::Empty)
        ));
    }

    /// Tallies what it saw, so an extraction can be checked to have added the
    /// inflating thread's and every worker's share back together.
    #[derive(Clone, Default)]
    struct Tallying {
        headers: u64,
        contents: u64,
    }
    impl AddAssign for Tallying {
        fn add_assign(&mut self, other: Self) {
            self.headers += other.headers;
            self.contents += other.contents;
        }
    }
    impl FileStreamHooks for Tallying {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
            self.headers += 1;
            (f.size > 64).then_some((Decision::ListOnly, FileLabel::default()))
        }
        fn on_content(&mut self, _f: &FileInventoryEntry, content: &[u8]) -> (Decision, FileLabel) {
            self.contents += 1;
            match content.contains(&0) {
                true => (Decision::ListOnly, FileLabel::default()),
                false => (Decision::Parse, FileLabel::default()),
            }
        }
    }

    #[test]
    fn a_large_archive_is_extracted_whole_and_the_hooks_tally_all_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let big = vec![b'x'; 100];
        let mut files: Vec<(String, Vec<u8>)> = (0..600)
            .map(|i| {
                let body: Vec<u8> = match i % 3 {
                    0 => b"\x00blob".to_vec(),
                    1 => big.clone(),
                    _ => b"fn f() {}".to_vec(),
                };
                (format!("project-main/src/mod{}/file{i}.rs", i % 7), body)
            })
            .collect();
        files.sort();
        let entries: Vec<Entry> = files
            .iter()
            .map(|(path, body)| Entry::File(path, body))
            .collect();
        let data = build_archive(&entries);

        let mut hooks = Tallying::default();
        let inv = extract_tar_gz(&data[..], dir.path(), &mut hooks).unwrap();

        assert_eq!(inv.len(), 600);
        assert_eq!(inv.by_decision(Decision::Parse).count(), 200);
        assert!(inv.iter().map(|e| &e.path).is_sorted());
        assert_eq!(
            (hooks.headers, hooks.contents),
            (600, 400),
            "header settlements on the inflating thread and content ones on the workers all add up"
        );
        assert_eq!(
            files_under(dir.path()),
            200,
            "only parsed files are written"
        );
    }

    fn files_under(root: &Path) -> usize {
        std::fs::read_dir(root)
            .unwrap()
            .flatten()
            .map(|e| match e.file_type().unwrap().is_dir() {
                true => files_under(&e.path()),
                false => 1,
            })
            .sum()
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
            Decision::Parse
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
        extract_tar_gz(&data[..], dir.path(), &mut ParseAll).unwrap();
        assert_eq!(std::fs::read(dir.path().join("big.txt")).unwrap(), body);
    }

    /// Skips files above a byte limit, so the test can observe which size the
    /// guard was handed.
    #[derive(Clone)]
    struct MaxSize(u64);
    impl AddAssign for MaxSize {
        fn add_assign(&mut self, _: Self) {}
    }
    impl FileStreamHooks for MaxSize {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
            (f.size > self.0).then_some((Decision::ListOnly, FileLabel::default()))
        }
    }

    /// An entry can carry its real size in a PAX record with the base header
    /// size left at zero. The guard has to be given the real size, otherwise
    /// the file is read into memory before anything can reject it.
    #[test]
    fn oversize_entry_is_filtered_when_the_size_comes_from_a_pax_record() {
        let dir = tempfile::tempdir().unwrap();
        let body = vec![b'a'; 4096];

        let mut tb = tar::Builder::new(Vec::new());
        let pax = &b"13 size=4096\n"[..];
        let mut ph = tar::Header::new_gnu();
        ph.set_entry_type(tar::EntryType::XHeader);
        ph.set_size(pax.len() as u64);
        ph.set_mode(0o644);
        tb.append_data(&mut ph, "PaxHeaders/size", pax).unwrap();

        let mut h = tar::Header::new_gnu();
        h.set_size(0);
        h.set_mode(0o644);
        tb.append_data(&mut h, "project-main/big.txt", &body[..])
            .unwrap();

        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(&tb.into_inner().unwrap()).unwrap();
        let data = enc.finish().unwrap();

        let inv = extract_tar_gz(&data[..], dir.path(), &mut MaxSize(64)).unwrap();

        assert_eq!(paths(&inv), vec!["big.txt"]);
        assert_eq!(inv[0].size, 4096);
        assert_eq!(inv[0].decision, Decision::ListOnly);
        assert!(!dir.path().join("big.txt").exists());
    }
}
