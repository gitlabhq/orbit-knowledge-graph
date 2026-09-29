//! A Gitaly tar.gz. Inflating is one sequential stream, so that thread only
//! reads: it checks each path, applies the header passes, and sends the bytes
//! of every file that needs them ahead through a bounded channel. Workers run
//! the content passes on those bytes, write the files that load to `target`,
//! and hand back the settled `File`.

use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use flate2::read::GzDecoder;
use rayon::prelude::*;
use tracing::warn;

use super::{Decision, File, Inventory, Need, Pass, SourceError, check};

/// How many files' bytes may wait for a worker; with the per-file size cap
/// this bounds the bytes in flight.
const LOOKAHEAD: usize = 64;

pub fn extract<R: Read>(
    reader: R,
    target_dir: &Path,
    passes: &impl Pass,
) -> Result<Inventory, SourceError> {
    std::fs::create_dir_all(target_dir)?;
    let target = target_dir.canonicalize()?;
    let (sender, receiver) = sync_channel::<Pending>(LOOKAHEAD);

    let (inflated, settled) = std::thread::scope(|scope| {
        let workers = scope.spawn(|| settle(receiver, &target, passes));
        let inflated = inflate(reader, &target, passes, &sender);
        drop(sender);
        (inflated, workers.join().expect("tar workers panicked"))
    });
    // A failure on either side closes the channel and ends the other; the
    // side that failed on its own has the error worth reporting.
    let (mut files, symlinks) = match (inflated, settled) {
        (Err(inflate_error), Err(_)) => return Err(inflate_error),
        (inflated, settled) => {
            let Inflated {
                mut files,
                symlinks,
            } = inflated?;
            files.extend(settled?);
            (files, symlinks)
        }
    };

    for (link_path, link_target) in symlinks {
        crate::fs::safe_create_dir_all(&link_path, &target).map_err(std::io::Error::other)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&link_target, &link_path)?;
    }
    let removed = crate::fs::validate_symlinks(&target).map_err(std::io::Error::other)?;
    if !removed.is_empty() {
        let removed: std::collections::HashSet<String> = removed
            .iter()
            .map(|r| r.relative_path.to_string_lossy().into_owned())
            .collect();
        files.retain(|file| !removed.contains(&file.path));
    }
    Ok(Inventory::new(files))
}

/// A file whose bytes came off the stream, waiting for a worker.
struct Pending {
    file: File,
    dest: PathBuf,
    bytes: Vec<u8>,
}

fn settle(
    receiver: Receiver<Pending>,
    target: &Path,
    passes: &impl Pass,
) -> Result<Vec<File>, SourceError> {
    receiver
        .into_iter()
        .par_bridge()
        .map(|pending| pending.settle(target, passes))
        .filter_map(Result::transpose)
        .collect()
}

impl Pending {
    fn settle(self, target: &Path, passes: &impl Pass) -> Result<Option<File>, SourceError> {
        let Pending {
            mut file,
            dest,
            bytes,
        } = self;
        check(passes, &mut file, &bytes);
        match file.decision {
            Decision::Drop => Ok(None),
            Decision::ListOnly => Ok(Some(file)),
            Decision::Parse | Decision::Load => {
                let written = crate::fs::resolve_dest_within(target, &dest)
                    .and_then(std::fs::File::create)
                    .and_then(|mut out| out.write_all(&bytes));
                match written {
                    Ok(()) => Ok(Some(file)),
                    Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                        Err(SourceError::Io(e))
                    }
                    Err(e) => {
                        warn!(entry = %file.path, error = %e, "skipping archive entry that could not be written");
                        Ok(None)
                    }
                }
            }
        }
    }
}

/// What the inflating thread settled itself, and the symlinks to create once
/// every regular file exists so none can redirect a write outside `target`.
#[derive(Default)]
struct Inflated {
    files: Vec<File>,
    symlinks: Vec<(PathBuf, PathBuf)>,
}

fn inflate<R: Read>(
    reader: R,
    target: &Path,
    passes: &impl Pass,
    workers: &SyncSender<Pending>,
) -> Result<Inflated, SourceError> {
    let mut archive = ::tar::Archive::new(GzDecoder::new(reader));
    let mut archive_root: Option<OsString> = None;
    let mut inflated = Inflated::default();
    let mut any_entry_seen = false;
    let entries = archive
        .entries()
        .map_err(|e| SourceError::Io(std::io::Error::other(e)))?;

    for entry in entries {
        let mut entry = match entry {
            Ok(e) => {
                any_entry_seen = true;
                e
            }
            Err(e) if !any_entry_seen && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                warn!(error = %e, "archive stream truncated before first entry; treating as empty");
                return Err(SourceError::Empty);
            }
            Err(e) => return Err(SourceError::Io(e)),
        };

        let entry_type = entry.header().entry_type();
        if entry_type == ::tar::EntryType::XGlobalHeader || entry_type == ::tar::EntryType::XHeader
        {
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
            return Err(SourceError::Io(std::io::Error::other(format!(
                "path traversal detected: {}",
                relative_path.display()
            ))));
        }
        let dest = target.join(&relative_path);
        let path = relative_path.to_string_lossy().into_owned();

        if entry_type == ::tar::EntryType::Symlink || entry_type == ::tar::EntryType::Link {
            let mut file = File::symlink(path, entry.size());
            passes.header(&mut file)?;
            if file.decision != Decision::Drop {
                let link_target = entry
                    .link_name()
                    .map_err(std::io::Error::other)?
                    .map(|cow| cow.into_owned())
                    .unwrap_or_default();
                inflated.symlinks.push((dest, link_target));
                inflated.files.push(file);
            }
            continue;
        }
        if entry_type == ::tar::EntryType::Regular {
            let mut file = File::new(path, entry.size());
            let need = passes.header(&mut file)?;
            if need == Need::Nothing && !file.loads() {
                if file.decision != Decision::Drop {
                    inflated.files.push(file);
                }
                continue;
            }
            let mut bytes = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut bytes)?;
            if workers.send(Pending { file, dest, bytes }).is_err() {
                return Ok(inflated);
            }
            continue;
        }
        let unpacked = crate::fs::resolve_dest_within(target, &dest)
            .and_then(|dest_canonical| entry.unpack(&dest_canonical).map(|_| ()));
        match unpacked {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(SourceError::Io(e));
            }
            Err(e) => {
                warn!(entry = %relative_path.display(), error = %e, "skipping archive entry that could not be unpacked");
            }
        }
    }
    Ok(inflated)
}

/// Strip the Gitaly archive root (`<slug>-<ref>/`). The first entry records
/// the root; later entries must share it.
fn strip_archive_root(
    path: &Path,
    detected_root: &mut Option<OsString>,
) -> Result<PathBuf, SourceError> {
    let mut components = path.components();
    let first = match components.next() {
        Some(c) => c.as_os_str().to_os_string(),
        None => return Ok(PathBuf::new()),
    };
    match detected_root {
        None => *detected_root = Some(first),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::files::CapExceeded;
    use flate2::Compression;
    use flate2::write::GzEncoder;

    struct ParseAll;
    impl Pass for ParseAll {}

    /// Drops files by extension (header) and by a NUL in content; mirrors the
    /// shape of the production `CodeFilter` without depending on code-graph.
    struct TestFilter;
    impl Pass for TestFilter {
        fn header(&self, f: &mut File) -> Result<Need, CapExceeded> {
            if Path::new(&f.path).extension().and_then(|e| e.to_str()) == Some("png") {
                f.decision = Decision::ListOnly;
            }
            Ok(Need::Nothing)
        }
        fn content(&self, f: &mut File, content: &[u8]) {
            if content.contains(&0) {
                f.decision = Decision::ListOnly;
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

    fn paths(inv: &[File]) -> Vec<&str> {
        inv.iter().map(|e| e.path.as_str()).collect()
    }

    #[test]
    fn extracts_and_strips_archive_root() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/src/lib.rs", b"pub mod lib;"),
        ]);
        extract(&data[..], dir.path(), &ParseAll).unwrap();
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

        extract(&data[..], dir.path(), &ParseAll).unwrap();
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
        let inv = extract(&data[..], dir.path(), &ParseAll).unwrap();
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
        let inv = extract(&data[..], dir.path(), &ParseAll).unwrap();
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
        let inv = extract(&data[..], dir.path(), &ParseAll).unwrap();
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

        let err = extract(&data[..], dir.path(), &ParseAll).unwrap_err();
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
        extract(&data[..], dir.path(), &ParseAll).unwrap();
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
        let inv = extract(&data[..], dir.path(), &ParseAll).unwrap();
        assert_eq!(paths(&inv), vec!["legit.txt"]);
    }

    #[test]
    fn allows_valid_internal_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let data = build_archive(&[
            Entry::File("root/src/lib.rs", b"real content"),
            Entry::Symlink("root/bin/run", "../src/lib.rs"),
        ]);
        extract(&data[..], dir.path(), &ParseAll).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("bin/run")).unwrap(),
            "real content"
        );
    }

    #[test]
    fn empty_and_truncated_bodies_are_classified_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            extract(&[][..], dir.path(), &ParseAll),
            Err(SourceError::Empty)
        ));
        let full = build_archive(&[Entry::File("project-main/src/main.rs", b"fn main() {}")]);
        let truncated = &full[..full.len() / 2];
        assert!(matches!(
            extract(truncated, dir.path(), &ParseAll),
            Err(SourceError::Empty)
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
        let inv = extract(&data[..], dir.path(), &TestFilter).unwrap();

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
        extract(&data[..], dir.path(), &ParseAll).unwrap();
        assert_eq!(std::fs::read(dir.path().join("big.txt")).unwrap(), body);
    }

    /// Skips files above a byte limit, so the test can observe which size the
    /// guard was handed.
    struct MaxSize(u64);
    impl Pass for MaxSize {
        fn header(&self, f: &mut File) -> Result<Need, CapExceeded> {
            if f.size > self.0 {
                f.decision = Decision::ListOnly;
            }
            Ok(Need::Nothing)
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

        let inv = extract(&data[..], dir.path(), &MaxSize(64)).unwrap();

        assert_eq!(paths(&inv), vec!["big.txt"]);
        assert_eq!(inv[0].size, 4096);
        assert_eq!(inv[0].decision, Decision::ListOnly);
        assert!(!dir.path().join("big.txt").exists());
    }
}

#[cfg(test)]
mod backpressure {
    use super::*;
    use crate::files::CapExceeded;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts files whose bytes are off the stream but not yet settled by a
    /// worker, and remembers the most that were in flight at once.
    #[derive(Default)]
    struct SlowSettle {
        in_flight: AtomicUsize,
        peak: AtomicUsize,
    }
    impl Pass for SlowSettle {
        fn header(&self, _: &mut File) -> Result<Need, CapExceeded> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            Ok(Need::Bytes)
        }
        fn content(&self, _: &mut File, _: &[u8]) {
            std::thread::sleep(std::time::Duration::from_micros(200));
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// The inflating thread is faster than the workers; it must wait on the
    /// channel instead of inflating the whole archive into memory.
    #[test]
    fn inflating_never_runs_more_than_the_lookahead_ahead_of_the_workers() {
        let dir = tempfile::tempdir().unwrap();
        let mut tb = tar::Builder::new(Vec::new());
        for i in 0..2_000 {
            let mut h = tar::Header::new_gnu();
            h.set_size(8);
            h.set_mode(0o644);
            h.set_cksum();
            tb.append_data(&mut h, format!("root/f{i}.rs"), &b"fn a(){}"[..])
                .unwrap();
        }
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(&tb.into_inner().unwrap()).unwrap();
        let data = gz.finish().unwrap();
        let pass = SlowSettle::default();

        let inv = extract(&data[..], dir.path(), &pass).unwrap();

        assert_eq!(inv.len(), 2_000);
        let ceiling = LOOKAHEAD + rayon::current_num_threads() + 1;
        let peak = pass.peak.load(Ordering::SeqCst);
        assert!(peak <= ceiling, "peak in flight {peak} exceeds {ceiling}");
        assert!(peak > 1, "workers must run alongside the inflating thread");
    }
}
