//! A Gitaly tar.gz. Inflating is one sequential stream, so that thread only
//! reads: it checks each path, offers the file to the repository filesystem,
//! and sends the bytes of every file the filesystem wants ahead through a
//! bounded channel. Workers finish those offers with the bytes. Nothing
//! touches the disk.

use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use flate2::read::GzDecoder;
use rayon::prelude::*;
use tracing::warn;

use super::{Offer, SourceError, Vfs};

/// How many files' bytes may wait for a worker; with the per-file size cap
/// this bounds the bytes in flight.
const LOOKAHEAD: usize = 64;

pub fn extract<R: Read>(reader: R, vfs: &Vfs) -> Result<(), SourceError> {
    let (sender, receiver) = sync_channel::<Pending<'_>>(LOOKAHEAD);
    let (inflated, settled) = std::thread::scope(|scope| {
        let workers = scope.spawn(|| settle(receiver));
        let inflated = inflate(reader, vfs, &sender);
        drop(sender);
        (inflated, workers.join().expect("tar workers panicked"))
    });
    // A failure on either side closes the channel and ends the other; the
    // side that failed on its own has the error worth reporting.
    match (inflated, settled) {
        (Err(inflate_error), Err(_)) => Err(inflate_error),
        (inflated, settled) => {
            inflated?;
            settled
        }
    }
}

/// A file whose bytes came off the stream, waiting for a worker.
struct Pending<'a> {
    offer: Offer<'a>,
    bytes: Vec<u8>,
}

fn settle(receiver: Receiver<Pending<'_>>) -> Result<(), SourceError> {
    receiver
        .into_iter()
        .par_bridge()
        .try_for_each(|Pending { offer, bytes }| offer.with_bytes(bytes))
}

fn inflate<'a, R: Read>(
    reader: R,
    vfs: &'a Vfs,
    workers: &SyncSender<Pending<'a>>,
) -> Result<(), SourceError> {
    let mut archive = ::tar::Archive::new(GzDecoder::new(reader));
    let mut archive_root: Option<OsString> = None;
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
        let is_symlink =
            entry_type == ::tar::EntryType::Symlink || entry_type == ::tar::EntryType::Link;
        // Directories exist because files are in them; other entry types
        // (PAX headers, devices, fifos) have no place in a checkout.
        if entry_type != ::tar::EntryType::Regular && !is_symlink {
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
        let path = relative_path.to_string_lossy();

        let Some(offer) = vfs.offer(&path, entry.size(), is_symlink)? else {
            continue;
        };
        // A symlink is a node with no bytes of its own; so is a file the
        // header already settled, whose body is not worth inflating.
        if is_symlink || !(offer.loads() || offer.wants_bytes()) {
            offer.without_bytes();
            continue;
        }
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut bytes)?;
        if workers.send(Pending { offer, bytes }).is_err() {
            return Ok(());
        }
    }
    Ok(())
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
    use crate::files::{CapExceeded, Decision, File, Need, Pass};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

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

    /// A node the repository lists but keeps no bytes for.
    fn listed_only(vfs: &Vfs, path: &str) -> bool {
        vfs.exists(Path::new(path))
            && vfs.read(Path::new(path)).unwrap_err().kind() == std::io::ErrorKind::Unsupported
    }

    fn paths(vfs: &Vfs) -> Vec<String> {
        vfs.files().into_iter().map(|e| e.path).collect()
    }

    #[test]
    fn extracts_and_strips_archive_root() {
        let vfs = Vfs::new(ParseAll, None);
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/src/lib.rs", b"pub mod lib;"),
        ]);
        extract(&data[..], &vfs).unwrap();
        assert_eq!(
            vfs.read_to_string(Path::new("src/main.rs")).unwrap(),
            "fn main() {}"
        );
        assert!(!vfs.exists(Path::new("project-main")));
    }

    #[test]
    fn skips_pax_global_and_per_file_headers() {
        let vfs = Vfs::new(ParseAll, None);
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

        extract(&data[..], &vfs).unwrap();
        assert_eq!(
            vfs.read_to_string(Path::new("src/main.rs")).unwrap(),
            "fn main() {}"
        );
    }

    #[test]
    fn skips_entry_outside_archive_root_and_keeps_the_rest() {
        let vfs = Vfs::new(ParseAll, None);
        let data = build_archive(&[
            Entry::File("root-a/file1.rs", b"a"),
            Entry::File("root-b/file2.rs", b"b"),
        ]);
        extract(&data[..], &vfs).unwrap();
        assert_eq!(paths(&vfs), vec!["file1.rs"]);
        assert!(vfs.exists(Path::new("file1.rs")));
    }

    /// Git allows names longer than a filesystem would; without a disk in
    /// the way, so do we.
    #[test]
    fn names_too_long_for_a_disk_are_ordinary_names_here() {
        let vfs = Vfs::new(ParseAll, None);
        let long = format!("root/{}/{}.rs", "d".repeat(300), "f".repeat(300));
        let data = build_archive(&[
            Entry::File("root/src/main.rs", b"fn main() {}"),
            Entry::File(&long, b"fn f() {}"),
        ]);
        extract(&data[..], &vfs).unwrap();
        assert_eq!(vfs.len(), 2);
        assert_eq!(
            vfs.read_to_string(Path::new(&long["root/".len()..]))
                .unwrap(),
            "fn f() {}"
        );
    }

    #[test]
    fn rejects_path_traversal() {
        let vfs = Vfs::new(ParseAll, None);
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

        let err = extract(&data[..], &vfs).unwrap_err();
        assert!(err.to_string().contains("path traversal"), "got: {err}");
    }

    /// A symlink is a node in the tree with no bytes, wherever it points; there
    /// is no disk for it to escape.
    #[test]
    fn symlinks_are_listed_and_never_read() {
        let vfs = Vfs::new(ParseAll, None);
        let data = build_archive(&[
            Entry::File("root/src/lib.rs", b"real content"),
            Entry::Symlink("root/bin/run", "../src/lib.rs"),
            Entry::Symlink("root/escape", "/etc/passwd"),
        ]);
        extract(&data[..], &vfs).unwrap();

        assert_eq!(paths(&vfs), vec!["bin/run", "escape", "src/lib.rs"]);
        let decision = |p: &str| vfs.file(Path::new(p)).unwrap().decision;
        assert_eq!(decision("bin/run"), Decision::ListOnly);
        assert_eq!(decision("escape"), Decision::ListOnly);
        assert!(listed_only(&vfs, "bin/run"));
        assert!(listed_only(&vfs, "escape"));
        assert_eq!(
            vfs.read_to_string(Path::new("src/lib.rs")).unwrap(),
            "real content"
        );
    }

    #[test]
    fn empty_and_truncated_bodies_are_classified_empty() {
        let vfs = Vfs::new(ParseAll, None);
        assert!(matches!(extract(&[][..], &vfs), Err(SourceError::Empty)));
        let full = build_archive(&[Entry::File("project-main/src/main.rs", b"fn main() {}")]);
        let truncated = &full[..full.len() / 2];
        assert!(matches!(extract(truncated, &vfs), Err(SourceError::Empty)));
    }

    #[test]
    fn list_only_files_are_recorded_but_not_written() {
        let vfs = Vfs::new(TestFilter, None);
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/assets/logo.png", b"\x89PNGdata"),
            Entry::File("project-main/model/weights.onnx", b"\x00\x01\x02blob"),
        ]);
        extract(&data[..], &vfs).unwrap();

        assert_eq!(
            paths(&vfs),
            vec!["assets/logo.png", "model/weights.onnx", "src/main.rs"]
        );
        assert_eq!(
            vfs.file(Path::new("src/main.rs")).unwrap().decision,
            Decision::Parse
        );
        assert_eq!(
            vfs.file(Path::new("assets/logo.png")).unwrap().decision,
            Decision::ListOnly
        );
        assert_eq!(
            vfs.file(Path::new("model/weights.onnx")).unwrap().decision,
            Decision::ListOnly
        );
        assert!(vfs.read(Path::new("src/main.rs")).is_ok());
        assert!(listed_only(&vfs, "assets/logo.png"));
        assert!(listed_only(&vfs, "model/weights.onnx"));
    }

    #[test]
    fn text_file_larger_than_sniff_window_is_written_in_full() {
        let vfs = Vfs::new(ParseAll, None);
        let body: Vec<u8> = (0..12_000).map(|i| ((i % 254) + 1) as u8).collect();
        let data = build_archive(&[Entry::File("project-main/big.txt", &body)]);
        extract(&data[..], &vfs).unwrap();
        assert_eq!(&*vfs.read(Path::new("big.txt")).unwrap(), &body[..]);
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
        let vfs = Vfs::new(MaxSize(64), None);
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

        extract(&data[..], &vfs).unwrap();

        assert_eq!(paths(&vfs), vec!["big.txt"]);
        let big = vfs.file(Path::new("big.txt")).unwrap();
        assert_eq!((big.size, big.decision), (4096, Decision::ListOnly));
        assert!(listed_only(&vfs, "big.txt"));
    }
}

#[cfg(test)]
mod backpressure {
    use super::*;
    use crate::files::{CapExceeded, File, Need, Pass};
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts files whose bytes are off the stream but not yet settled by a
    /// worker, and remembers the most that were in flight at once.
    #[derive(Default)]
    struct SlowSettle {
        in_flight: AtomicUsize,
        peak: Arc<AtomicUsize>,
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
        let peak = Arc::new(AtomicUsize::new(0));
        let vfs = Vfs::new(
            SlowSettle {
                in_flight: AtomicUsize::new(0),
                peak: peak.clone(),
            },
            None,
        );
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

        extract(&data[..], &vfs).unwrap();

        assert_eq!(vfs.len(), 2_000);
        let ceiling = LOOKAHEAD + rayon::current_num_threads() + 1;
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak <= ceiling, "peak in flight {peak} exceeds {ceiling}");
        assert!(peak > 1, "workers must run alongside the inflating thread");
    }
}
