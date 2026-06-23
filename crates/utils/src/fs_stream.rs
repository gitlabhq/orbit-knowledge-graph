//! One filtering and limit surface for every file source. A repository's files
//! arrive two ways — a Gitaly tar ([`crate::archive`]) and a directory walk
//! ([`crate::walk`]) — and both run every entry through one [`FileStreamHooks`]
//! policy via [`step`]; the sources carry no filtering of their own.

use std::path::{Component, Path};

use rustc_hash::FxHashMap;

/// Per-file outcome of the hook pipeline. The two loaded states split the
/// materialize axis from the parse axis: both `Parse` and `Load` make the bytes
/// available (on disk for the tar source); only `Parse` is sent to a parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, strum::Display, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Decision {
    /// Load the bytes and parse them. The only parse candidate downstream.
    #[default]
    Parse,
    /// Load the bytes but don't parse — resolver inputs (manifests, etc.) and
    /// content duplicates: available to resolvers, never sent to a parser.
    Load,
    /// Record the file as a node without loading its bytes.
    ListOnly,
    /// Exclude the file entirely.
    Drop,
}

/// A file discovered in a repository, recorded whether or not its bytes were
/// loaded. `decision` drives parse selection, so both sources agree without
/// re-checking disk state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInventoryEntry {
    pub path: String,
    pub size: u64,
    pub decision: Decision,
}

/// Normalize each path, drop duplicates (first wins), and sort. Sources call
/// this so every consumer receives one canonical inventory.
pub fn canonicalize_inventory(entries: Vec<FileInventoryEntry>) -> Vec<FileInventoryEntry> {
    let mut by_path: FxHashMap<String, (u64, Decision)> = FxHashMap::default();
    for entry in entries {
        let Some(path) = normalize_relative_path(&entry.path) else {
            continue;
        };
        by_path.entry(path).or_insert((entry.size, entry.decision));
    }
    let mut entries: Vec<_> = by_path
        .into_iter()
        .map(|(path, (size, decision))| FileInventoryEntry {
            path,
            size,
            decision,
        })
        .collect();
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

/// Normalize a `/`-joined relative path: drop `.` segments, reject anything that
/// climbs out (`..`, root, prefix). `None` if nothing remains.
fn normalize_relative_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

/// An aggregate cap tripped mid-stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: &'static str,
    pub count: u64,
    pub cap: u64,
}

/// Fatal, whole-stream failure: a cap tripped or the source could not be read,
/// so the run is abandoned rather than indexing a partial/oversized repository.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] std::io::Error),
    /// The source produced no entries (empty or truncated archive); callers
    /// treat it as an empty repository, not a retryable failure.
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
}

/// The filtering and accounting policy for a file stream. Each method defaults
/// to a pass-through; a consumer implements only what it needs and holds its
/// state (e.g. [`Counter`]s) in `self`. Generic, no `dyn`.
pub trait FileStreamHooks {
    /// Charge aggregate counters; called for every entry (so excluded blobs
    /// still count toward a total-bytes cap). `Err` aborts the stream.
    fn admit(&mut self, _file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        Ok(())
    }
    /// Settle from path + size alone, before any bytes are read. `Some` is final
    /// (and must not be `Parse` — that needs content); `None` reads the content.
    fn on_header(&mut self, _file: &FileInventoryEntry) -> Option<Decision> {
        None
    }
    /// Decide with the file's full (size-capped) content; only reached when
    /// `on_header` returned `None`.
    fn on_content(&mut self, _file: &FileInventoryEntry, _content: &[u8]) -> Decision {
        Decision::Parse
    }
    /// Settle a non-regular entry (symlink, etc.) — no content to sniff, never a
    /// parse candidate. Routed here (instead of decided in the source) so the
    /// filter stays the single decision point. Defaults to a bare node.
    fn on_non_regular(&mut self, _file: &FileInventoryEntry) -> Decision {
        Decision::ListOnly
    }
}

/// Run the per-file hook sequence: `admit` charges caps for every file, then
/// `on_header` may settle it without reading, and otherwise `content` is filled
/// via `sniff` and `on_content` gives the final [`Decision`]. `content` is
/// caller-owned to reuse across entries.
pub fn step<H: FileStreamHooks>(
    hooks: &mut H,
    file: &FileInventoryEntry,
    content: &mut Vec<u8>,
    sniff: impl FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
) -> Result<Decision, StreamError> {
    hooks.admit(file)?;
    content.clear();
    if let Some(settled) = hooks.on_header(file) {
        return Ok(settled);
    }
    sniff(content)?;
    Ok(hooks.on_content(file, content))
}

/// A capped running total (`cap == 0` = unlimited); the first `add` to overflow
/// short-circuits the stream.
pub struct Counter {
    metric: &'static str,
    cap: u64,
    count: u64,
}

impl Counter {
    pub fn new(metric: &'static str, cap: u64) -> Self {
        Self {
            metric,
            cap,
            count: 0,
        }
    }

    pub fn add(&mut self, n: u64) -> Result<(), CapExceeded> {
        self.count = self.count.saturating_add(n);
        if self.cap != 0 && self.count > self.cap {
            return Err(CapExceeded {
                metric: self.metric,
                count: self.count,
                cap: self.cap,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_admits_until_cap_then_short_circuits() {
        let mut bytes = Counter::new("bytes", 100);
        assert!(bytes.add(60).is_ok());
        assert_eq!(
            bytes.add(60),
            Err(CapExceeded {
                metric: "bytes",
                count: 120,
                cap: 100
            })
        );
    }

    #[test]
    fn zero_cap_is_unlimited() {
        let mut files = Counter::new("files", 0);
        assert!(files.add(u64::MAX).is_ok());
        assert!(files.add(u64::MAX).is_ok());
    }

    struct TestHooks {
        bytes: Counter,
    }

    impl FileStreamHooks for TestHooks {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<Decision> {
            f.path.ends_with(".png").then_some(Decision::Drop)
        }
        fn admit(&mut self, f: &FileInventoryEntry) -> Result<(), CapExceeded> {
            self.bytes.add(f.size)
        }
    }

    fn entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Parse,
        }
    }

    #[test]
    fn step_settles_in_header_without_sniffing() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", 0),
        };
        let mut prefix = Vec::new();
        let d = step(&mut h, &entry("a.png", 10), &mut prefix, |_| {
            panic!("a header-settled file must never be sniffed")
        })
        .unwrap();
        assert_eq!(d, Decision::Drop);
    }

    #[test]
    fn step_admits_kept_file() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", 100),
        };
        let mut prefix = Vec::new();
        let d = step(&mut h, &entry("a.rs", 10), &mut prefix, |buf| {
            buf.extend_from_slice(b"fn main");
            Ok(())
        })
        .unwrap();
        assert_eq!(d, Decision::Parse);
    }

    #[test]
    fn step_charges_cap_before_keep_decision() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", 5),
        };
        let mut prefix = Vec::new();
        let err = step(&mut h, &entry("a.rs", 10), &mut prefix, |_| Ok(())).unwrap_err();
        assert!(matches!(err, StreamError::Cap(_)));
    }

    #[test]
    fn step_caps_charge_even_header_dropped_files() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", 5),
        };
        let mut prefix = Vec::new();
        let err = step(&mut h, &entry("blob.png", 10), &mut prefix, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, StreamError::Cap(_)),
            "a dropped file's bytes must still count toward the cap"
        );
    }

    #[test]
    fn canonicalize_dedups_normalizes_and_sorts() {
        let inv = canonicalize_inventory(vec![
            entry("./src/main.rs", 10),
            entry("src/main.rs", 10),
            entry("a/b.rs", 10),
        ]);
        let paths: Vec<&str> = inv.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["a/b.rs", "src/main.rs"]);
    }

    #[test]
    fn canonicalize_drops_traversal_entries() {
        let inv = canonicalize_inventory(vec![entry("../escape", 10), entry("ok.rs", 10)]);
        let paths: Vec<&str> = inv.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["ok.rs"]);
    }
}
