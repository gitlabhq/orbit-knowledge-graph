//! One filtering and limit surface for every file source.
//!
//! A repository's files reach the indexer two ways — a Gitaly tar (the server,
//! [`crate::archive`]) and a directory walk (orbit-local, [`crate::walk`]) — and
//! historically each filtered differently. Both now feed their entries through
//! one [`FileStreamHooks`] policy via [`step`]: the sources carry no filtering,
//! the hooks decide [`Decision`] per file and enforce aggregate [`Counter`]
//! caps. What we load, what we record as a bare node, and how large a repository
//! we accept all live in one place.

/// Per-file outcome of the hook pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, strum::Display, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Decision {
    /// Load the bytes (materialize on disk) and record the file. The only
    /// decision whose file is a parse candidate downstream.
    #[default]
    Keep,
    /// Record the file as a node but never read its bytes (binary, excluded,
    /// oversize, minified).
    ListOnly,
    /// Exclude the file entirely; it is not recorded.
    Drop,
}

/// A file discovered in a repository, recorded whether or not its bytes were
/// loaded. `path` is repository-relative. `decision` is the verdict the stream
/// reached; downstream parse selection keeps only [`Decision::Keep`] entries, so
/// both file sources agree without re-deriving eligibility from disk state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInventoryEntry {
    pub path: String,
    pub size: u64,
    pub decision: Decision,
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
    /// The source produced no entries because its stream ended before any could
    /// be read (empty or truncated archive). Callers classify this as an
    /// empty-repository outcome, not a retryable failure.
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
}

/// The filtering and accounting policy for a file stream. Every method is
/// optional (defaults to a pass-through), so a consumer implements only what it
/// needs and holds its own state — e.g. [`Counter`]s — in `self`. Generic, no
/// `dyn`: the driver is monomorphized over the concrete `Self`.
///
/// This is the single place filtering lives; the sources (tar, dir walk) carry
/// none of their own.
pub trait FileStreamHooks {
    /// Charge aggregate counters for the whole repository. Called once per
    /// source entry regardless of the keep decision, so caps see every byte
    /// (a repo of nothing but excluded blobs still trips a total-bytes cap).
    /// `Err` aborts the entire stream.
    fn admit(&mut self, _file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        Ok(())
    }
    /// Decide from path + size alone, before any bytes are read. A non-`Keep`
    /// verdict here is final and the file's content is never read.
    fn on_header(&mut self, _file: &FileInventoryEntry) -> Decision {
        Decision::Keep
    }
    /// Decide with the file's full (size-capped) content available — binary
    /// sniff, minified/long-line detection. Only reached for files `on_header`
    /// kept; the final say.
    fn on_content(&mut self, _file: &FileInventoryEntry, _content: &[u8]) -> Decision {
        Decision::Keep
    }
}

/// Run the per-file hook sequence and return the [`Decision`] the caller acts
/// on (write + record, record only, or skip). The whole driver loop minus the
/// source-specific "pull next entry" and "write the bytes": `admit` charges
/// caps for every file, then `on_header` can settle the verdict without reading
/// content, and only a `Keep` proceeds to fill `prefix` via `sniff` and consult
/// `on_content`. `prefix` is caller-owned so it can be reused across entries.
pub fn step<H: FileStreamHooks>(
    hooks: &mut H,
    file: &FileInventoryEntry,
    prefix: &mut Vec<u8>,
    sniff: impl FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
) -> Result<Decision, StreamError> {
    hooks.admit(file)?;
    prefix.clear();
    match hooks.on_header(file) {
        Decision::Keep => {}
        settled => return Ok(settled),
    }
    sniff(prefix)?;
    Ok(hooks.on_content(file, prefix))
}

/// A capped running total (`cap == 0` = unlimited). Compose as many as a source
/// needs — total bytes, file count, anything — and `add` to each per kept file;
/// the first to overflow short-circuits the stream.
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
        fn on_header(&mut self, f: &FileInventoryEntry) -> Decision {
            if f.path.ends_with(".png") {
                Decision::Drop
            } else {
                Decision::Keep
            }
        }
        fn admit(&mut self, f: &FileInventoryEntry) -> Result<(), CapExceeded> {
            self.bytes.add(f.size)
        }
    }

    fn entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Keep,
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
        assert_eq!(d, Decision::Keep);
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
}
