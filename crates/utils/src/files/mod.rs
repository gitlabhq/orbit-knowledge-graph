//! A repository as a read-only filesystem, loaded once from a source.
//!
//! ```text
//! Source ──put──▶ Loading<T> ──freeze──▶ Vfs<T>
//!                   │                      read / read_dir / stat
//!                   ▼                      files / subtree / usage
//!            Pass::header (path, size)
//!            Pass::content (bytes)       →  Decision<T>: Keep(T) | List(why) | Drop(why)
//! ```
//!
//! Decisions are policy and belong to a [`Pass`]; where bytes live, when they
//! are read and what a path resolves to is mechanism and belongs to the store.
//! Paths resolve lexically under `/`; nothing outside the repository is
//! reachable, through a symlink or otherwise.

pub mod sources;
#[cfg(test)]
mod tests;
mod vfs;

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

pub use sources::Source;
pub use vfs::{Kind, Loading, Put, Stat, Vfs};

/// What becomes of a file. `Pending` is the start state and, after `header`,
/// means "the bytes decide"; it is never observable once the store is loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Decision<T> {
    #[default]
    Pending,
    Keep(T),
    List(&'static str),
    Drop(&'static str),
}

/// A domain's tag on a kept file. `Default` is what a `Pending` file becomes
/// when no pass objected to its bytes.
pub trait Tag: Copy + Default + Send + Sync + 'static {}
impl<T: Copy + Default + Send + Sync + 'static> Tag for T {}

/// One file of the repository and what the passes decided about it.
#[derive(Debug, Clone)]
pub struct File<T> {
    pub path: String,
    pub size: u64,
    decided: Decision<T>,
    /// A file linked from disk is checked by the content passes on its first
    /// read, after the store is frozen; this is that one late verdict.
    verdict: OnceLock<Decision<T>>,
}

impl<T: Tag> File<T> {
    pub fn new(path: String, size: u64) -> Self {
        Self {
            path,
            size,
            decided: Decision::Pending,
            verdict: OnceLock::new(),
        }
    }

    pub fn decision(&self) -> Decision<T> {
        self.verdict.get().copied().unwrap_or(self.decided)
    }

    pub fn decide(&mut self, decision: Decision<T>) {
        self.decided = decision;
    }

    pub fn keeps(&self) -> bool {
        matches!(self.decision(), Decision::Keep(_))
    }

    /// `Pending` after the content passes means no policy objected.
    fn settle(&mut self) {
        if matches!(self.decided, Decision::Pending) {
            self.decided = Decision::Keep(T::default());
        }
    }
}

/// A pure function of path, size and bytes. It never sees a symlink, never
/// counts anything and cannot fail. `header` runs on every file; `content`
/// runs once, on the one read, for files still `Pending` or `Keep` after it.
/// A `Drop` from either leaves no node, except when the one read is a
/// parser's first `read` of a linked file: the store is frozen by then, so
/// that node stays and reads as `Unsupported`.
pub trait Pass: Send + Sync {
    type Tag: Tag;

    fn header(&self, _file: &mut File<Self::Tag>) {}

    fn content(&self, _file: &mut File<Self::Tag>, _bytes: &[u8]) {}

    fn then<B: Pass<Tag = Self::Tag>>(self, next: B) -> Then<Self, B>
    where
        Self: Sized,
    {
        Then(self, next)
    }
}

/// Two passes in order: the second sees the first's decision.
pub struct Then<A, B>(A, B);

impl<A: Pass, B: Pass<Tag = A::Tag>> Pass for Then<A, B> {
    type Tag = A::Tag;

    fn header(&self, file: &mut File<Self::Tag>) {
        self.0.header(file);
        self.1.header(file);
    }

    fn content(&self, file: &mut File<Self::Tag>, bytes: &[u8]) {
        self.0.content(file, bytes);
        self.1.content(file, bytes);
    }
}

/// No policy: keep everything.
impl Pass for () {
    type Tag = ();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: &'static str,
    pub count: u64,
    pub cap: u64,
}

/// Whole-source failure: the run stops rather than index a partial repository.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] io::Error),
    /// The source held no entries (empty or truncated archive); callers treat
    /// it as an empty repository, not a failure to retry.
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
    #[error("load cancelled")]
    Cancelled,
}

/// Resource caps. Every domain wants them, so the store enforces them and the
/// passes never count. Per store: a process running N loads at once divides
/// its budget by N.
#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    /// Over → `List("oversize")`.
    pub file_bytes: Option<u64>,
    /// Over → `SourceError::Cap`.
    pub total_bytes: Option<u64>,
    /// Over → `SourceError::Cap`.
    pub files: Option<usize>,
    /// Over → bytes spill to the scratch file. `Some(0)` spills everything.
    pub resident_bytes: Option<u64>,
    /// Over → `SourceError::Cap`, before the disk says `ENOSPC`.
    pub spilled_bytes: Option<u64>,
}

/// How the store runs. Nothing here changes what is kept or refused.
#[derive(Default)]
pub struct Options {
    /// Where the scratch file lives; `None` is `$TMPDIR`. Point it at a real
    /// volume, not a tmpfs.
    pub scratch_dir: Option<PathBuf>,
    /// LZ4 every spilled blob. Around 3:1 on source text at GB/s both ways.
    pub compress_spill: bool,
    /// Polled once per `put`; any token plugs in with a closure.
    pub cancelled: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

/// The aggregates only the store knows. Tallies by reason come from `files()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    pub files: usize,
    pub bytes: u64,
    pub kept: u64,
    pub resident: u64,
    pub spilled: u64,
    /// What content-addressing saved.
    pub deduped_bytes: u64,
    /// Entries that named a path already taken; the last one wins, as `tar x`
    /// does.
    pub duplicate_paths: usize,
}

pub(crate) type Bytes = Arc<[u8]>;
