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

mod limits;
mod loading;
mod path;
mod policy;
mod scratch;
pub mod sources;
#[cfg(test)]
mod tests;
mod vfs;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

pub use limits::{CapExceeded, Limits};
pub use loading::{Loading, Put};
pub use policy::{Decision, File, Pass, Tag, Then};
pub use sources::Source;
pub use vfs::{Kind, Stat, Vfs};

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
