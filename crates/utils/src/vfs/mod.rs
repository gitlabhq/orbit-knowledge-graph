//! A repository as a read-only filesystem, loaded once from a source.
//!
//! ```text
//! Source ──put──▶ Loading<T> ──finish──▶ Vfs<T>
//!                   │                      read / read_dir / stat
//!                   ▼                      files / subtree / usage
//!            Pass::metadata (&File)
//!            Pass::content (&File)       →  Decision<T>: Keep(T) | List(why)
//! ```
//!
//! Decisions are policy and belong to a [`Pass`]; where bytes live, when they
//! are read and what a path resolves to is mechanism and belongs to the store.
//! Virtual paths resolve lexically under `/`. Sources own transport and host-path validation.
//! On-demand readers can return live content. Repeated reads can perform I/O again.
//!
//! Cancellation is cooperative between files, not an interrupt for blocked source I/O.
//! Memory limits cover stored content, not node metadata, decompression or caller buffers.
//! Every valid offered path is cataloged; duplicate paths use the last offered entry.
//! Reader failures remain attached to content and are returned by `read`, without a policy change.
//! Source failures, storage failures, cancellation, and global caps abort loading.

mod limits;
mod loading;
mod path;
mod policy;
mod scratch;
mod store;

#[cfg(test)]
mod tests;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

pub use limits::{CapExceeded, Limits};
pub use loading::{ContentReader, Loading, Put};
pub use policy::{Decision, File, Pass, Tag, Then};
pub use store::{Kind, Stat, Vfs};

pub trait Source {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] io::Error),
    #[error("load cancelled")]
    Cancelled,
}

#[derive(Default)]
pub struct Options {
    /// Defaults to the system temporary directory; use a disk-backed volume to offload RAM.
    pub scratch_dir: Option<PathBuf>,
    pub compress_spill: bool,
    pub cancelled: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    pub files: usize,
    pub bytes: u64,
    pub kept: u64,
    pub resident: u64,
    pub spilled: u64,
    pub deduped_bytes: u64,
    pub duplicate_paths: usize,
}

type Bytes = Arc<[u8]>;
