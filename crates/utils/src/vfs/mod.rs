//! A repository as a read-only filesystem, loaded once from a source.
//!
//! ```text
//! Source ──put──▶ Loading<T> ──freeze──▶ Vfs<T>
//!                   │                      read / read_dir / stat
//!                   ▼                      files / subtree / usage
//!            Pass::metadata (&File)
//!            Pass::content (&File)       →  Decision<T>: Keep(T) | List(why)
//! ```
//!
//! Decisions are policy and belong to a [`Pass`]; where bytes live, when they
//! are read and what a path resolves to is mechanism and belongs to the store.
//! Virtual paths resolve lexically under `/`. Directory sources keep host paths separate
//! and reject symlink traversal during disk reads. Custom sources select trusted host paths.
//! Disk-linked contents are live, not snapshots; callers must provide a stable directory
//! when they need one revision. Repeated reads can perform I/O again.
//!
//! Cancellation is cooperative between files, not an interrupt for blocked source I/O.
//! Memory limits cover stored content, not node metadata, decompression or caller buffers.

#![doc = include_str!("README.md")]

mod limits;
mod loading;
mod path;
mod policy;
mod scratch;
pub mod sources;
mod store;
mod syscalls;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

pub use limits::{CapExceeded, Limits};
pub use loading::{Loading, Put};
pub use policy::{Decision, File, Pass, Tag, Then};
pub use sources::Source;
pub use store::{Kind, Stat, Vfs};

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] io::Error),
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
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
