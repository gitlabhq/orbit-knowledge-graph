mod limits;
mod loading;
mod policy;
mod scratch;
pub mod sources;
mod store;

#[cfg(test)]
mod tests;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

pub use limits::{CapExceeded, LimitKind, Limits};
pub use loading::{ContentReader, Loading, Put};
pub use policy::{Decision, File, Pass, Tag, Then};
pub use store::{Kind, Stat, Vfs};

pub trait Source {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError>;
}

impl<'a, I, P> Source for I
where
    I: IntoIterator<Item = (P, Put<'a>)>,
    P: AsRef<str>,
{
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        self.into_iter()
            .try_for_each(|(path, input)| into.put(path.as_ref(), input))
    }
}

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
    pub dropped_listed: usize,
}

type Bytes = Arc<[u8]>;
