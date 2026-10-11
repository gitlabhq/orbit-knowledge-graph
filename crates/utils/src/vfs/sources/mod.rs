//! Where files come from. Every source uses only `Loading::put`.

mod archive;
mod diff;
mod directory;
mod memory;

pub use archive::Archive;
pub use diff::Diff;
pub use directory::Directory;
pub use memory::Memory;

use super::{Loading, Put, Source, SourceError, Tag};
