//! Where files come from. Every source uses only `Loading::put`.

mod archive;
mod directory;

pub use archive::Archive;
pub use directory::{Changeset, Directory};

use super::{Loading, Put, Source, SourceError, Tag};

fn is_safe_relative_path(path: &std::path::Path) -> bool {
    path.components()
        .all(|part| matches!(part, std::path::Component::Normal(_)))
}

/// Files in hand: tests and small callers.
pub struct Memory(pub Vec<(String, Vec<u8>)>);

impl Source for Memory {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        self.0
            .into_iter()
            .try_for_each(|(path, bytes)| into.put(&path, Put::Bytes(bytes)))
    }
}
