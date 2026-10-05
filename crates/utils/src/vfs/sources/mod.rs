//! Where files come from. Every source uses only `Loading::put`.

mod archive;
mod checkout;

pub use archive::Archive;
pub use checkout::{Changed, Checkout};

use super::{Loading, Put, SourceError, Tag};

pub trait Source {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError>;
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
