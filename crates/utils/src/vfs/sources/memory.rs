use super::{Loading, Put, Source, SourceError, Tag};

pub struct Memory(pub Vec<(String, Vec<u8>)>);

impl Source for Memory {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        self.0
            .into_iter()
            .try_for_each(|(path, bytes)| into.put(&path, Put::Bytes(bytes)))
    }
}
