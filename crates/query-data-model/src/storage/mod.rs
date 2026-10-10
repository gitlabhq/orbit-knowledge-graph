pub mod relational;

pub use relational::{Relational, RelationalBackend};

pub trait StorageModel {
    type Schema: std::fmt::Debug;
    type Mapping: std::fmt::Debug;
}

#[derive(Debug)]
pub struct Storage<T: StorageModel> {
    pub schema: T::Schema,
    pub mapping: T::Mapping,
}

impl<T: StorageModel> Storage<T> {
    pub fn new(schema: T::Schema, mapping: T::Mapping) -> Self {
        Self { schema, mapping }
    }
}
