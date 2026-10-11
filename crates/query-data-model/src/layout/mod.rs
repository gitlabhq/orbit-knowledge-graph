pub mod relational;

pub use relational::{Relational, RelationalBackend};

pub trait LayoutModel {
    type Schema: std::fmt::Debug;
    type Mapping: std::fmt::Debug;
}

#[derive(Debug)]
pub struct Layout<T: LayoutModel> {
    pub schema: T::Schema,
    pub mapping: T::Mapping,
}

impl<T: LayoutModel> Layout<T> {
    pub fn new(schema: T::Schema, mapping: T::Mapping) -> Self {
        Self { schema, mapping }
    }
}
