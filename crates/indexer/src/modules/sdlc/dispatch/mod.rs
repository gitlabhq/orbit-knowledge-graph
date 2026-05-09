pub(crate) mod boundaries;
mod entity;
mod global;
mod namespace;

pub use entity::{EntityDescriptor, EntityDispatcher};
pub use global::GlobalDispatcher;
pub use namespace::NamespaceDispatcher;
