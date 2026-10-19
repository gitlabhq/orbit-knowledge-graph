mod compact;
mod mutable;

pub use compact::Compact;
pub(crate) use compact::Entry;
pub use mutable::Mutable;

pub(crate) const NONE: u32 = u32::MAX;

pub trait Storage {
    type Node;
    type Id: Copy + Eq;

    fn index(id: Self::Id) -> u32;
    fn id(&self, index: u32) -> Self::Id;
    fn is_removed(&self, id: Self::Id) -> bool;
    fn node(&self, id: u32) -> &Self::Node;
    fn node_mut(&mut self, id: u32) -> &mut Self::Node;
    fn parent(&self, id: u32) -> Option<u32>;
    fn first_child(&self, id: u32) -> Option<u32>;
    fn last_child(&self, id: u32) -> Option<u32>;
    fn next_sibling(&self, id: u32) -> Option<u32>;
    fn previous_sibling(&self, id: u32) -> Option<u32>;
}
