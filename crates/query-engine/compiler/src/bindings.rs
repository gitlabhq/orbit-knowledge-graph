use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Definition(Arc<String>);

impl Definition {
    pub fn new(hint: impl Into<String>) -> Self {
        Self(Arc::new(hint.into()))
    }

    pub fn hint(&self) -> &str {
        &self.0
    }
}

impl PartialEq for Definition {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Definition {}

impl Hash for Definition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.0), state);
    }
}
