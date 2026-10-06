use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Definition(Arc<DefinitionData>);

#[derive(Debug)]
struct DefinitionData {
    hint: String,
    exports: Vec<Export>,
}

impl Definition {
    pub fn new(hint: impl Into<String>, exports: Vec<Export>) -> Self {
        Self(Arc::new(DefinitionData {
            hint: hint.into(),
            exports,
        }))
    }

    pub fn hint(&self) -> &str {
        &self.0.hint
    }

    pub fn exports(&self) -> &[Export] {
        &self.0.exports
    }
}

#[derive(Debug, Clone)]
pub struct Export(Arc<String>);

impl Export {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::new(name.into()))
    }
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl PartialEq for Export {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Export {}
impl Hash for Export {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.0), state);
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
