use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Symbol(usize);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Identifier {
    Named(String),
    Generated(Symbol),
}

impl Identifier {
    pub fn generated() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self::Generated(Symbol(NEXT.fetch_add(1, Ordering::Relaxed)))
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Named(name) => Some(name),
            Self::Generated(_) => None,
        }
    }
}

impl From<String> for Identifier {
    fn from(name: String) -> Self {
        Self::Named(name)
    }
}

impl From<&str> for Identifier {
    fn from(name: &str) -> Self {
        Self::Named(name.into())
    }
}

impl From<&String> for Identifier {
    fn from(name: &String) -> Self {
        Self::Named(name.clone())
    }
}

impl From<&Identifier> for Identifier {
    fn from(identifier: &Identifier) -> Self {
        identifier.clone()
    }
}

impl PartialEq<str> for Identifier {
    fn eq(&self, other: &str) -> bool {
        self.name() == Some(other)
    }
}

impl PartialEq<&str> for Identifier {
    fn eq(&self, other: &&str) -> bool {
        self.name() == Some(*other)
    }
}
