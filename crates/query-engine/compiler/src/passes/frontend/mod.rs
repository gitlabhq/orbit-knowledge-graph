//! Query frontends. Each is the first phase of its own pipeline preset in
//! `config.rs` and lowers the raw query text to a compiler
//! [`Input`](crate::input::Input). Every phase after it is shared.

pub mod gql;
pub mod json_dsl;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frontend {
    JsonDsl,
    Gql,
}

impl Frontend {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "json" => Some(Self::JsonDsl),
            "gql" => Some(Self::Gql),
            _ => None,
        }
    }
}
