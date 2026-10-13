//! Each pass module exposes a compiler phase consumed by `config.rs`.

pub mod bind;
pub mod check;
pub mod codegen;
pub mod cursor;
pub mod enforce;
pub(crate) mod errors;
pub mod frontend;
pub mod hydrate;
pub mod lower;
pub mod plan;
pub mod response_policy;
pub mod restrict;
pub mod security;
pub mod settings;
