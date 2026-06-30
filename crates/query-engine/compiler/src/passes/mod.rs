//! Each pass module exposes a pure function (e.g. `normalize::normalize`,
//! `restrict::restrict`) consumed by the phase functions in `config.rs`.

pub mod check;
pub mod codegen;
pub mod enforce;
mod errors;
pub mod hydrate;
pub mod lower;
pub mod normalize;
pub mod plan;
pub mod restrict;
pub mod security;
pub mod settings;
pub mod shared;
pub mod validate;
