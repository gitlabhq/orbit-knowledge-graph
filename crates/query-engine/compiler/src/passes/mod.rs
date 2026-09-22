//! Each pass module exposes a pure function (e.g. `normalize::normalize`,
//! `restrict::restrict`) consumed by the phase functions in `config.rs`.

pub mod check;
pub mod codegen;
pub mod cursor;
pub mod enforce;
mod errors;
pub mod frontend;
pub mod hydrate;
pub mod lower;
pub mod lower_v2;
pub mod normalize;
pub mod optimize;
pub mod plan;
pub mod plan_v2;
pub mod restrict;
pub mod security;
pub mod settings;
pub mod sexpr;
pub mod shared;
pub mod validate;
