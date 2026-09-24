//! Each pass module exposes a pure function (e.g. `normalize::normalize`,
//! `restrict::restrict`) consumed by the phase functions in `config.rs`.

pub mod check;
pub mod codegen;
pub mod cursor;
pub mod enforce;
mod errors;
pub mod frontend;
pub mod hydrate;
pub mod logical_v3;
pub mod lower_v3;
pub mod normalize;
pub mod physical_v3;
pub mod plan_node;
// pub mod prototype_v3;
pub mod restrict;
pub mod security;
pub mod settings;
pub mod shared;
pub mod validate;
