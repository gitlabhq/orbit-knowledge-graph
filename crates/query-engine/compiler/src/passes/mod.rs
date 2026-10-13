//! Each pass module exposes a pure function (e.g. `normalize::normalize`,
//! `restrict::restrict`) consumed by the phase functions in `config.rs`.

pub mod codegen;
pub mod frontend;
pub mod hydrate;
pub mod lower;
pub mod normalize;
pub mod page;
pub mod plan;
pub mod relationships;
pub mod response_policy;
pub mod restrict;
pub mod result_contract;
pub mod security;
pub mod security_check;
pub mod settings;
pub mod validate;
