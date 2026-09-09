//! Query frontends. Each is the first phase of its own pipeline preset in
//! `config.rs` and lowers the raw query text to a compiler
//! [`Input`](crate::input::Input). Every phase after it is shared.

pub mod gql;
pub mod json_dsl;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames, strum::IntoStaticStr,
)]
pub enum Frontend {
    #[strum(serialize = "json")]
    JsonDsl,
    #[strum(serialize = "gql")]
    Gql,
}
