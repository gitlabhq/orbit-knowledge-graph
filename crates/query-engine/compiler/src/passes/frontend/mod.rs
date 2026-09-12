//! Query frontends. Each lowers raw statement text to a language-neutral
//! [`Statement`]: graph queries become the compiler [`Input`] that the shared
//! phases consume; schema calls become a [`SchemaRequest`] for the
//! `schema_call` pipeline in `config.rs`.

pub mod gql;
pub mod json_dsl;

use crate::input::Input;

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString)]
pub enum Frontend {
    #[strum(serialize = "json")]
    JsonDsl,
    #[strum(serialize = "gql")]
    Gql,
}

#[derive(Debug)]
pub enum Statement {
    Query(Box<Input>),
    Schema(SchemaRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRequest {
    pub node: Option<String>,
}
