pub mod gql;

use query_data_model::Model;
use query_data_model::plan::logical::LogicalCatalog;
use query_data_model::surface::{Statement, json};

use crate::Result;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames, strum::IntoStaticStr,
)]
pub enum Frontend {
    #[strum(serialize = "json")]
    JsonDsl,
    #[strum(serialize = "gql")]
    Gql,
}

pub fn catalog(model: &Model, admin: bool) -> LogicalCatalog<'_> {
    match admin {
        true => model.logical().as_admin(),
        false => model.logical(),
    }
}

pub fn parse(raw: &str, frontend: Frontend, catalog: LogicalCatalog<'_>) -> Result<Statement> {
    let statement = match frontend {
        Frontend::JsonDsl => json::parse(raw, catalog),
        Frontend::Gql => query_data_model::surface::gql::parse(raw, catalog),
    };
    Ok(statement?)
}
