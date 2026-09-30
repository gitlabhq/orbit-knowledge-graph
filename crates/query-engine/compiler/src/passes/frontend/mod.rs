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
