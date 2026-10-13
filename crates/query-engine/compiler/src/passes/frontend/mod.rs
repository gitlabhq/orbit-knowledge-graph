pub mod gql;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames, strum::IntoStaticStr,
)]
pub enum Frontend {
    #[strum(serialize = "gql")]
    Gql,
}

impl Frontend {
    /// Query-language names, as written in request selectors and test fixtures.
    pub fn names() -> &'static [&'static str] {
        <Self as strum::VariantNames>::VARIANTS
    }
}
