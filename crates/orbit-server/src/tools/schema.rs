use std::sync::LazyLock;

use semver::Version;

const QUERY_RESPONSE_SCHEMA: &str =
    include_str!(concat!(env!("SCHEMA_DIR"), "/query_response.json"));
pub static QUERY_DSL_VERSION: LazyLock<Version> = LazyLock::new(|| {
    orbit_versions::VERSIONS
        .query_dsl
        .parse()
        .expect("QUERY_DSL_VERSION must be valid semver")
});

pub fn query_dsl_version() -> String {
    QUERY_DSL_VERSION.to_string()
}

pub fn query_response_schema() -> &'static str {
    QUERY_RESPONSE_SCHEMA
}
