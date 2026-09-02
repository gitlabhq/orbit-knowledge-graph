//! Pinned versions from `config/versions.yaml`, embedded at compile time.
//! Dependency-light on purpose so build scripts can use it too.

use std::sync::LazyLock;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Versions {
    pub schema: u32,
    pub query_dsl: String,
    pub raw_output_format: String,
    pub goon_output_format: String,
    pub duckdb: String,
    pub gitlab_system_note_actions: String,
}

pub static VERSIONS: LazyLock<Versions> =
    LazyLock::new(|| parse(include_str!(env!("VERSIONS_FILE"))).expect("config/versions.yaml"));

/// Parses any revision's `versions.yaml` text, e.g. `git show` output.
pub fn parse(yaml: &str) -> Result<Versions, serde_saphyr::Error> {
    serde_saphyr::from_str(yaml)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_pin() {
        assert!(VERSIONS.schema > 0);
        assert!(VERSIONS.duckdb.starts_with('v'));
        assert_eq!(VERSIONS.gitlab_system_note_actions.len(), 40);
    }
}
