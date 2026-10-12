//! Pinned versions from `config/versions.yaml`, embedded at compile time.
//! Dependency-light on purpose so build scripts can use it too.
//!
//! Format and security constraints (key patterns, hex lengths, path
//! restrictions) are enforced by `config/schemas/versions.schema.json`
//! and validated in CI. The Rust structs provide typed access and
//! `deny_unknown_fields` catches structural drift at compile time.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Versions {
    pub schema: u32,
    pub query_dsl: String,
    pub raw_output_format: String,
    pub toon_output_format: String,
    pub gql_output_format: String,
    pub vendored: BTreeMap<String, VendoredDependency>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VendoredDependency {
    pub version: Option<String>,
    pub vendor_dir: Option<String>,
    pub vendor_script: Option<String>,
    pub check_script: Option<String>,
    pub pins: Option<BTreeMap<String, String>>,
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
        assert!(!VERSIONS.vendored.is_empty());
    }

    #[test]
    fn gitlab_system_note_actions_entry_is_complete() {
        let entry = VERSIONS
            .vendored
            .get("gitlab_system_note_actions")
            .expect("vendored.gitlab_system_note_actions");
        let version = entry.version.as_deref().expect("version");
        assert_eq!(version.len(), 40);
        assert!(entry.vendor_dir.is_some());
        assert!(entry.check_script.is_some());
    }

    #[test]
    fn iglu_entry_is_complete() {
        let entry = VERSIONS.vendored.get("iglu").expect("vendored.iglu");
        assert!(entry.vendor_dir.is_some());
        assert!(entry.vendor_script.is_some());
        assert!(entry.check_script.is_some());

        let pins = entry.pins.as_ref().expect("iglu.pins");
        for name in [
            "orbit_query",
            "orbit_common",
            "orbit_code_indexing",
            "orbit_sdlc_indexing",
        ] {
            let version = pins
                .get(name)
                .unwrap_or_else(|| panic!("missing iglu pin for {name}"));
            assert!(!version.is_empty(), "empty iglu pin for {name}");
        }
    }
}
