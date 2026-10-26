use std::path::Path;

use orbit_server_config::{AppConfig, QuerySettings};
use query_engine::compiler::input::QueryType;
use strum::VariantNames;

/// Checks a config the way startup does before it starts anything.
pub fn run(overlay: Option<&Path>) -> anyhow::Result<()> {
    let config = AppConfig::check(overlay)?;
    query_keys(&config.query)?;
    config.probe_server_bind_address()?;
    Ok(())
}

pub fn query_keys(query: &QuerySettings) -> anyhow::Result<()> {
    let invalid_keys = query.validate_keys(QueryType::VARIANTS);
    anyhow::ensure!(
        invalid_keys.is_empty(),
        "unknown query type(s) in config: {invalid_keys:?} (valid: {:?})",
        QueryType::VARIANTS,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(yaml: &str) -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, yaml).unwrap();
        run(Some(&path))
    }

    #[test]
    fn the_embedded_defaults_pass() {
        check("").unwrap();
    }

    #[test]
    fn an_unknown_query_type_fails() {
        let error = check("query:\n  overrides:\n    bogus: {}\n").unwrap_err();

        assert!(error.to_string().contains("bogus"), "{error}");
    }

    #[test]
    fn conflicting_probe_addresses_fail() {
        let yaml = "probe_server:\n  bind_address: \"0.0.0.0:9100\"\nmetrics:\n  prometheus:\n    port: 9200\n";

        check(yaml).unwrap_err();
    }
}
