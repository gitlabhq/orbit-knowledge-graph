//! Health check configuration.

use std::net::SocketAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A namespace with lists of deployments and/or statefulsets to monitor.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct NamespaceTarget {
    pub namespace: String,
    #[serde(default)]
    pub deployments: Vec<String>,
    #[serde(default, rename = "statefulSets")]
    pub stateful_sets: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HealthCheckConfig {
    pub bind_address: SocketAddr,
    #[serde(default)]
    pub targets: Vec<NamespaceTarget>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_targets_from_yaml() {
        let yaml = r#"
bind_address: "0.0.0.0:4201"
targets:
  - namespace: gkg
    deployments:
      - gkg-indexer
      - gkg-webserver
    statefulSets:
      - clickhouse
  - namespace: siphon
    deployments:
      - siphon-consumer
"#;
        let config: HealthCheckConfig = orbit_utils::yaml::from_str(yaml).unwrap();
        assert_eq!(config.targets.len(), 2);
        assert_eq!(config.targets[0].namespace, "gkg");
        assert_eq!(config.targets[0].deployments.len(), 2);
        assert_eq!(config.targets[0].stateful_sets, vec!["clickhouse"]);
        assert_eq!(config.targets[1].namespace, "siphon");
        assert!(config.targets[1].stateful_sets.is_empty());
    }

    #[test]
    fn empty_targets_is_valid_default() {
        let yaml = "bind_address: \"0.0.0.0:4201\"";
        let config: HealthCheckConfig = orbit_utils::yaml::from_str(yaml).unwrap();
        assert!(config.targets.is_empty());
    }

    #[test]
    fn namespace_target_omits_optional_lists() {
        let yaml = "namespace: gkg";
        let target: NamespaceTarget = orbit_utils::yaml::from_str(yaml).unwrap();
        assert!(target.deployments.is_empty());
        assert!(target.stateful_sets.is_empty());
    }
}
