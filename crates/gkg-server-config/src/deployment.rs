//! Identifies the GitLab deployment hosting this GKG instance so telemetry
//! (Snowplow, OTel) can be segmented by instance type and environment.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentKind {
    Com,
    Dedicated,
    #[default]
    SelfManaged,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentEnvironment {
    Development,
    Staging,
    #[default]
    Production,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct DeploymentConfig {
    #[serde(rename = "type")]
    pub kind: DeploymentKind,
    pub environment: DeploymentEnvironment,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_self_managed_production() {
        let cfg = DeploymentConfig::default();
        assert_eq!(cfg.kind, DeploymentKind::SelfManaged);
        assert_eq!(cfg.environment, DeploymentEnvironment::Production);
    }

    #[test]
    fn parses_snake_case_yaml() {
        let cfg: DeploymentConfig =
            serde_yaml::from_str("type: com\nenvironment: staging\n").unwrap();
        assert_eq!(cfg.kind, DeploymentKind::Com);
        assert_eq!(cfg.environment, DeploymentEnvironment::Staging);
    }
}
