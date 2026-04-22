//! Analytics configuration: deployment identity today, with room for
//! opt-in/opt-out, transport, and auth settings as the `gkg-analytics` crate
//! grows.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsConfig {
    pub deployment: DeploymentConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct DeploymentConfig {
    #[serde(rename = "type")]
    pub kind: DeploymentKind,
    pub environment: DeploymentEnvironment,
}

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
    #[default]
    Development,
    Staging,
    Production,
}

impl DeploymentEnvironment {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeploymentEnvironment::Development => "development",
            DeploymentEnvironment::Staging => "staging",
            DeploymentEnvironment::Production => "production",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_self_managed_development() {
        let cfg = AnalyticsConfig::default();
        assert_eq!(cfg.deployment.kind, DeploymentKind::SelfManaged);
        assert_eq!(
            cfg.deployment.environment,
            DeploymentEnvironment::Development
        );
    }

    #[test]
    fn parses_nested_yaml() {
        let cfg: AnalyticsConfig =
            serde_yaml::from_str("deployment:\n  type: com\n  environment: staging\n").unwrap();
        assert_eq!(cfg.deployment.kind, DeploymentKind::Com);
        assert_eq!(cfg.deployment.environment, DeploymentEnvironment::Staging);
    }
}
