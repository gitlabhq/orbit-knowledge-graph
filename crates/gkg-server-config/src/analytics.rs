//! Analytics configuration. `enabled` is false by default — operators must
//! opt in (Helm values). Self-managed never phones home unless explicitly
//! switched on.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsConfig {
    pub enabled: bool,
    pub collector_url: String,
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
            Self::Development => "development",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_empty_url() {
        let cfg = AnalyticsConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.collector_url, "");
        assert_eq!(cfg.deployment.kind, DeploymentKind::SelfManaged);
        assert_eq!(
            cfg.deployment.environment,
            DeploymentEnvironment::Development
        );
    }

    #[test]
    fn parses_full_yaml() {
        let cfg: AnalyticsConfig = serde_yaml::from_str(
            "enabled: true\n\
             collector_url: https://snowplow.trx.gitlab.net\n\
             deployment:\n  type: com\n  environment: production\n",
        )
        .unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.collector_url, "https://snowplow.trx.gitlab.net");
        assert_eq!(cfg.deployment.kind, DeploymentKind::Com);
        assert_eq!(
            cfg.deployment.environment,
            DeploymentEnvironment::Production
        );
    }
}
