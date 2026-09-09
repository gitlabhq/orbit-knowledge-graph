//! Analytics configuration. `enabled` is false in `config/default.yaml`;
//! operators must opt in (Helm values). Self-managed never phones home unless
//! explicitly switched on.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsConfig {
    pub enabled: bool,
    pub collector_url: String,
    pub deployment: DeploymentConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct DeploymentConfig {
    #[serde(rename = "type")]
    pub kind: DeploymentKind,
    pub environment: DeploymentEnvironment,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentKind {
    #[strum(serialize = ".com")]
    Com,
    #[strum(serialize = "dedicated")]
    Dedicated,
    #[strum(serialize = "self-managed")]
    SelfManaged,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DeploymentEnvironment {
    Development,
    Staging,
    Production,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_empty_url() {
        let cfg = crate::AppConfig::embedded_defaults().analytics;
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
        let cfg: AnalyticsConfig = orbit_utils::yaml::from_str(
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
