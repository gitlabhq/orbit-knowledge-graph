//! Analytics configuration. An explicit `enabled` wins. When it is unset (the
//! `config/default.yaml` default), events are sent only while GitLab reports
//! that its product usage data setting is on.
//!
//! `local_collector_url` is a second destination on self-managed, the Data
//! Insights Platform running next to Orbit. It gets every event whatever
//! `enabled` says: the events stay in the customer's cluster, and Service Ping
//! decides what leaves it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsConfig {
    pub enabled: Option<bool>,
    pub collector_url: String,
    #[serde(default)]
    pub local_collector_url: String,
    pub deployment: DeploymentConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct DeploymentConfig {
    #[serde(rename = "type")]
    pub kind: DeploymentKind,
    pub environment: DeploymentEnvironment,
}

impl AnalyticsConfig {
    pub fn may_send_events(&self) -> bool {
        self.enabled != Some(false)
    }

    pub fn follows_gitlab_setting(&self) -> bool {
        self.enabled.is_none()
    }

    /// The local collector, on self-managed only. GitLab.com and Dedicated do
    /// not send Service Ping, so a stray value there is ignored.
    pub fn local_collector(&self) -> Option<&str> {
        let url = self.local_collector_url.trim();
        (!url.is_empty() && self.deployment.kind == DeploymentKind::SelfManaged).then_some(url)
    }
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
    fn defaults_follow_the_gitlab_setting_with_an_empty_url() {
        let cfg = crate::AppConfig::embedded_defaults().analytics;
        assert!(cfg.follows_gitlab_setting());
        assert_eq!(cfg.collector_url, "");
        assert_eq!(cfg.local_collector(), None);
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
        assert_eq!(cfg.enabled, Some(true));
        assert_eq!(cfg.collector_url, "https://snowplow.trx.gitlab.net");
        assert_eq!(cfg.deployment.kind, DeploymentKind::Com);
        assert_eq!(
            cfg.deployment.environment,
            DeploymentEnvironment::Production
        );
    }

    #[test]
    fn local_collector_is_self_managed_only() {
        for (kind, want) in [
            ("self_managed", Some("http://dip:8082")),
            ("com", None),
            ("dedicated", None),
        ] {
            let cfg: AnalyticsConfig = orbit_utils::yaml::from_str(&format!(
                "enabled: false\n\
                 collector_url: \"\"\n\
                 local_collector_url: http://dip:8082\n\
                 deployment:\n  type: {kind}\n  environment: production\n"
            ))
            .unwrap();
            assert_eq!(cfg.local_collector(), want, "{kind}");
        }
    }
}
