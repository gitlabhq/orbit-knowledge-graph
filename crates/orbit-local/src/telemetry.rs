use labkit_events::StructuredEvent;
use orbit_analytics::{AnalyticsTracker, SnowplowAnalyticsTracker};

use crate::settings;

const DEFAULT_COLLECTOR_URL: &str = "https://snowplowprd.trx.gitlab.net";
const APP_ID: &str = "orbit";
const CATEGORY: &str = "orbit_cli";

const ENABLED_ENV: &str = "ORBIT_TELEMETRY_ENABLED";
const COLLECTOR_URL_ENV: &str = "ORBIT_TELEMETRY_COLLECTOR_URL";

pub struct TelemetryConfig {
    pub enabled: bool,
    pub collector_url: String,
    pub app_id: &'static str,
}

impl TelemetryConfig {
    pub fn build_tracker(&self) -> Option<SnowplowAnalyticsTracker> {
        if !self.enabled {
            return None;
        }
        SnowplowAnalyticsTracker::new(&self.collector_url, self.app_id).ok()
    }
}

pub fn resolve_from_env() -> TelemetryConfig {
    resolve(
        |key| std::env::var(key).ok(),
        settings::load().telemetry.enabled,
    )
}

pub fn emit_command_event<T: AnalyticsTracker + ?Sized>(tracker: &T, action: &str) {
    if let Ok(event) = StructuredEvent::builder(CATEGORY, action).build() {
        tracker.track(event);
    }
}

fn resolve(
    get_env: impl Fn(&str) -> Option<String>,
    persisted_enabled: Option<bool>,
) -> TelemetryConfig {
    let enabled = get_env(ENABLED_ENV)
        .as_deref()
        .and_then(settings::parse_bool)
        .or(persisted_enabled)
        .unwrap_or(true);

    let collector_url = get_env(COLLECTOR_URL_ENV)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_COLLECTOR_URL.to_string());

    TelemetryConfig {
        enabled,
        collector_url,
        app_id: APP_ID,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    #[test]
    fn defaults_to_enabled_with_default_collector() {
        let cfg = resolve(env_from(&[]), None);
        assert!(cfg.enabled);
        assert_eq!(cfg.collector_url, DEFAULT_COLLECTOR_URL);
        assert_eq!(cfg.app_id, "orbit");
    }

    #[test]
    fn persisted_setting_disables() {
        let cfg = resolve(env_from(&[]), Some(false));
        assert!(!cfg.enabled);
    }

    #[test]
    fn env_overrides_persisted_setting() {
        let cfg = resolve(env_from(&[(ENABLED_ENV, "false")]), Some(true));
        assert!(!cfg.enabled);
    }

    #[test]
    fn unrecognized_env_falls_back_to_persisted() {
        let cfg = resolve(env_from(&[(ENABLED_ENV, "maybe")]), Some(false));
        assert!(!cfg.enabled);
    }

    #[test]
    fn collector_url_env_overrides_default() {
        let cfg = resolve(
            env_from(&[(COLLECTOR_URL_ENV, "https://collector.example.test")]),
            None,
        );
        assert_eq!(cfg.collector_url, "https://collector.example.test");
    }

    #[test]
    fn emit_sends_one_event_with_action() {
        let tracker = orbit_analytics::InMemoryAnalyticsTracker::new();
        emit_command_event(&tracker, "remote_query");
        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].category(), CATEGORY);
        assert_eq!(events[0].action(), "remote_query");
    }
}
