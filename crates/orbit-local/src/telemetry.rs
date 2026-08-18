//! CLI telemetry: resolve whether to send Snowplow usage events, and emit them.
//!
//! Phase 1 of the unified Orbit telemetry rollout (see issue #1161). The CLI
//! sends structured events under `app_id = "orbit"`. The server keeps
//! `gkg-server` until a later, coordinated migration. Events carry no custom
//! context yet; the shared `orbit_common` context and its `surface` field are a
//! follow-up.

use std::path::Path;

use labkit_events::StructuredEvent;
use orbit_analytics::{AnalyticsTracker, SnowplowAnalyticsTracker};
use serde::Deserialize;

use crate::workspace::Workspace;

const DEFAULT_COLLECTOR_URL: &str = "https://snowplowprd.trx.gitlab.net";
const APP_ID: &str = "orbit";
const CATEGORY: &str = "orbit_cli";

const ENABLED_ENV: &str = "ORBIT_TELEMETRY_ENABLED";
const COLLECTOR_URL_ENV: &str = "ORBIT_TELEMETRY_COLLECTOR_URL";
const SETTINGS_FILE: &str = "telemetry.json";

/// Telemetry settings resolved for a single CLI invocation.
pub struct TelemetryConfig {
    pub enabled: bool,
    pub collector_url: String,
    pub app_id: &'static str,
}

impl TelemetryConfig {
    /// Build a Snowplow tracker, or `None` when telemetry is off or the tracker
    /// cannot be built. A build failure must not break the command, so the error
    /// is logged and swallowed.
    pub fn build_tracker(&self) -> Option<SnowplowAnalyticsTracker> {
        if !self.enabled {
            return None;
        }
        match SnowplowAnalyticsTracker::new(&self.collector_url, self.app_id) {
            Ok(tracker) => Some(tracker),
            Err(e) => {
                tracing::debug!(error = %e, "failed to build telemetry tracker; telemetry disabled");
                None
            }
        }
    }
}

/// Resolve telemetry settings from the real environment and the saved setting.
/// `no_telemetry` is the `--no-telemetry` flag.
pub fn resolve_from_env(no_telemetry: bool) -> TelemetryConfig {
    let persisted = Workspace::default_root()
        .ok()
        .and_then(|root| load_persisted_enabled(&root));
    resolve(no_telemetry, |key| std::env::var(key).ok(), persisted)
}

/// Send one `orbit_cli` command-usage event. The event is queued; the caller
/// flushes it with [`SnowplowAnalyticsTracker::shutdown`] before exit.
pub fn emit_command_event<T: AnalyticsTracker + ?Sized>(tracker: &T, action: &str) {
    match StructuredEvent::builder(CATEGORY, action).build() {
        Ok(event) => tracker.track(event),
        Err(e) => tracing::debug!(error = %e, "failed to build telemetry event"),
    }
}

fn resolve(
    no_telemetry: bool,
    get_env: impl Fn(&str) -> Option<String>,
    persisted_enabled: Option<bool>,
) -> TelemetryConfig {
    let enabled = if no_telemetry {
        false
    } else if let Some(from_env) = get_env(ENABLED_ENV).as_deref().and_then(parse_bool) {
        from_env
    } else {
        persisted_enabled.unwrap_or(true)
    };

    let collector_url = get_env(COLLECTOR_URL_ENV)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_COLLECTOR_URL.to_string());

    TelemetryConfig {
        enabled,
        collector_url,
        app_id: APP_ID,
    }
}

fn load_persisted_enabled(root: &Path) -> Option<bool> {
    let raw = std::fs::read_to_string(root.join(SETTINGS_FILE)).ok()?;
    serde_json::from_str::<PersistedSettings>(&raw)
        .ok()?
        .enabled
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[derive(Deserialize)]
struct PersistedSettings {
    enabled: Option<bool>,
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
        let cfg = resolve(false, env_from(&[]), None);
        assert!(cfg.enabled);
        assert_eq!(cfg.collector_url, DEFAULT_COLLECTOR_URL);
        assert_eq!(cfg.app_id, "orbit");
    }

    #[test]
    fn no_telemetry_flag_forces_off_over_env_and_setting() {
        let cfg = resolve(true, env_from(&[(ENABLED_ENV, "true")]), Some(true));
        assert!(!cfg.enabled);
    }

    #[test]
    fn env_overrides_persisted_setting() {
        let cfg = resolve(false, env_from(&[(ENABLED_ENV, "false")]), Some(true));
        assert!(!cfg.enabled);
    }

    #[test]
    fn persisted_setting_used_without_flag_or_env() {
        let cfg = resolve(false, env_from(&[]), Some(false));
        assert!(!cfg.enabled);
    }

    #[test]
    fn collector_url_env_overrides_default() {
        let cfg = resolve(
            false,
            env_from(&[(COLLECTOR_URL_ENV, "https://collector.example.test")]),
            None,
        );
        assert_eq!(cfg.collector_url, "https://collector.example.test");
    }

    #[test]
    fn unrecognized_enabled_env_is_ignored() {
        let cfg = resolve(false, env_from(&[(ENABLED_ENV, "maybe")]), None);
        assert!(cfg.enabled);
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
