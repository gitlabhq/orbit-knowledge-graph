use std::sync::LazyLock;

use labkit_events::StructuredEvent;
use orbit_analytics::{
    AnalyticsTracker, OrbitCommonContext, SnowplowAnalyticsTracker, orbit_common,
};
use regex::Regex;

use crate::settings;

const DEFAULT_COLLECTOR_URL: &str = "https://snowplowprd.trx.gitlab.net";
const APP_ID: &str = "orbit";
const CATEGORY: &str = "orbit_cli";

const ENABLED_ENV: &str = "ORBIT_TELEMETRY_ENABLED";
const COLLECTOR_URL_ENV: &str = "ORBIT_TELEMETRY_COLLECTOR_URL";

static AGENT_VALUE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9._-]{1,64}$").expect("static regex"));

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

pub fn emit_command_event<T: AnalyticsTracker + ?Sized>(
    tracker: &T,
    action: &str,
    coding_agent: Option<&str>,
) {
    if let Ok(event) = StructuredEvent::builder(CATEGORY, action)
        .context(build_common_context(action, coding_agent))
        .build()
    {
        tracker.track(event);
    }
}

fn build_common_context(action: &str, coding_agent: Option<&str>) -> OrbitCommonContext {
    let targets_saas = action.starts_with("remote")
        && crate::remote::client::instance_host()
            .as_deref()
            .is_some_and(crate::remote::client::is_gitlab_com);
    let (deployment_type, environment) = deployment_for(targets_saas);
    OrbitCommonContext::new(orbit_common::OrbitCommon {
        deployment_type,
        surface: Some(orbit_common::OrbitCommonSurface::Cli),
        environment,
        coding_agent: coding_agent
            .and_then(|a| a.parse::<orbit_common::OrbitCommonCodingAgent>().ok()),
        correlation_id: None,
        instance_id: None,
        unique_instance_id: None,
        host_name: None,
        organization_id: None,
        root_namespace_ids: None,
        schema_version: None,
    })
}

pub fn detect_coding_agent(get_env: impl Fn(&str) -> Option<String>) -> Option<String> {
    if let Some(v) = get_env("AI_AGENT")
        && AGENT_VALUE_RE.is_match(&v)
    {
        return Some(v);
    }

    if get_env("CLAUDECODE").as_deref() == Some("1") {
        return Some("claude-code".into());
    }
    if get_env("CODEX_THREAD_ID").is_some_and(|v| !v.is_empty()) {
        return Some("codex".into());
    }
    if get_env("CURSOR_AGENT").as_deref() == Some("1") {
        return Some("cursor".into());
    }
    if get_env("GEMINI_CLI").as_deref() == Some("1") {
        return Some("gemini".into());
    }
    if get_env("OPENCODE").as_deref() == Some("1") {
        return Some("opencode".into());
    }
    if get_env("ROO_CLI_RUNTIME").as_deref() == Some("1") {
        return Some("roo-code".into());
    }

    match get_env("TERM_PROGRAM")
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("cursor") => Some("cursor-terminal".into()),
        Some("windsurf") => Some("windsurf-terminal".into()),
        Some("zed") => Some("zed-terminal".into()),
        _ => None,
    }
}

fn deployment_for(
    targets_saas: bool,
) -> (
    orbit_common::OrbitCommonDeploymentType,
    orbit_common::OrbitCommonEnvironment,
) {
    use orbit_common::OrbitCommonDeploymentType as Deployment;
    let (deployment, environment) = if targets_saas {
        (Deployment::Com, "production")
    } else {
        (Deployment::Unknown, "unknown")
    };
    (
        deployment,
        environment
            .parse()
            .expect("static environment string is valid"),
    )
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
    fn deployment_for_only_asserts_saas() {
        use orbit_common::OrbitCommonDeploymentType as Deployment;
        let (dt, env) = deployment_for(true);
        assert_eq!(dt, Deployment::Com);
        assert_eq!(env.to_string(), "production");

        let (dt, env) = deployment_for(false);
        assert_eq!(dt, Deployment::Unknown);
        assert_eq!(env.to_string(), "unknown");
    }

    #[test]
    fn emit_sends_one_event_with_action() {
        let tracker = orbit_analytics::InMemoryAnalyticsTracker::new();
        emit_command_event(&tracker, "remote_query", None);
        let events = tracker.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].category(), CATEGORY);
        assert_eq!(events[0].action(), "remote_query");
    }

    #[test]
    fn ai_agent_env_returns_raw_value() {
        let agent = detect_coding_agent(env_from(&[("AI_AGENT", "my-custom-agent")]));
        assert_eq!(agent.as_deref(), Some("my-custom-agent"));
    }

    #[test]
    fn ai_agent_rejects_too_long() {
        let long = "a".repeat(65);
        let agent = detect_coding_agent(env_from(&[("AI_AGENT", &long)]));
        assert_eq!(agent, None);
    }

    #[test]
    fn ai_agent_rejects_invalid_chars() {
        let agent = detect_coding_agent(env_from(&[("AI_AGENT", "bad agent/value")]));
        assert_eq!(agent, None);
    }

    #[test]
    fn claudecode_detected() {
        let agent = detect_coding_agent(env_from(&[("CLAUDECODE", "1")]));
        assert_eq!(agent.as_deref(), Some("claude-code"));
    }

    #[test]
    fn codex_detected_from_thread_id() {
        let agent = detect_coding_agent(env_from(&[("CODEX_THREAD_ID", "abc-123")]));
        assert_eq!(agent.as_deref(), Some("codex"));
    }

    #[test]
    fn cursor_agent_detected() {
        let agent = detect_coding_agent(env_from(&[("CURSOR_AGENT", "1")]));
        assert_eq!(agent.as_deref(), Some("cursor"));
    }

    #[test]
    fn gemini_detected() {
        let agent = detect_coding_agent(env_from(&[("GEMINI_CLI", "1")]));
        assert_eq!(agent.as_deref(), Some("gemini"));
    }

    #[test]
    fn opencode_detected() {
        let agent = detect_coding_agent(env_from(&[("OPENCODE", "1")]));
        assert_eq!(agent.as_deref(), Some("opencode"));
    }

    #[test]
    fn roo_code_detected() {
        let agent = detect_coding_agent(env_from(&[("ROO_CLI_RUNTIME", "1")]));
        assert_eq!(agent.as_deref(), Some("roo-code"));
    }

    #[test]
    fn term_program_cursor_returns_terminal_suffix() {
        let agent = detect_coding_agent(env_from(&[("TERM_PROGRAM", "Cursor")]));
        assert_eq!(agent.as_deref(), Some("cursor-terminal"));
    }

    #[test]
    fn term_program_windsurf() {
        let agent = detect_coding_agent(env_from(&[("TERM_PROGRAM", "Windsurf")]));
        assert_eq!(agent.as_deref(), Some("windsurf-terminal"));
    }

    #[test]
    fn term_program_zed() {
        let agent = detect_coding_agent(env_from(&[("TERM_PROGRAM", "zed")]));
        assert_eq!(agent.as_deref(), Some("zed-terminal"));
    }

    #[test]
    fn explicit_agent_beats_term_program() {
        let agent =
            detect_coding_agent(env_from(&[("CLAUDECODE", "1"), ("TERM_PROGRAM", "Cursor")]));
        assert_eq!(agent.as_deref(), Some("claude-code"));
    }

    #[test]
    fn ai_agent_beats_all_others() {
        let agent = detect_coding_agent(env_from(&[
            ("AI_AGENT", "custom"),
            ("CLAUDECODE", "1"),
            ("CURSOR_AGENT", "1"),
        ]));
        assert_eq!(agent.as_deref(), Some("custom"));
    }

    #[test]
    fn no_env_returns_none() {
        let agent = detect_coding_agent(env_from(&[]));
        assert_eq!(agent, None);
    }

    #[test]
    fn unknown_term_program_returns_none() {
        let agent = detect_coding_agent(env_from(&[("TERM_PROGRAM", "ghostty")]));
        assert_eq!(agent, None);
    }
}
