use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::workspace::Workspace;

const SETTINGS_FILE: &str = "settings.json";

pub const KNOWN_KEYS: &[&str] = &["telemetry.enabled"];

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub telemetry: TelemetrySettings,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetrySettings {
    pub enabled: Option<bool>,
}

pub fn load() -> Settings {
    Workspace::default_root()
        .map(|root| load_from(&root))
        .unwrap_or_default()
}

pub fn get(key: &str) -> Result<Option<String>> {
    read_key(&load(), key)
}

pub fn set(key: &str, value: &str) -> Result<(String, PathBuf)> {
    let root = Workspace::default_root()?;
    let mut settings = load_from(&root);
    let normalized = apply_set(&mut settings, key, value)?;
    let path = save_to(&root, &settings)?;
    Ok((normalized, path))
}

pub fn list() -> Vec<(String, Option<String>)> {
    let settings = load();
    KNOWN_KEYS
        .iter()
        .map(|key| {
            let value = read_key(&settings, key).ok().flatten();
            (key.to_string(), value)
        })
        .collect()
}

pub fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn read_key(settings: &Settings, key: &str) -> Result<Option<String>> {
    match key {
        "telemetry.enabled" => Ok(settings.telemetry.enabled.map(|b| b.to_string())),
        _ => bail!("unknown setting `{key}` (known: {})", KNOWN_KEYS.join(", ")),
    }
}

fn apply_set(settings: &mut Settings, key: &str, value: &str) -> Result<String> {
    match key {
        "telemetry.enabled" => {
            let parsed = parse_bool(value)
                .with_context(|| format!("`{key}` expects a boolean, got `{value}`"))?;
            settings.telemetry.enabled = Some(parsed);
            Ok(parsed.to_string())
        }
        _ => bail!("unknown setting `{key}` (known: {})", KNOWN_KEYS.join(", ")),
    }
}

fn load_from(root: &Path) -> Settings {
    std::fs::read_to_string(root.join(SETTINGS_FILE))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn save_to(root: &Path, settings: &Settings) -> Result<PathBuf> {
    std::fs::create_dir_all(root)?;
    let path = root.join(SETTINGS_FILE);
    let json = serde_json::to_string_pretty(settings)?;
    std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_read_roundtrips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = load_from(dir.path());
        apply_set(&mut settings, "telemetry.enabled", "false").unwrap();
        save_to(dir.path(), &settings).unwrap();

        let reloaded = load_from(dir.path());
        assert_eq!(
            read_key(&reloaded, "telemetry.enabled").unwrap(),
            Some("false".to_string())
        );
    }

    #[test]
    fn missing_file_reads_as_unset() {
        let dir = tempfile::tempdir().unwrap();
        let settings = load_from(dir.path());
        assert_eq!(read_key(&settings, "telemetry.enabled").unwrap(), None);
    }

    #[test]
    fn unknown_key_is_rejected() {
        let mut settings = Settings::default();
        assert!(apply_set(&mut settings, "nope.key", "true").is_err());
        assert!(read_key(&settings, "nope.key").is_err());
    }

    #[test]
    fn non_boolean_value_is_rejected() {
        let mut settings = Settings::default();
        assert!(apply_set(&mut settings, "telemetry.enabled", "maybe").is_err());
    }
}
