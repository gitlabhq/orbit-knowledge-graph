use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::workspace::Workspace;

const SETTINGS_FILE: &str = "settings.json";

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

pub fn save(settings: &Settings) -> Result<PathBuf> {
    let root = Workspace::default_root()?;
    save_to(&root, settings)
}

pub fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn load_from(root: &Path) -> Settings {
    let path = root.join(SETTINGS_FILE);
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|e| {
            eprintln!("warning: ignoring invalid {}: {e}", path.display());
            Settings::default()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Settings::default(),
        Err(e) => {
            eprintln!("warning: cannot read {}: {e}", path.display());
            Settings::default()
        }
    }
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
    fn save_then_load_roundtrips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let settings = Settings {
            telemetry: TelemetrySettings {
                enabled: Some(false),
            },
        };
        save_to(dir.path(), &settings).unwrap();

        let reloaded = load_from(dir.path());
        assert_eq!(reloaded.telemetry.enabled, Some(false));
    }

    #[test]
    fn missing_file_loads_default() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load_from(dir.path()).telemetry.enabled, None);
    }

    #[test]
    fn corrupt_file_loads_default_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(SETTINGS_FILE), "not json {{").unwrap();
        assert_eq!(load_from(dir.path()).telemetry.enabled, None);
    }
}
