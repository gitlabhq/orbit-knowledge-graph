use anyhow::{Context, Result, bail};

use crate::settings::{self, Settings};

const KNOWN_KEYS: &[&str] = &["telemetry.enabled"];

pub fn get(key: &str) -> Result<()> {
    match read_key(&settings::load(), key)? {
        Some(value) => println!("{value}"),
        None => println!("(unset)"),
    }
    Ok(())
}

pub fn set(key: &str, value: &str) -> Result<()> {
    let mut settings = settings::load();
    let normalized = apply_set(&mut settings, key, value)?;
    let path = settings::save(&settings)?;
    println!("{key} = {normalized} (saved to {})", path.display());
    Ok(())
}

pub fn list() -> Result<()> {
    let settings = settings::load();
    for key in KNOWN_KEYS {
        match read_key(&settings, key)? {
            Some(value) => println!("{key} = {value}"),
            None => println!("{key} = (unset)"),
        }
    }
    Ok(())
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
            let parsed = settings::parse_bool(value)
                .with_context(|| format!("`{key}` expects a boolean, got `{value}`"))?;
            settings.telemetry.enabled = Some(parsed);
            Ok(parsed.to_string())
        }
        _ => bail!("unknown setting `{key}` (known: {})", KNOWN_KEYS.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_set_then_read_key_roundtrips() {
        let mut settings = Settings::default();
        apply_set(&mut settings, "telemetry.enabled", "false").unwrap();
        assert_eq!(
            read_key(&settings, "telemetry.enabled").unwrap(),
            Some("false".to_string())
        );
    }

    #[test]
    fn unset_key_reads_as_none() {
        assert_eq!(
            read_key(&Settings::default(), "telemetry.enabled").unwrap(),
            None
        );
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
