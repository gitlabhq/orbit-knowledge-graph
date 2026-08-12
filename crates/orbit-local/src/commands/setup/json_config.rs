//! Read/modify/write for assistant JSON config files (Claude settings,
//! OpenCode config). Invalid JSON is a hard error, never a silent overwrite:
//! clobbering a user's settings file is worse than failing the setup.

use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

pub(super) fn read_object(path: &Path) -> Result<Value> {
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let value: Value = serde_json::from_str(&raw).with_context(|| {
                format!(
                    "{} is not valid JSON; fix or remove it and re-run",
                    path.display()
                )
            })?;
            if !value.is_object() {
                bail!("{} is not a JSON object", path.display());
            }
            Ok(value)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(json!({})),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}

pub(super) fn write_object(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut raw = serde_json::to_string_pretty(value).context("failed to serialize JSON")?;
    raw.push('\n');
    std::fs::write(path, raw).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_reads_as_empty_object() {
        let dir = tempfile::tempdir().unwrap();
        let value = read_object(&dir.path().join("nope.json")).unwrap();
        assert_eq!(value, json!({}));
    }

    #[test]
    fn invalid_json_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "{not json").unwrap();
        let err = read_object(&path).unwrap_err();
        assert!(err.to_string().contains("not valid JSON"));
    }

    #[test]
    fn non_object_root_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("array.json");
        std::fs::write(&path, "[]").unwrap();
        assert!(read_object(&path).is_err());
    }
}
