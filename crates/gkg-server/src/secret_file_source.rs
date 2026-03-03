use std::fs;
use std::path::{Path, PathBuf};

use config::Source;
use config::Value;
use config::ValueKind;

/// Reads Kubernetes-mounted secret files and merges them into config.
///
/// K8s projects each secret key as a file under a mount path:
///   /etc/secrets/gitlab/jwt/signing_key  →  content becomes `gitlab.jwt.signing_key`
///
/// Directory structure maps to dot-separated config keys,
/// file content becomes the value. Empty files are skipped.
#[derive(Clone, Debug)]
pub struct SecretFileSource {
    root: PathBuf,
}

impl SecretFileSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Source for SecretFileSource {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<config::Map<String, Value>, config::ConfigError> {
        let mut map = config::Map::new();

        if !self.root.exists() {
            return Ok(map);
        }

        collect_files(&self.root, &self.root, &mut map)?;
        Ok(map)
    }
}

fn collect_files(
    root: &Path,
    dir: &Path,
    map: &mut config::Map<String, Value>,
) -> Result<(), config::ConfigError> {
    let entries = fs::read_dir(dir).map_err(|e| {
        config::ConfigError::Message(format!("failed to read secret dir {}: {e}", dir.display()))
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            config::ConfigError::Message(format!("failed to read entry in {}: {e}", dir.display()))
        })?;

        let path = entry.path();

        // Skip hidden files (K8s uses ..data symlinks for atomic updates)
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
        {
            continue;
        }

        if path.is_dir() {
            collect_files(root, &path, map)?;
        } else {
            let key = root
                .strip_prefix(root)
                .ok()
                .and_then(|_| path.strip_prefix(root).ok())
                .map(|rel| {
                    rel.components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join(".")
                })
                .unwrap_or_default();

            if key.is_empty() {
                continue;
            }

            let content = fs::read_to_string(&path).map_err(|e| {
                config::ConfigError::Message(format!(
                    "failed to read secret file {}: {e}",
                    path.display()
                ))
            })?;

            let trimmed = content.trim();
            if trimmed.is_empty() {
                continue;
            }

            map.insert(
                key,
                Value::new(
                    Some(&path.display().to_string()),
                    ValueKind::String(trimmed.to_owned()),
                ),
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_secrets(files: &[(&str, &str)]) -> TempDir {
        let dir = TempDir::new().unwrap();
        for (path, content) in files {
            let full = dir.path().join(path);
            fs::create_dir_all(full.parent().unwrap()).unwrap();
            fs::write(&full, content).unwrap();
        }
        dir
    }

    #[test]
    fn reads_flat_secret_file() {
        let dir = setup_secrets(&[("my_key", "my_value")]);
        let source = SecretFileSource::new(dir.path());
        let map = source.collect().unwrap();

        assert_eq!(map["my_key"].clone().into_string().unwrap(), "my_value");
    }

    #[test]
    fn reads_nested_secret_files() {
        let dir = setup_secrets(&[
            ("gitlab/jwt/signing_key", "secret-sign"),
            ("gitlab/jwt/verifying_key", "secret-verify"),
            ("gitlab/base_url", "https://gitlab.example.com"),
        ]);

        let source = SecretFileSource::new(dir.path());
        let map = source.collect().unwrap();

        assert_eq!(
            map["gitlab.jwt.signing_key"].clone().into_string().unwrap(),
            "secret-sign"
        );
        assert_eq!(
            map["gitlab.jwt.verifying_key"]
                .clone()
                .into_string()
                .unwrap(),
            "secret-verify"
        );
        assert_eq!(
            map["gitlab.base_url"].clone().into_string().unwrap(),
            "https://gitlab.example.com"
        );
    }

    #[test]
    fn trims_whitespace_and_newlines() {
        let dir = setup_secrets(&[("token", "  my-secret\n")]);
        let source = SecretFileSource::new(dir.path());
        let map = source.collect().unwrap();

        assert_eq!(map["token"].clone().into_string().unwrap(), "my-secret");
    }

    #[test]
    fn skips_empty_files() {
        let dir = setup_secrets(&[("empty", ""), ("present", "value")]);
        let source = SecretFileSource::new(dir.path());
        let map = source.collect().unwrap();

        assert!(!map.contains_key("empty"));
        assert!(map.contains_key("present"));
    }

    #[test]
    fn skips_hidden_files() {
        let dir = setup_secrets(&[(".hidden", "secret"), ("visible", "value")]);
        let source = SecretFileSource::new(dir.path());
        let map = source.collect().unwrap();

        assert!(!map.contains_key(".hidden"));
        assert!(map.contains_key("visible"));
    }

    #[test]
    fn skips_hidden_directories() {
        let dir = setup_secrets(&[("..data/token", "secret"), ("real/token", "value")]);
        let source = SecretFileSource::new(dir.path());
        let map = source.collect().unwrap();

        assert!(!map.contains_key("..data.token"));
        assert_eq!(map["real.token"].clone().into_string().unwrap(), "value");
    }

    #[test]
    fn nonexistent_root_returns_empty_map() {
        let source = SecretFileSource::new("/nonexistent/path/that/does/not/exist");
        let map = source.collect().unwrap();

        assert!(map.is_empty());
    }

    #[test]
    fn integrates_with_config_builder() {
        let dir = setup_secrets(&[
            ("gitlab/jwt/signing_key", "file-secret"),
            ("gitlab/base_url", "https://from-file.example.com"),
        ]);

        let config = config::Config::builder()
            .add_source(SecretFileSource::new(dir.path()))
            .build()
            .unwrap();

        assert_eq!(
            config.get_string("gitlab.jwt.signing_key").unwrap(),
            "file-secret"
        );
        assert_eq!(
            config.get_string("gitlab.base_url").unwrap(),
            "https://from-file.example.com"
        );
    }
}
