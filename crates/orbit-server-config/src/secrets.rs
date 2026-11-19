//! Reading a Kubernetes secret mount as a configuration layer.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

/// Reads a secret mount into a configuration document.
///
/// Kubernetes projects each key of a secret as a file, so the directory
/// structure is the config path: `/etc/secrets/gitlab/jwt/signing_key` becomes
/// `gitlab.jwt.signing_key`. Empty files are skipped, so a secret an
/// environment does not set leaves the value underneath it alone. A root that
/// does not exist yields an empty document.
pub fn tree(root: &Path) -> Result<Value, SecretError> {
    if !root.exists() {
        return Ok(Value::Object(Map::new()));
    }
    read_dir(root).map(Value::Object)
}

fn read_dir(dir: &Path) -> Result<Map<String, Value>, SecretError> {
    let mut document = Map::new();

    for entry in fs::read_dir(dir).map_err(|source| SecretError::new(dir, source))? {
        let path = entry
            .map_err(|source| SecretError::new(dir, source))?
            .path();

        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        // Kubernetes stages atomic updates behind a `..data` symlink.
        if name.starts_with('.') {
            continue;
        }

        if path.is_dir() {
            let nested = read_dir(&path)?;
            if !nested.is_empty() {
                document.insert(name.to_owned(), Value::Object(nested));
            }
            continue;
        }

        let content =
            fs::read_to_string(&path).map_err(|source| SecretError::new(&path, source))?;
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            document.insert(name.to_owned(), Value::String(trimmed.to_owned()));
        }
    }

    Ok(document)
}

#[derive(Debug, thiserror::Error)]
#[error("failed to read the secret mount at {}: {source}", path.display())]
pub struct SecretError {
    path: PathBuf,
    source: std::io::Error,
}

impl SecretError {
    fn new(path: &Path, source: std::io::Error) -> Self {
        Self {
            path: path.to_path_buf(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn mount(files: &[(&str, &str)]) -> TempDir {
        let dir = TempDir::new().unwrap();
        for (path, content) in files {
            let full = dir.path().join(path);
            fs::create_dir_all(full.parent().unwrap()).unwrap();
            fs::write(&full, content).unwrap();
        }
        dir
    }

    #[test]
    fn reads_a_flat_secret_file() {
        let dir = mount(&[("my_key", "my_value")]);
        assert_eq!(tree(dir.path()).unwrap(), json!({"my_key": "my_value"}));
    }

    #[test]
    fn directories_become_nested_keys() {
        let dir = mount(&[
            ("gitlab/jwt/signing_key", "secret-sign"),
            ("gitlab/jwt/verifying_key", "secret-verify"),
            ("gitlab/base_url", "https://gitlab.example.com"),
        ]);

        assert_eq!(
            tree(dir.path()).unwrap(),
            json!({
                "gitlab": {
                    "base_url": "https://gitlab.example.com",
                    "jwt": {
                        "signing_key": "secret-sign",
                        "verifying_key": "secret-verify",
                    },
                },
            })
        );
    }

    #[test]
    fn trims_whitespace_and_newlines() {
        let dir = mount(&[("token", "  my-secret\n")]);
        assert_eq!(tree(dir.path()).unwrap(), json!({"token": "my-secret"}));
    }

    #[test]
    fn skips_empty_files() {
        let dir = mount(&[("empty", ""), ("present", "value")]);
        assert_eq!(tree(dir.path()).unwrap(), json!({"present": "value"}));
    }

    #[test]
    fn skips_hidden_files() {
        let dir = mount(&[(".hidden", "secret"), ("visible", "value")]);
        assert_eq!(tree(dir.path()).unwrap(), json!({"visible": "value"}));
    }

    #[test]
    fn skips_the_kubernetes_data_symlink_directory() {
        let dir = mount(&[("..data/token", "secret"), ("real/token", "value")]);
        assert_eq!(
            tree(dir.path()).unwrap(),
            json!({"real": {"token": "value"}})
        );
    }

    #[test]
    fn a_directory_of_empty_files_contributes_nothing() {
        let dir = mount(&[("graph/password", "")]);
        assert_eq!(tree(dir.path()).unwrap(), json!({}));
    }

    #[test]
    fn a_missing_root_is_an_empty_document() {
        assert_eq!(
            tree(Path::new("/nonexistent/path/that/does/not/exist")).unwrap(),
            json!({})
        );
    }

    #[test]
    fn secrets_overlay_the_layers_beneath_them() {
        let dir = mount(&[("graph/password", "from-secret")]);
        let mut document = json!({"graph": {"url": "http://graph", "password": "from-overlay"}});

        crate::merge::merge(&mut document, tree(dir.path()).unwrap());

        assert_eq!(
            document,
            json!({"graph": {"url": "http://graph", "password": "from-secret"}})
        );
    }
}
