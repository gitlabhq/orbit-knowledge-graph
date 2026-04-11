use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DependencyInfo {
    pub package_name: String,
    pub package_version: String,
    pub source_url: Option<String>,
    pub checksum: Option<String>,
    pub is_dev_dependency: bool,
    pub is_direct_dependency: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockfileKind {
    PackageLockJson,
    YarnLock,
    PnpmLockYaml,
    BunLock,
}

const LOCKFILE_CANDIDATES: &[(LockfileKind, &str)] = &[
    (LockfileKind::PnpmLockYaml, "pnpm-lock.yaml"),
    (LockfileKind::YarnLock, "yarn.lock"),
    (LockfileKind::BunLock, "bun.lock"),
    (LockfileKind::PackageLockJson, "package-lock.json"),
];

pub fn detect_lockfile(root_dir: &Path) -> Option<(LockfileKind, PathBuf)> {
    for &(kind, filename) in LOCKFILE_CANDIDATES {
        let path = root_dir.join(filename);
        if path.is_file() {
            return Some((kind, path));
        }
    }
    None
}

pub fn parse_lockfile(kind: LockfileKind, content: &str) -> Result<Vec<DependencyInfo>, String> {
    match kind {
        LockfileKind::PackageLockJson => parse_package_lock_json(content),
        LockfileKind::YarnLock => parse_yarn_lock(content),
        LockfileKind::PnpmLockYaml => parse_pnpm_lock_yaml(content),
        LockfileKind::BunLock => parse_bun_lock(content),
    }
}

// --- package-lock.json (npm) ---

#[derive(Deserialize)]
struct PackageLockRoot {
    #[serde(default)]
    packages: HashMap<String, PackageLockEntry>,
}

#[derive(Deserialize)]
struct PackageLockEntry {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    resolved: Option<String>,
    #[serde(default)]
    integrity: Option<String>,
    #[serde(default)]
    dev: bool,
    #[serde(default)]
    link: bool,
}

fn parse_package_lock_json(content: &str) -> Result<Vec<DependencyInfo>, String> {
    let root: PackageLockRoot =
        serde_json::from_str(content).map_err(|e| format!("invalid package-lock.json: {e}"))?;

    let mut deps = Vec::with_capacity(root.packages.len());
    for (key, entry) in &root.packages {
        if key.is_empty() || entry.link {
            continue;
        }

        let package_name = key
            .rsplit_once("node_modules/")
            .map(|(_, name)| name)
            .unwrap_or(key)
            .to_string();

        let version = match &entry.version {
            Some(v) => v.clone(),
            None => continue,
        };

        let is_direct =
            key.starts_with("node_modules/") && key.matches("node_modules/").count() == 1;

        deps.push(DependencyInfo {
            package_name,
            package_version: version,
            source_url: entry.resolved.clone(),
            checksum: entry.integrity.clone(),
            is_dev_dependency: entry.dev,
            is_direct_dependency: is_direct,
        });
    }

    deps.sort_by(|a, b| a.package_name.cmp(&b.package_name));
    Ok(deps)
}

// --- yarn.lock ---

fn parse_yarn_lock(content: &str) -> Result<Vec<DependencyInfo>, String> {
    let mut deps = Vec::new();
    let mut current_header: Option<String> = None;
    let mut version: Option<String> = None;
    let mut resolved: Option<String> = None;
    let mut integrity: Option<String> = None;

    let flush = |header: &str,
                 version: Option<String>,
                 resolved: Option<String>,
                 integrity: Option<String>,
                 deps: &mut Vec<DependencyInfo>| {
        let version = match version {
            Some(v) => v,
            None => return,
        };

        let (package_name, _) = parse_yarn_header(header);
        if package_name.is_empty() {
            return;
        }

        deps.push(DependencyInfo {
            package_name,
            package_version: version,
            source_url: resolved,
            checksum: integrity,
            is_dev_dependency: false,
            is_direct_dependency: false,
        });
    };

    for line in content.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        let is_indented = line.starts_with("  ") || line.starts_with('\t');

        if !is_indented {
            if let Some(ref header) = current_header {
                flush(
                    header,
                    version.take(),
                    resolved.take(),
                    integrity.take(),
                    &mut deps,
                );
            }
            current_header = Some(line.trim_end_matches(':').to_string());
            version = None;
            resolved = None;
            integrity = None;
        } else if current_header.is_some() {
            let trimmed = line.trim();
            if let Some(val) = trimmed.strip_prefix("version ") {
                version = Some(unquote(val));
            } else if let Some(val) = trimmed.strip_prefix("resolved ") {
                resolved = Some(unquote(val));
            } else if let Some(val) = trimmed.strip_prefix("integrity ") {
                integrity = Some(unquote(val));
            }
        }
    }

    if let Some(ref header) = current_header {
        flush(
            header,
            version.take(),
            resolved.take(),
            integrity.take(),
            &mut deps,
        );
    }

    deps.sort_by(|a, b| a.package_name.cmp(&b.package_name));
    Ok(deps)
}

fn parse_yarn_header(header: &str) -> (String, String) {
    let first_entry = header.split(", ").next().unwrap_or(header);
    let cleaned = unquote(first_entry);

    if let Some(idx) = cleaned.rfind('@')
        && idx > 0
    {
        return (cleaned[..idx].to_string(), cleaned[idx + 1..].to_string());
    }
    (cleaned, String::new())
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

// --- pnpm-lock.yaml ---

#[derive(Deserialize)]
struct PnpmLockRoot {
    #[serde(default)]
    packages: HashMap<String, PnpmPackageEntry>,
    #[serde(default, rename = "devDependencies")]
    dev_dependencies: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    dependencies: HashMap<String, serde_yaml::Value>,
}

#[derive(Deserialize)]
struct PnpmPackageEntry {
    #[serde(default)]
    resolution: Option<PnpmResolution>,
    #[serde(default)]
    dev: Option<bool>,
}

#[derive(Deserialize)]
struct PnpmResolution {
    #[serde(default)]
    tarball: Option<String>,
    #[serde(default)]
    integrity: Option<String>,
}

fn parse_pnpm_lock_yaml(content: &str) -> Result<Vec<DependencyInfo>, String> {
    let root: PnpmLockRoot =
        serde_yaml::from_str(content).map_err(|e| format!("invalid pnpm-lock.yaml: {e}"))?;

    let direct_names: std::collections::HashSet<&str> = root
        .dependencies
        .keys()
        .chain(root.dev_dependencies.keys())
        .map(|s| s.as_str())
        .collect();

    let dev_names: std::collections::HashSet<&str> =
        root.dev_dependencies.keys().map(|s| s.as_str()).collect();

    let mut deps = Vec::with_capacity(root.packages.len());
    for (specifier, entry) in &root.packages {
        let (name, version) = parse_pnpm_specifier(specifier);
        if name.is_empty() {
            continue;
        }

        let is_dev = entry
            .dev
            .unwrap_or_else(|| dev_names.contains(name.as_str()));

        let (tarball, integrity) = entry
            .resolution
            .as_ref()
            .map(|r| (r.tarball.clone(), r.integrity.clone()))
            .unwrap_or_default();

        deps.push(DependencyInfo {
            package_name: name.clone(),
            package_version: version,
            source_url: tarball,
            checksum: integrity,
            is_dev_dependency: is_dev,
            is_direct_dependency: direct_names.contains(name.as_str()),
        });
    }

    deps.sort_by(|a, b| a.package_name.cmp(&b.package_name));
    Ok(deps)
}

fn parse_pnpm_specifier(specifier: &str) -> (String, String) {
    let s = specifier.strip_prefix('/').unwrap_or(specifier);
    let s = s.split('(').next().unwrap_or(s);

    if let Some(at_idx) = s.rfind('@')
        && at_idx > 0
    {
        let name = &s[..at_idx];
        let version = &s[at_idx + 1..];
        return (name.to_string(), version.to_string());
    }
    (String::new(), String::new())
}

// --- bun.lock (JSONC) ---

#[derive(Deserialize)]
struct BunLockRoot {
    #[serde(default)]
    packages: HashMap<String, serde_json::Value>,
}

fn strip_jsonc_trailing_commas(content: &str) -> String {
    let mut result = String::with_capacity(content.len());
    let mut in_string = false;
    let mut escape_next = false;
    let chars: Vec<char> = content.chars().collect();

    for i in 0..chars.len() {
        let ch = chars[i];

        if escape_next {
            result.push(ch);
            escape_next = false;
            continue;
        }

        if ch == '\\' && in_string {
            result.push(ch);
            escape_next = true;
            continue;
        }

        if ch == '"' {
            in_string = !in_string;
            result.push(ch);
            continue;
        }

        if in_string {
            result.push(ch);
            continue;
        }

        if ch == ',' {
            let next_non_ws = chars[i + 1..].iter().find(|c| !c.is_ascii_whitespace());
            if next_non_ws == Some(&'}') || next_non_ws == Some(&']') {
                continue;
            }
        }

        result.push(ch);
    }

    result
}

fn parse_bun_lock(content: &str) -> Result<Vec<DependencyInfo>, String> {
    let cleaned = strip_jsonc_trailing_commas(content);
    let root: BunLockRoot =
        serde_json::from_str(&cleaned).map_err(|e| format!("invalid bun.lock: {e}"))?;

    let mut deps = Vec::with_capacity(root.packages.len());
    for (key, value) in &root.packages {
        if key.is_empty() {
            continue;
        }

        let (version, resolved, integrity, dev) = match value {
            serde_json::Value::Array(arr) => {
                let ver = arr.first().and_then(|v| v.as_str()).unwrap_or_default();
                let res = arr.get(1).and_then(|v| v.as_str());
                let integ = arr.get(2).and_then(|v| v.as_str());
                let is_dev = arr.get(3).and_then(|v| v.as_bool()).unwrap_or(false);
                (
                    ver.to_string(),
                    res.map(|s| s.to_string()),
                    integ.map(|s| s.to_string()),
                    is_dev,
                )
            }
            serde_json::Value::Object(obj) => {
                let ver = obj
                    .get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let res = obj
                    .get("resolved")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let integ = obj
                    .get("integrity")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let is_dev = obj.get("dev").and_then(|v| v.as_bool()).unwrap_or(false);
                (ver, res, integ, is_dev)
            }
            _ => continue,
        };

        if version.is_empty() {
            continue;
        }

        let package_name = key
            .rsplit_once('@')
            .filter(|(prefix, _)| !prefix.is_empty())
            .map(|(prefix, _)| prefix.to_string())
            .unwrap_or_else(|| key.clone());

        deps.push(DependencyInfo {
            package_name,
            package_version: version,
            source_url: resolved,
            checksum: integrity,
            is_dev_dependency: dev,
            is_direct_dependency: false,
        });
    }

    deps.sort_by(|a, b| a.package_name.cmp(&b.package_name));
    Ok(deps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_parse_package_lock_json() {
        let content = r#"{
            "lockfileVersion": 3,
            "packages": {
                "": {
                    "name": "my-app",
                    "version": "1.0.0"
                },
                "node_modules/react": {
                    "version": "18.2.0",
                    "resolved": "https://registry.npmjs.org/react/-/react-18.2.0.tgz",
                    "integrity": "sha512-abc123",
                    "dev": false
                },
                "node_modules/typescript": {
                    "version": "5.3.3",
                    "resolved": "https://registry.npmjs.org/typescript/-/typescript-5.3.3.tgz",
                    "integrity": "sha512-def456",
                    "dev": true
                },
                "node_modules/react/node_modules/loose-envify": {
                    "version": "1.4.0",
                    "resolved": "https://registry.npmjs.org/loose-envify/-/loose-envify-1.4.0.tgz",
                    "integrity": "sha512-ghi789"
                }
            }
        }"#;

        let deps = parse_lockfile(LockfileKind::PackageLockJson, content).unwrap();
        assert_eq!(deps.len(), 3);

        let react = deps.iter().find(|d| d.package_name == "react").unwrap();
        assert_eq!(react.package_version, "18.2.0");
        assert!(!react.is_dev_dependency);
        assert!(react.is_direct_dependency);
        assert_eq!(
            react.source_url.as_deref(),
            Some("https://registry.npmjs.org/react/-/react-18.2.0.tgz")
        );
        assert_eq!(react.checksum.as_deref(), Some("sha512-abc123"));

        let ts = deps
            .iter()
            .find(|d| d.package_name == "typescript")
            .unwrap();
        assert!(ts.is_dev_dependency);
        assert!(ts.is_direct_dependency);

        let envify = deps
            .iter()
            .find(|d| d.package_name == "loose-envify")
            .unwrap();
        assert!(!envify.is_direct_dependency);
    }

    #[test]
    fn test_parse_yarn_lock() {
        let content = r#"# THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY.
# yarn lockfile v1


"@babel/core@^7.0.0":
  version "7.23.9"
  resolved "https://registry.yarnpkg.com/@babel/core/-/core-7.23.9.tgz#abc123"
  integrity sha512-babelHash

react@^18.0.0:
  version "18.2.0"
  resolved "https://registry.yarnpkg.com/react/-/react-18.2.0.tgz#def456"
  integrity sha512-reactHash
"#;

        let deps = parse_lockfile(LockfileKind::YarnLock, content).unwrap();
        assert_eq!(deps.len(), 2);

        let babel = deps
            .iter()
            .find(|d| d.package_name == "@babel/core")
            .unwrap();
        assert_eq!(babel.package_version, "7.23.9");
        assert_eq!(babel.checksum.as_deref(), Some("sha512-babelHash"));

        let react = deps.iter().find(|d| d.package_name == "react").unwrap();
        assert_eq!(react.package_version, "18.2.0");
        assert!(react.source_url.is_some());
    }

    #[test]
    fn test_parse_pnpm_lock_yaml() {
        let content = r#"lockfileVersion: '6.0'
dependencies:
  react:
    specifier: ^18.0.0
    version: 18.2.0
devDependencies:
  typescript:
    specifier: ^5.0.0
    version: 5.3.3
packages:
  /react@18.2.0:
    resolution:
      integrity: sha512-reactHash
      tarball: https://registry.npmjs.org/react/-/react-18.2.0.tgz
    dev: false
  /typescript@5.3.3:
    resolution:
      integrity: sha512-tsHash
    dev: true
  /js-tokens@4.0.0:
    resolution:
      integrity: sha512-tokensHash
"#;

        let deps = parse_lockfile(LockfileKind::PnpmLockYaml, content).unwrap();
        assert_eq!(deps.len(), 3);

        let react = deps.iter().find(|d| d.package_name == "react").unwrap();
        assert_eq!(react.package_version, "18.2.0");
        assert!(!react.is_dev_dependency);
        assert!(react.is_direct_dependency);
        assert!(react.source_url.is_some());

        let ts = deps
            .iter()
            .find(|d| d.package_name == "typescript")
            .unwrap();
        assert!(ts.is_dev_dependency);
        assert!(ts.is_direct_dependency);

        let tokens = deps.iter().find(|d| d.package_name == "js-tokens").unwrap();
        assert!(!tokens.is_direct_dependency);
    }

    #[test]
    fn test_parse_bun_lock() {
        let content = r#"{
            "packages": {
                "react@18.2.0": ["18.2.0", "https://registry.npmjs.org/react/-/react-18.2.0.tgz", "sha512-reactHash", false],
                "typescript@5.3.3": ["5.3.3", "https://registry.npmjs.org/typescript/-/typescript-5.3.3.tgz", "sha512-tsHash", true],
            }
        }"#;

        let deps = parse_lockfile(LockfileKind::BunLock, content).unwrap();
        assert_eq!(deps.len(), 2);

        let react = deps.iter().find(|d| d.package_name == "react").unwrap();
        assert_eq!(react.package_version, "18.2.0");
        assert!(!react.is_dev_dependency);

        let ts = deps
            .iter()
            .find(|d| d.package_name == "typescript")
            .unwrap();
        assert!(ts.is_dev_dependency);
    }

    #[test]
    fn test_detect_lockfile() {
        let dir = TempDir::new().unwrap();

        assert!(detect_lockfile(dir.path()).is_none());

        fs::write(dir.path().join("package-lock.json"), "{}").unwrap();
        let (kind, _) = detect_lockfile(dir.path()).unwrap();
        assert_eq!(kind, LockfileKind::PackageLockJson);

        fs::write(dir.path().join("pnpm-lock.yaml"), "lockfileVersion: '6.0'").unwrap();
        let (kind, _) = detect_lockfile(dir.path()).unwrap();
        assert_eq!(kind, LockfileKind::PnpmLockYaml);
    }

    #[test]
    fn test_strip_jsonc_trailing_commas() {
        let input = r#"{"a": 1, "b": [1, 2,], "c": {"x": 1,},}"#;
        let cleaned = strip_jsonc_trailing_commas(input);
        let _: serde_json::Value = serde_json::from_str(&cleaned).unwrap();
    }

    #[test]
    fn test_parse_yarn_header_scoped() {
        let (name, version) = parse_yarn_header(r#""@babel/core@^7.0.0""#);
        assert_eq!(name, "@babel/core");
        assert_eq!(version, "^7.0.0");
    }

    #[test]
    fn test_parse_pnpm_specifier_with_peers() {
        let (name, version) = parse_pnpm_specifier("/eslint-plugin-react@7.33.2(eslint@8.56.0)");
        assert_eq!(name, "eslint-plugin-react");
        assert_eq!(version, "7.33.2");
    }
}
