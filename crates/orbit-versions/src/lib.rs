//! Pinned versions from `config/versions.yaml`, embedded at compile time.
//! Dependency-light on purpose so build scripts can use it too.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Versions {
    pub schema: u32,
    pub query_dsl: String,
    pub raw_output_format: String,
    pub toon_output_format: String,
    pub gitlab_system_note_actions: String,
    pub vendored: BTreeMap<String, VendoredDependency>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VendoredDependency {
    pub version: Option<String>,
    pub vendor_dir: Option<String>,
    pub vendor_script: Option<String>,
    pub check_script: Option<String>,
    pub extensions: Option<BTreeMap<String, Extension>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Extension {
    pub source_revision: Option<String>,
    pub source_archive_sha256: Option<String>,
    pub binaries: Option<BTreeMap<String, String>>,
}

impl Versions {
    pub fn validate(&self) -> Result<(), String> {
        if self.vendored.is_empty() {
            return Err("vendored section is empty".into());
        }
        for (name, dep) in &self.vendored {
            if let Some(v) = &dep.version
                && (v.is_empty() || v != v.trim())
            {
                return Err(format!(
                    "vendored.{name}.version is empty or has whitespace"
                ));
            }
            if let Some(dir) = &dep.vendor_dir
                && (dir.is_empty() || dir.starts_with('/') || dir.contains(".."))
            {
                return Err(format!(
                    "vendored.{name}.vendor_dir must be non-empty, relative, without ..: {dir}"
                ));
            }
            for (label, path) in [
                ("vendor_script", &dep.vendor_script),
                ("check_script", &dep.check_script),
            ] {
                if let Some(path) = path {
                    if !path.ends_with(".sh") {
                        return Err(format!("vendored.{name}.{label} must end in .sh: {path}"));
                    }
                    if path.starts_with('/') || path.contains("..") {
                        return Err(format!(
                            "vendored.{name}.{label} must be relative without ..: {path}"
                        ));
                    }
                }
            }
            if let Some(exts) = &dep.extensions {
                for (ext_name, ext) in exts {
                    if let Some(rev) = &ext.source_revision {
                        validate_hex(
                            rev,
                            40,
                            &format!("vendored.{name}.extensions.{ext_name}.source_revision"),
                        )?;
                    }
                    if let Some(sha) = &ext.source_archive_sha256 {
                        validate_hex(
                            sha,
                            64,
                            &format!("vendored.{name}.extensions.{ext_name}.source_archive_sha256"),
                        )?;
                    }
                    if let Some(bins) = &ext.binaries {
                        for (platform, sha) in bins {
                            validate_hex(
                                sha,
                                64,
                                &format!(
                                    "vendored.{name}.extensions.{ext_name}.binaries.{platform}"
                                ),
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_hex(value: &str, expected_len: usize, field: &str) -> Result<(), String> {
    if value.len() != expected_len {
        return Err(format!(
            "{field}: expected {expected_len} hex chars, got {}",
            value.len()
        ));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(format!("{field}: must be lowercase hex"));
    }
    Ok(())
}

pub static VERSIONS: LazyLock<Versions> = LazyLock::new(|| {
    let versions: Versions =
        parse(include_str!(env!("VERSIONS_FILE"))).expect("config/versions.yaml");
    versions
        .validate()
        .expect("config/versions.yaml validation");
    versions
});

/// Parses any revision's `versions.yaml` text, e.g. `git show` output.
pub fn parse(yaml: &str) -> Result<Versions, serde_saphyr::Error> {
    serde_saphyr::from_str(yaml)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_pin() {
        assert!(VERSIONS.schema > 0);
        assert_eq!(VERSIONS.gitlab_system_note_actions.len(), 40);
        VERSIONS.validate().unwrap();
    }

    #[test]
    fn duckdb_entry_is_complete() {
        let duckdb = VERSIONS.vendored.get("duckdb").expect("vendored.duckdb");
        let version = duckdb.version.as_deref().expect("duckdb.version");
        assert!(version.starts_with('v'));
        assert!(duckdb.vendor_dir.is_some());
        assert!(duckdb.vendor_script.is_some());
        assert!(duckdb.check_script.is_some());

        let exts = duckdb.extensions.as_ref().expect("duckdb.extensions");
        let fts = exts.get("fts").expect("duckdb.extensions.fts");
        assert_eq!(fts.source_revision.as_ref().map(|s| s.len()), Some(40));
        assert_eq!(
            fts.source_archive_sha256.as_ref().map(|s| s.len()),
            Some(64)
        );

        let bins = fts.binaries.as_ref().expect("fts.binaries");
        let expected_platforms = [
            "linux_amd64",
            "linux_arm64",
            "linux_amd64_musl",
            "linux_arm64_musl",
            "osx_amd64",
            "osx_arm64",
            "windows_amd64",
        ];
        for platform in expected_platforms {
            let sha = bins
                .get(platform)
                .unwrap_or_else(|| panic!("missing fts binary for {platform}"));
            assert_eq!(sha.len(), 64, "bad checksum length for {platform}");
        }
    }

    #[test]
    fn validate_rejects_bad_hex() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        let fts = duckdb.extensions.as_mut().unwrap().get_mut("fts").unwrap();
        fts.source_revision = Some("SHORT".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_absolute_vendor_dir() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.vendor_dir = Some("/etc/passwd".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_traversal_vendor_dir() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.vendor_dir = Some("crates/../../etc".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_vendor_dir() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.vendor_dir = Some(String::new());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_traversal_script_path() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.vendor_script = Some("../escape/evil.sh".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_absolute_script_path() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.check_script = Some("/tmp/evil.sh".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_vendored_section() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        versions.vendored.clear();
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_whitespace_version() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.version = Some(" v1.5.5 ".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_non_sh_script() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        duckdb.vendor_script = Some("scripts/evil.py".into());
        assert!(versions.validate().is_err());
    }

    #[test]
    fn validate_rejects_uppercase_hex() {
        let mut versions = parse(include_str!(env!("VERSIONS_FILE"))).unwrap();
        let duckdb = versions.vendored.get_mut("duckdb").unwrap();
        let fts = duckdb.extensions.as_mut().unwrap().get_mut("fts").unwrap();
        fts.source_revision = Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into());
        assert!(versions.validate().is_err());
    }
}
