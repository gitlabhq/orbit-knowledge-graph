use std::collections::BTreeMap;
use std::sync::LazyLock;

use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const SKILL_NAME: &str = "orbit";
const MANIFEST: &str = "SKILL.md";

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit"]
struct SkillAssets;

static CATALOG: LazyLock<SkillCatalog> = LazyLock::new(|| {
    SkillCatalog::load_embedded().expect("Orbit skills are validated by orbit-server/build.rs")
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillMetadata {
    pub name: String,
    pub version: String,
    pub tree_sha256: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillFile {
    pub path: String,
    pub sha256: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillTree {
    #[serde(flatten)]
    pub metadata: SkillMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<SkillFile>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Error)]
#[error("Unknown skill {name:?}. Known skills: {known_names:?}")]
pub struct SkillNotFound {
    pub name: String,
    pub known_names: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontmatter {
    name: String,
    version: semver::Version,
    description: String,
    #[serde(default)]
    license: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug)]
struct EmbeddedSkill {
    metadata: SkillMetadata,
    files: Vec<SkillFile>,
}

#[derive(Debug)]
struct SkillCatalog {
    skills: BTreeMap<String, EmbeddedSkill>,
}

impl SkillCatalog {
    fn load_embedded() -> Result<Self, String> {
        let mut files = Vec::new();
        for path in SkillAssets::iter() {
            let asset = SkillAssets::get(&path)
                .ok_or_else(|| format!("embedded skill file {path:?} is unreadable"))?;
            let content = String::from_utf8(asset.data.into_owned())
                .map_err(|error| format!("embedded skill file {path:?} is not UTF-8: {error}"))?;
            files.push(SkillFile {
                path: path.into_owned(),
                sha256: sha256_hex(content.as_bytes()),
                content,
            });
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));

        let manifest = files
            .iter()
            .find(|file| file.path == MANIFEST)
            .ok_or_else(|| format!("embedded {SKILL_NAME} skill is missing {MANIFEST}"))?;
        let frontmatter = parse_frontmatter(&manifest.content)?;
        if frontmatter.name != SKILL_NAME {
            return Err(format!(
                "embedded skill name {:?} does not match {SKILL_NAME:?}",
                frontmatter.name
            ));
        }
        let _ = (&frontmatter.license, &frontmatter.metadata);

        let metadata = SkillMetadata {
            name: frontmatter.name,
            version: frontmatter.version.to_string(),
            tree_sha256: tree_sha256(&files),
            description: frontmatter.description,
        };
        let mut skills = BTreeMap::new();
        skills.insert(metadata.name.clone(), EmbeddedSkill { metadata, files });
        Ok(Self { skills })
    }

    fn list(&self) -> Vec<SkillMetadata> {
        self.skills
            .values()
            .map(|skill| skill.metadata.clone())
            .collect()
    }

    fn get(&self, name: &str, metadata_only: bool) -> Result<SkillTree, SkillNotFound> {
        let skill = self.skills.get(name).ok_or_else(|| SkillNotFound {
            name: name.to_string(),
            known_names: self.skills.keys().cloned().collect(),
        })?;
        Ok(SkillTree {
            metadata: skill.metadata.clone(),
            files: (!metadata_only).then(|| skill.files.clone()),
        })
    }
}

fn parse_frontmatter(manifest: &str) -> Result<Frontmatter, String> {
    let content = manifest
        .strip_prefix("---\n")
        .ok_or_else(|| format!("embedded {MANIFEST} has no YAML frontmatter"))?;
    let (frontmatter, _) = content
        .split_once("\n---\n")
        .ok_or_else(|| format!("embedded {MANIFEST} has unterminated YAML frontmatter"))?;
    orbit_utils::yaml::from_str(frontmatter)
        .map_err(|error| format!("parsing embedded {MANIFEST} frontmatter: {error}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(64);
    for byte in digest.as_ref() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

/// Hashes files in path order as `path || NUL || u64_be(content_len) || content`.
/// Paths are normalized UTF-8 and cannot contain NUL, while the fixed-width
/// length makes boundaries unambiguous for arbitrary file bytes.
fn tree_sha256(files: &[SkillFile]) -> String {
    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.path.as_bytes());
        hasher.update([0]);
        hasher.update((file.content.len() as u64).to_be_bytes());
        hasher.update(file.content.as_bytes());
    }
    hex_digest(hasher.finalize())
}

pub fn list_skills() -> Vec<SkillMetadata> {
    CATALOG.list()
}

pub fn get_skill(name: &str, metadata_only: bool) -> Result<SkillTree, SkillNotFound> {
    CATALOG.get(name, metadata_only)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_exposes_manifest_metadata_and_tree_hash() {
        let skills = list_skills();
        assert_eq!(skills.len(), 1);
        let skill = &skills[0];
        assert_eq!(skill.name, "orbit");
        assert_eq!(skill.version, "0.29.0");
        assert!(skill.description.starts_with("Use the `glab orbit` CLI"));
        assert_eq!(skill.tree_sha256.len(), 64);
    }

    #[test]
    fn full_tree_is_sorted_and_every_hash_matches_content() {
        let tree = get_skill("orbit", false).unwrap();
        let files = tree.files.unwrap();
        assert_eq!(files.len(), 10);
        assert!(files.windows(2).all(|pair| pair[0].path < pair[1].path));
        for file in &files {
            assert_eq!(
                file.sha256,
                sha256_hex(file.content.as_bytes()),
                "{}",
                file.path
            );
        }
        assert_eq!(tree.metadata.tree_sha256, tree_sha256(&files));
    }

    #[test]
    fn canonical_tree_hash_is_pinned_to_the_embedded_tree() {
        let tree = get_skill("orbit", false).unwrap();
        assert_eq!(
            tree.metadata.tree_sha256,
            "d3658ea9a95ede60e89be9948e84e7793f398aff7152d99c747f66508c04efa1",
            "skill content changes require a version bump and a reviewed hash update"
        );
    }

    #[test]
    fn metadata_only_omits_files() {
        let full = get_skill("orbit", false).unwrap();
        let metadata = get_skill("orbit", true).unwrap();
        assert_eq!(metadata.metadata, full.metadata);
        assert!(metadata.files.is_none());
        let encoded = serde_json::to_value(metadata).unwrap();
        assert!(!encoded.as_object().unwrap().contains_key("files"));
    }

    #[test]
    fn compact_whole_tree_envelope_stays_below_transport_budget() {
        let tree = get_skill("orbit", false).unwrap();
        let source_bytes: usize = tree
            .files
            .as_ref()
            .unwrap()
            .iter()
            .map(|file| file.content.len())
            .sum();
        let encoded = serde_json::to_vec(&tree).unwrap();
        assert!(
            source_bytes < 100_000,
            "source tree grew to {source_bytes} bytes"
        );
        assert!(
            encoded.len() < 128 * 1024,
            "envelope grew to {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn unknown_skill_error_carries_sorted_known_names() {
        let error = get_skill("unknown", false).unwrap_err();
        assert_eq!(error.name, "unknown");
        assert_eq!(error.known_names, ["orbit"]);
    }
}
