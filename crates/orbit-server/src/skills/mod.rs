//! Skills stay outside the agent command registry because they are passive artifacts.
//! Version is not a content hash: concurrent bumps or an explicit skip can reuse it.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use rust_embed::Embed;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

const SKILL_NAME: &str = "orbit";
const START_SKILL_NAME: &str = "orbit-start";
const MANIFEST: &str = "SKILL.md";

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit"]
struct SkillAssets;

const MIGRATION_SKILL_NAME: &str = "orbit-gql-migration";

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit-gql-migration"]
struct MigrationAssets;

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit-start"]
struct StartAssets;

static CATALOG: LazyLock<SkillCatalog> = LazyLock::new(|| {
    SkillCatalog::load_embedded()
        .expect("embedded skills passed full frontmatter and tree validation in tests")
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillMetadata {
    pub name: String,
    pub version: String,
    pub description: String,
    pub compatibility: String,
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
        let orbit = load_skill::<SkillAssets>(SKILL_NAME)?;
        let migration = load_skill::<MigrationAssets>(MIGRATION_SKILL_NAME)?;
        let start = load_skill::<StartAssets>(START_SKILL_NAME)?;
        let skills = [orbit, migration, start]
            .into_iter()
            .map(|skill| (skill.metadata.name.clone(), skill))
            .collect();
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

fn load_skill<A: Embed>(name: &str) -> Result<EmbeddedSkill, String> {
    let mut files = Vec::new();
    for path in A::iter() {
        let asset =
            A::get(&path).ok_or_else(|| format!("embedded skill file {path:?} is unreadable"))?;
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
        .ok_or_else(|| format!("embedded {name} skill is missing {MANIFEST}"))?;
    let frontmatter = orbit_prompts::parse_skill_frontmatter(&manifest.content, name)
        .map_err(|error| format!("embedded {error}"))?;

    let metadata = SkillMetadata {
        name: frontmatter.name,
        version: frontmatter.version.to_string(),
        description: frontmatter.description,
        compatibility: frontmatter.compatibility,
    };
    Ok(EmbeddedSkill { metadata, files })
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
    fn list_exposes_manifest_metadata() {
        let skills = list_skills();
        assert_eq!(
            skills
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            [SKILL_NAME, MIGRATION_SKILL_NAME, START_SKILL_NAME]
        );
        let skill = &skills[0];
        assert_eq!(skill.name, "orbit");
        let manifest = SkillAssets::get(MANIFEST).unwrap();
        let frontmatter = orbit_prompts::parse_skill_frontmatter(
            std::str::from_utf8(&manifest.data).unwrap(),
            SKILL_NAME,
        )
        .unwrap();
        assert_eq!(skill.version, frontmatter.version.to_string());
        assert!(skill.description.starts_with("Use Orbit"));
        assert!(skill.compatibility.contains("Orbit CLI"));
    }

    #[test]
    fn wrapper_skill_description_matches_orbit_skill() {
        let wrapper_manifest = include_str!(concat!(env!("SKILLS_DIR"), "/orbit-wrapper/SKILL.md"));
        let wrapper = orbit_prompts::parse_skill_frontmatter(wrapper_manifest, "orbit-wrapper")
            .expect("skills/orbit-wrapper/SKILL.md frontmatter must parse");
        let orbit = list_skills().remove(0);
        assert_eq!(
            wrapper.description, orbit.description,
            "skills/orbit-wrapper/SKILL.md description drifted from skills/orbit/SKILL.md: \
             copy the orbit description into the wrapper and bump the wrapper `version`"
        );
    }

    #[test]
    fn served_trees_have_expected_files_hashes_and_valid_links() {
        let paths = [
            "SKILL.md",
            "references/gql.md",
            "references/maintaining.md",
            "references/reporting.md",
            "references/troubleshooting.md",
        ];
        {
            let files = get_skill("orbit", false).unwrap().files.unwrap();
            assert_eq!(
                files
                    .iter()
                    .map(|file| file.path.as_str())
                    .collect::<Vec<_>>(),
                paths
            );
            let remote = tempfile::tempdir().unwrap();
            for file in files {
                assert_eq!(
                    file.sha256,
                    sha256_hex(file.content.as_bytes()),
                    "{}",
                    file.path
                );
                let path = remote.path().join(file.path);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, file.content).unwrap();
            }
            orbit_prompts::validate_skill_pair(
                remote.path(),
                std::path::Path::new(env!("SKILLS_DIR")).join("orbit-cli"),
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../orbit-cli/src/main.rs"),
            )
            .unwrap_or_else(|error| panic!("{error}"));
        }
    }

    #[test]
    fn migration_skill_is_served_unchanged() {
        let tree = get_skill(MIGRATION_SKILL_NAME, false).unwrap();
        assert!(!tree.metadata.version.contains("+gql"));
        let files = tree.files.unwrap();
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            [MANIFEST]
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
        assert_eq!(
            error.known_names,
            [SKILL_NAME, MIGRATION_SKILL_NAME, START_SKILL_NAME]
        );
    }

    #[test]
    fn start_skill_is_served_unchanged() {
        let tree = get_skill(START_SKILL_NAME, false).unwrap();
        assert_eq!(tree.metadata.name, START_SKILL_NAME);
        assert!(tree.metadata.description.starts_with("Walk a user through"));

        let files = tree.files.unwrap();
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            [MANIFEST]
        );
        let manifest = &files[0];
        assert_eq!(manifest.sha256, sha256_hex(manifest.content.as_bytes()));
        let source = StartAssets::get(MANIFEST).unwrap();
        assert_eq!(manifest.content.as_bytes(), source.data.as_ref());
        assert!(manifest.content.contains("glab orbit skills get orbit"));
        assert!(manifest.content.contains("Do not use `glab orbit`"));
    }
}
