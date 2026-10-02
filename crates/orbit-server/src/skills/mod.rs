//! Skills stay outside the agent command registry because they are passive artifacts.
//! Version is not a content hash: concurrent bumps or an explicit skip can reuse it.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use query_engine::compiler::Frontend;
use rust_embed::Embed;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

const SKILL_NAME: &str = "orbit";
const MANIFEST: &str = "SKILL.md";
const GQL_MANIFEST: &str = "SKILL.gql.md";

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit"]
struct SkillAssets;

static JSON_CATALOG: LazyLock<SkillCatalog> = LazyLock::new(|| {
    SkillCatalog::load_embedded(Frontend::JsonDsl)
        .expect("embedded Orbit skill passed full frontmatter and tree validation at build time")
});

static GQL_CATALOG: LazyLock<SkillCatalog> = LazyLock::new(|| {
    SkillCatalog::load_embedded(Frontend::Gql)
        .expect("embedded Orbit GQL manifest has valid frontmatter")
});

fn served_in(path: &str, frontend: Frontend) -> bool {
    match path {
        GQL_MANIFEST => false,
        "references/gql.md" => frontend == Frontend::Gql,
        "references/query_language.md"
        | "references/recipes.md"
        | "references/remote_repo_map.md"
        | "scripts/remote_repo_map.py" => frontend == Frontend::JsonDsl,
        _ => true,
    }
}

fn catalog(frontend: Frontend) -> &'static SkillCatalog {
    match frontend {
        Frontend::JsonDsl => &JSON_CATALOG,
        Frontend::Gql => &GQL_CATALOG,
    }
}

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
    fn load_embedded(frontend: Frontend) -> Result<Self, String> {
        let mut files = Vec::new();
        for path in SkillAssets::iter() {
            if !served_in(&path, frontend) {
                continue;
            }
            let source = if path == MANIFEST && frontend == Frontend::Gql {
                GQL_MANIFEST
            } else {
                &path
            };
            let asset = SkillAssets::get(source)
                .ok_or_else(|| format!("embedded skill file {source:?} is unreadable"))?;
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
        let frontmatter = orbit_prompts::parse_skill_frontmatter(&manifest.content, SKILL_NAME)
            .map_err(|error| format!("embedded {error}"))?;

        let metadata = SkillMetadata {
            name: frontmatter.name,
            version: frontmatter.version.to_string(),
            description: frontmatter.description,
            compatibility: frontmatter.compatibility,
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

pub fn list_skills(frontend: Frontend) -> Vec<SkillMetadata> {
    catalog(frontend).list()
}

pub fn get_skill(
    name: &str,
    frontend: Frontend,
    metadata_only: bool,
) -> Result<SkillTree, SkillNotFound> {
    catalog(frontend).get(name, metadata_only)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_exposes_manifest_metadata() {
        let skills = list_skills(Frontend::JsonDsl);
        assert_eq!(skills.len(), 1);
        let skill = &skills[0];
        assert_eq!(skill.name, "orbit");
        assert_eq!(skill.version, "0.33.1");
        assert!(skill.description.starts_with("Use the `glab orbit` CLI"));
        assert!(skill.compatibility.contains("Orbit CLI"));
    }

    #[test]
    fn gql_callers_get_the_gql_manifest() {
        let json = list_skills(Frontend::JsonDsl).remove(0);
        let tree = get_skill("orbit", Frontend::Gql, false).unwrap();
        assert_eq!(
            tree.metadata,
            SkillMetadata {
                version: format!("{}+gql", json.version),
                ..json
            }
        );
        let files = tree.files.unwrap();
        assert_eq!(files[0].path, MANIFEST);
        assert!(files[0].content.contains("CALL db.schema()"));
        assert!(files.iter().any(|file| file.path == "references/gql.md"));
        assert!(
            files
                .iter()
                .all(|file| file.path != "references/recipes.md")
        );
    }

    #[test]
    fn served_trees_have_expected_files_hashes_and_valid_links() {
        for (frontend, paths) in [
            (
                Frontend::JsonDsl,
                &[
                    "SKILL.md",
                    "references/local_repo_map.md",
                    "references/maintaining.md",
                    "references/query_language.md",
                    "references/recipes.md",
                    "references/remote_repo_map.md",
                    "references/reporting.md",
                    "references/troubleshooting.md",
                    "scripts/remote_repo_map.py",
                ][..],
            ),
            (
                Frontend::Gql,
                &[
                    "SKILL.md",
                    "references/gql.md",
                    "references/local_repo_map.md",
                    "references/maintaining.md",
                    "references/reporting.md",
                    "references/troubleshooting.md",
                ][..],
            ),
        ] {
            let files = get_skill("orbit", frontend, false).unwrap().files.unwrap();
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
            .unwrap_or_else(|error| panic!("{frontend:?}: {error}"));
        }
    }

    #[test]
    fn metadata_only_omits_files() {
        let full = get_skill("orbit", Frontend::JsonDsl, false).unwrap();
        let metadata = get_skill("orbit", Frontend::JsonDsl, true).unwrap();
        assert_eq!(metadata.metadata, full.metadata);
        assert!(metadata.files.is_none());
        let encoded = serde_json::to_value(metadata).unwrap();
        assert!(!encoded.as_object().unwrap().contains_key("files"));
    }

    #[test]
    fn compact_whole_tree_envelope_stays_below_transport_budget() {
        let tree = get_skill("orbit", Frontend::JsonDsl, false).unwrap();
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
        let error = get_skill("unknown", Frontend::JsonDsl, false).unwrap_err();
        assert_eq!(error.name, "unknown");
        assert_eq!(error.known_names, ["orbit"]);
    }
}
