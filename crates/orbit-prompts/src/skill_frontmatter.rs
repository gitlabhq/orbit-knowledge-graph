const MANIFEST: &str = "SKILL.md";
const SKILL_NAME: &str = "orbit";

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillFrontmatter {
    pub name: String,
    pub version: semver::Version,
    pub description: String,
    #[serde(default, rename = "license")]
    _license: Option<String>,
    #[serde(default, rename = "metadata")]
    _metadata: Option<serde::de::IgnoredAny>,
}

pub fn parse_skill_frontmatter(manifest: &str) -> Result<SkillFrontmatter, String> {
    let normalized = manifest.replace("\r\n", "\n");
    let content = normalized
        .strip_prefix("---\n")
        .ok_or_else(|| format!("{MANIFEST} has no YAML frontmatter"))?;
    let (frontmatter, _) = content
        .split_once("\n---\n")
        .ok_or_else(|| format!("{MANIFEST} has unterminated YAML frontmatter"))?;
    let parsed: SkillFrontmatter = orbit_utils::yaml::from_str(frontmatter)
        .map_err(|error| format!("parsing {MANIFEST} frontmatter: {error}"))?;
    if parsed.name != SKILL_NAME {
        return Err(format!(
            "skill name {:?} does not match {SKILL_NAME:?}",
            parsed.name
        ));
    }
    if parsed.description.trim().is_empty() {
        return Err("skill description must not be empty".to_string());
    }
    Ok(parsed)
}
