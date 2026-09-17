//! Serves the bundled `orbit-cli` skill from the binary itself. The skill
//! directory is embedded at compile time via `rust-embed` (same pattern the
//! ontology and named-queries crates use), so the content is always matched to
//! the binary version and present for every install method — `glab orbit
//! local`, a release-tarball download, or a dev build — with no packaging step.

use anyhow::{Result, bail};
use rust_embed::Embed;
use serde::Deserialize;

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit-cli"]
struct SkillAssets;

const MANIFEST: &str = "SKILL.md";
const DEFAULT_SKILL: &str = "orbit";
const KNOWN_SKILLS: &[&str] = &[DEFAULT_SKILL];

#[derive(Debug, PartialEq, Eq)]
enum Request {
    List,
    Print { name: String, path: String },
}

#[derive(Deserialize)]
struct Frontmatter {
    description: String,
}

/// Appended (never prepended, so the YAML frontmatter stays first) to the
/// served manifest. The on-disk SKILL.md links to `references/local/*.md`
/// with working-tree-relative paths that do not resolve when the only artifact is
/// the binary; this tells the reader the version-matched access path instead.
fn manifest_binary_hint() -> String {
    let launcher = crate::commands::setup::spec::launcher();
    format!(
        "\n\n---\n\nYou are viewing this via the `orbit` binary; the links above are relative to the on-disk skill tree. Fetch referenced files with `{launcher} skills orbit <path>` (e.g. `{launcher} skills references/local/sql.md`).\n"
    )
}

pub(crate) fn run(name_or_path: Option<String>, path: Option<String>) -> Result<()> {
    match resolve(name_or_path.as_deref(), path.as_deref())? {
        Request::List => print_skill_list()?,
        Request::Print { path, .. } => print_skill_file(&path)?,
    }
    Ok(())
}

fn resolve(name_or_path: Option<&str>, path: Option<&str>) -> Result<Request> {
    let Some(first) = name_or_path else {
        return Ok(Request::List);
    };

    if is_skill_name(first) {
        if !KNOWN_SKILLS.contains(&first) {
            bail!(
                "unknown skill name {first:?}. Known skills:\n{}",
                known_skill_list()
            );
        }
        return Ok(Request::Print {
            name: first.to_string(),
            path: path.unwrap_or(MANIFEST).to_string(),
        });
    }

    if path.is_some() {
        bail!("a path shorthand cannot be followed by another path");
    }
    Ok(Request::Print {
        name: DEFAULT_SKILL.to_string(),
        path: first.to_string(),
    })
}

fn is_skill_name(value: &str) -> bool {
    !value.contains(['/', '.'])
        && !value.is_empty()
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn print_skill_list() -> Result<()> {
    let manifest =
        lookup(MANIFEST).ok_or_else(|| anyhow::anyhow!("embedded {MANIFEST} missing"))?;
    let description = manifest_description(&manifest)?;
    let description = description.split_whitespace().collect::<Vec<_>>().join(" ");
    for name in KNOWN_SKILLS {
        println!("{name} — {description}");
    }
    Ok(())
}

fn manifest_description(manifest: &str) -> Result<String> {
    let frontmatter = manifest
        .strip_prefix("---\n")
        .and_then(|content| content.split_once("\n---\n"))
        .map(|(frontmatter, _)| frontmatter)
        .ok_or_else(|| anyhow::anyhow!("embedded {MANIFEST} has invalid frontmatter"))?;
    let parsed: Frontmatter = orbit_utils::yaml::from_str(frontmatter)?;
    Ok(parsed.description)
}

fn print_skill_file(requested: &str) -> Result<()> {
    let Some(rendered) = render(requested) else {
        bail!(
            "unknown skill file {requested:?}. Available files:\n{}",
            available_list()
        );
    };
    print!("{rendered}");
    Ok(())
}

fn known_skill_list() -> String {
    KNOWN_SKILLS
        .iter()
        .map(|name| format!("  {name}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render(requested: &str) -> Option<String> {
    let contents = lookup(requested)?;
    if requested == MANIFEST {
        Some(format!("{contents}{}", manifest_binary_hint()))
    } else {
        Some(contents)
    }
}

fn lookup(requested: &str) -> Option<String> {
    let file = SkillAssets::get(requested)?;
    String::from_utf8(file.data.into_owned()).ok()
}

fn available_list() -> String {
    let mut files: Vec<String> = SkillAssets::iter().map(|p| format!("  {p}")).collect();
    files.sort();
    files.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_embedded_and_has_a_description() {
        let manifest = lookup(MANIFEST).unwrap();
        assert!(manifest.contains("orbit-cli"));
        assert!(
            manifest_description(&manifest)
                .unwrap()
                .contains("Orbit CLI")
        );
    }

    #[test]
    fn arguments_disambiguate_names_and_paths() {
        assert_eq!(resolve(None, None).unwrap(), Request::List);
        assert_eq!(
            resolve(Some("SKILL.md"), None).unwrap(),
            Request::Print {
                name: "orbit".to_string(),
                path: "SKILL.md".to_string(),
            }
        );
        assert_eq!(
            resolve(Some("references/local/sql.md"), None).unwrap(),
            Request::Print {
                name: "orbit".to_string(),
                path: "references/local/sql.md".to_string(),
            }
        );
        assert_eq!(
            resolve(Some("orbit"), None).unwrap(),
            Request::Print {
                name: "orbit".to_string(),
                path: "SKILL.md".to_string(),
            }
        );
        assert_eq!(
            resolve(Some("orbit"), Some("SKILL.md")).unwrap(),
            Request::Print {
                name: "orbit".to_string(),
                path: "SKILL.md".to_string(),
            }
        );
        for path in ["ORBIT", ""] {
            assert_eq!(
                resolve(Some(path), None).unwrap(),
                Request::Print {
                    name: "orbit".to_string(),
                    path: path.to_string(),
                }
            );
        }

        let error = resolve(Some("unknown-name"), None).unwrap_err().to_string();
        assert!(error.contains("unknown skill name"));
        assert!(error.contains("orbit"));

        let error = resolve(
            Some("references/local/sql.md"),
            Some("references/local/repo_map.md"),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("path shorthand cannot be followed by another path"));
    }

    #[test]
    fn reference_files_are_embedded() {
        assert!(lookup("references/local/sql.md").is_some());
        assert!(lookup("references/local/repo_map.md").is_some());
    }

    #[test]
    fn unknown_path_is_not_found() {
        assert!(lookup("references/does-not-exist.md").is_none());
    }

    #[test]
    fn escaping_and_unknown_paths_do_not_resolve() {
        for path in [
            "../Cargo.toml",
            "../../etc/passwd",
            "/etc/passwd",
            "references/local/../../../secret",
            "./SKILL.md",
            "",
        ] {
            assert!(lookup(path).is_none(), "{path:?} must not resolve");
        }
    }

    #[test]
    fn embedded_set_is_non_trivial() {
        assert!(
            SkillAssets::iter().count() >= 3,
            "expected the manifest plus at least references/ content"
        );
    }

    #[test]
    fn served_manifest_carries_binary_hint_but_subfiles_do_not() {
        let manifest = render(MANIFEST).unwrap();
        assert!(manifest.starts_with("---"), "frontmatter must stay first");
        assert!(manifest.contains("`orbit skills references/local/sql.md`"));

        assert!(
            !render("references/local/sql.md")
                .unwrap()
                .contains("skills orbit <path>")
        );
        assert!(
            !render("references/local/repo_map.md")
                .unwrap()
                .contains("skills orbit <path>")
        );
    }
}
