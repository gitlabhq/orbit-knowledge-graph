//! Serves the bundled `orbit-local` skill from the binary itself. The skill
//! directory is embedded at compile time via `rust-embed` (same pattern the
//! ontology and named-queries crates use), so the content is always matched to
//! the binary version and present for every install method — `glab orbit
//! local`, a release-tarball download, or a dev build — with no packaging step.
//! `repo_map.py` still needs a filesystem path to run; `orbit skill
//! scripts/repo_map.py > /tmp/repo_map.py` reconstitutes one anywhere the binary
//! is.

use anyhow::{Result, bail};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "$SKILLS_DIR/orbit-local"]
struct SkillAssets;

const MANIFEST: &str = "SKILL.md";

/// Appended (never prepended, so the YAML frontmatter stays first) to the
/// served manifest. The on-disk SKILL.md links to `references/*.md` with
/// working-tree-relative paths that do not resolve when the only artifact is
/// the binary; this tells the reader the version-matched access path instead.
const MANIFEST_BINARY_HINT: &str = "\n\n---\n\nYou are viewing this via the `orbit` binary; the links above are relative to the on-disk skill tree. Fetch referenced files with `orbit skill <path>` (e.g. `orbit skill references/sql.md`).\n";

pub(crate) fn run(path: Option<String>) -> Result<()> {
    let requested = path.as_deref().unwrap_or(MANIFEST);

    let Some(rendered) = render(requested) else {
        bail!(
            "unknown skill file {requested:?}. Available files:\n{}",
            available_list()
        );
    };

    print!("{rendered}");
    Ok(())
}

fn render(requested: &str) -> Option<String> {
    let contents = lookup(requested)?;
    if requested == MANIFEST {
        Some(format!("{contents}{MANIFEST_BINARY_HINT}"))
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
    fn manifest_is_embedded_and_is_the_default() {
        assert!(lookup(MANIFEST).is_some());
        assert!(lookup(MANIFEST).unwrap().contains("orbit-local"));
    }

    #[test]
    fn reference_and_script_files_are_embedded() {
        assert!(lookup("references/sql.md").is_some());
        assert!(lookup("scripts/repo_map.py").is_some());
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
            "references/../../secret",
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
            "expected the manifest plus at least references/ and scripts/ content"
        );
    }

    #[test]
    fn served_manifest_carries_binary_hint_but_subfiles_do_not() {
        let manifest = render(MANIFEST).unwrap();
        assert!(manifest.starts_with("---"), "frontmatter must stay first");
        assert!(manifest.contains("orbit skill references/sql.md"));

        assert!(
            !render("references/sql.md")
                .unwrap()
                .contains("orbit skill <path>")
        );
        assert!(
            !render("scripts/repo_map.py")
                .unwrap()
                .contains("orbit skill <path>")
        );
    }
}
