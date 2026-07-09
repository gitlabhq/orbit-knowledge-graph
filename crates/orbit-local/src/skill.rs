//! Serves the bundled `orbit-local` skill from the binary itself. The file
//! table is embedded at build time from `skills/orbit-local/` (see `build.rs`),
//! so the content is always matched to the binary version and present for every
//! install method — `glab orbit local`, a release-tarball download, or a dev
//! build — with no packaging step. `repo_map.py` still needs a filesystem path
//! to run; `orbit skill scripts/repo_map.py > /tmp/repo_map.py` reconstitutes
//! one anywhere the binary is.

use anyhow::{Result, bail};

include!(concat!(env!("OUT_DIR"), "/skill_files.rs"));

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
        Some(contents.to_string())
    }
}

fn lookup(requested: &str) -> Option<&'static str> {
    if !is_safe_relative(requested) {
        return None;
    }
    SKILL_FILES
        .iter()
        .find(|(rel, _)| *rel == requested)
        .map(|(_, contents)| *contents)
}

/// Guards against absolute paths and `..`/root-escaping traversal. The embedded
/// table only holds forward-slash relative paths, so anything that could climb
/// out of the skill root is rejected before the lookup.
fn is_safe_relative(requested: &str) -> bool {
    if requested.is_empty() || requested.starts_with('/') || requested.contains('\\') {
        return false;
    }
    !requested
        .split('/')
        .any(|c| c == ".." || c == "." || c.is_empty())
}

fn available_list() -> String {
    SKILL_FILES
        .iter()
        .map(|(rel, _)| format!("  {rel}"))
        .collect::<Vec<_>>()
        .join("\n")
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
    fn traversal_and_absolute_paths_are_rejected() {
        for path in [
            "../Cargo.toml",
            "../../etc/passwd",
            "/etc/passwd",
            "references/../../secret",
            "./SKILL.md",
            "references\\sql.md",
            "",
        ] {
            assert!(!is_safe_relative(path), "{path:?} must be rejected");
            assert!(lookup(path).is_none(), "{path:?} must not resolve");
        }
    }

    #[test]
    fn embedded_set_is_non_trivial() {
        assert!(
            SKILL_FILES.len() >= 3,
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
