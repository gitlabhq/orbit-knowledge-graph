//! Managed instruction-file block: a marker-delimited section spliced into AGENTS.md /
//! CLAUDE.md. Re-running setup replaces the section in place, so upgrades refresh the guidance
//! without duplicating it or touching the rest of the file.

use std::path::Path;

use anyhow::{Context, Result};

const BLOCK_BEGIN: &str = "<!-- orbit:setup:begin -->";
const BLOCK_END: &str = "<!-- orbit:setup:end -->";

fn rendered_block(mode: super::spec::Mode) -> String {
    format!(
        "{BLOCK_BEGIN}\n{}\n{BLOCK_END}",
        super::spec::instructions(mode)
    )
}

pub(super) fn upsert_block_in_file(
    path: &Path,
    label: &str,
    mode: super::spec::Mode,
) -> Result<()> {
    let block = rendered_block(mode);
    let (updated, action) = match std::fs::read_to_string(path) {
        Ok(existing) => match splice_block(&existing, &block) {
            Some(updated) => (updated, "orbit section updated"),
            None => {
                super::backup_once(path, label)?;
                (append_block(&existing, &block), "orbit section written")
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (format!("{block}\n"), "created with orbit section")
        }
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, updated).with_context(|| format!("failed to write {}", path.display()))?;
    println!("  {label}  ->  {action}");
    Ok(())
}

pub(super) fn strip_block_from_file(path: &Path, label: &str) -> Result<()> {
    let existing = match std::fs::read_to_string(path) {
        Ok(existing) => existing,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };
    let Some(remaining) = strip_block(&existing) else {
        return Ok(());
    };
    if remaining.trim().is_empty() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
        println!("  {label}  ->  removed (was orbit-only)");
    } else {
        std::fs::write(path, remaining)
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("  {label}  ->  orbit section removed");
    }
    Ok(())
}

fn splice_block(existing: &str, block: &str) -> Option<String> {
    let (start, end) = block_span(existing)?;
    let mut updated = String::with_capacity(existing.len() + block.len());
    updated.push_str(&existing[..start]);
    updated.push_str(block);
    updated.push_str(&existing[end..]);
    Some(updated)
}

fn append_block(existing: &str, block: &str) -> String {
    if existing.trim().is_empty() {
        format!("{block}\n")
    } else {
        format!("{}\n\n{block}\n", existing.trim_end())
    }
}

fn strip_block(existing: &str) -> Option<String> {
    let (start, end) = block_span(existing)?;
    let before = existing[..start].trim_end();
    let after = existing[end..].trim_start();
    Some(match (before.is_empty(), after.is_empty()) {
        (true, _) => after.to_string(),
        (false, true) => format!("{before}\n"),
        (false, false) => format!("{before}\n\n{after}"),
    })
}

fn block_span(existing: &str) -> Option<(usize, usize)> {
    let start = existing.find(BLOCK_BEGIN)?;
    let end = existing.find(BLOCK_END)? + BLOCK_END.len();
    (end > start).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::super::spec::Mode;
    use super::*;

    #[test]
    fn append_then_splice_is_idempotent() {
        let block = rendered_block(Mode::Local);
        let appended = append_block("# My project\n\nSome rules.\n", &block);
        assert!(appended.starts_with("# My project"));
        assert!(appended.ends_with(&format!("{BLOCK_END}\n")));

        let respliced = splice_block(&appended, &block).expect("markers must be found");
        assert_eq!(respliced, appended);
    }

    #[test]
    fn splice_preserves_surrounding_content() {
        let existing = format!("# Before\n\n{BLOCK_BEGIN}\nold content\n{BLOCK_END}\n\n# After\n");
        let updated = splice_block(&existing, &rendered_block(Mode::Local)).unwrap();
        assert!(updated.starts_with("# Before"));
        assert!(updated.ends_with("# After\n"));
        assert!(updated.contains("orbit local grep"));
        assert!(!updated.contains("old content"));
    }

    #[test]
    fn strip_removes_block_and_keeps_neighbors() {
        let existing = format!("# Before\n\n{}\n\n# After\n", rendered_block(Mode::Local));
        let remaining = strip_block(&existing).unwrap();
        assert_eq!(remaining, "# Before\n\n# After\n");
    }

    #[test]
    fn strip_on_orbit_only_file_leaves_nothing() {
        let existing = format!("{}\n", rendered_block(Mode::Local));
        assert_eq!(strip_block(&existing).unwrap(), "");
    }

    #[test]
    fn strip_without_markers_is_none() {
        assert!(strip_block("# Just a readme\n").is_none());
    }

    #[test]
    fn modes_replace_each_other_rather_than_combining() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");

        upsert_block_in_file(&path, "AGENTS.md", Mode::Remote).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("glab orbit remote"));
        assert!(!written.contains("orbit local sql"));

        upsert_block_in_file(&path, "AGENTS.md", Mode::Local).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("orbit local sql"));
        assert!(!written.contains("glab orbit remote"));
        assert_eq!(written.matches(BLOCK_BEGIN).count(), 1);
    }

    #[test]
    fn upsert_creates_updates_and_strip_restores() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        std::fs::write(&path, "# My rules\n").unwrap();

        upsert_block_in_file(&path, "AGENTS.md", Mode::Local).unwrap();
        upsert_block_in_file(&path, "AGENTS.md", Mode::Local).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written.matches(BLOCK_BEGIN).count(), 1);
        assert!(written.contains("# My rules"));

        strip_block_from_file(&path, "AGENTS.md").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "# My rules\n");
    }

    #[test]
    fn strip_deletes_file_that_was_orbit_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("CLAUDE.md");

        upsert_block_in_file(&path, "CLAUDE.md", Mode::Local).unwrap();
        assert!(path.is_file());

        strip_block_from_file(&path, "CLAUDE.md").unwrap();
        assert!(!path.exists());
    }
}
