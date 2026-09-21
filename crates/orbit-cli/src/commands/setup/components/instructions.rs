use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{Installer, Report, backup_once, remove_file};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent};

const BLOCK_BEGIN: &str = "<!-- orbit:setup:begin -->";
const BLOCK_END: &str = "<!-- orbit:setup:end -->";

pub(super) struct Instructions;

impl Installer for Instructions {
    fn plan(&self, assistant: Agent, target: &Target) -> Result<Vec<String>> {
        Ok(vec![target.resolve(&assistant.instruction_file)?.1])
    }

    fn install(&self, assistants: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in files(assistants, target)? {
            upsert_block_in_file(&path, &label, report)?;
        }
        Ok(())
    }

    fn remove(&self, assistants: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in files(assistants, target)? {
            strip_block_from_file(&path, target, &label, report)?;
        }
        Ok(())
    }
}

fn files(assistants: &[Agent], target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let mut resolved: Vec<(PathBuf, String)> = assistants
        .iter()
        .map(|assistant| target.resolve(&assistant.instruction_file))
        .collect::<Result<_>>()?;
    resolved.sort_by(|a, b| a.0.cmp(&b.0));
    resolved.dedup_by(|a, b| a.0 == b.0);

    let mut files: Vec<(PathBuf, String)> = Vec::new();
    let mut canonicals: Vec<PathBuf> = Vec::new();
    for (path, label) in resolved {
        let canonical = dunce::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if canonicals.contains(&canonical) {
            continue;
        }
        canonicals.push(canonical);
        files.push((path, label));
    }
    Ok(files)
}

fn rendered_block() -> String {
    format!("{BLOCK_BEGIN}\n{}\n{BLOCK_END}", spec::instructions())
}

fn upsert_block_in_file(path: &Path, label: &str, report: &mut Report) -> Result<()> {
    let block = rendered_block();
    let (updated, action) = match std::fs::read_to_string(path) {
        Ok(existing) => match splice_block(&existing, &block) {
            Some(updated) => (updated, "orbit section updated"),
            None => {
                backup_once(path, label, report)?;
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
    report.note(label, action);
    Ok(())
}

fn strip_block_from_file(
    path: &Path,
    target: &Target,
    label: &str,
    report: &mut Report,
) -> Result<()> {
    let existing = match std::fs::read_to_string(path) {
        Ok(existing) => existing,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };
    let Some(remaining) = strip_block(&existing) else {
        return Ok(());
    };
    if remaining.trim().is_empty() {
        remove_file(path, target)?;
        report.note(label, "removed (was orbit-only)");
    } else {
        std::fs::write(path, remaining)
            .with_context(|| format!("failed to write {}", path.display()))?;
        report.note(label, "orbit section removed");
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
    use super::*;

    fn specs_for(names: &[&str]) -> Vec<Agent> {
        names.iter().map(|name| spec::get(name).unwrap()).collect()
    }

    fn project(dir: &Path) -> Target {
        Target::project(Some(dir.to_path_buf())).unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn files_dedupes_symlinked_claude_md() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# rules\n").unwrap();
        std::os::unix::fs::symlink("AGENTS.md", dir.path().join("CLAUDE.md")).unwrap();

        let files = files(&specs_for(&["claude", "codex"]), &project(dir.path()));
        let labels: Vec<String> = files.unwrap().into_iter().map(|(_, label)| label).collect();
        assert_eq!(labels, vec!["AGENTS.md"]);
    }

    #[test]
    fn files_split_when_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let files = files(&specs_for(&["claude", "pi"]), &project(dir.path()));
        let labels: Vec<String> = files.unwrap().into_iter().map(|(_, label)| label).collect();
        assert_eq!(labels, vec!["AGENTS.md", "CLAUDE.md"]);
    }

    #[test]
    fn global_files_are_per_assistant() {
        let files = files(&specs_for(&["claude", "codex", "pi"]), &Target::Global);
        let labels: Vec<String> = files.unwrap().into_iter().map(|(_, label)| label).collect();
        assert_eq!(
            labels,
            vec![
                "~/.claude/CLAUDE.md",
                "~/.codex/AGENTS.md",
                "~/.pi/agent/AGENTS.md"
            ]
        );
    }

    #[test]
    fn append_then_splice_is_idempotent() {
        let block = rendered_block();
        let appended = append_block("# My project\n\nSome rules.\n", &block);
        assert!(appended.starts_with("# My project"));
        assert!(appended.ends_with(&format!("{BLOCK_END}\n")));

        let respliced = splice_block(&appended, &block).expect("markers must be found");
        assert_eq!(respliced, appended);
    }

    #[test]
    fn splice_preserves_surrounding_content() {
        let existing = format!("# Before\n\n{BLOCK_BEGIN}\nold content\n{BLOCK_END}\n\n# After\n");
        let updated = splice_block(&existing, &rendered_block()).unwrap();
        assert!(updated.starts_with("# Before"));
        assert!(updated.ends_with("# After\n"));
        assert!(updated.contains("`orbit grep"));
        assert!(!updated.contains("old content"));
    }

    #[test]
    fn strip_removes_block_and_keeps_neighbors() {
        let existing = format!("# Before\n\n{}\n\n# After\n", rendered_block());
        let remaining = strip_block(&existing).unwrap();
        assert_eq!(remaining, "# Before\n\n# After\n");
    }

    #[test]
    fn strip_on_orbit_only_file_leaves_nothing() {
        let existing = format!("{}\n", rendered_block());
        assert_eq!(strip_block(&existing).unwrap(), "");
    }

    #[test]
    fn strip_without_markers_is_none() {
        assert!(strip_block("# Just a readme\n").is_none());
    }

    #[test]
    fn upsert_creates_updates_and_strip_restores() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("AGENTS.md");
        std::fs::write(&path, "# My rules\n").unwrap();

        upsert_block_in_file(&path, "AGENTS.md", &mut Report::default()).unwrap();
        upsert_block_in_file(&path, "AGENTS.md", &mut Report::default()).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written.matches(BLOCK_BEGIN).count(), 1);
        assert!(written.contains("# My rules"));

        strip_block_from_file(
            &path,
            &project(dir.path()),
            "AGENTS.md",
            &mut Report::default(),
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "# My rules\n");
    }

    #[test]
    fn strip_deletes_file_that_was_orbit_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("CLAUDE.md");

        upsert_block_in_file(&path, "CLAUDE.md", &mut Report::default()).unwrap();
        assert!(path.is_file());

        strip_block_from_file(
            &path,
            &project(dir.path()),
            "CLAUDE.md",
            &mut Report::default(),
        )
        .unwrap();
        assert!(!path.exists());
    }
}
