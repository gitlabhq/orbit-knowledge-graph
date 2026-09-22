use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{
    Installer, Report, backup_once, drop_backup_when_restored, remove_file_and_empty_parents,
};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent};

const BLOCK_BEGIN: &str = "<!-- orbit:setup:begin -->";
const BLOCK_END: &str = "<!-- orbit:setup:end -->";

pub(super) struct Instructions;

impl Installer for Instructions {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        Ok(vec![target.resolve(&agent.instruction_file)?.1])
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in instruction_files(agents, target)? {
            upsert_block_in_file(&path, &label, report)?;
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in instruction_files(agents, target)? {
            strip_block_from_file(&path, target, &label, report)?;
        }
        Ok(())
    }
}

fn instruction_files(agents: &[Agent], target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let mut resolved: Vec<(PathBuf, String)> = agents
        .iter()
        .map(|agent| target.resolve(&agent.instruction_file))
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
    format!("{BLOCK_BEGIN}\n{}\n{BLOCK_END}", spec::instructions_text())
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
        remove_file_and_empty_parents(path, target)?;
        report.note(label, "removed (was orbit-only)");
    } else {
        std::fs::write(path, remaining)
            .with_context(|| format!("failed to write {}", path.display()))?;
        report.note(label, "orbit section removed");
        drop_backup_when_restored(path, label, report)?;
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
