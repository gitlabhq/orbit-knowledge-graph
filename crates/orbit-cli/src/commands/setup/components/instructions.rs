use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{
    Installer, Report, backup_once, drop_backup_when_restored, file_mentions, mcp,
    remove_file_and_empty_parents, write_unless_unchanged,
};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent};

const BLOCK_BEGIN: &str = "<!-- orbit:setup:begin -->";
const BLOCK_END: &str = "<!-- orbit:setup:end -->";

pub(super) struct Instructions;

impl Installer for Instructions {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        let mut files = vec![instruction_file(agent, target)?.1];
        files.extend(mcp::retired_label(agent, target)?);
        Ok(files)
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in instruction_files(agents, target)? {
            upsert_block_in_file(&path, &label, report)?;
        }
        for &agent in agents {
            drop_orbit_only_primary(agent, target, report)?;
        }
        mcp::remove_retired(agents, target, report)
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in removable_files(agents, target)? {
            strip_block_from_file(&path, target, &label, report)?;
        }
        mcp::remove_retired(agents, target, report)
    }

    fn is_installed(&self, agent: Agent, target: &Target) -> bool {
        candidate_files(agent, target).is_ok_and(|files| {
            files
                .iter()
                .any(|(path, _)| file_mentions(path, BLOCK_BEGIN))
        }) || mcp::retired_label(agent, target).is_ok_and(|label| label.is_some())
    }
}

fn fallback_file(agent: Agent, target: &Target) -> Option<(PathBuf, String)> {
    let name = agent.project_instruction_fallback.as_ref()?;
    match target {
        Target::Project(root) => Some((root.join(name), name.clone())),
        Target::Global(_) => None,
    }
}

fn holds_only_block(path: &Path) -> bool {
    let is_symlink = std::fs::symlink_metadata(path).is_ok_and(|meta| meta.is_symlink());
    !is_symlink
        && std::fs::read_to_string(path)
            .is_ok_and(|text| strip_block(&text).is_some_and(|rest| rest.trim().is_empty()))
}

/// The file that receives the block. With a fallback, the primary file is used
/// only when it carries the user's own content; otherwise the fallback is.
fn instruction_file(agent: Agent, target: &Target) -> Result<(PathBuf, String)> {
    let primary = target.resolve(&agent.instruction_file)?;
    let Some(fallback) = fallback_file(agent, target) else {
        return Ok(primary);
    };
    let primary_is_users =
        std::fs::symlink_metadata(&primary.0).is_ok() && !holds_only_block(&primary.0);
    Ok(if primary_is_users { primary } else { fallback })
}

/// Every file a block may live in, so removal finds it even if the user added a
/// CLAUDE.md after setup.
fn candidate_files(agent: Agent, target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let mut files = vec![target.resolve(&agent.instruction_file)?];
    files.extend(fallback_file(agent, target));
    Ok(files)
}

/// Older setups created a CLAUDE.md holding only the block, which hides AGENTS.md.
fn drop_orbit_only_primary(agent: Agent, target: &Target, report: &mut Report) -> Result<()> {
    let (primary, label) = target.resolve(&agent.instruction_file)?;
    if instruction_file(agent, target)?.0 != primary && holds_only_block(&primary) {
        remove_file_and_empty_parents(&primary, target)?;
        report.note(&label, "removed (was orbit-only)");
    }
    Ok(())
}

fn removable_files(agents: &[Agent], target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let mut all = Vec::new();
    for &agent in agents {
        all.extend(candidate_files(agent, target)?);
    }
    dedupe_files(all)
}

fn instruction_files(agents: &[Agent], target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let resolved = agents
        .iter()
        .map(|&agent| instruction_file(agent, target))
        .collect::<Result<_>>()?;
    dedupe_files(resolved)
}

fn dedupe_files(mut resolved: Vec<(PathBuf, String)>) -> Result<Vec<(PathBuf, String)>> {
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
    write_unless_unchanged(path, label, &updated, action, report)
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
