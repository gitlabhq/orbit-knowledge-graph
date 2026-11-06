use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{
    Installer, Report, drop_backup_when_restored, file_mentions, mcp, remove_file_and_empty_parents,
};
use crate::commands::setup::Target;
use crate::commands::setup::spec::Agent;

const BLOCK_BEGIN: &str = "<!-- orbit:setup:begin -->";
const BLOCK_END: &str = "<!-- orbit:setup:end -->";

/// Each agent loads the Orbit instructions from its plugin or hook, so setup writes no
/// instruction-file block. Install and uninstall remove the block earlier releases wrote.
pub(super) struct Instructions;

impl Installer for Instructions {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        let mut files: Vec<String> = holding_block(&[agent], target)?
            .into_iter()
            .map(|(_, label)| label)
            .collect();
        files.extend(mcp::retired_label(agent, target)?);
        Ok(files)
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        self.remove(agents, target, report)
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for (path, label) in holding_block(agents, target)? {
            strip_block_from_file(&path, target, &label, report)?;
        }
        mcp::remove_retired(agents, target, report)
    }

    fn is_installed(&self, agent: Agent, target: &Target) -> bool {
        holding_block(&[agent], target).is_ok_and(|files| !files.is_empty())
            || mcp::retired_label(agent, target).is_ok_and(|label| label.is_some())
    }
}

fn holding_block(agents: &[Agent], target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let mut files: Vec<(PathBuf, String)> = Vec::new();
    let mut canonicals: Vec<PathBuf> = Vec::new();
    for agent in agents {
        let (path, label) = target.resolve(&agent.instruction_file)?;
        let canonical = dunce::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if canonicals.contains(&canonical) || !file_mentions(&path, BLOCK_BEGIN) {
            continue;
        }
        canonicals.push(canonical.clone());
        files.push((canonical, label));
    }
    Ok(files)
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

fn strip_block(existing: &str) -> Option<String> {
    let mut text = strip_one_block(existing)?;
    while let Some(fewer) = strip_one_block(&text) {
        text = fewer;
    }
    Some(text)
}

fn strip_one_block(existing: &str) -> Option<String> {
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
