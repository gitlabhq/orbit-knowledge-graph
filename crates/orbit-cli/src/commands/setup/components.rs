mod hooks;
mod instructions;
pub(super) mod json;
mod mcp;
mod skill;

use std::fmt::Display;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::plan::Selection;
use super::spec::{self, Agent};
use super::{Component, Target};

pub(super) trait Installer {
    fn plan(&self, assistant: Agent, target: &Target) -> Result<Vec<String>>;

    fn install(&self, assistants: &[Agent], target: &Target, report: &mut Report) -> Result<()>;

    fn remove(&self, assistants: &[Agent], target: &Target, report: &mut Report) -> Result<()>;
}

pub(super) fn for_component(component: Component) -> &'static dyn Installer {
    match component {
        Component::Instructions => &instructions::Instructions,
        Component::Hooks => &hooks::Hooks,
        Component::Skill => &skill::Skill,
        Component::Mcp => &mcp::McpServer,
    }
}

pub(super) struct Outcome {
    pub(super) group: String,
    pub(super) label: String,
    pub(super) action: String,
}

#[derive(Default)]
pub(super) struct Report {
    group: String,
    pub(super) outcomes: Vec<Outcome>,
}

impl Report {
    pub(super) fn group(&mut self, title: impl Into<String>) {
        self.group = title.into();
    }

    pub(super) fn note(&mut self, label: &str, action: impl Display) {
        self.outcomes.push(Outcome {
            group: self.group.clone(),
            label: label.to_string(),
            action: action.to_string(),
        });
    }
}

pub(super) fn install(selection: &Selection, target: &Target, report: &mut Report) -> Result<()> {
    for component in &selection.components {
        report.group(component.label());
        for_component(*component).install(&selection.assistants, target, report)?;
    }
    if spec::launcher() == spec::GLAB_LAUNCHER {
        report.group("glab");
        ensure_glab_auto_run(report);
    }
    Ok(())
}

pub(super) fn remove(selection: &Selection, target: &Target, report: &mut Report) -> Result<()> {
    for component in &selection.components {
        report.group(component.label());
        for_component(*component).remove(&selection.assistants, target, report)?;
    }
    Ok(())
}

fn ensure_glab_auto_run(report: &mut Report) {
    let ok = std::process::Command::new("glab")
        .args(["config", "set", "orbit_cli_auto_run", "true"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if ok {
        report.note(
            "glab config",
            "orbit_cli_auto_run=true (hooks run without prompting)",
        );
    } else {
        report.note(
            "glab config",
            "could not set orbit_cli_auto_run; run `glab config set orbit_cli_auto_run true` \
             so the installed hooks never prompt",
        );
    }
}

fn backup_once(path: &Path, label: &str, report: &mut Report) -> Result<()> {
    let backup = backup_path(path);
    if backup.exists() {
        return Ok(());
    }
    std::fs::copy(path, &backup)
        .with_context(|| format!("failed to back up {}", path.display()))?;
    report.note(label, format!("backup at {label}.orbit-backup"));
    Ok(())
}

fn remove_file(path: &Path, target: &Target) -> Result<()> {
    std::fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    remove_empty_parents(path, &target.root()?);
    Ok(())
}

fn remove_empty_parents(path: &Path, stop: &Path) {
    let parents = path
        .ancestors()
        .skip(1)
        .take_while(|directory| *directory != stop && directory.starts_with(stop));
    for directory in parents {
        if std::fs::remove_dir(directory).is_err() {
            return;
        }
    }
}

fn backup_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".orbit-backup");
    path.with_file_name(name)
}
