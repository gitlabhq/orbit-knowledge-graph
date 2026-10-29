pub(super) mod agent_plugins;
mod hooks;
mod instructions;
pub(super) mod json;
mod mcp;
mod skill;

use std::collections::BTreeSet;
use std::fmt::Display;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::plan::Selection;
use super::spec::{self, Agent};
use super::{Component, Target};

pub(super) use skill::records_full_skill;

pub(super) trait Installer {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>>;

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()>;

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()>;

    fn is_installed(&self, agent: Agent, target: &Target) -> bool;
}

pub(super) fn installed_agents(components: &BTreeSet<Component>, target: &Target) -> Vec<Agent> {
    spec::agents()
        .filter(|agent| {
            components
                .iter()
                .any(|component| installer_for(*component).is_installed(*agent, target))
        })
        .collect()
}

fn file_mentions(path: &Path, marker: &str) -> bool {
    std::fs::read_to_string(path).is_ok_and(|text| text.contains(marker))
}

pub(super) fn installer_for(component: Component) -> &'static dyn Installer {
    match component {
        Component::Instructions => &instructions::Instructions,
        Component::Hooks => &hooks::Hooks,
        Component::Skill => &skill::WITHOUT_FILES,
        Component::Mcp => &mcp::McpServer,
        Component::AgentPlugins => &agent_plugins::AgentPlugins,
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
    pub(super) fn start_group(&mut self, title: impl Into<String>) {
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
        report.start_group(component.label());
        let installer: &dyn Installer = match component {
            Component::Skill => &skill::Skill {
                request: &selection.skill,
            },
            other => installer_for(*other),
        };
        installer.install(&selection.agents, target, report)?;
    }
    if spec::launcher() == spec::GLAB_LAUNCHER {
        report.start_group("glab");
        ensure_glab_auto_run(report);
    }
    Ok(())
}

pub(super) fn remove(selection: &Selection, target: &Target, report: &mut Report) -> Result<()> {
    for component in &selection.components {
        report.start_group(component.label());
        installer_for(*component).remove(&selection.agents, target, report)?;
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

fn write_file(path: &Path, contents: impl AsRef<[u8]>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
}

fn write_unless_unchanged(
    path: &Path,
    label: &str,
    contents: &str,
    action: &str,
    report: &mut Report,
) -> Result<()> {
    if std::fs::read(path).is_ok_and(|current| current == contents.as_bytes()) {
        report.note(label, "unchanged");
        return Ok(());
    }
    write_file(path, contents)?;
    report.note(label, action);
    Ok(())
}

fn backup_once(path: &Path, label: &str, report: &mut Report) -> Result<()> {
    let backup = backup_path(path);
    let written_this_run = report.outcomes.iter().any(|outcome| outcome.label == label);
    if written_this_run || !path.exists() {
        return Ok(());
    }
    match std::fs::symlink_metadata(&backup) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            anyhow::bail!(
                "{} is a symlink; remove it and run setup again",
                backup.display()
            );
        }
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(e).with_context(|| format!("failed to inspect {}", backup.display()));
        }
    }
    match copy_new(path, &backup) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("failed to back up {}", path.display())),
    }
    report.note(label, format!("backup at {label}.orbit-backup"));
    Ok(())
}

/// Copies `source` to `destination` only if nothing exists there, without
/// following a link at `destination`. A partial copy is removed on failure.
fn copy_new(source: &Path, destination: &Path) -> std::io::Result<()> {
    let mut input = std::fs::File::open(source)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.mode(input.metadata()?.permissions().mode());
    }
    let mut output = options.open(destination)?;
    if let Err(e) = std::io::copy(&mut input, &mut output) {
        drop(output);
        let _ = std::fs::remove_file(destination);
        return Err(e);
    }
    Ok(())
}

fn remove_file_and_empty_parents(path: &Path, target: &Target) -> Result<()> {
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

fn drop_backup_when_restored(path: &Path, label: &str, report: &mut Report) -> Result<()> {
    let backup = backup_path(path);
    let restored = match (std::fs::read(path), std::fs::read(&backup)) {
        (Ok(current), Ok(original)) => current == original,
        _ => false,
    };
    if restored {
        std::fs::remove_file(&backup)
            .with_context(|| format!("failed to remove {}", backup.display()))?;
        report.note(label, "backup removed (file is back to its original)");
    }
    Ok(())
}

fn backup_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".orbit-backup");
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    #[test]
    fn copy_new_refuses_a_dangling_link_without_creating_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let link = dir.path().join("link");
        let outside = dir.path().join("outside");
        std::fs::write(&source, "data").unwrap();
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        let error = copy_new(&source, &link).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert!(!outside.exists());
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    }

    #[test]
    fn copy_new_keeps_existing_files_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        std::fs::write(&source, "new").unwrap();
        std::fs::write(&destination, "old").unwrap();

        let error = copy_new(&source, &destination).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read_to_string(&destination).unwrap(), "old");
    }

    #[cfg(unix)]
    #[test]
    fn copy_new_keeps_contents_and_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        std::fs::write(&source, "data").unwrap();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o750)).unwrap();

        copy_new(&source, &destination).unwrap();

        assert_eq!(std::fs::read_to_string(&destination).unwrap(), "data");
        assert_eq!(mode_of(&destination), 0o750);
    }

    #[test]
    fn copy_new_leaves_no_partial_file_when_the_copy_fails() {
        let dir = tempfile::tempdir().unwrap();
        let unreadable_source = dir.path().join("source-dir");
        let destination = dir.path().join("destination");
        std::fs::create_dir(&unreadable_source).unwrap();

        assert!(copy_new(&unreadable_source, &destination).is_err());

        assert!(destination.symlink_metadata().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn backup_once_refuses_a_symlinked_backup_path() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("CLAUDE.md");
        let outside = dir.path().join("outside");
        std::fs::write(&file, "data").unwrap();
        std::os::unix::fs::symlink(&outside, backup_path(&file)).unwrap();
        let mut report = Report::default();

        let error = backup_once(&file, "CLAUDE.md", &mut report).unwrap_err();

        assert!(error.to_string().contains("remove it"), "{error}");
        assert!(!outside.exists());
        assert!(report.outcomes.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn backup_once_skips_a_link_to_an_existing_file_with_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("CLAUDE.md");
        let existing = dir.path().join("existing");
        std::fs::write(&file, "data").unwrap();
        std::fs::write(&existing, "keep").unwrap();
        std::os::unix::fs::symlink(&existing, backup_path(&file)).unwrap();

        assert!(backup_once(&file, "CLAUDE.md", &mut Report::default()).is_err());

        assert_eq!(std::fs::read_to_string(&existing).unwrap(), "keep");
    }

    #[test]
    fn backup_once_keeps_an_existing_backup() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("CLAUDE.md");
        std::fs::write(&file, "new").unwrap();
        std::fs::write(backup_path(&file), "old").unwrap();
        let mut report = Report::default();

        backup_once(&file, "CLAUDE.md", &mut report).unwrap();

        assert_eq!(std::fs::read_to_string(backup_path(&file)).unwrap(), "old");
        assert!(report.outcomes.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn backup_once_creates_a_backup_with_the_source_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("CLAUDE.md");
        std::fs::write(&file, "data").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o750)).unwrap();
        let mut report = Report::default();

        backup_once(&file, "CLAUDE.md", &mut report).unwrap();

        let backup = backup_path(&file);
        assert_eq!(std::fs::read_to_string(&backup).unwrap(), "data");
        assert_eq!(mode_of(&backup), 0o750);
        assert_eq!(report.outcomes.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn install_refuses_a_symlinked_backup_path_and_writes_nothing_through_it() {
        let dir = tempfile::tempdir().unwrap();
        let target = Target::project(Some(dir.path().to_path_buf())).unwrap();
        let agent = spec::agents().next().unwrap();
        let (file, _) = target.resolve(&agent.instruction_file).unwrap();
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "existing notes\n").unwrap();
        let outside = dir.path().join("outside");
        std::os::unix::fs::symlink(&outside, backup_path(&file)).unwrap();
        let mut report = Report::default();

        let result = installer_for(Component::Instructions).install(&[agent], &target, &mut report);

        assert!(result.is_err());
        assert!(!outside.exists());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "existing notes\n");
    }
}
