use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{Installer, Report, remove_empty_parents, write_file};
use crate::commands::setup::Target;
use crate::commands::setup::spec::Agent;
use crate::skill::{INSTALL_DIR_NAME, embedded_files};

pub(super) struct Skill;

impl Installer for Skill {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        let mut paths: Vec<String> = Vec::new();
        for skill in skill_targets(&[agent], target)? {
            paths.push(skill.label);
            paths.extend(skill.link.map(|link| format!("{} (link)", link.label)));
        }
        Ok(paths)
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for skill in skill_targets(agents, target)? {
            write_skill_files(&skill.root, &skill.label, report)?;
            if let Some(link) = &skill.link {
                link_skill_dir(&link.path, &skill.root, &skill.label, &link.label, report)?;
            }
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for skill in skill_targets(agents, target)? {
            if let Some(link) = &skill.link {
                unlink_skill_dir(&link.path, &link.label, target, report)?;
            }
            remove_skill_files(&skill.root, &skill.label, target, report)?;
        }
        Ok(())
    }

    fn is_installed(&self, agent: Agent, target: &Target) -> bool {
        skill_targets(&[agent], target).is_ok_and(|targets| {
            targets
                .iter()
                .any(|skill| skill.root.join("SKILL.md").exists())
        })
    }
}

struct SkillTarget {
    root: PathBuf,
    label: String,
    link: Option<SkillLink>,
}

struct SkillLink {
    path: PathBuf,
    label: String,
}

fn skill_targets(agents: &[Agent], target: &Target) -> Result<Vec<SkillTarget>> {
    let mut targets: Vec<SkillTarget> = Vec::new();
    for dirs in agents.iter().filter_map(|agent| agent.skills.as_ref()) {
        let (dir, dir_label) = target.resolve(&dirs.dir)?;
        let root = dir.join(INSTALL_DIR_NAME);
        let link = match &dirs.link {
            Some(scoped) => {
                let (link_dir, link_label) = target.resolve(scoped)?;
                Some(SkillLink {
                    path: link_dir.join(INSTALL_DIR_NAME),
                    label: format!("{link_label}/{INSTALL_DIR_NAME}"),
                })
            }
            None => None,
        };

        match targets.iter_mut().find(|existing| existing.root == root) {
            Some(existing) => {
                if existing.link.is_none() {
                    existing.link = link;
                }
            }
            None => targets.push(SkillTarget {
                root,
                label: format!("{dir_label}/{INSTALL_DIR_NAME}"),
                link,
            }),
        }
    }
    Ok(targets)
}

fn write_skill_files(skill_root: &Path, label: &str, report: &mut Report) -> Result<()> {
    for (relative, contents) in embedded_files() {
        let destination = skill_root.join(&relative);
        if std::fs::read(&destination).is_ok_and(|current| current == contents) {
            continue;
        }
        write_file(&destination, contents)?;
    }
    report.note(label, "skill installed");
    Ok(())
}

fn link_skill_dir(
    link_path: &Path,
    skill_root: &Path,
    skill_label: &str,
    label: &str,
    report: &mut Report,
) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(link_path) {
        let state = if metadata.is_symlink() {
            "already linked"
        } else {
            "kept (exists)"
        };
        report.note(label, state);
        return Ok(());
    }
    if let Some(parent) = link_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    match symlink_dir(&relative_path_from_link(link_path, skill_root), link_path) {
        Ok(()) => {
            report.note(label, format!("linked to {skill_label}"));
            Ok(())
        }
        Err(_) => write_skill_files(link_path, label, report),
    }
}

fn remove_skill_files(
    skill_root: &Path,
    label: &str,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    if !skill_root.exists() {
        return Ok(());
    }
    let mut kept = false;
    let mut directories: BTreeSet<PathBuf> = BTreeSet::new();
    for (relative, contents) in embedded_files() {
        let installed = skill_root.join(&relative);
        match std::fs::read(&installed) {
            Ok(current) if current == contents => {
                std::fs::remove_file(&installed)
                    .with_context(|| format!("failed to remove {}", installed.display()))?;
            }
            Ok(_) => kept = true,
            Err(_) => {}
        }
        directories.extend(
            installed
                .ancestors()
                .skip(1)
                .take_while(|directory| directory.starts_with(skill_root))
                .map(Path::to_path_buf),
        );
    }

    let mut deepest_first: Vec<PathBuf> = directories.into_iter().collect();
    deepest_first.sort_by_key(|directory| std::cmp::Reverse(directory.components().count()));
    for directory in &deepest_first {
        let _ = std::fs::remove_dir(directory);
    }
    remove_empty_parents(skill_root, &target.root()?);

    if kept {
        report.note(label, "kept (edited since install; delete it by hand)");
    } else {
        report.note(label, "skill removed");
    }
    Ok(())
}

fn unlink_skill_dir(
    link_path: &Path,
    label: &str,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(link_path) else {
        return Ok(());
    };
    if !metadata.is_symlink() {
        return remove_skill_files(link_path, label, target, report);
    }
    remove_symlink(link_path)
        .with_context(|| format!("failed to remove {}", link_path.display()))?;
    remove_empty_parents(link_path, &target.root()?);
    report.note(label, "link removed");
    Ok(())
}

fn relative_path_from_link(link_path: &Path, target: &Path) -> PathBuf {
    let link_dir = link_path.parent().unwrap_or(link_path);
    let shared = link_dir
        .components()
        .zip(target.components())
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for _ in shared..link_dir.components().count() {
        relative.push("..");
    }
    relative.extend(target.components().skip(shared));
    relative
}

#[cfg(unix)]
fn symlink_dir(target: &Path, link_path: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link_path)
}

#[cfg(windows)]
fn symlink_dir(target: &Path, link_path: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link_path)
}

#[cfg(unix)]
fn remove_symlink(link_path: &Path) -> std::io::Result<()> {
    std::fs::remove_file(link_path)
}

#[cfg(windows)]
fn remove_symlink(link_path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir(link_path)
}
