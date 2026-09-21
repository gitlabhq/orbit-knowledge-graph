use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{Change, Report};
use crate::commands::setup::Target;
use crate::commands::setup::spec::AssistantSpec;
use crate::skill::{INSTALL_DIR_NAME, embedded_files};

pub(super) struct Skill;

impl Change for Skill {
    fn plan(&self, assistant: &AssistantSpec, target: &Target) -> Result<Option<String>> {
        Ok(targets(&[assistant], target)?
            .into_iter()
            .next()
            .map(|skill| match skill.link {
                Some((_, link_label)) => format!("{} (linked from {link_label})", skill.label),
                None => skill.label,
            }))
    }

    fn install(
        &self,
        assistants: &[&AssistantSpec],
        target: &Target,
        report: &mut Report,
    ) -> Result<()> {
        for skill in targets(assistants, target)? {
            write_files(&skill.root, &skill.label, report)?;
            if let Some((link_path, link_label)) = &skill.link {
                link(link_path, &skill.root, &skill.label, link_label, report)?;
            }
        }
        Ok(())
    }

    fn remove(
        &self,
        assistants: &[&AssistantSpec],
        target: &Target,
        report: &mut Report,
    ) -> Result<()> {
        for skill in targets(assistants, target)? {
            if let Some((link_path, link_label)) = &skill.link {
                unlink(link_path, link_label, report)?;
            }
            remove_files(&skill.root, &skill.label, report)?;
        }
        Ok(())
    }
}

struct SkillTarget {
    root: PathBuf,
    label: String,
    link: Option<(PathBuf, String)>,
}

fn targets(assistants: &[&AssistantSpec], target: &Target) -> Result<Vec<SkillTarget>> {
    let mut targets: Vec<SkillTarget> = Vec::new();
    for dirs in assistants
        .iter()
        .filter_map(|assistant| assistant.skills.as_ref())
    {
        let (dir, dir_label) = target.resolve(&dirs.dir)?;
        let root = dir.join(INSTALL_DIR_NAME);
        let link = dirs
            .link
            .as_ref()
            .map(|link| target.resolve(link))
            .transpose()?
            .map(|(dir, label)| {
                (
                    dir.join(INSTALL_DIR_NAME),
                    format!("{label}/{INSTALL_DIR_NAME}"),
                )
            });

        match targets.iter_mut().find(|existing| existing.root == root) {
            Some(existing) => existing.link = existing.link.take().or(link),
            None => targets.push(SkillTarget {
                root,
                label: format!("{dir_label}/{INSTALL_DIR_NAME}"),
                link,
            }),
        }
    }
    Ok(targets)
}

fn write_files(skill_root: &Path, label: &str, report: &mut Report) -> Result<()> {
    for (relative, contents) in embedded_files() {
        let destination = skill_root.join(&relative);
        if std::fs::read(&destination).is_ok_and(|current| current == contents) {
            continue;
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&destination, contents)
            .with_context(|| format!("failed to write {}", destination.display()))?;
    }
    report.note(label, "skill installed");
    Ok(())
}

fn link(
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

    match symlink_dir(&relative_to(link_path, skill_root), link_path) {
        Ok(()) => {
            report.note(label, format!("linked to {skill_label}"));
            Ok(())
        }
        Err(_) => write_files(link_path, label, report),
    }
}

fn remove_files(skill_root: &Path, label: &str, report: &mut Report) -> Result<()> {
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
    remove_empty_skills_dirs(skill_root);

    if kept {
        report.note(label, "kept (edited since install; delete it by hand)");
    } else {
        report.note(label, "skill removed");
    }
    Ok(())
}

fn unlink(link_path: &Path, label: &str, report: &mut Report) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(link_path) else {
        return Ok(());
    };
    if !metadata.is_symlink() {
        return remove_files(link_path, label, report);
    }
    remove_symlink(link_path)
        .with_context(|| format!("failed to remove {}", link_path.display()))?;
    remove_empty_skills_dirs(link_path);
    report.note(label, "link removed");
    Ok(())
}

fn remove_empty_skills_dirs(skill_root: &Path) {
    for directory in skill_root.ancestors().skip(1).take(2) {
        let _ = std::fs::remove_dir(directory);
    }
}

fn relative_to(link_path: &Path, target: &Path) -> PathBuf {
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
