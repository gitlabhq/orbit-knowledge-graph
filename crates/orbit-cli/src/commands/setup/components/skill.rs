use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::json;
use super::{Installer, Report, backup_once, file_mentions, remove_empty_parents, write_file};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent};
use crate::skill::{INSTALL_DIR_NAME, LEGACY_INSTALL_DIR_NAME, embedded_files};

/// Written into every directory this component installs. Content cannot identify it, because the
/// full skill depends on the instance.
const MARKER_FILE: &str = ".orbit-managed";
const MARKER_CONTENTS: &str =
    "Installed by `orbit setup`; `orbit uninstall` removes this directory.\n";

static NO_FILES: BTreeMap<String, Vec<u8>> = BTreeMap::new();

/// For plans, removal, and detection, which write no files.
pub(super) static WITHOUT_FILES: Skill<'static> = Skill { tree: &NO_FILES };

pub(super) struct Skill<'a> {
    pub(super) tree: &'a BTreeMap<String, Vec<u8>>,
}

impl Installer for Skill<'_> {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        let mut paths: Vec<String> = Vec::new();
        for skill in skill_targets(&[agent], target)? {
            paths.push(skill.label);
            paths.extend(skill.link.map(|link| format!("{} (link)", link.label)));
        }
        if let Some(permissions) = agent
            .skills
            .as_ref()
            .and_then(|dirs| dirs.permissions.as_ref())
        {
            paths.push(target.resolve(&permissions.file)?.1);
        }
        Ok(paths)
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for skill in skill_targets(agents, target)? {
            if let Some(legacy_link) = &skill.legacy_link {
                remove_legacy_link(
                    legacy_link,
                    &skill.legacy_link_label,
                    &skill.legacy_root,
                    target,
                    report,
                )?;
            }
            remove_legacy(&skill.legacy_root, &skill.legacy_label, target, report)?;
            write_skill_files(&skill.root, &skill.label, self.tree, report)?;
            if let Some(link) = &skill.link {
                link_skill_dir(
                    &link.path,
                    &skill.root,
                    &skill.label,
                    &link.label,
                    self.tree,
                    report,
                )?;
            }
        }
        for agent in agents {
            install_permissions(*agent, target, report)?;
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for skill in skill_targets(agents, target)? {
            if let Some(legacy_link) = &skill.legacy_link {
                remove_legacy_link(
                    legacy_link,
                    &skill.legacy_link_label,
                    &skill.legacy_root,
                    target,
                    report,
                )?;
            }
            remove_legacy(&skill.legacy_root, &skill.legacy_label, target, report)?;
            if let Some(link) = &skill.link {
                unlink_skill_dir(&link.path, &link.label, target, report)?;
            }
            remove_skill_files(&skill.root, &skill.label, target, report)?;
        }
        for agent in agents {
            remove_permissions(*agent, target, report)?;
        }
        Ok(())
    }

    fn is_installed(&self, agent: Agent, target: &Target) -> bool {
        skill_targets(&[agent], target).is_ok_and(|targets| {
            targets.iter().any(|skill| {
                skill.root.join("SKILL.md").exists() || skill.legacy_root.join("SKILL.md").exists()
            })
        })
    }
}

struct SkillTarget {
    root: PathBuf,
    label: String,
    link: Option<SkillLink>,
    legacy_root: PathBuf,
    legacy_label: String,
    legacy_link: Option<PathBuf>,
    legacy_link_label: String,
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
        let (link, legacy_link, legacy_link_label) = match &dirs.link {
            Some(scoped) => {
                let (link_dir, link_label) = target.resolve(scoped)?;
                (
                    Some(SkillLink {
                        path: link_dir.join(INSTALL_DIR_NAME),
                        label: format!("{link_label}/{INSTALL_DIR_NAME}"),
                    }),
                    Some(link_dir.join(LEGACY_INSTALL_DIR_NAME)),
                    format!("{link_label}/{LEGACY_INSTALL_DIR_NAME}"),
                )
            }
            None => (None, None, String::new()),
        };

        match targets.iter_mut().find(|existing| existing.root == root) {
            Some(existing) => {
                if existing.link.is_none() {
                    existing.link = link;
                    existing.legacy_link = legacy_link;
                    existing.legacy_link_label = legacy_link_label;
                }
            }
            None => targets.push(SkillTarget {
                root,
                label: format!("{dir_label}/{INSTALL_DIR_NAME}"),
                link,
                legacy_root: dir.join(LEGACY_INSTALL_DIR_NAME),
                legacy_label: format!("{dir_label}/{LEGACY_INSTALL_DIR_NAME}"),
                legacy_link,
                legacy_link_label,
            }),
        }
    }
    Ok(targets)
}

fn install_permissions(agent: Agent, target: &Target, report: &mut Report) -> Result<()> {
    let Some(permissions) = agent
        .skills
        .as_ref()
        .and_then(|dirs| dirs.permissions.as_ref())
    else {
        return Ok(());
    };
    let (path, label) = target.resolve(&permissions.file)?;
    let mut root = json::read_object(&path)?;
    let mut changed = false;
    for rule in &permissions.rules {
        let rule = rule.replace("{{orbit}}", spec::launcher());
        changed |= json::append_unique(&mut root, &ALLOW_PATH.map(String::from), &rule)
            .with_context(|| format!("failed to update {}", path.display()))?;
    }
    // Hooks share this file and back it up before they write it.
    let hooks_wrote_it = agent
        .json_merges
        .iter()
        .any(|merge| file_mentions(&path, &merge.marker));
    if changed {
        if !hooks_wrote_it {
            backup_once(&path, &label, report)?;
        }
        json::write_object(&path, &root)?;
        report.note(&label, "skill commands allowed");
    }
    Ok(())
}

fn remove_permissions(agent: Agent, target: &Target, report: &mut Report) -> Result<()> {
    let Some(permissions) = agent
        .skills
        .as_ref()
        .and_then(|dirs| dirs.permissions.as_ref())
    else {
        return Ok(());
    };
    let (path, label) = target.resolve(&permissions.file)?;
    if !path.exists() {
        return Ok(());
    }
    let mut root = json::read_object(&path)?;
    let mut changed = false;
    // Either distribution may have written the rules, whichever one runs the uninstall.
    for launcher in [spec::DIRECT_LAUNCHER, spec::GLAB_LAUNCHER] {
        for rule in &permissions.rules {
            let rule = rule.replace("{{orbit}}", launcher);
            changed |= json::remove_value(&mut root, &ALLOW_PATH.map(String::from), &rule);
        }
    }
    if changed {
        json::write_or_delete_when_empty(&path, &root, target, &label, report)?;
    }
    Ok(())
}

const ALLOW_PATH: [&str; 2] = ["permissions", "allow"];

fn write_skill_files(
    skill_root: &Path,
    label: &str,
    tree: &BTreeMap<String, Vec<u8>>,
    report: &mut Report,
) -> Result<()> {
    let mut files = tree.clone();
    files.insert(MARKER_FILE.to_string(), MARKER_CONTENTS.as_bytes().to_vec());
    if read_tree(skill_root).is_some_and(|current| current == files) {
        report.note(label, "unchanged");
        return Ok(());
    }
    // Replace the directory so files of a previous install (another skill kind or source) go away.
    remove_path(skill_root)?;
    for (relative, contents) in files {
        write_file(&skill_root.join(relative), contents)?;
    }
    report.note(label, "skill installed");
    Ok(())
}

fn read_tree(root: &Path) -> Option<BTreeMap<String, Vec<u8>>> {
    fn collect(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) -> Option<()> {
        for entry in std::fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            if path.is_dir() {
                collect(root, &path, files)?;
            } else {
                let relative = path.strip_prefix(root).ok()?;
                files.insert(
                    relative.to_string_lossy().replace('\\', "/"),
                    std::fs::read(&path).ok()?,
                );
            }
        }
        Some(())
    }
    let mut files = BTreeMap::new();
    collect(root, root, &mut files)?;
    Some(files)
}

fn remove_path(path: &Path) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    let removed = match metadata.is_dir() {
        true => std::fs::remove_dir_all(path),
        false => remove_symlink(path),
    };
    removed.with_context(|| format!("failed to remove {}", path.display()))
}

fn link_skill_dir(
    link_path: &Path,
    skill_root: &Path,
    skill_label: &str,
    label: &str,
    tree: &BTreeMap<String, Vec<u8>>,
    report: &mut Report,
) -> Result<()> {
    let link_target = relative_path_from_link(link_path, skill_root);
    if std::fs::read_link(link_path).is_ok_and(|current| current == link_target) {
        report.note(label, "already linked");
        return Ok(());
    }
    // Anything else here (a foreign skill, a stale copy, another link) would shadow the install.
    remove_path(link_path)?;
    if let Some(parent) = link_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    match symlink_dir(&link_target, link_path) {
        Ok(()) => {
            report.note(label, format!("linked to {skill_label}"));
            Ok(())
        }
        Err(_) => write_skill_files(link_path, label, tree, report),
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
    if !skill_root.join(MARKER_FILE).exists() {
        report.note(
            label,
            "kept (not installed by orbit setup; delete it by hand)",
        );
        return Ok(());
    }
    remove_path(skill_root)?;
    remove_empty_parents(skill_root, &target.root()?);
    report.note(label, "skill removed");
    Ok(())
}

/// Removes the link an earlier release made to the `orbit-cli` skill. Other links are the user's.
fn remove_legacy_link(
    link: &Path,
    label: &str,
    legacy_root: &Path,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let ours = std::fs::read_link(link)
        .is_ok_and(|current| current == relative_path_from_link(link, legacy_root));
    if !ours {
        return Ok(());
    }
    remove_path(link)?;
    remove_empty_parents(link, &target.root()?);
    report.note(label, "legacy skill removed");
    Ok(())
}

/// Removes what an earlier release installed as `orbit-cli`. A marked directory goes whole. An
/// unmarked one with the old manifest name loses only the files that still match what this
/// release ships; anything the user added or edited stays.
fn remove_legacy(path: &Path, label: &str, target: &Target, report: &mut Report) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.is_symlink() || !path.is_dir() {
        return Ok(());
    }
    if path.join(MARKER_FILE).exists() {
        remove_path(path)?;
    } else if has_legacy_manifest(path) {
        let mut directories = BTreeSet::new();
        for (relative, contents) in embedded_files() {
            let file = path.join(&relative);
            let unedited = std::fs::read(&file).is_ok_and(|current| current == contents);
            if unedited && std::fs::remove_file(&file).is_ok() {
                directories.extend(
                    file.ancestors()
                        .skip(1)
                        .take_while(|directory| directory.starts_with(path))
                        .map(Path::to_path_buf),
                );
            }
        }
        let mut deepest_first: Vec<PathBuf> = directories.into_iter().collect();
        deepest_first.sort_by_key(|directory| std::cmp::Reverse(directory.components().count()));
        for directory in &deepest_first {
            let _ = std::fs::remove_dir(directory);
        }
        if path.exists() {
            report.note(
                label,
                "kept (has files that changed since install; delete it by hand)",
            );
            return Ok(());
        }
    } else {
        return Ok(());
    }
    remove_empty_parents(path, &target.root()?);
    report.note(label, "legacy skill removed");
    Ok(())
}

fn has_legacy_manifest(dir: &Path) -> bool {
    std::fs::read_to_string(dir.join("SKILL.md")).is_ok_and(|manifest| {
        manifest
            .lines()
            .any(|line| line.trim_end() == format!("name: {LEGACY_INSTALL_DIR_NAME}"))
    })
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
