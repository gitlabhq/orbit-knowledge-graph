use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::json;
use super::{
    Installer, Report, backup_once, backup_path, file_mentions, remove_empty_parents, write_file,
};
use crate::commands::setup::spec::{self, Agent};
use crate::commands::setup::{SkillKind, SkillRequest, Target};
use crate::skill::{INSTALL_DIR_NAME, LEGACY_INSTALL_DIR_NAME, embedded_files};

const MANIFEST: &str = "SKILL.md";
/// Ownership record next to the skill directory, so the directory itself stays what the
/// agents (or `glab skills`) expect.
const MARKER_FILE: &str = ".orbit-managed";

static NO_REQUEST: SkillRequest = SkillRequest {
    kind: None,
    full_tree: None,
};

pub(super) static WITHOUT_FILES: Skill<'static> = Skill {
    request: &NO_REQUEST,
};

pub(super) struct Skill<'a> {
    pub(super) request: &'a SkillRequest,
}

#[derive(Serialize, Deserialize)]
struct Marker {
    kind: SkillKind,
    agents: BTreeSet<String>,
    /// Digest of the tree setup wrote, to tell it from a directory put there since.
    #[serde(default)]
    digest: String,
}

fn tree_digest(tree: &BTreeMap<String, Vec<u8>>) -> String {
    let mut hasher = Sha256::new();
    for (path, contents) in tree {
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update((contents.len() as u64).to_le_bytes());
        hasher.update(contents);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Whether `path` still holds what setup wrote, as opposed to a directory created there after
/// the record was left behind. A lone wrapper `SKILL.md` counts too when it only differs in its version, which is what
/// `glab skills update` writes from another release.
fn is_ours(path: &Path) -> bool {
    let Some(marker) = read_marker(path) else {
        return false;
    };
    let Some(current) = read_tree(path) else {
        return false;
    };
    tree_digest(&current) == marker.digest || is_wrapper_tree(&current)
}

/// A lone `SKILL.md` equal to this release's wrapper for either launcher, apart from its
/// `version:` line. That is what `glab skills update` writes when glab ships another release.
fn is_wrapper_tree(tree: &BTreeMap<String, Vec<u8>>) -> bool {
    let [(path, contents)] = tree.iter().collect::<Vec<_>>()[..] else {
        return false;
    };
    if path != MANIFEST {
        return false;
    }
    let current = mask_version(&String::from_utf8_lossy(contents));
    [spec::DIRECT_LAUNCHER, spec::GLAB_LAUNCHER]
        .into_iter()
        .filter_map(|launcher| crate::skill::wrapper_tree_for(launcher).ok())
        .filter_map(|wrapper| wrapper.get(MANIFEST).cloned())
        .any(|manifest| mask_version(&String::from_utf8_lossy(&manifest)) == current)
}

fn mask_version(manifest: &str) -> String {
    let mut delimiters = 0;
    manifest
        .split('\n')
        .map(|line| {
            if line == "---" {
                delimiters += 1;
            }
            match delimiters == 1 && line.starts_with("version:") {
                true => "version:",
                false => line,
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn marker_path(skill_root: &Path) -> PathBuf {
    skill_root.parent().unwrap_or(skill_root).join(MARKER_FILE)
}

fn read_marker(skill_root: &Path) -> Option<Marker> {
    let contents = std::fs::read_to_string(marker_path(skill_root)).ok()?;
    Some(serde_json::from_str(&contents).unwrap_or(Marker {
        kind: SkillKind::Wrapper,
        agents: BTreeSet::new(),
        digest: String::new(),
    }))
}

fn write_marker(skill_root: &Path, marker: &Marker) -> Result<()> {
    let contents = serde_json::to_string(marker).context("failed to encode the skill marker")?;
    write_file(&marker_path(skill_root), format!("{contents}\n"))
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
            migrate_legacy(&skill, target, report)?;
            let kind = self.kind_for(&skill);
            if kind == SkillKind::Wrapper && has_full_only_owner(&skill) {
                eprintln!(
                    "warning: {} gets the wrapper, which an agent using it (such as Duo) cannot run when it loads a skill",
                    skill.label
                );
            }
            let tree = self.tree_for(kind)?;
            write_skill_files(
                &skill.root,
                &skill.label,
                kind,
                &skill.agents,
                &tree,
                report,
            )?;
            if let Some(link) = &skill.link {
                link_skill_dir(link, &skill, kind, &tree, report)?;
            }
        }
        for agent in agents {
            install_permissions(*agent, target, report)?;
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for skill in skill_targets(agents, target)? {
            migrate_legacy(&skill, target, report)?;
            if let Some(link) = &skill.link {
                unlink_skill_dir(link, &skill.link_agents, target, report)?;
            }
            release_skill_files(&skill.root, &skill.label, &skill.agents, target, report)?;
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

/// Removes what earlier releases installed as `orbit-cli`, directory and Claude link alike.
fn migrate_legacy(skill: &SkillTarget, target: &Target, report: &mut Report) -> Result<()> {
    if let Some(legacy_link) = &skill.legacy_link {
        remove_legacy_link(
            legacy_link,
            &skill.legacy_link_label,
            &skill.legacy_root,
            target,
            report,
        )?;
        // Where symlinks fail, the earlier release wrote a real copy instead.
        remove_legacy(legacy_link, &skill.legacy_link_label, target, report)?;
    }
    remove_legacy(&skill.legacy_root, &skill.legacy_label, target, report)
}

/// Whether an agent selected now, or recorded for this directory, needs the full tree.
fn has_full_only_owner(skill: &SkillTarget) -> bool {
    let recorded = read_marker(&skill.root).map(|marker| marker.agents);
    skill.requires_full
        || spec::agents().any(|agent| {
            recorded
                .as_ref()
                .is_some_and(|owners| owners.contains(&agent.name))
                && agent.skills.as_ref().is_some_and(|dirs| dirs.requires_full)
        })
}

/// Whether any of these agents' skill directories was last installed as the full tree.
pub(in crate::commands::setup) fn records_full_skill(agents: &[Agent], target: &Target) -> bool {
    skill_targets(agents, target).is_ok_and(|targets| {
        targets.iter().any(|skill| {
            read_marker(&skill.root).is_some_and(|marker| marker.kind == SkillKind::Full)
        })
    })
}

impl Skill<'_> {
    /// An explicit choice wins. Otherwise a sharing agent that needs the full tree decides, and
    /// the last install keeps its kind so a rerun for another agent does not swap it.
    fn kind_for(&self, skill: &SkillTarget) -> SkillKind {
        if let Some(requested) = self.request.kind {
            return requested;
        }
        if skill.requires_full {
            return SkillKind::Full;
        }
        read_marker(&skill.root).map_or(SkillKind::Wrapper, |marker| marker.kind)
    }

    fn tree_for(&self, kind: SkillKind) -> Result<BTreeMap<String, Vec<u8>>> {
        match (kind, &self.request.full_tree) {
            (SkillKind::Wrapper, _) => crate::skill::wrapper_tree(),
            (SkillKind::Full, Some(tree)) => Ok(tree.clone()),
            (SkillKind::Full, None) => crate::skill::embedded_full_tree(),
        }
    }
}

struct SkillTarget {
    root: PathBuf,
    label: String,
    agents: BTreeSet<String>,
    requires_full: bool,
    link_agents: BTreeSet<String>,
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
    for (agent_name, dirs) in agents
        .iter()
        .filter_map(|agent| Some((agent.name.clone(), agent.skills.as_ref()?)))
    {
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

        let link_agents: BTreeSet<String> = link.iter().map(|_| agent_name.clone()).collect();
        match targets.iter_mut().find(|existing| existing.root == root) {
            Some(existing) => {
                existing.agents.insert(agent_name);
                existing.requires_full |= dirs.requires_full;
                existing.link_agents.extend(link_agents);
                if existing.link.is_none() {
                    existing.link = link;
                    existing.legacy_link = legacy_link;
                    existing.legacy_link_label = legacy_link_label;
                }
            }
            None => targets.push(SkillTarget {
                root,
                label: format!("{dir_label}/{INSTALL_DIR_NAME}"),
                agents: BTreeSet::from([agent_name]),
                requires_full: dirs.requires_full,
                link_agents,
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
    kind: SkillKind,
    agents: &BTreeSet<String>,
    tree: &BTreeMap<String, Vec<u8>>,
    report: &mut Report,
) -> Result<()> {
    let mut owners = read_marker(skill_root)
        .map(|marker| marker.agents)
        .unwrap_or_default();
    owners.extend(agents.iter().cloned());
    let marker = Marker {
        kind,
        agents: owners,
        digest: tree_digest(tree),
    };
    // A tree identical to ours (for example one `glab skills install` wrote) is adopted as is.
    if read_tree(skill_root).is_some_and(|current| current == *tree) {
        write_marker(skill_root, &marker)?;
        report.note(label, "unchanged");
        return Ok(());
    }
    make_room(skill_root, label, report)?;
    for (relative, contents) in tree {
        write_file(&skill_root.join(relative), contents)?;
    }
    write_marker(skill_root, &marker)?;
    report.note(label, "skill installed");
    Ok(())
}

/// Clears `path` for a new install. What `orbit setup` wrote, unchanged, is replaced; anything else is
/// renamed to a sibling whose `SKILL.md` no longer loads, so the user's files survive.
fn make_room(path: &Path, label: &str, report: &mut Report) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    if !metadata.is_dir() || is_ours(path) {
        remove_path(path)?;
        let _ = std::fs::remove_file(marker_path(path));
        return Ok(());
    }
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let first_backup = path.with_file_name(format!("{name}.orbit-backup"));
    let mut backup = first_backup.clone();
    for attempt in 1.. {
        if !backup.exists() {
            break;
        }
        backup = path.with_file_name(format!("{name}.orbit-backup.{attempt}"));
    }
    std::fs::rename(path, &backup)
        .with_context(|| format!("failed to move {} aside", path.display()))?;
    let manifest = backup.join(MANIFEST);
    if manifest.exists() {
        std::fs::rename(&manifest, backup.join(format!("{MANIFEST}.orbit-backup")))
            .with_context(|| format!("failed to rename {}", manifest.display()))?;
    }
    let kept = backup.file_name().unwrap_or_default().to_string_lossy();
    report.note(label, format!("existing skill kept as {kept}"));
    Ok(())
}

fn read_tree(root: &Path) -> Option<BTreeMap<String, Vec<u8>>> {
    fn collect(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) -> Option<()> {
        for entry in std::fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            if path.file_name().is_some_and(|name| name == ".DS_Store") {
                continue;
            }
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
    link: &SkillLink,
    skill: &SkillTarget,
    kind: SkillKind,
    tree: &BTreeMap<String, Vec<u8>>,
    report: &mut Report,
) -> Result<()> {
    let link_target = relative_path_from_link(&link.path, &skill.root);
    if std::fs::read_link(&link.path).is_ok_and(|current| current == link_target) {
        report.note(&link.label, "already linked");
        return Ok(());
    }
    make_room(&link.path, &link.label, report)?;
    if let Some(parent) = link.path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    match symlink_dir(&link_target, &link.path) {
        Ok(()) => {
            report.note(&link.label, format!("linked to {}", skill.label));
            Ok(())
        }
        Err(_) => write_skill_files(
            &link.path,
            &link.label,
            kind,
            &skill.link_agents,
            tree,
            report,
        ),
    }
}

/// Drops `agents` from the owners of the skill and deletes it once nobody is left, so removing
/// one agent keeps the skill the others still load.
fn release_skill_files(
    skill_root: &Path,
    label: &str,
    agents: &BTreeSet<String>,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let exists = std::fs::symlink_metadata(skill_root).is_ok();
    let Some(mut marker) = read_marker(skill_root) else {
        if exists {
            report.note(
                label,
                "kept (not installed by orbit setup; delete it by hand)",
            );
        }
        return Ok(());
    };
    marker.agents.retain(|owner| !agents.contains(owner));
    if exists && !is_ours(skill_root) {
        // Leave the changed directory unrecorded once nobody uses it, so a later setup backs it up.
        match marker.agents.is_empty() {
            true => {
                let _ = std::fs::remove_file(marker_path(skill_root));
            }
            false => write_marker(skill_root, &marker)?,
        }
        report.note(
            label,
            "kept (changed since orbit setup wrote it; delete it by hand)",
        );
        return Ok(());
    }
    if !marker.agents.is_empty() {
        write_marker(skill_root, &marker)?;
        let remaining: Vec<&str> = marker.agents.iter().map(String::as_str).collect();
        if exists {
            report.note(label, format!("kept for {}", remaining.join(", ")));
        }
        return Ok(());
    }
    remove_path(skill_root)?;
    let _ = std::fs::remove_file(marker_path(skill_root));
    remove_empty_parents(skill_root, &target.root()?);
    if exists {
        report.note(label, "skill removed");
    }
    Ok(())
}

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

/// An unmarked directory loses only files that still match this release; user edits stay.
fn remove_legacy(path: &Path, label: &str, target: &Target, report: &mut Report) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.is_symlink() || !path.is_dir() {
        return Ok(());
    }
    if !has_legacy_manifest(path) {
        return Ok(());
    }
    retire_legacy_manifest(path, label, report)?;
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
            "kept (has files that differ from this release, edited or from an older one; delete it by hand)",
        );
        return Ok(());
    }
    remove_empty_parents(path, &target.root()?);
    report.note(label, "legacy skill removed");
    Ok(())
}

/// A differing manifest must stop being a skill, or the agent lists `orbit-cli` next to `orbit`.
/// The caller removes an unedited one with the other shipped files.
fn retire_legacy_manifest(dir: &Path, label: &str, report: &mut Report) -> Result<()> {
    let manifest = dir.join(MANIFEST);
    let shipped = embedded_files()
        .into_iter()
        .find(|(relative, _)| relative == MANIFEST)
        .map(|(_, contents)| contents);
    let Ok(current) = std::fs::read(&manifest) else {
        return Ok(());
    };
    if shipped.is_some_and(|shipped| shipped == current) {
        return Ok(());
    }
    let first_backup = backup_path(&manifest);
    if std::fs::read(&first_backup).is_ok_and(|backup| backup == current) {
        return std::fs::remove_file(&manifest)
            .with_context(|| format!("failed to remove {}", manifest.display()));
    }
    // Never overwrite an earlier backup; it may hold the user's own edit.
    let mut backup = first_backup.clone();
    for attempt in 1.. {
        if !backup.exists() {
            break;
        }
        backup = first_backup.with_extension(format!("orbit-backup.{attempt}"));
    }
    std::fs::rename(&manifest, &backup)
        .with_context(|| format!("failed to rename {}", manifest.display()))?;
    let name = backup.file_name().unwrap_or_default().to_string_lossy();
    report.note(
        label,
        format!(
            "{MANIFEST} differs from this release (edited or from an older one); kept as {name}"
        ),
    );
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
    link: &SkillLink,
    agents: &BTreeSet<String>,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(&link.path) else {
        return Ok(());
    };
    if !metadata.is_symlink() {
        return release_skill_files(&link.path, &link.label, agents, target, report);
    }
    remove_symlink(&link.path)
        .with_context(|| format!("failed to remove {}", link.path.display()))?;
    remove_empty_parents(&link.path, &target.root()?);
    report.note(&link.label, "link removed");
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
