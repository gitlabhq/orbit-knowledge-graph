use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::skill;

pub(super) fn install(skill_root: &Path, label: &str) -> Result<()> {
    for (relative, contents) in skill::embedded_files() {
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
    println!("  {label}  ->  skill installed");
    Ok(())
}

pub(super) fn link(link_path: &Path, skill_root: &Path, label: &str) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(link_path) {
        let state = if metadata.is_symlink() {
            "already linked"
        } else {
            "kept (exists)"
        };
        println!("  {label}  ->  {state}");
        return Ok(());
    }
    if let Some(parent) = link_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    match symlink_dir(&relative_to(link_path, skill_root), link_path) {
        Ok(()) => {
            println!("  {label}  ->  linked to {}", skill_root.display());
            Ok(())
        }
        Err(_) => install(link_path, label),
    }
}

pub(super) fn remove(skill_root: &Path, label: &str) -> Result<()> {
    if !skill_root.exists() {
        return Ok(());
    }
    let mut kept = false;
    let mut directories: BTreeSet<PathBuf> = BTreeSet::new();
    for (relative, contents) in skill::embedded_files() {
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
        println!("  {label}  ->  kept (edited since install; delete it by hand)");
    } else {
        println!("  {label}  ->  skill removed");
    }
    Ok(())
}

pub(super) fn unlink(link_path: &Path, label: &str) -> Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(link_path) else {
        return Ok(());
    };
    if !metadata.is_symlink() {
        return remove(link_path, label);
    }
    remove_symlink(link_path)
        .with_context(|| format!("failed to remove {}", link_path.display()))?;
    remove_empty_skills_dirs(link_path);
    println!("  {label}  ->  link removed");
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
