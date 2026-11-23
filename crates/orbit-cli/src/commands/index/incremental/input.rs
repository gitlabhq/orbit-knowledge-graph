use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use code_graph_incremental::language::SupportLang;
use orbit_utils::vfs::{
    Limits, Loading, Options, Source, SourceError, Tag, Vfs,
    sources::{Diff, Directory},
};
use orbit_utils::vfs_policies::CodePolicy;

use crate::workspace;

const MAX_FILE_BYTES: u64 = 5_000_000;
const MEMORY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

pub(super) type Repository = Arc<Vfs<()>>;

pub(super) fn directory(root: &Path) -> Result<Repository> {
    let mut tracked_ignored = workspace::git_tracked_ignored(root)?;
    tracked_ignored.retain(|path| under_real_directories(root, path));
    load(WorkingTree {
        root,
        tracked_ignored,
    })
}

/// The per-path load fails on a parent that is no longer a real directory, and
/// sparse checkout, skip-worktree or assume-unchanged can keep such paths listed.
fn under_real_directories(root: &Path, path: &str) -> bool {
    Path::new(path)
        .ancestors()
        .skip(1)
        .filter(|parent| !parent.as_os_str().is_empty())
        .all(|parent| std::fs::symlink_metadata(root.join(parent)).is_ok_and(|meta| meta.is_dir()))
}

struct WorkingTree<'a> {
    root: &'a Path,
    tracked_ignored: Vec<String>,
}

impl Source for WorkingTree<'_> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        Directory(self.root).fill(into)?;
        Diff {
            root: self.root,
            paths: self.tracked_ignored,
        }
        .fill(into)
    }
}

pub(super) fn paths(root: &Path, paths: Vec<String>) -> Result<Repository> {
    load(Diff { root, paths })
}

fn load(source: impl orbit_utils::vfs::Source) -> Result<Repository> {
    Ok(Arc::new(Vfs::load(
        source,
        CodePolicy,
        Limits {
            file_bytes: Some(MAX_FILE_BYTES),
            resident_bytes: Some(MEMORY_BUDGET_BYTES),
            ..Limits::default()
        },
        Options::default(),
    )?))
}

pub(super) fn split(
    repository: &Vfs<()>,
    owner: Option<SupportLang>,
) -> BTreeMap<&'static str, (SupportLang, Vec<String>)> {
    let mut families: BTreeMap<&'static str, (SupportLang, Vec<String>)> = BTreeMap::new();
    let mut unparsed = Vec::new();
    for file in repository.files() {
        let language = file.keeps().then(|| parsed_language(&file.path)).flatten();
        match language {
            Some(language) => families
                .entry(language.family())
                .or_insert_with(|| (language.family_members()[0], Vec::new()))
                .1
                .push(file.path.to_string()),
            None => unparsed.push(file.path.to_string()),
        }
    }
    let owner = owner.or_else(|| families.values().next().map(|(family, _)| *family));
    for (family, paths) in families.values_mut() {
        if Some(*family) == owner {
            continue;
        }
        let names = manifests(*family);
        paths.extend(
            unparsed
                .iter()
                .filter(|path| names.iter().any(|name| file_name(path) == name))
                .cloned(),
        );
    }
    if let Some(owner) = owner.filter(|_| !unparsed.is_empty()) {
        families
            .entry(owner.family())
            .or_insert_with(|| (owner, Vec::new()))
            .1
            .append(&mut unparsed);
    }
    families
}

fn manifests(family: SupportLang) -> Vec<String> {
    code_graph_incremental::Env::builder(family)
        .build()
        .map(|env| {
            env.project
                .parse_files
                .iter()
                .map(|file| file.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

pub(super) fn readers(
    path: &str,
    owner: Option<SupportLang>,
    families: &[SupportLang],
) -> Vec<SupportLang> {
    if let Some(language) = parsed_language(path) {
        return vec![language.family_members()[0]];
    }
    families
        .iter()
        .copied()
        .filter(|family| {
            Some(*family) == owner
                || manifests(*family)
                    .iter()
                    .any(|name| file_name(path) == name)
        })
        .collect()
}

pub(super) fn split_removed(
    paths: Vec<String>,
    owner: Option<SupportLang>,
    families: &[SupportLang],
) -> BTreeMap<&'static str, Vec<String>> {
    let readers: Vec<_> = families
        .iter()
        .filter(|family| Some(**family) != owner)
        .map(|family| (family.family(), manifests(*family)))
        .collect();
    let mut removed: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    for path in paths {
        if let Some(language) = parsed_language(&path) {
            removed.entry(language.family()).or_default().push(path);
            continue;
        }
        for (family, names) in &readers {
            if names.iter().any(|name| file_name(&path) == name) {
                removed.entry(family).or_default().push(path.clone());
            }
        }
        if let Some(owner) = owner {
            removed.entry(owner.family()).or_default().push(path);
        }
    }
    removed
}

fn parsed_language(path: &str) -> Option<SupportLang> {
    SupportLang::from_path(path).filter(|language| language.has_rules())
}
