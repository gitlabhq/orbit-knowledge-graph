//! Turning a repository file inventory into work: normalize and dedup the raw
//! entries, select and group the files to parse, and build the structural
//! file/directory graph. The stream produces the inventory; this consumes it.

use std::path::{Component, Path};

use gkg_utils::fs_stream::{Decision, FileInventoryEntry};
use rustc_hash::FxHashMap;

use crate::v2::config::{Language, LanguageFamily, detect_language_from_path};
use crate::v2::linker::CodeGraph;

/// Input to a language pipeline: file path (source read on demand).
pub type FileInput = String;

/// A file paired with the specific [`Language`] that should parse it. Used when
/// a family groups multiple languages into one pipeline invocation (e.g. C and
/// C++ in `CFamily`).
pub struct FamilyFileInput {
    pub language: Language,
    pub path: FileInput,
}

/// Dedup by normalized path and sort. Drops entries whose path escapes the root.
pub fn canonical_file_inventory(
    entries: impl IntoIterator<Item = FileInventoryEntry>,
) -> Vec<FileInventoryEntry> {
    let mut by_path = FxHashMap::default();
    for entry in entries {
        let Some(path) = normalize_inventory_path(&entry.path) else {
            continue;
        };
        by_path.entry(path).or_insert((entry.size, entry.decision));
    }

    let mut entries: Vec<_> = by_path
        .into_iter()
        .map(|(path, (size, decision))| FileInventoryEntry {
            path,
            size,
            decision,
        })
        .collect();
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

/// Normalize a `/`-joined relative path: drop `.` segments, reject anything that
/// climbs out (`..`, root, prefix). `None` if nothing remains.
fn normalize_inventory_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// Select the parse candidates (loaded, language-detected, under `max_files`;
/// `0` = unlimited) and group them by language family. Also returns the path →
/// language map for the structural graph.
pub fn group_parseable_inventory(
    inventory: &[FileInventoryEntry],
    max_files: usize,
) -> (
    FxHashMap<LanguageFamily, Vec<FamilyFileInput>>,
    FxHashMap<String, Language>,
) {
    let mut groups: FxHashMap<LanguageFamily, Vec<FamilyFileInput>> = FxHashMap::default();
    let mut parsed_file_languages = FxHashMap::default();
    let mut accepted_files = 0usize;

    for entry in inventory {
        // Only loaded files are parse candidates; the stream settled the rest as
        // ListOnly.
        if entry.decision != Decision::Keep {
            continue;
        }
        let Some(lang) = detect_language_from_path(&entry.path) else {
            continue;
        };
        if max_files > 0 && accepted_files >= max_files {
            continue;
        }

        accepted_files += 1;
        parsed_file_languages.insert(entry.path.clone(), lang);
        groups
            .entry(lang.family())
            .or_default()
            .push(FamilyFileInput {
                language: lang,
                path: entry.path.clone(),
            });
    }

    (groups, parsed_file_languages)
}

/// Build the structural graph: one node per inventory entry, labeled with the
/// language it was parsed as (if any).
pub fn build_file_inventory_graph(
    root: &Path,
    inventory: &[FileInventoryEntry],
    parsed_file_languages: &FxHashMap<String, Language>,
) -> CodeGraph {
    let mut graph = CodeGraph::new_with_root(root.to_string_lossy().to_string());
    for entry in inventory {
        let language = parsed_file_languages.get(&entry.path).copied();
        graph.add_unparsed_file(&entry.path, language, entry.size);
    }
    graph.drop_construction_indexes();
    graph
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keep(path: &str) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size: 10,
            decision: Decision::Keep,
        }
    }

    fn grouped_count(inventory: &[FileInventoryEntry], max_files: usize) -> usize {
        group_parseable_inventory(inventory, max_files)
            .0
            .values()
            .map(Vec::len)
            .sum()
    }

    #[test]
    fn grouping_respects_max_files() {
        let inventory = [keep("a.java"), keep("b.java"), keep("c.java")];
        assert_eq!(grouped_count(&inventory, 2), 2);
    }

    #[test]
    fn grouping_keeps_only_loaded_files() {
        let inventory = [
            keep("app.js"),
            FileInventoryEntry {
                path: "vendor/jquery.min.js".into(),
                size: 10,
                decision: Decision::ListOnly,
            },
        ];
        assert_eq!(
            grouped_count(&inventory, 0),
            1,
            "only Keep files are parse candidates"
        );
    }

    #[test]
    fn canonical_inventory_dedups_and_normalizes() {
        let inventory =
            canonical_file_inventory([keep("./src/main.rs"), keep("src/main.rs"), keep("a/b.rs")]);
        let paths: Vec<&str> = inventory.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["a/b.rs", "src/main.rs"]);
    }

    #[test]
    fn normalize_rejects_traversal() {
        assert_eq!(
            normalize_inventory_path("src/main.rs").as_deref(),
            Some("src/main.rs")
        );
        assert_eq!(normalize_inventory_path("./a/./b").as_deref(), Some("a/b"));
        assert_eq!(normalize_inventory_path("../escape"), None);
        assert_eq!(normalize_inventory_path("."), None);
    }
}
