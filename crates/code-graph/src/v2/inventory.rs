//! Turning a (canonical) repository file inventory into work: select and group
//! the files to parse, and build the structural file/directory graph. The stream
//! produces and canonicalizes the inventory; this consumes it.

use std::path::Path;

use gkg_utils::fs_stream::{Decision, FileInventoryEntry};
use rustc_hash::FxHashMap;

use crate::v2::config::{Language, LanguageFamily, detect_language_from_path};
use crate::v2::error::FileReason;
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
        // The stream already settled parse candidacy (parsable, loaded, deduped);
        // here we only group them by language.
        if entry.decision != Decision::Parse {
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

pub fn build_file_inventory_graph(
    root: &Path,
    inventory: &[FileInventoryEntry],
    parsed_file_languages: &FxHashMap<String, Language>,
    reasons: &FxHashMap<&str, FileReason>,
) -> CodeGraph {
    let mut graph = CodeGraph::new_with_root(root.to_string_lossy().to_string());
    for entry in inventory {
        let language = parsed_file_languages.get(&entry.path).copied();
        let reason = reasons
            .get(entry.path.as_str())
            .copied()
            .unwrap_or_default();
        graph.add_unparsed_file(&entry.path, language, entry.size, reason);
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
            decision: Decision::Parse,
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
}
