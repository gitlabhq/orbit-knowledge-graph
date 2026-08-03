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

/// Count the files [`group_parseable_inventory`] would parse (minus its `max_files` cap), so a
/// repository is sized by parse work rather than raw file count.
pub fn parseable_file_count(inventory: &[FileInventoryEntry]) -> usize {
    inventory
        .iter()
        .filter(|entry| {
            entry.decision == Decision::Parse && detect_language_from_path(&entry.path).is_some()
        })
        .count()
}

pub struct ParseCandidates {
    pub groups: FxHashMap<LanguageFamily, Vec<FamilyFileInput>>,
    pub file_languages: FxHashMap<String, Language>,
    pub shed_over_byte_cap: usize,
}

/// `0` disables either cap. The inventory arrives path-sorted, so the shed set is
/// the same on every run over the same commit.
pub fn group_parseable_inventory(
    inventory: &[FileInventoryEntry],
    max_files: usize,
    max_parse_bytes: u64,
) -> ParseCandidates {
    let mut groups: FxHashMap<LanguageFamily, Vec<FamilyFileInput>> = FxHashMap::default();
    let mut file_languages = FxHashMap::default();
    let mut accepted_files = 0usize;
    let mut accepted_bytes = 0u64;
    let mut shed_over_byte_cap = 0usize;

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
        if max_parse_bytes > 0 && accepted_bytes >= max_parse_bytes {
            shed_over_byte_cap += 1;
            continue;
        }

        accepted_files += 1;
        accepted_bytes += entry.size;
        file_languages.insert(entry.path.clone(), lang);
        groups
            .entry(lang.family())
            .or_default()
            .push(FamilyFileInput {
                language: lang,
                path: entry.path.clone(),
            });
    }

    ParseCandidates {
        groups,
        file_languages,
        shed_over_byte_cap,
    }
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
        sized(path, 10)
    }

    fn sized(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Parse,
        }
    }

    fn grouped_count(inventory: &[FileInventoryEntry], max_files: usize) -> usize {
        group_parseable_inventory(inventory, max_files, 0)
            .groups
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
    fn byte_cap_sheds_candidates_past_the_ceiling() {
        let inventory = [sized("a.py", 100), sized("b.py", 100), sized("c.py", 100)];

        let candidates = group_parseable_inventory(&inventory, 0, 150);

        assert_eq!(candidates.groups.values().map(Vec::len).sum::<usize>(), 2);
        assert_eq!(candidates.shed_over_byte_cap, 1);
        assert!(candidates.file_languages.contains_key("a.py"));
        assert!(candidates.file_languages.contains_key("b.py"));
        assert!(!candidates.file_languages.contains_key("c.py"));
    }

    #[test]
    fn byte_cap_of_zero_sheds_nothing() {
        let inventory = [sized("a.py", 1_000_000), sized("b.py", 1_000_000)];

        let candidates = group_parseable_inventory(&inventory, 0, 0);

        assert_eq!(candidates.groups.values().map(Vec::len).sum::<usize>(), 2);
        assert_eq!(candidates.shed_over_byte_cap, 0);
    }

    #[test]
    fn byte_cap_shed_set_is_deterministic_across_runs() {
        let inventory = [
            sized("a.py", 100),
            sized("b.py", 100),
            sized("c.py", 100),
            sized("d.py", 100),
        ];

        let first = group_parseable_inventory(&inventory, 0, 250);
        let second = group_parseable_inventory(&inventory, 0, 250);

        assert_eq!(first.shed_over_byte_cap, second.shed_over_byte_cap);
        let mut first_kept: Vec<_> = first.file_languages.keys().cloned().collect();
        let mut second_kept: Vec<_> = second.file_languages.keys().cloned().collect();
        first_kept.sort();
        second_kept.sort();
        assert_eq!(first_kept, second_kept);
        assert_eq!(first_kept, vec!["a.py", "b.py", "c.py"]);
    }

    // JsPipeline never recorded graph bytes, so the old in-parse budget missed these.
    #[test]
    fn byte_cap_covers_typescript_and_javascript() {
        let inventory = [sized("a.ts", 100), sized("b.js", 100), sized("c.ts", 100)];

        let candidates = group_parseable_inventory(&inventory, 0, 150);

        assert_eq!(candidates.shed_over_byte_cap, 1);
        assert!(!candidates.file_languages.contains_key("c.ts"));
    }

    fn with_decision(path: &str, decision: Decision) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size: 10,
            decision,
        }
    }

    #[test]
    fn parseable_count_excludes_non_parse_decisions() {
        let inventory = [
            keep("a.rs"),
            keep("b.rs"),
            with_decision("Cargo.lock", Decision::Load),
            with_decision("logo.png", Decision::ListOnly),
            with_decision("ignored", Decision::Drop),
        ];
        assert_eq!(parseable_file_count(&inventory), 2);
    }

    #[test]
    fn parseable_count_ignores_parse_flagged_files_of_unknown_extension() {
        let inventory = [
            keep("a.rs"),
            with_decision("generated.xyz", Decision::Parse),
        ];
        assert_eq!(
            parseable_file_count(&inventory),
            1,
            "a Parse entry with no language mapping produces no parse work, matching group_parseable_inventory"
        );
    }
}
