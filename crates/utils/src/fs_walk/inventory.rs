use std::hash::Hash;
use std::ops::Deref;

use rustc_hash::FxHashMap;

use super::stream::{
    Decision, FileInventoryEntry, FileStreamHooks, StreamError, canonicalize_inventory, step,
};

#[derive(Debug, Clone)]
pub struct FileInventory(Vec<FileInventoryEntry>);

impl FileInventory {
    pub fn new(entries: Vec<FileInventoryEntry>) -> Self {
        Self(canonicalize_inventory(entries))
    }

    pub fn by_decision(&self, decision: Decision) -> impl Iterator<Item = &FileInventoryEntry> {
        self.0.iter().filter(move |e| e.decision == decision)
    }

    pub fn parseable(&self) -> impl Iterator<Item = &FileInventoryEntry> {
        self.by_decision(Decision::Parse)
    }

    pub fn loaded(&self) -> impl Iterator<Item = &FileInventoryEntry> {
        self.by_decision(Decision::Load)
    }

    pub fn listed(&self) -> impl Iterator<Item = &FileInventoryEntry> {
        self.by_decision(Decision::ListOnly)
    }

    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|e| e.path.as_str())
    }

    pub fn find(&self, path: &str) -> Option<&FileInventoryEntry> {
        self.0
            .binary_search_by(|e| e.path.as_str().cmp(path))
            .ok()
            .map(|i| &self.0[i])
    }

    pub fn contains(&self, path: &str) -> bool {
        self.0
            .binary_search_by(|e| e.path.as_str().cmp(path))
            .is_ok()
    }

    pub fn total_bytes(&self) -> u64 {
        self.0.iter().map(|e| e.size).sum()
    }

    pub fn count_by(&self, pred: impl Fn(&FileInventoryEntry) -> bool) -> usize {
        self.0.iter().filter(|e| pred(e)).count()
    }

    pub fn group_by<K, F>(&self, classifier: F) -> FxHashMap<K, Vec<&FileInventoryEntry>>
    where
        K: Hash + Eq,
        F: Fn(&FileInventoryEntry) -> Option<K>,
    {
        let mut groups: FxHashMap<K, Vec<&FileInventoryEntry>> = FxHashMap::default();
        for entry in &self.0 {
            if let Some(key) = classifier(entry) {
                groups.entry(key).or_default().push(entry);
            }
        }
        groups
    }

    /// Run a refinement pass over the inventory using the same
    /// [`FileStreamHooks`] trait. `read_content` provides file bytes on
    /// demand (return `None` to settle from the header alone). Entries
    /// reclassified as [`Decision::Drop`] are removed.
    pub fn refine<H: FileStreamHooks>(
        self,
        hooks: &mut H,
        read_content: impl Fn(&str) -> Option<Vec<u8>>,
    ) -> Result<Self, StreamError> {
        let mut out = Vec::with_capacity(self.0.len());
        let mut buf = Vec::new();
        for mut entry in self.0 {
            let (decision, label) = step(hooks, &entry, &mut buf, |buf| {
                if let Some(bytes) = read_content(&entry.path) {
                    buf.extend_from_slice(&bytes);
                }
                Ok(())
            })?;
            entry.decision = decision;
            entry.label = label;
            if entry.decision != Decision::Drop {
                out.push(entry);
            }
        }
        Ok(Self(out))
    }

    /// Mutate entries in place. For lightweight adjustments that don't need
    /// the full hook pipeline (e.g. upgrading a `Load` to `Parse` after
    /// an external classifier confirms the language).
    pub fn reclassify(mut self, mut f: impl FnMut(&mut FileInventoryEntry)) -> Self {
        for entry in &mut self.0 {
            f(entry);
        }
        self.0.retain(|e| e.decision != Decision::Drop);
        Self(self.0)
    }

    pub fn into_inner(self) -> Vec<FileInventoryEntry> {
        self.0
    }
}

impl Deref for FileInventory {
    type Target = [FileInventoryEntry];

    fn deref(&self) -> &[FileInventoryEntry] {
        &self.0
    }
}

impl From<Vec<FileInventoryEntry>> for FileInventory {
    fn from(entries: Vec<FileInventoryEntry>) -> Self {
        Self::new(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, size: u64, decision: Decision) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision,
            label: Default::default(),
        }
    }

    fn sample() -> FileInventory {
        FileInventory::new(vec![
            entry("src/main.rs", 100, Decision::Parse),
            entry("src/lib.rs", 200, Decision::Parse),
            entry("Cargo.toml", 50, Decision::Load),
            entry("logo.png", 5000, Decision::ListOnly),
            entry(".gitignore", 30, Decision::Load),
        ])
    }

    #[test]
    fn by_decision_filters_correctly() {
        let inv = sample();
        assert_eq!(inv.parseable().count(), 2);
        assert_eq!(inv.loaded().count(), 2);
        assert_eq!(inv.listed().count(), 1);
    }

    #[test]
    fn total_bytes_sums_all_entries() {
        let inv = sample();
        assert_eq!(inv.total_bytes(), 5380);
    }

    #[test]
    fn find_and_contains() {
        let inv = sample();
        assert!(inv.contains("src/main.rs"));
        assert!(!inv.contains("missing.rs"));
        assert_eq!(inv.find("Cargo.toml").unwrap().size, 50);
        assert!(inv.find("missing.rs").is_none());
    }

    #[test]
    fn paths_returns_all() {
        let inv = sample();
        let paths: Vec<&str> = inv.paths().collect();
        assert_eq!(paths.len(), 5);
        assert!(paths.contains(&"src/main.rs"));
    }

    #[test]
    fn count_by_with_predicate() {
        let inv = sample();
        assert_eq!(inv.count_by(|e| e.size > 100), 2);
    }

    #[test]
    fn deref_gives_slice_access() {
        let inv = sample();
        assert_eq!(inv.len(), 5);
        assert!(!inv.is_empty());
    }

    #[test]
    fn canonicalizes_on_construction() {
        let inv = FileInventory::new(vec![
            entry("./src/main.rs", 10, Decision::Parse),
            entry("src/main.rs", 10, Decision::Parse),
            entry("a/b.rs", 10, Decision::Load),
        ]);
        assert_eq!(inv.len(), 2);
        let paths: Vec<&str> = inv.paths().collect();
        assert_eq!(paths, vec!["a/b.rs", "src/main.rs"]);
    }

    #[test]
    fn group_by_classifies_entries() {
        let inv = sample();
        let groups = inv.group_by(|e| Some(e.decision));
        assert_eq!(groups[&Decision::Parse].len(), 2);
        assert_eq!(groups[&Decision::Load].len(), 2);
        assert_eq!(groups[&Decision::ListOnly].len(), 1);
    }

    #[test]
    fn group_by_skips_none() {
        let inv = sample();
        let groups = inv.group_by(|e| {
            if e.decision == Decision::Parse {
                Some("parseable")
            } else {
                None
            }
        });
        assert_eq!(groups.len(), 1);
        assert_eq!(groups["parseable"].len(), 2);
    }

    #[test]
    fn reclassify_upgrades_and_drops() {
        let inv = sample();
        let inv = inv.reclassify(|e| {
            if e.path == "Cargo.toml" {
                e.decision = Decision::Parse;
            }
            if e.path == "logo.png" {
                e.decision = Decision::Drop;
            }
        });
        assert_eq!(inv.find("Cargo.toml").unwrap().decision, Decision::Parse);
        assert!(!inv.contains("logo.png"));
        assert_eq!(inv.len(), 4);
    }

    use crate::fs_walk::{ContentClass, FileLabel, SkipReason};

    struct UpgradeTextToParseHooks;
    impl FileStreamHooks for UpgradeTextToParseHooks {
        fn on_header(&mut self, file: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
            if file.label.content == ContentClass::Text && file.decision == Decision::Load {
                Some((Decision::Parse, file.label.clone()))
            } else {
                Some((file.decision, file.label.clone()))
            }
        }
    }

    #[test]
    fn refine_runs_hooks_over_existing_inventory() {
        let inv = FileInventory::new(vec![
            FileInventoryEntry {
                path: "src/main.rs".into(),
                size: 100,
                decision: Decision::Parse,
                label: FileLabel {
                    skip: None,
                    content: ContentClass::Text,
                    extension: Some("rs".into()),
                },
            },
            FileInventoryEntry {
                path: "Cargo.toml".into(),
                size: 50,
                decision: Decision::Load,
                label: FileLabel {
                    skip: None,
                    content: ContentClass::Text,
                    extension: Some("toml".into()),
                },
            },
            FileInventoryEntry {
                path: "logo.png".into(),
                size: 5000,
                decision: Decision::ListOnly,
                label: FileLabel {
                    skip: Some(SkipReason::ExcludedExtension),
                    content: ContentClass::Unknown,
                    extension: Some("png".into()),
                },
            },
        ]);

        let mut hooks = UpgradeTextToParseHooks;
        let inv = inv.refine(&mut hooks, |_| None).unwrap();

        assert_eq!(inv.find("Cargo.toml").unwrap().decision, Decision::Parse);
        assert_eq!(inv.find("src/main.rs").unwrap().decision, Decision::Parse);
        assert_eq!(inv.find("logo.png").unwrap().decision, Decision::ListOnly);
    }
}
