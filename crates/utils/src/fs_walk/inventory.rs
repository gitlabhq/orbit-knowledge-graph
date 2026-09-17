use std::hash::Hash;
use std::ops::Deref;

use rustc_hash::FxHashMap;

use super::stream::{Decision, FileInventoryEntry, canonicalize_inventory};

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
}
