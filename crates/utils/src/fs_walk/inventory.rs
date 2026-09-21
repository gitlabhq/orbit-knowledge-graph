use std::hash::Hash;
use std::ops::Deref;

use rustc_hash::FxHashMap;

use super::stream::{
    Decision, FileInventoryEntry, FileStreamHooks, StreamError, canonicalize_inventory,
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

    /// Run a second [`FileStreamHooks`] pass over the inventory.
    /// Unsettled entries are classified in one `on_contents` batch call.
    pub fn refine<H: FileStreamHooks>(
        self,
        hooks: &mut H,
        read_content: impl Fn(&str) -> Option<Vec<u8>>,
    ) -> Result<Self, StreamError> {
        let mut settled = Vec::with_capacity(self.0.len());
        let mut unsettled = Vec::new();

        for (i, entry) in self.0.iter().enumerate() {
            hooks.admit(entry)?;
            if let Some(result) = hooks.on_header(entry) {
                settled.push((i, result));
            } else {
                unsettled.push(i);
            }
        }

        let contents: Vec<Vec<u8>> = unsettled
            .iter()
            .map(|&i| read_content(&self.0[i].path).unwrap_or_default())
            .collect();
        let batch: Vec<(&FileInventoryEntry, &[u8])> = unsettled
            .iter()
            .zip(&contents)
            .map(|(&i, bytes)| (&self.0[i], bytes.as_slice()))
            .collect();
        let content_results = hooks.on_contents(&batch);

        let mut out = Vec::with_capacity(self.0.len());
        let mut settled_iter = settled.into_iter().peekable();
        let mut content_iter = content_results.into_iter();
        for (i, mut entry) in self.0.into_iter().enumerate() {
            let (decision, label) =
                if settled_iter.peek().is_some_and(|(si, _)| *si == i) {
                    settled_iter.next().unwrap().1
                } else {
                    content_iter.next().unwrap()
                };
            entry.decision = decision;
            entry.label = label;
            if entry.decision != Decision::Drop {
                out.push(entry);
            }
        }
        Ok(Self(out))
    }

    /// Mutate entries in place without the hook pipeline. Drops entries
    /// reclassified as `Drop`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_walk::{ContentClass, FileLabel, SkipReason};

    fn entry(path: &str, size: u64, decision: Decision) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision,
            label: Default::default(),
        }
    }

    fn labeled(path: &str, size: u64, decision: Decision, label: FileLabel) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision,
            label,
        }
    }

    #[test]
    fn canonicalizes_dedup_and_sort_on_construction() {
        let inv = FileInventory::new(vec![
            entry("./src/main.rs", 10, Decision::Parse),
            entry("src/main.rs", 10, Decision::Parse),
            entry("a/b.rs", 10, Decision::Load),
        ]);
        assert_eq!(inv.len(), 2);
        assert_eq!(inv.find("a/b.rs").unwrap().decision, Decision::Load);
        assert!(inv.find("src/main.rs").is_some());
        assert!(!inv.contains("missing.rs"));
    }

    #[test]
    fn reclassify_upgrades_and_drops() {
        let inv = FileInventory::new(vec![
            entry("Cargo.toml", 50, Decision::Load),
            entry("logo.png", 5000, Decision::ListOnly),
            entry("src/main.rs", 100, Decision::Parse),
        ]);
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
        assert_eq!(inv.len(), 2);
    }

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
    fn refine_upgrades_text_to_parse_and_preserves_others() {
        let text = FileLabel {
            skip: None,
            content: ContentClass::Text,
            detail: None,
            extension: Some("toml".into()),
        };
        let skipped = FileLabel {
            skip: Some(SkipReason::ExcludedExtension),
            content: ContentClass::Unknown,
            detail: None,
            extension: Some("png".into()),
        };
        let inv = FileInventory::new(vec![
            labeled("Cargo.toml", 50, Decision::Load, text),
            labeled("logo.png", 5000, Decision::ListOnly, skipped),
            entry("src/main.rs", 100, Decision::Parse),
        ]);

        let mut hooks = UpgradeTextToParseHooks;
        let inv = inv.refine(&mut hooks, |_| None).unwrap();

        assert_eq!(inv.find("Cargo.toml").unwrap().decision, Decision::Parse);
        assert_eq!(inv.find("src/main.rs").unwrap().decision, Decision::Parse);
        assert_eq!(inv.find("logo.png").unwrap().decision, Decision::ListOnly);
    }
}
