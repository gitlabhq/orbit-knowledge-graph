use std::path::{Component, Path};

use rustc_hash::FxHashMap;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum SkipReason {
    Oversize,
    ExcludedExtension,
    Binary,
    NotUtf8,
    Minified,
    LineTooLong,
    NonRegularFile,
    LfsPointer,
    ExcludedDirectory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ContentClass {
    #[default]
    Unknown,
    Text,
    Code,
    Binary,
    MinifiedCode,
    LfsPointer,
    NonRegular,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileLabel {
    pub skip: Option<SkipReason>,
    pub content: ContentClass,
    pub detail: Option<String>,
    pub extension: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, strum::Display, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Decision {
    #[default]
    Parse,
    Load,
    ListOnly,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInventoryEntry {
    pub path: String,
    pub size: u64,
    pub decision: Decision,
    pub label: FileLabel,
}

pub fn canonicalize_inventory(entries: Vec<FileInventoryEntry>) -> Vec<FileInventoryEntry> {
    let mut by_path: FxHashMap<String, (u64, Decision, FileLabel)> = FxHashMap::default();
    for entry in entries {
        let Some(path) = normalize_relative_path(&entry.path) else {
            continue;
        };
        by_path
            .entry(path)
            .or_insert((entry.size, entry.decision, entry.label));
    }
    let mut entries: Vec<_> = by_path
        .into_iter()
        .map(|(path, (size, decision, label))| FileInventoryEntry {
            path,
            size,
            decision,
            label,
        })
        .collect();
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

fn normalize_relative_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: &'static str,
    pub count: u64,
    pub cap: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] std::io::Error),
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
}

pub trait FileStreamHooks {
    fn admit(&mut self, _file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        Ok(())
    }
    fn on_header(&mut self, _file: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
        None
    }
    fn on_content(&mut self, _file: &FileInventoryEntry, _content: &[u8]) -> (Decision, FileLabel) {
        (Decision::Parse, FileLabel::default())
    }
    fn on_non_regular(&mut self, _file: &FileInventoryEntry) -> (Decision, FileLabel) {
        (Decision::ListOnly, FileLabel::default())
    }
}

pub fn step<H: FileStreamHooks>(
    hooks: &mut H,
    file: &FileInventoryEntry,
    content: &mut Vec<u8>,
    sniff: impl FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
) -> Result<(Decision, FileLabel), StreamError> {
    hooks.admit(file)?;
    content.clear();
    if let Some(settled) = hooks.on_header(file) {
        return Ok(settled);
    }
    sniff(content)?;
    Ok(hooks.on_content(file, content))
}

pub struct Counter {
    metric: &'static str,
    cap: Option<u64>,
    count: u64,
}

impl Counter {
    pub fn new(metric: &'static str, cap: Option<u64>) -> Self {
        Self {
            metric,
            cap,
            count: 0,
        }
    }

    pub fn add(&mut self, n: u64) -> Result<(), CapExceeded> {
        self.count = self.count.saturating_add(n);
        if let Some(cap) = self.cap.filter(|&cap| self.count > cap) {
            return Err(CapExceeded {
                metric: self.metric,
                count: self.count,
                cap,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_admits_until_cap_then_short_circuits() {
        let mut bytes = Counter::new("bytes", Some(100));
        assert!(bytes.add(60).is_ok());
        assert_eq!(
            bytes.add(60),
            Err(CapExceeded {
                metric: "bytes",
                count: 120,
                cap: 100
            })
        );
    }

    #[test]
    fn zero_cap_is_unlimited() {
        let mut files = Counter::new("files", None);
        assert!(files.add(u64::MAX).is_ok());
        assert!(files.add(u64::MAX).is_ok());
    }

    struct TestHooks {
        bytes: Counter,
    }

    impl FileStreamHooks for TestHooks {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
            f.path
                .ends_with(".png")
                .then_some((Decision::Drop, FileLabel::default()))
        }
        fn admit(&mut self, f: &FileInventoryEntry) -> Result<(), CapExceeded> {
            self.bytes.add(f.size)
        }
    }

    fn entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Parse,
            label: Default::default(),
        }
    }

    #[test]
    fn step_settles_in_header_without_sniffing() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", None),
        };
        let mut prefix = Vec::new();
        let (d, _) = step(&mut h, &entry("a.png", 10), &mut prefix, |_| {
            panic!("a header-settled file must never be sniffed")
        })
        .unwrap();
        assert_eq!(d, Decision::Drop);
    }

    #[test]
    fn step_admits_kept_file() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", Some(100)),
        };
        let mut prefix = Vec::new();
        let (d, _) = step(&mut h, &entry("a.rs", 10), &mut prefix, |buf| {
            buf.extend_from_slice(b"fn main");
            Ok(())
        })
        .unwrap();
        assert_eq!(d, Decision::Parse);
    }

    #[test]
    fn step_charges_cap_before_keep_decision() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", Some(5)),
        };
        let mut prefix = Vec::new();
        let err = step(&mut h, &entry("a.rs", 10), &mut prefix, |_| Ok(())).unwrap_err();
        assert!(matches!(err, StreamError::Cap(_)));
    }

    #[test]
    fn step_caps_charge_even_header_dropped_files() {
        let mut h = TestHooks {
            bytes: Counter::new("bytes", Some(5)),
        };
        let mut prefix = Vec::new();
        let err = step(&mut h, &entry("blob.png", 10), &mut prefix, |_| Ok(())).unwrap_err();
        assert!(
            matches!(err, StreamError::Cap(_)),
            "a dropped file's bytes must still count toward the cap"
        );
    }

    #[test]
    fn canonicalize_dedups_normalizes_and_sorts() {
        let inv = canonicalize_inventory(vec![
            entry("./src/main.rs", 10),
            entry("src/main.rs", 10),
            entry("a/b.rs", 10),
        ]);
        let paths: Vec<&str> = inv.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["a/b.rs", "src/main.rs"]);
    }

    #[test]
    fn canonicalize_drops_traversal_entries() {
        let inv = canonicalize_inventory(vec![entry("../escape", 10), entry("ok.rs", 10)]);
        let paths: Vec<&str> = inv.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["ok.rs"]);
    }
}
