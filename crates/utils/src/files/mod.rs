//! Every file a repository offers, from a checkout on disk or a Gitaly tar,
//! goes through one state machine:
//!
//! ```text
//! File{path, size}
//!   -> header passes   (path and size only)          -> Drop | ListOnly | Load | Parse
//!   -> read once       (by the source, or by whoever parses)
//!   -> content passes  (on those bytes, once)        -> Drop | ListOnly | Load | Parse
//!   -> Inventory
//! ```
//!
//! Passes are policy (`CodeFilter`, a content classifier). Sources (`disk`,
//! `tar`) only supply bytes. A `Pass` chains with `then`, and every pass in
//! the chain sees what the passes before it decided, so a later pass can
//! refine an earlier one.

pub mod disk;
pub mod tar;
pub mod vfs;

pub use vfs::{ContentId, DirEntry, Metadata, Vfs};

use std::ops::Deref;
use std::path::{Component, Path};

use rustc_hash::FxHashMap;

/// Why a file was not loaded. Snake_case for metric labels.
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

/// What the passes learned about a file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Label {
    pub skip: Option<SkipReason>,
    pub content: ContentClass,
    /// Fine-grained content type from an external classifier such as Magika.
    pub detail: Option<String>,
    pub extension: Option<String>,
}

/// What happens to a file. `Parse` and `Load` both make the bytes available
/// (on disk for the tar source); only `Parse` reaches a parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, strum::Display, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Decision {
    #[default]
    Parse,
    Load,
    ListOnly,
    Drop,
}

/// One file of the repository and where it is in the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub path: String,
    pub size: u64,
    pub symlink: bool,
    pub decision: Decision,
    pub label: Label,
    /// Whether the content passes have run. A source that decides from the
    /// header alone leaves this false; whoever reads the bytes runs them.
    pub checked: bool,
}

impl File {
    pub fn new(path: String, size: u64) -> Self {
        Self {
            path,
            size,
            symlink: false,
            decision: Decision::Parse,
            label: Label::default(),
            checked: false,
        }
    }

    pub fn symlink(path: String, size: u64) -> Self {
        Self {
            symlink: true,
            ..Self::new(path, size)
        }
    }

    pub fn loads(&self) -> bool {
        matches!(self.decision, Decision::Parse | Decision::Load)
    }
}

/// Whether a header pass wants the bytes too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    Nothing,
    Bytes,
}

/// A step of the state machine. `header` sees the path and size and may
/// decide; `content` sees the bytes and may refine. Both take `&self`: a
/// pass that counts uses atomics, so every worker shares one pass.
pub trait Pass: Send + Sync {
    fn header(&self, _file: &mut File) -> Result<Need, CapExceeded> {
        Ok(Need::Nothing)
    }

    fn content(&self, _file: &mut File, _bytes: &[u8]) {}

    fn then<B: Pass>(self, next: B) -> Then<Self, B>
    where
        Self: Sized,
    {
        Then(self, next)
    }
}

/// Two passes in order: the second sees the first's decision.
pub struct Then<A, B>(pub A, pub B);

impl<A: Pass, B: Pass> Pass for Then<A, B> {
    fn header(&self, file: &mut File) -> Result<Need, CapExceeded> {
        let first = self.0.header(file)?;
        let second = self.1.header(file)?;
        Ok(match (first, second) {
            (Need::Nothing, Need::Nothing) => Need::Nothing,
            _ => Need::Bytes,
        })
    }

    fn content(&self, file: &mut File, bytes: &[u8]) {
        self.0.content(file, bytes);
        self.1.content(file, bytes);
    }
}

impl<P: Pass + ?Sized> Pass for &P {
    fn header(&self, file: &mut File) -> Result<Need, CapExceeded> {
        (**self).header(file)
    }

    fn content(&self, file: &mut File, bytes: &[u8]) {
        (**self).content(file, bytes)
    }
}

impl<P: Pass + ?Sized> Pass for std::sync::Arc<P> {
    fn header(&self, file: &mut File) -> Result<Need, CapExceeded> {
        (**self).header(file)
    }

    fn content(&self, file: &mut File, bytes: &[u8]) {
        (**self).content(file, bytes)
    }
}

/// No policy: every file parses.
impl Pass for () {}

/// The content stage: the passes see the bytes, once.
pub fn check(passes: &impl Pass, file: &mut File, bytes: &[u8]) {
    passes.content(file, bytes);
    file.checked = true;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: &'static str,
    pub count: u64,
    pub cap: u64,
}

/// A capped running total; the first `add` to overflow trips the cap.
/// `None` = unlimited. Shared between workers as it is.
pub struct Counter {
    metric: &'static str,
    cap: Option<u64>,
    count: std::sync::atomic::AtomicU64,
}

impl Counter {
    pub fn new(metric: &'static str, cap: Option<u64>) -> Self {
        Self {
            metric,
            cap,
            count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn add(&self, n: u64) -> Result<(), CapExceeded> {
        let count = self
            .count
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(n);
        match self.cap.filter(|&cap| count > cap) {
            Some(cap) => Err(CapExceeded {
                metric: self.metric,
                count,
                cap,
            }),
            None => Ok(()),
        }
    }
}

/// Fatal, whole-source failure: a cap tripped or the source could not be
/// read, so the run stops rather than index a partial repository.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] std::io::Error),
    /// The source held no entries (empty or truncated archive); callers
    /// treat it as an empty repository, not a failure to retry.
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
}

/// Every file the source produced, one per normalized path, sorted.
#[derive(Debug, Clone)]
pub struct Inventory(Vec<File>);

impl Inventory {
    pub fn new(files: Vec<File>) -> Self {
        let mut by_path: FxHashMap<String, File> = FxHashMap::default();
        for mut file in files {
            let Some(path) = normalize_relative_path(&file.path) else {
                continue;
            };
            file.path = path.clone();
            by_path.entry(path).or_insert(file);
        }
        let mut files: Vec<File> = by_path.into_values().collect();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Self(files)
    }

    pub fn total_bytes(&self) -> u64 {
        self.0.iter().map(|f| f.size).sum()
    }

    pub fn into_inner(self) -> Vec<File> {
        self.0
    }
}

impl Deref for Inventory {
    type Target = [File];

    fn deref(&self) -> &[File] {
        &self.0
    }
}

/// A `/`-joined relative path without `.` segments; `None` if it climbs out
/// (`..`, a root, a prefix) or nothing remains.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str) -> File {
        File::new(path.into(), 10)
    }

    #[test]
    fn an_inventory_normalizes_dedups_and_sorts() {
        let inventory = Inventory::new(vec![
            file("./src/main.rs"),
            file("src/main.rs"),
            file("a/b.rs"),
            file("../escape.rs"),
        ]);
        let paths: Vec<&str> = inventory.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["a/b.rs", "src/main.rs"]);
    }

    #[test]
    fn a_counter_admits_until_its_cap_then_trips_across_threads() {
        let bytes = Counter::new("bytes", Some(100));
        assert!(bytes.add(60).is_ok());
        let tripped = std::thread::scope(|scope| scope.spawn(|| bytes.add(60)).join().unwrap());
        assert_eq!(
            tripped,
            Err(CapExceeded {
                metric: "bytes",
                count: 120,
                cap: 100
            })
        );
        assert!(Counter::new("files", None).add(u64::MAX).is_ok());
    }

    struct Header(Decision);
    impl Pass for Header {
        fn header(&self, file: &mut File) -> Result<Need, CapExceeded> {
            file.decision = self.0;
            Ok(Need::Nothing)
        }
    }

    struct WantsBytes;
    impl Pass for WantsBytes {
        fn header(&self, _: &mut File) -> Result<Need, CapExceeded> {
            Ok(Need::Bytes)
        }
        fn content(&self, file: &mut File, bytes: &[u8]) {
            if bytes.contains(&0) {
                file.decision = Decision::ListOnly;
            }
        }
    }

    #[test]
    fn a_chain_lets_the_later_pass_refine_the_earlier_decision() {
        let passes = Header(Decision::Load).then(WantsBytes);
        let mut f = file("a.dat");
        assert_eq!(passes.header(&mut f).unwrap(), Need::Bytes);
        assert_eq!(f.decision, Decision::Load);
        check(&passes, &mut f, b"\x00");
        assert_eq!((f.decision, f.checked), (Decision::ListOnly, true));
    }
}
