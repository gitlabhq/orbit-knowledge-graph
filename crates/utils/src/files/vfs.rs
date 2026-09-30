//! The repository as a filesystem. A source puts files in (`write`, `link`,
//! `list`); the passes decide per file whether it parses, loads, is only
//! listed, or is dropped; the filesystem keeps a node for every file that
//! stays and bytes only for those that load. Readers see `std::fs` verbs
//! over repo-relative paths (`/` is the repository root) and never learn
//! where bytes live: content-addressed, so identical files at many paths are
//! kept once, resident in memory up to a budget and appended to one anonymous
//! scratch file past it; a checkout on disk is linked, never copied.

use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex, OnceLock};

use rustc_hash::FxHashMap;
use sha2::{Digest, Sha256};

use super::{Counter, Decision, File, Label, Need, Pass, SkipReason, SourceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub len: u64,
    pub is_dir: bool,
    pub is_symlink: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// Why a parser got no source: the passes turned the file down (its label
/// says why), or its bytes could not be read.
#[derive(Debug)]
pub enum Unread {
    Listed(Label),
    Unreadable(io::Error),
}

/// SHA-256 of a file's bytes: the identity content is stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentId([u8; 32]);

impl ContentId {
    fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

pub struct Vfs {
    passes: Arc<dyn Pass>,
    nodes: Mutex<FxHashMap<String, Node>>,
    blobs: Mutex<FxHashMap<ContentId, Blob>>,
    children: Mutex<FxHashMap<String, FxHashMap<String, bool>>>,
    resident: Counter,
    scratch: OnceLock<Scratch>,
}

impl std::fmt::Debug for Vfs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vfs")
            .field("files", &self.len())
            .field("distinct_contents", &lock(&self.blobs).len())
            .finish()
    }
}

/// One file of the repository: what the passes decided, and its bytes if
/// they decided it loads.
#[derive(Debug, Clone)]
struct Node {
    file: File,
    slot: Option<Slot>,
    /// Whether the content passes have seen this file. A linked file is
    /// checked on its first `source` read.
    checked: bool,
}

/// Where a loading file's bytes are.
#[derive(Debug, Clone)]
enum Slot {
    Stored(ContentId),
    Linked(std::path::PathBuf),
}

/// Where a distinct content is.
#[derive(Debug, Clone)]
enum Blob {
    Memory(Arc<[u8]>),
    Spilled { offset: u64, len: u64 },
}

/// One anonymous append-only file: sequential writes, positional reads,
/// gone when the store is.
struct Scratch {
    file: std::fs::File,
    end: Mutex<u64>,
}

impl Scratch {
    fn open() -> io::Result<Self> {
        Ok(Self {
            file: tempfile::tempfile()?,
            end: Mutex::new(0),
        })
    }

    fn append(&self, bytes: &[u8]) -> io::Result<u64> {
        let offset = {
            let mut end = self.end.lock().unwrap_or_else(|e| e.into_inner());
            let offset = *end;
            *end += bytes.len() as u64;
            offset
        };
        self.file.write_all_at(bytes, offset)?;
        Ok(offset)
    }

    fn read(&self, offset: u64, len: u64) -> io::Result<Arc<[u8]>> {
        let mut bytes = vec![0u8; len as usize];
        self.file.read_exact_at(&mut bytes, offset)?;
        Ok(bytes.into())
    }
}

impl Default for Vfs {
    fn default() -> Self {
        Self::new((), None)
    }
}

/// Putting files in. `stat` runs the header passes and hands back the entry,
/// so a source can tell whether the bytes are needed before it pays to
/// produce them; `write`, `link` and `list` are the one-call forms.
impl Vfs {
    /// `budget` caps the bytes kept in memory; `Some(0)` spills everything,
    /// `None` keeps everything.
    pub fn new(passes: impl Pass + 'static, budget: Option<u64>) -> Self {
        Self {
            passes: Arc::new(passes),
            nodes: Mutex::default(),
            blobs: Mutex::default(),
            children: Mutex::default(),
            resident: Counter::new("resident_bytes", budget),
            scratch: OnceLock::new(),
        }
    }

    /// The header passes' view of a file, before any bytes; `None` if they
    /// dropped it or the path climbs out of the repository.
    pub fn stat(
        &self,
        path: &str,
        size: u64,
        symlink: bool,
    ) -> Result<Option<Entry<'_>>, SourceError> {
        let Some(key) = key(Path::new(path)) else {
            return Ok(None);
        };
        let mut file = match symlink {
            true => File::symlink(key, size),
            false => File::new(key, size),
        };
        let need = self.passes.header(&mut file)?;
        Ok((file.decision != Decision::Drop).then_some(Entry {
            vfs: self,
            file,
            need,
        }))
    }

    /// A file whose bytes are in hand.
    pub fn write(&self, path: &str, bytes: Vec<u8>) -> Result<(), SourceError> {
        match self.stat(path, bytes.len() as u64, false)? {
            Some(entry) => entry.write(bytes),
            None => Ok(()),
        }
    }

    /// A file that lives on disk: read now only if a pass asks to see it,
    /// linked where it is otherwise.
    pub fn link(
        &self,
        path: &str,
        on_disk: std::path::PathBuf,
        size: u64,
    ) -> Result<(), SourceError> {
        match self.stat(path, size, false)? {
            Some(entry) => entry.link(on_disk),
            None => Ok(()),
        }
    }

    /// A file with no bytes: a symlink, say. A node, nothing more.
    pub fn list(&self, path: &str, size: u64, symlink: bool) -> Result<(), SourceError> {
        if let Some(entry) = self.stat(path, size, symlink)? {
            entry.list();
        }
        Ok(())
    }

    fn keep(&self, file: File, slot: Option<Slot>, checked: bool) {
        self.index(&file.path);
        let key = file.path.clone();
        lock(&self.nodes).insert(
            key,
            Node {
                file,
                slot,
                checked,
            },
        );
    }

    /// Bytes stored once per distinct content. Charged and stored under one
    /// lock, so two workers adding the same content at once cannot both pay.
    fn store(&self, bytes: Vec<u8>) -> io::Result<ContentId> {
        let id = ContentId::of(&bytes);
        let mut blobs = lock(&self.blobs);
        if let std::collections::hash_map::Entry::Vacant(slot) = blobs.entry(id) {
            let blob = match self.resident.add(bytes.len() as u64) {
                Ok(()) => Blob::Memory(bytes.into()),
                Err(_) => Blob::Spilled {
                    len: bytes.len() as u64,
                    offset: self.scratch()?.append(&bytes)?,
                },
            };
            slot.insert(blob);
        }
        Ok(id)
    }

    fn scratch(&self) -> io::Result<&Scratch> {
        if let Some(scratch) = self.scratch.get() {
            return Ok(scratch);
        }
        let _ = self.scratch.set(Scratch::open()?);
        Ok(self.scratch.get().expect("scratch was just set"))
    }

    fn index(&self, key: &str) {
        let mut children = lock(&self.children);
        let mut child = key;
        let mut is_dir = false;
        loop {
            let (parent, name) = match child.rsplit_once('/') {
                Some((parent, name)) => (parent, name),
                None => ("", child),
            };
            children
                .entry(parent.to_owned())
                .or_default()
                .insert(name.to_owned(), is_dir);
            if parent.is_empty() {
                return;
            }
            child = parent;
            is_dir = true;
        }
    }
}

/// A file after the header passes, before its bytes.
pub struct Entry<'a> {
    vfs: &'a Vfs,
    file: File,
    need: Need,
}

impl Entry<'_> {
    /// Whether the file keeps its bytes at all.
    pub fn loads(&self) -> bool {
        self.file.loads()
    }

    /// Whether a pass needs the bytes before deciding.
    pub fn needs_bytes(&self) -> bool {
        self.need == Need::Bytes
    }

    /// The bytes, once: the content passes see them and the file is kept
    /// with them if it still loads.
    pub fn write(mut self, bytes: Vec<u8>) -> Result<(), SourceError> {
        self.vfs.passes.content(&mut self.file, &bytes);
        let slot = match self.file.loads() {
            true => Some(Slot::Stored(self.vfs.store(bytes)?)),
            false => None,
        };
        self.vfs.keep(self.file, slot, true);
        Ok(())
    }

    /// The file stays on disk, where it already is storage. If a pass asked
    /// to see it, it is read now for the decision and the bytes are let go;
    /// otherwise the parser's first read decides.
    pub fn link(mut self, on_disk: std::path::PathBuf) -> Result<(), SourceError> {
        if !self.loads() {
            self.list();
            return Ok(());
        }
        if !self.needs_bytes() {
            self.vfs.keep(self.file, Some(Slot::Linked(on_disk)), false);
            return Ok(());
        }
        let bytes = std::fs::read(&on_disk)?;
        self.vfs.passes.content(&mut self.file, &bytes);
        let slot = self.file.loads().then_some(Slot::Linked(on_disk));
        self.vfs.keep(self.file, slot, true);
        Ok(())
    }

    /// No bytes: a node in the tree, nothing more.
    pub fn list(mut self) {
        if self.file.loads() {
            self.file.decision = Decision::ListOnly;
        }
        self.vfs.keep(self.file, None, true);
    }
}

/// Reading, with `std::fs` semantics: a missing path is `NotFound`, reading a
/// directory is an error, a file kept without bytes is `Unsupported`,
/// `read_dir` lists direct children in no particular order.
impl Vfs {
    pub fn read(&self, path: &Path) -> io::Result<Arc<[u8]>> {
        let key = key(path).ok_or_else(not_found)?;
        let node = lock(&self.nodes).get(&key).cloned();
        match node {
            Some(Node {
                slot: Some(slot), ..
            }) => self.bytes(&slot),
            Some(Node { file, .. }) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("{key} is listed only: {}", reason(&file.label)),
            )),
            None if lock(&self.children).contains_key(&key) => Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("{key} is a directory"),
            )),
            None => Err(not_found()),
        }
    }

    /// The parser's read: the content passes see a linked file's bytes on
    /// this first read and may turn it down, which the node then records.
    pub fn source(&self, path: &Path) -> Result<Arc<[u8]>, Unread> {
        let key = key(path).ok_or_else(|| Unread::Unreadable(not_found()))?;
        let node = lock(&self.nodes).get(&key).cloned();
        let Some(node) = node else {
            return Err(Unread::Unreadable(not_found()));
        };
        let Some(slot) = node.slot else {
            return Err(Unread::Listed(node.file.label));
        };
        let bytes = self.bytes(&slot).map_err(Unread::Unreadable)?;
        if node.checked {
            return Ok(bytes);
        }
        let mut file = node.file;
        self.passes.content(&mut file, &bytes);
        let loads = file.loads();
        let label = file.label.clone();
        let slot = loads.then_some(slot);
        self.keep(file, slot, true);
        match loads {
            true => Ok(bytes),
            false => Err(Unread::Listed(label)),
        }
    }

    fn bytes(&self, slot: &Slot) -> io::Result<Arc<[u8]>> {
        match slot {
            Slot::Stored(id) => {
                let blob = lock(&self.blobs).get(id).cloned();
                match blob {
                    Some(Blob::Memory(bytes)) => Ok(bytes),
                    Some(Blob::Spilled { offset, len }) => self.scratch()?.read(offset, len),
                    None => Err(not_found()),
                }
            }
            Slot::Linked(on_disk) => std::fs::read(on_disk).map(Into::into),
        }
    }

    pub fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        let key = key(path).ok_or_else(not_found)?;
        if let Some(node) = lock(&self.nodes).get(&key) {
            return Ok(Metadata {
                len: node.file.size,
                is_dir: false,
                is_symlink: node.file.symlink,
            });
        }
        if key.is_empty() || lock(&self.children).contains_key(&key) {
            return Ok(Metadata {
                len: 0,
                is_dir: true,
                is_symlink: false,
            });
        }
        Err(not_found())
    }

    pub fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
        let key = key(path).ok_or_else(not_found)?;
        match lock(&self.children).get(&key) {
            Some(children) => Ok(children
                .iter()
                .map(|(name, &is_dir)| DirEntry {
                    name: name.clone(),
                    is_dir,
                })
                .collect()),
            None if lock(&self.nodes).contains_key(&key) => Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!("{key} is a file"),
            )),
            None => Err(not_found()),
        }
    }

    pub fn read_to_string(&self, path: &Path) -> io::Result<String> {
        let bytes = self.read(path)?;
        std::str::from_utf8(&bytes)
            .map(str::to_owned)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn exists(&self, path: &Path) -> bool {
        self.metadata(path).is_ok()
    }

    pub fn is_file(&self, path: &Path) -> bool {
        self.metadata(path).is_ok_and(|m| !m.is_dir)
    }

    pub fn is_dir(&self, path: &Path) -> bool {
        self.metadata(path).is_ok_and(|m| m.is_dir)
    }

    /// What the passes decided about one file.
    pub fn file(&self, path: &Path) -> Option<File> {
        lock(&self.nodes).get(&key(path)?).map(|n| n.file.clone())
    }

    /// Every file the repository kept, sorted by path: the inventory.
    pub fn files(&self) -> Vec<File> {
        let mut files: Vec<File> = lock(&self.nodes).values().map(|n| n.file.clone()).collect();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        files
    }

    pub fn total_bytes(&self) -> u64 {
        lock(&self.nodes).values().map(|n| n.file.size).sum()
    }

    pub fn len(&self) -> usize {
        lock(&self.nodes).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The identity of a stored file's content; `None` for linked files,
    /// whose bytes were never read, and for files kept without bytes.
    pub fn content_id(&self, path: &Path) -> Option<ContentId> {
        match lock(&self.nodes).get(&key(path)?)?.slot {
            Some(Slot::Stored(id)) => Some(id),
            _ => None,
        }
    }
}

fn reason(label: &Label) -> String {
    label
        .skip
        .map_or_else(|| "no bytes".to_string(), |s: SkipReason| s.to_string())
}

/// A repo-relative `/`-joined key; the repository root is `""`. `None` if
/// the path climbs out of the repository.
fn key(path: &Path) -> Option<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir | Component::RootDir => {}
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(parts.join("/"))
}

fn not_found() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "no such file in the repository")
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::files::CapExceeded;

    /// Drops pngs at the header, NUL-bearing files at the content, wants to
    /// see manifests now. The shape of the production `CodeFilter`.
    struct TestFilter;
    impl Pass for TestFilter {
        fn header(&self, f: &mut File) -> Result<Need, CapExceeded> {
            if f.symlink {
                f.decision = Decision::ListOnly;
                f.label.skip = Some(SkipReason::NonRegularFile);
            } else if f.path.ends_with(".png") {
                f.decision = Decision::ListOnly;
                f.label.skip = Some(SkipReason::ExcludedExtension);
            } else if f.path.ends_with(".toml") {
                f.decision = Decision::Load;
                return Ok(Need::Bytes);
            }
            Ok(Need::Nothing)
        }
        fn content(&self, f: &mut File, bytes: &[u8]) {
            if bytes.contains(&0) {
                f.decision = Decision::ListOnly;
                f.label.skip = Some(SkipReason::Binary);
            }
        }
    }

    fn names(mut entries: Vec<DirEntry>) -> Vec<(String, bool)> {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        entries.into_iter().map(|e| (e.name, e.is_dir)).collect()
    }

    fn decision(vfs: &Vfs, path: &str) -> Decision {
        vfs.file(Path::new(path)).unwrap().decision
    }

    #[test]
    fn behaves_like_a_filesystem_rooted_at_the_repository() {
        let vfs = Vfs::default();
        vfs.write("src/main.rs", b"fn main() {}".to_vec()).unwrap();
        vfs.write("/src/lib/mod.rs", b"pub mod a;".to_vec())
            .unwrap();
        vfs.write("./README.md", b"# hi".to_vec()).unwrap();

        assert_eq!(
            &*vfs.read(Path::new("src/main.rs")).unwrap(),
            b"fn main() {}"
        );
        assert_eq!(
            &*vfs.read(Path::new("/src/main.rs")).unwrap(),
            b"fn main() {}"
        );
        assert_eq!(
            vfs.read_to_string(Path::new("src/lib/mod.rs")).unwrap(),
            "pub mod a;"
        );
        assert_eq!(
            vfs.metadata(Path::new("src/main.rs")).unwrap(),
            Metadata {
                len: 12,
                is_dir: false,
                is_symlink: false,
            }
        );
        assert!(vfs.is_dir(Path::new("src")));
        assert!(vfs.is_dir(Path::new("/")));
        assert!(!vfs.exists(Path::new("src/missing.rs")));
        assert!(!vfs.exists(Path::new("../escape.rs")));
        assert_eq!(
            names(vfs.read_dir(Path::new("/")).unwrap()),
            [("README.md".into(), false), ("src".into(), true)]
        );
        assert_eq!(
            names(vfs.read_dir(Path::new("src")).unwrap()),
            [("lib".into(), true), ("main.rs".into(), false)]
        );

        let kinds = |path: &str| vfs.read(Path::new(path)).map(|_| ()).unwrap_err().kind();
        assert_eq!(kinds("src"), io::ErrorKind::IsADirectory);
        assert_eq!(kinds("nope"), io::ErrorKind::NotFound);
        assert_eq!(
            vfs.read_dir(Path::new("README.md")).unwrap_err().kind(),
            io::ErrorKind::NotADirectory
        );
    }

    /// The passes settle every file put in; only files that load cost bytes,
    /// the rest are nodes with a reason, and a dropped file leaves no trace.
    #[test]
    fn the_passes_decide_what_is_kept_and_what_is_only_listed() {
        struct DropLogs;
        impl Pass for DropLogs {
            fn header(&self, f: &mut File) -> Result<Need, CapExceeded> {
                if f.path.ends_with(".log") {
                    f.decision = Decision::Drop;
                }
                Ok(Need::Nothing)
            }
        }
        let vfs = Vfs::new(TestFilter.then(DropLogs), None);
        vfs.write("src/main.rs", b"fn main() {}".to_vec()).unwrap();
        vfs.write("assets/logo.png", b"\x89PNG".to_vec()).unwrap();
        vfs.write("model/weights.bin", b"\x00\x01".to_vec())
            .unwrap();
        vfs.write("build.log", b"noise".to_vec()).unwrap();
        vfs.list("link.rs", 0, true).unwrap();

        let rows: Vec<(String, Decision)> = vfs
            .files()
            .into_iter()
            .map(|f| (f.path, f.decision))
            .collect();
        assert_eq!(
            rows,
            [
                ("assets/logo.png".into(), Decision::ListOnly),
                ("link.rs".into(), Decision::ListOnly),
                ("model/weights.bin".into(), Decision::ListOnly),
                ("src/main.rs".into(), Decision::Parse),
            ]
        );
        assert!(vfs.exists(Path::new("assets/logo.png")));
        assert!(vfs.metadata(Path::new("link.rs")).unwrap().is_symlink);
        assert_eq!(
            vfs.read(Path::new("model/weights.bin")).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert!(matches!(
            vfs.source(Path::new("assets/logo.png")),
            Err(Unread::Listed(label)) if label.skip == Some(SkipReason::ExcludedExtension)
        ));
        assert!(!vfs.exists(Path::new("build.log")));
        assert_eq!(lock(&vfs.blobs).len(), 1, "only the parse file cost bytes");
    }

    /// A checkout is linked, not copied; a linked file the passes want to
    /// see is read at link time for the decision only, the rest on the
    /// parser's first read, where the passes may still turn it down and the
    /// node records it.
    #[test]
    fn a_linked_checkout_is_checked_on_first_read() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, body: &[u8]| {
            let on_disk = dir.path().join(name);
            std::fs::write(&on_disk, body).unwrap();
            (on_disk, body.len() as u64)
        };
        let vfs = Vfs::new(TestFilter, None);
        for name in ["lib.rs", "blob.rs", "Cargo.toml"] {
            let (on_disk, len) = write(
                name,
                match name {
                    "blob.rs" => b"\x00\x01 not rust".as_slice(),
                    "Cargo.toml" => b"[package]",
                    _ => b"pub fn f() {}",
                },
            );
            vfs.link(name, on_disk, len).unwrap();
        }

        assert_eq!(vfs.content_id(Path::new("lib.rs")), None, "linked, unread");
        assert_eq!(
            vfs.content_id(Path::new("Cargo.toml")),
            None,
            "decided at link, then linked"
        );
        assert_eq!(decision(&vfs, "Cargo.toml"), Decision::Load);
        assert_eq!(decision(&vfs, "blob.rs"), Decision::Parse, "not yet read");

        assert_eq!(&*vfs.source(Path::new("lib.rs")).unwrap(), b"pub fn f() {}");
        assert!(matches!(
            vfs.source(Path::new("blob.rs")),
            Err(Unread::Listed(label)) if label.skip == Some(SkipReason::Binary)
        ));
        assert_eq!(decision(&vfs, "blob.rs"), Decision::ListOnly);
        assert_eq!(
            vfs.read(Path::new("blob.rs")).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(vfs.metadata(Path::new("lib.rs")).unwrap().len, 13);
    }

    #[test]
    fn identical_content_at_many_paths_is_stored_once_and_stays_many_files() {
        let vfs = Vfs::new((), Some(100));
        let body = vec![b'x'; 80];
        vfs.write("a/one.js", body.clone()).unwrap();
        vfs.write("b/two.js", body.clone()).unwrap();
        vfs.write("c/three.js", body.clone()).unwrap();

        assert_eq!(vfs.len(), 3);
        assert_eq!(
            vfs.content_id(Path::new("a/one.js")),
            vfs.content_id(Path::new("c/three.js"))
        );
        assert!(vfs.scratch.get().is_none(), "240 shared bytes fit in 100");
        assert_eq!(
            names(vfs.read_dir(Path::new("b")).unwrap()),
            [("two.js".into(), false)]
        );
        assert_eq!(&*vfs.read(Path::new("c/three.js")).unwrap(), &body[..]);
    }

    #[test]
    fn bytes_past_the_budget_spill_and_read_back_identically() {
        let vfs = Vfs::new((), Some(10));
        vfs.write("small.rs", b"fits".to_vec()).unwrap();
        vfs.write("big.rs", vec![b'b'; 64]).unwrap();
        vfs.write("later.rs", b"also spilled".to_vec()).unwrap();

        assert!(vfs.scratch.get().is_some());
        assert_eq!(&*vfs.read(Path::new("small.rs")).unwrap(), b"fits");
        assert_eq!(&*vfs.read(Path::new("big.rs")).unwrap(), &[b'b'; 64][..]);
        assert_eq!(&*vfs.read(Path::new("later.rs")).unwrap(), b"also spilled");
        assert_eq!(vfs.metadata(Path::new("big.rs")).unwrap().len, 64);
    }

    #[test]
    fn a_zero_budget_spills_everything() {
        let vfs = Vfs::new((), Some(0));
        vfs.write("a.rs", b"a".to_vec()).unwrap();
        assert!(vfs.scratch.get().is_some());
        assert_eq!(&*vfs.read(Path::new("a.rs")).unwrap(), b"a");
    }

    /// Eight workers writing the same 80 bytes at once must charge the budget
    /// once; a budget of 100 leaves no room for a double charge.
    #[test]
    fn concurrent_writes_of_one_content_charge_the_budget_once() {
        let vfs = Vfs::new((), Some(100));
        let body = vec![b'x'; 80];
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let (vfs, body) = (&vfs, &body);
                scope.spawn(move || vfs.write(&format!("w{worker}.js"), body.clone()).unwrap());
            }
        });
        assert_eq!(vfs.len(), 8);
        assert!(
            vfs.scratch.get().is_none(),
            "the shared content was charged more than once"
        );
    }

    #[test]
    fn concurrent_writers_share_the_store_safely() {
        let vfs = Vfs::new((), Some(1_000));
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let vfs = &vfs;
                scope.spawn(move || {
                    for i in 0..200 {
                        let body = format!("worker {worker} file {i}").repeat(4).into_bytes();
                        vfs.write(&format!("w{worker}/f{i}.txt"), body).unwrap();
                    }
                });
            }
        });
        assert_eq!(vfs.len(), 1_600);
        assert_eq!(
            vfs.read_to_string(Path::new("w3/f7.txt")).unwrap(),
            "worker 3 file 7".repeat(4)
        );
        assert_eq!(vfs.read_dir(Path::new("w5")).unwrap().len(), 200);
    }
}
