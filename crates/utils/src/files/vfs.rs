//! The repository as a filesystem. Readers see `std::fs` verbs over
//! repo-relative paths (`/` is the repository root); where a file's bytes
//! live is the store's business. Bytes written to it are content-addressed,
//! so identical files at many paths are kept once, resident in memory up to
//! a budget and appended to one anonymous scratch file past it. A checkout
//! on disk is linked, never copied.

use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex, OnceLock};

use rustc_hash::FxHashMap;
use sha2::{Digest, Sha256};

use super::Counter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub len: u64,
    pub is_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
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
    paths: Mutex<FxHashMap<String, Slot>>,
    blobs: Mutex<FxHashMap<ContentId, Blob>>,
    children: Mutex<FxHashMap<String, FxHashMap<String, bool>>>,
    resident: Counter,
    scratch: OnceLock<Scratch>,
}

/// Where a path's bytes are.
#[derive(Debug, Clone)]
enum Slot {
    Stored(ContentId),
    Linked {
        on_disk: std::path::PathBuf,
        len: u64,
    },
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
        Self::with_budget(None)
    }
}

impl Vfs {
    /// `budget` caps the bytes kept in memory; `Some(0)` spills everything,
    /// `None` keeps everything.
    pub fn with_budget(budget: Option<u64>) -> Self {
        Self {
            paths: Mutex::default(),
            blobs: Mutex::default(),
            children: Mutex::default(),
            resident: Counter::new("resident_bytes", budget),
            scratch: OnceLock::new(),
        }
    }

    /// Store a file's bytes under `path`. Bytes already stored under another
    /// path are shared, not copied.
    pub fn write(&self, path: &str, bytes: Vec<u8>) -> io::Result<()> {
        let key = key(Path::new(path)).ok_or_else(not_found)?;
        let id = ContentId::of(&bytes);
        let known = lock(&self.blobs).contains_key(&id);
        if !known {
            let blob = match self.resident.add(bytes.len() as u64) {
                Ok(()) => Blob::Memory(bytes.into()),
                Err(_) => Blob::Spilled {
                    len: bytes.len() as u64,
                    offset: self.scratch()?.append(&bytes)?,
                },
            };
            lock(&self.blobs).entry(id).or_insert(blob);
        }
        self.index(&key);
        lock(&self.paths).insert(key, Slot::Stored(id));
        Ok(())
    }

    /// Present a file that stays where it is on disk.
    pub fn link(&self, path: &str, on_disk: std::path::PathBuf, len: u64) {
        let Some(key) = key(Path::new(path)) else {
            return;
        };
        self.index(&key);
        lock(&self.paths).insert(key, Slot::Linked { on_disk, len });
    }

    /// The identity of a stored file's content; `None` for linked files,
    /// whose bytes were never read.
    pub fn content_id(&self, path: &Path) -> Option<ContentId> {
        match lock(&self.paths).get(&key(path)?)? {
            Slot::Stored(id) => Some(*id),
            Slot::Linked { .. } => None,
        }
    }

    pub fn len(&self) -> usize {
        lock(&self.paths).len()
    }

    /// Every file path, repository-relative, in no particular order.
    pub fn paths(&self) -> Vec<String> {
        lock(&self.paths).keys().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

/// Reading, with `std::fs` semantics: a missing path is `NotFound`, reading
/// a directory is an error, `read_dir` lists direct children in no
/// particular order.
impl Vfs {
    pub fn read(&self, path: &Path) -> io::Result<Arc<[u8]>> {
        let key = key(path).ok_or_else(not_found)?;
        let slot = lock(&self.paths).get(&key).cloned();
        match slot {
            Some(Slot::Stored(id)) => {
                let blob = lock(&self.blobs).get(&id).cloned();
                match blob {
                    Some(Blob::Memory(bytes)) => Ok(bytes),
                    Some(Blob::Spilled { offset, len }) => self.scratch()?.read(offset, len),
                    None => Err(not_found()),
                }
            }
            Some(Slot::Linked { on_disk, .. }) => std::fs::read(on_disk).map(Into::into),
            None if lock(&self.children).contains_key(&key) => Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("{key} is a directory"),
            )),
            None => Err(not_found()),
        }
    }

    pub fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        let key = key(path).ok_or_else(not_found)?;
        if let Some(slot) = lock(&self.paths).get(&key) {
            let len = match slot {
                Slot::Linked { len, .. } => *len,
                Slot::Stored(id) => match lock(&self.blobs).get(id) {
                    Some(Blob::Memory(bytes)) => bytes.len() as u64,
                    Some(Blob::Spilled { len, .. }) => *len,
                    None => 0,
                },
            };
            return Ok(Metadata { len, is_dir: false });
        }
        if key.is_empty() || lock(&self.children).contains_key(&key) {
            return Ok(Metadata {
                len: 0,
                is_dir: true,
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
            None if lock(&self.paths).contains_key(&key) => Err(io::Error::new(
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

    fn names(mut entries: Vec<DirEntry>) -> Vec<(String, bool)> {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        entries.into_iter().map(|e| (e.name, e.is_dir)).collect()
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
                is_dir: false
            }
        );
        assert!(vfs.is_dir(Path::new("src")));
        assert!(vfs.is_dir(Path::new("/")));
        assert!(vfs.is_dir(Path::new("")));
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

    #[test]
    fn identical_content_at_many_paths_is_stored_once_and_stays_many_files() {
        let vfs = Vfs::with_budget(Some(100));
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
        let vfs = Vfs::with_budget(Some(10));
        vfs.write("small.rs", b"fits".to_vec()).unwrap();
        vfs.write("big.rs", vec![b'b'; 64].clone()).unwrap();
        vfs.write("later.rs", b"also spilled".to_vec()).unwrap();

        assert!(vfs.scratch.get().is_some());
        assert_eq!(&*vfs.read(Path::new("small.rs")).unwrap(), b"fits");
        assert_eq!(&*vfs.read(Path::new("big.rs")).unwrap(), &[b'b'; 64][..]);
        assert_eq!(&*vfs.read(Path::new("later.rs")).unwrap(), b"also spilled");
        assert_eq!(vfs.metadata(Path::new("big.rs")).unwrap().len, 64);
    }

    #[test]
    fn a_zero_budget_spills_everything() {
        let vfs = Vfs::with_budget(Some(0));
        vfs.write("a.rs", b"a".to_vec()).unwrap();
        assert!(vfs.scratch.get().is_some());
        assert_eq!(&*vfs.read(Path::new("a.rs")).unwrap(), b"a");
    }

    #[test]
    fn a_linked_checkout_is_read_from_where_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let on_disk = dir.path().join("lib.rs");
        std::fs::write(&on_disk, b"pub fn f() {}").unwrap();
        let vfs = Vfs::default();
        vfs.link("src/lib.rs", on_disk, 13);

        assert_eq!(
            &*vfs.read(Path::new("src/lib.rs")).unwrap(),
            b"pub fn f() {}"
        );
        assert_eq!(vfs.metadata(Path::new("src/lib.rs")).unwrap().len, 13);
        assert_eq!(vfs.content_id(Path::new("src/lib.rs")), None);
        assert!(vfs.is_dir(Path::new("src")));
    }

    #[test]
    fn concurrent_writers_share_the_store_safely() {
        let vfs = Vfs::with_budget(Some(1_000));
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
