//! The store. `Loading` is the write side: sharded, lock-free on the hot
//! path, filled from any thread. `freeze` sorts the nodes once; the sorted
//! vec is the tree, so `Vfs` answers every read without a lock or a second
//! structure. Bytes are content-addressed: identical files at many paths
//! are kept once, in memory up to a budget and in one scratch file past it.
//! A checkout on disk is linked, never copied.

use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use rustc_hash::{FxHashMap, FxHasher};
use sha2::{Digest, Sha256};
use tracing::warn;

use super::{
    Bytes, CapExceeded, Decision, File, Limits, Options, Pass, Source, SourceError, Tag, Usage,
};

/// What a source hands the store for one path.
pub enum Put<'a> {
    /// Bytes in hand.
    Bytes(Vec<u8>),
    /// Bytes that cost something to produce: `read` is called only when a
    /// decision needs them, at most once.
    Lazy {
        size: u64,
        read: Box<dyn FnOnce() -> io::Result<Vec<u8>> + 'a>,
    },
    /// A file of a checkout: linked where it is, never copied.
    OnDisk { path: PathBuf, size: u64 },
    /// A symlink, target as written: relative to the link, or `/`-rooted.
    Symlink(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stat<T> {
    /// Canonical: `/`-rooted, `.` and `..` resolved, symlinks followed.
    pub path: PathBuf,
    pub kind: Kind,
    pub len: u64,
    /// Of the file reached. A directory has none.
    pub decision: Option<Decision<T>>,
    /// `Some(target)` when the path named a symlink itself.
    pub link: Option<PathBuf>,
}

const NODE_SHARDS: usize = 64;
const BLOB_SHARDS: usize = 256;
const MAX_LINK_DEPTH: usize = 40;
const LINK_REASON: &str = "symlink";
const OVERSIZE_REASON: &str = "oversize";

/// Write side. Nothing is readable until `freeze`.
pub struct Loading<T> {
    passes: Arc<dyn Pass<Tag = T>>,
    limits: Limits,
    cancelled: Option<Box<dyn Fn() -> bool + Send + Sync>>,
    nodes: Vec<Mutex<Vec<Node<T>>>>,
    blobs: Vec<Mutex<FxHashMap<ContentId, Blob>>>,
    scratch: Scratch,
    files: AtomicUsize,
    bytes: AtomicU64,
    resident: AtomicU64,
    deduped: AtomicU64,
}

/// Read side. Immutable; no lock on any verb.
pub struct Vfs<T> {
    passes: Arc<dyn Pass<Tag = T>>,
    nodes: Vec<Node<T>>,
    links: FxHashMap<String, String>,
    blobs: FxHashMap<ContentId, Blob>,
    scratch: Scratch,
    usage: Usage,
}

struct Node<T> {
    file: File<T>,
    slot: Option<Slot>,
    /// Whether the content passes have seen this file. A linked file kept on
    /// its header alone is checked on its first read.
    checked: bool,
}

enum Slot {
    Stored(ContentId),
    Linked(PathBuf),
    Link(String),
}

/// SHA-256 of a file's bytes: the identity content is stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ContentId([u8; 32]);

#[derive(Debug, Clone)]
enum Blob {
    Memory(Bytes),
    Spilled { offset: u64, len: u64, raw_len: u64 },
}

/// One anonymous append-only file, opened on the first spill: positional
/// writes from any thread, positional reads, gone when the store is.
struct Scratch {
    dir: Option<PathBuf>,
    compress: bool,
    cap: Option<u64>,
    file: OnceLock<std::fs::File>,
    end: AtomicU64,
}

impl Scratch {
    fn new(options: &Options, cap: Option<u64>) -> Self {
        Self {
            dir: options.scratch_dir.clone(),
            compress: options.compress_spill,
            cap,
            file: OnceLock::new(),
            end: AtomicU64::new(0),
        }
    }

    fn append(&self, bytes: &[u8]) -> Result<Blob, SourceError> {
        let raw_len = bytes.len() as u64;
        let compressed;
        let bytes = match self.compress {
            true => {
                compressed = lz4_flex::block::compress(bytes);
                compressed.as_slice()
            }
            false => bytes,
        };
        let len = bytes.len() as u64;
        let offset = charge(&self.end, "spilled_bytes", len, self.cap)?;
        self.file()?.write_all_at(bytes, offset)?;
        Ok(Blob::Spilled {
            offset,
            len,
            raw_len,
        })
    }

    fn read(&self, offset: u64, len: u64, raw_len: u64) -> io::Result<Bytes> {
        let mut bytes = vec![0u8; len as usize];
        self.file()?.read_exact_at(&mut bytes, offset)?;
        if self.compress {
            bytes = lz4_flex::block::decompress(&bytes, raw_len as usize)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
        Ok(bytes.into())
    }

    fn file(&self) -> io::Result<&std::fs::File> {
        if let Some(file) = self.file.get() {
            return Ok(file);
        }
        let file = match &self.dir {
            Some(dir) => tempfile::tempfile_in(dir)?,
            None => tempfile::tempfile()?,
        };
        let _ = self.file.set(file);
        Ok(self.file.get().expect("scratch file was just set"))
    }

    fn spilled(&self) -> u64 {
        self.end.load(Relaxed)
    }
}

/// Putting files in, from any thread.
impl<T: Tag> Loading<T> {
    pub fn new(passes: impl Pass<Tag = T> + 'static, limits: Limits, options: Options) -> Self {
        Self {
            passes: Arc::new(passes),
            scratch: Scratch::new(&options, limits.spilled_bytes),
            limits,
            cancelled: options.cancelled,
            nodes: (0..NODE_SHARDS).map(|_| Mutex::default()).collect(),
            blobs: (0..BLOB_SHARDS).map(|_| Mutex::default()).collect(),
            files: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            deduped: AtomicU64::new(0),
        }
    }

    /// One file. The header passes run now; the content passes run on the
    /// one read, which happens here for `Bytes` and `Lazy`, and on the first
    /// `read` for a linked file no pass asked to see first. A path that
    /// climbs above the root is not a file of the repository.
    pub fn put(&self, path: &str, what: Put<'_>) -> Result<(), SourceError> {
        if self.cancelled.as_ref().is_some_and(|cancelled| cancelled()) {
            return Err(SourceError::Cancelled);
        }
        let Some(key) = key(Path::new(path)).filter(|key| !key.is_empty()) else {
            return Ok(());
        };
        let size = match &what {
            Put::Bytes(bytes) => bytes.len() as u64,
            Put::Lazy { size, .. } | Put::OnDisk { size, .. } => *size,
            Put::Symlink(_) => 0,
        };
        charge(&self.files, "files", 1, self.limits.files.map(|n| n as u64))?;
        charge(&self.bytes, "total_bytes", size, self.limits.total_bytes)?;

        let mut file = File::new(key, size);
        if let Put::Symlink(target) = what {
            file.decide(Decision::List(LINK_REASON));
            return self.keep(file, Some(Slot::Link(target)), true);
        }
        match self.limits.file_bytes {
            Some(cap) if size > cap => file.decide(Decision::List(OVERSIZE_REASON)),
            _ => self.passes.header(&mut file),
        }
        match (file.decision(), what) {
            (Decision::Drop(_), _) => Ok(()),
            (Decision::List(_), _) => self.keep(file, None, true),
            (_, Put::Bytes(bytes)) => self.put_bytes(file, bytes),
            (_, Put::Lazy { read, .. }) => self.put_bytes(file, read()?),
            (Decision::Keep(_), Put::OnDisk { path, .. }) => {
                self.keep(file, Some(Slot::Linked(path)), false)
            }
            (Decision::Pending, Put::OnDisk { path, .. }) => self.put_sniffed(file, path),
            (_, Put::Symlink(_)) => unreachable!("symlinks return above"),
        }
    }

    fn put_bytes(&self, mut file: File<T>, bytes: Vec<u8>) -> Result<(), SourceError> {
        self.passes.content(&mut file, &bytes);
        file.settle();
        let slot = match file.keeps() {
            true => Some(Slot::Stored(self.store(bytes)?)),
            false => None,
        };
        self.keep(file, slot, true)
    }

    /// A pass wants the bytes before deciding: read for the decision only,
    /// then link. A live checkout moves under us; a file gone between its
    /// listing and this read is not a file of the repository.
    fn put_sniffed(&self, mut file: File<T>, on_disk: PathBuf) -> Result<(), SourceError> {
        let bytes = match std::fs::read(&on_disk) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(path = file.path, error = %e, "skipping a file that vanished before it was read");
                return Ok(());
            }
        };
        self.passes.content(&mut file, &bytes);
        file.settle();
        let slot = file.keeps().then_some(Slot::Linked(on_disk));
        self.keep(file, slot, true)
    }

    fn keep(&self, file: File<T>, slot: Option<Slot>, checked: bool) -> Result<(), SourceError> {
        let shard = &self.nodes[hash(&file.path) % NODE_SHARDS];
        lock(shard).push(Node {
            file,
            slot,
            checked,
        });
        Ok(())
    }

    /// Bytes stored once per distinct content. Checked and inserted under
    /// one shard lock, so two workers adding the same content cannot both
    /// pay for it.
    fn store(&self, bytes: Vec<u8>) -> Result<ContentId, SourceError> {
        let id = ContentId(Sha256::digest(&bytes).into());
        let len = bytes.len() as u64;
        let mut shard = lock(&self.blobs[id.0[0] as usize]);
        match shard.entry(id) {
            Entry::Occupied(_) => {
                self.deduped.fetch_add(len, Relaxed);
            }
            Entry::Vacant(vacant) => {
                let blob = match charge(
                    &self.resident,
                    "resident_bytes",
                    len,
                    self.limits.resident_bytes,
                ) {
                    Ok(_) => Blob::Memory(bytes.into()),
                    Err(_) => {
                        self.resident.fetch_sub(len, Relaxed);
                        self.scratch.append(&bytes)?
                    }
                };
                vacant.insert(blob);
            }
        }
        Ok(id)
    }

    /// Sort once; the sorted vec is the tree. Two entries for one path keep
    /// the later one, as `tar x` would.
    fn freeze(self) -> Vfs<T> {
        let mut nodes: Vec<Node<T>> = self
            .nodes
            .into_iter()
            .flat_map(|shard| shard.into_inner().unwrap_or_else(|e| e.into_inner()))
            .collect();
        nodes.sort_by(|a, b| a.file.path.cmp(&b.file.path));
        let offered = nodes.len();
        let mut deduped: Vec<Node<T>> = Vec::with_capacity(offered);
        for node in nodes {
            match deduped.last_mut() {
                Some(last) if last.file.path == node.file.path => *last = node,
                _ => deduped.push(node),
            }
        }
        let links = deduped
            .iter()
            .filter_map(|node| match &node.slot {
                Some(Slot::Link(target)) => Some((node.file.path.clone(), target.clone())),
                _ => None,
            })
            .collect();
        let blobs = self
            .blobs
            .into_iter()
            .flat_map(|shard| shard.into_inner().unwrap_or_else(|e| e.into_inner()))
            .collect();
        let usage = Usage {
            files: deduped.len(),
            bytes: self.bytes.load(Relaxed),
            kept: 0,
            resident: self.resident.load(Relaxed),
            spilled: self.scratch.spilled(),
            deduped_bytes: self.deduped.load(Relaxed),
            duplicate_paths: offered - deduped.len(),
        };
        Vfs {
            passes: self.passes,
            nodes: deduped,
            links,
            blobs,
            scratch: self.scratch,
            usage,
        }
    }
}

/// Reading, with `std::fs` semantics: a missing path is `NotFound`, reading
/// a directory is `IsADirectory`, a file kept without bytes is
/// `Unsupported`, and every verb follows symlinks.
impl<T: Tag> Vfs<T> {
    /// The one constructor: fill from `source`, then freeze.
    pub fn load(
        source: impl Source,
        passes: impl Pass<Tag = T> + 'static,
        limits: Limits,
        options: Options,
    ) -> Result<Self, SourceError> {
        let loading = Loading::new(passes, limits, options);
        source.fill(&loading)?;
        Ok(loading.freeze())
    }

    /// The bytes of a file. On a linked file no pass has seen, the content
    /// passes run on this first read, and a refusal becomes the file's
    /// decision from here on.
    pub fn read(&self, path: &Path) -> io::Result<Bytes> {
        let key = self.resolve(path)?;
        let Some(node) = self.node(&key) else {
            return Err(match self.is_dir(&key) {
                true => {
                    io::Error::new(io::ErrorKind::IsADirectory, format!("{key} is a directory"))
                }
                false => not_found(),
            });
        };
        let (Some(slot), Decision::Keep(_)) = (&node.slot, node.file.decision()) else {
            return Err(unsupported(&node.file));
        };
        let bytes = match slot {
            Slot::Stored(id) => self.blob(id)?,
            Slot::Linked(on_disk) => std::fs::read(on_disk)?.into(),
            Slot::Link(_) => return Err(not_found()),
        };
        if node.checked {
            return Ok(bytes);
        }
        let verdict = node.file.verdict.get_or_init(|| {
            let mut checked = File::new(node.file.path.clone(), node.file.size);
            checked.decide(node.file.decided);
            self.passes.content(&mut checked, &bytes);
            checked.decided
        });
        match verdict {
            Decision::Keep(_) => Ok(bytes),
            _ => Err(unsupported(&node.file)),
        }
    }

    /// Direct child names, in order.
    pub fn read_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        let key = self.resolve(path)?;
        if self.node(&key).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!("{key} is a file"),
            ));
        }
        if !self.is_dir(&key) {
            return Err(not_found());
        }
        let mut names: Vec<String> = Vec::new();
        for file in self.subtree_of(&key) {
            let rest = &file.path[key.len()..];
            let name = rest.trim_start_matches('/').split('/').next().unwrap_or("");
            if names.last().is_none_or(|last| last != name) {
                names.push(name.to_owned());
            }
        }
        Ok(names)
    }

    /// What is at `path`: the node reached through any symlinks, plus the
    /// link target if `path` itself is one. A dangling link is `NotFound`.
    pub fn stat(&self, path: &Path) -> io::Result<Stat<T>> {
        let own = self.resolve_parents(path)?;
        let link = self.links.get(&own).map(PathBuf::from);
        let key = self.resolve(path)?;
        let canonical = Path::new("/").join(&key);
        if let Some(node) = self.node(&key) {
            return Ok(Stat {
                path: canonical,
                kind: Kind::File,
                len: node.file.size,
                decision: Some(node.file.decision()),
                link,
            });
        }
        if !self.is_dir(&key) {
            return Err(not_found());
        }
        Ok(Stat {
            path: canonical,
            kind: Kind::Dir,
            len: 0,
            decision: None,
            link,
        })
    }

    /// Every file, sorted by path. Reflects decisions made on reads.
    pub fn files(&self) -> impl Iterator<Item = &File<T>> + use<'_, T> {
        self.nodes.iter().map(|node| &node.file)
    }

    /// The files below `dir`, sorted. Finding them is a binary search; the
    /// range is contiguous because the vec is sorted.
    pub fn subtree(&self, dir: &Path) -> impl Iterator<Item = &File<T>> + use<'_, T> {
        let key = key(dir).unwrap_or_default();
        self.subtree_of(&key)
    }

    /// `kept` is summed now, so a refusal on a first read is reflected.
    pub fn usage(&self) -> Usage {
        Usage {
            kept: self.files().filter(|f| f.keeps()).map(|f| f.size).sum(),
            ..self.usage
        }
    }

    fn subtree_of(&self, key: &str) -> impl Iterator<Item = &File<T>> + use<'_, T> {
        let prefix = match key.is_empty() {
            true => String::new(),
            false => format!("{key}/"),
        };
        let start = self.nodes.partition_point(|n| n.file.path < prefix);
        let end = start + self.nodes[start..].partition_point(|n| n.file.path.starts_with(&prefix));
        self.nodes[start..end].iter().map(|node| &node.file)
    }

    fn node(&self, key: &str) -> Option<&Node<T>> {
        self.nodes
            .binary_search_by(|node| node.file.path.as_str().cmp(key))
            .ok()
            .map(|i| &self.nodes[i])
    }

    fn is_dir(&self, key: &str) -> bool {
        key.is_empty() || self.subtree_of(key).next().is_some()
    }

    fn blob(&self, id: &ContentId) -> io::Result<Bytes> {
        match self.blobs.get(id) {
            Some(Blob::Memory(bytes)) => Ok(bytes.clone()),
            Some(Blob::Spilled {
                offset,
                len,
                raw_len,
            }) => self.scratch.read(*offset, *len, *raw_len),
            None => Err(not_found()),
        }
    }

    /// The key a path names once every symlink in it is followed.
    fn resolve(&self, path: &Path) -> io::Result<String> {
        let mut key = key(path).ok_or_else(not_found)?;
        if self.links.is_empty() {
            return Ok(key);
        }
        for _ in 0..MAX_LINK_DEPTH {
            match follow_first_link(&key, &self.links) {
                Some(next) => key = next?,
                None => return Ok(key),
            }
        }
        Err(io::Error::other(format!(
            "{} passes through too many symlinks",
            path.display()
        )))
    }

    /// Like `resolve`, but a symlink as the last component stays itself.
    fn resolve_parents(&self, path: &Path) -> io::Result<String> {
        let key = key(path).ok_or_else(not_found)?;
        let Some((parent, name)) = key.rsplit_once('/') else {
            return Ok(key);
        };
        let parent = self.resolve(Path::new(parent))?;
        Ok(match parent.is_empty() {
            true => name.to_string(),
            false => format!("{parent}/{name}"),
        })
    }
}

impl<T> std::fmt::Debug for Vfs<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vfs")
            .field("usage", &self.usage)
            .field("distinct_contents", &self.blobs.len())
            .finish()
    }
}

/// A running total with a cap: the first `add` to overflow trips it. Returns
/// the total before the add, which is the offset for an append.
fn charge<A: Atomic>(
    total: &A,
    metric: &'static str,
    n: u64,
    cap: Option<u64>,
) -> Result<u64, CapExceeded> {
    let before = total.fetch_add(n);
    let count = before.saturating_add(n);
    match cap.filter(|&cap| count > cap) {
        Some(cap) => Err(CapExceeded { metric, count, cap }),
        None => Ok(before),
    }
}

trait Atomic {
    fn fetch_add(&self, n: u64) -> u64;
}
impl Atomic for AtomicU64 {
    fn fetch_add(&self, n: u64) -> u64 {
        AtomicU64::fetch_add(self, n, Relaxed)
    }
}
impl Atomic for AtomicUsize {
    fn fetch_add(&self, n: u64) -> u64 {
        AtomicUsize::fetch_add(self, n as usize, Relaxed) as u64
    }
}

/// A repo-relative `/`-joined key; the repository root is `""`. `.` and `..`
/// resolve lexically; `None` if the path climbs above the root.
fn key(path: &Path) -> Option<String> {
    let mut key = String::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                if !key.is_empty() {
                    key.push('/');
                }
                key.push_str(&part.to_string_lossy());
            }
            Component::CurDir => {}
            Component::RootDir => key.clear(),
            Component::ParentDir => {
                if key.is_empty() {
                    return None;
                }
                key.truncate(key.rfind('/').unwrap_or(0));
            }
            Component::Prefix(_) => return None,
        }
    }
    Some(key)
}

/// Replace the first symlink component of `key` with its target: the rest of
/// the key follows. `None` when no component is a symlink; an error when the
/// target climbs out of the repository.
fn follow_first_link(key: &str, links: &FxHashMap<String, String>) -> Option<io::Result<String>> {
    let mut end = 0;
    loop {
        end = match key[end..].find('/') {
            Some(i) => end + i,
            None => key.len(),
        };
        let prefix = &key[..end];
        if let Some(target) = links.get(prefix) {
            let rest = &key[end..];
            let parent = prefix.rsplit_once('/').map_or("", |(parent, _)| parent);
            let resolved = match target.starts_with('/') {
                true => PathBuf::from(target),
                false => Path::new(parent).join(target),
            };
            return Some(
                self::key(&resolved)
                    .map(|k| format!("{k}{rest}"))
                    .ok_or_else(not_found),
            );
        }
        if end == key.len() {
            return None;
        }
        end += 1;
    }
}

fn hash(path: &str) -> usize {
    let mut hasher = FxHasher::default();
    path.hash(&mut hasher);
    hasher.finish() as usize
}

fn not_found() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "no such file in the repository")
}

fn unsupported<T: Tag>(file: &File<T>) -> io::Error {
    let why = match file.decision() {
        Decision::List(why) | Decision::Drop(why) => why,
        _ => "no bytes",
    };
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{} is listed only: {why}", file.path),
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
