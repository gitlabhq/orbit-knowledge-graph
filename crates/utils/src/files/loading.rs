//! The write side. A source puts files in from any thread; nodes and blobs
//! are sharded so the hot path takes no shared lock. `freeze` sorts once
//! and hands the result to `Vfs`.

use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard};

use rustc_hash::{FxHashMap, FxHasher};
use sha2::{Digest, Sha256};
use tracing::warn;

use super::limits::charge;
use super::path::key;
use super::scratch::{Blob, Scratch};
use super::{Decision, File, Limits, Options, Pass, SourceError, Tag, Usage, Vfs};

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

const NODE_SHARDS: usize = 64;
const BLOB_SHARDS: usize = 256;
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

pub(super) struct Node<T> {
    pub(super) file: File<T>,
    pub(super) slot: Option<Slot>,
    /// Whether the content passes have seen this file. A linked file kept on
    /// its header alone is checked on its first read.
    pub(super) checked: bool,
}

pub(super) enum Slot {
    Stored(ContentId),
    Linked(PathBuf),
    Link(String),
}

/// SHA-256 of a file's bytes: the identity content is stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ContentId(pub(super) [u8; 32]);

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
        match file.decision() {
            Decision::Drop(_) => Ok(()),
            Decision::Keep(_) => {
                let id = self.store(bytes)?;
                self.keep(file, Some(Slot::Stored(id)), true)
            }
            _ => self.keep(file, None, true),
        }
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
        match file.decision() {
            Decision::Drop(_) => Ok(()),
            Decision::Keep(_) => self.keep(file, Some(Slot::Linked(on_disk)), true),
            _ => self.keep(file, None, true),
        }
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
    pub(super) fn freeze(self) -> Vfs<T> {
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

fn hash(path: &str) -> usize {
    let mut hasher = FxHasher::default();
    path.hash(&mut hasher);
    hasher.finish() as usize
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
