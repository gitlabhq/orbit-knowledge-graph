//! Concurrent loading uses per-shard locks for nodes and content-addressed blobs.
//! Limits count offered files before policy runs. Rejected content is not materialized.
//! Disk files are linked; metadata-pending files are read for classification during loading.
//! Freezing sorts and deduplicates nodes in place, retaining the latest entry per path.

use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard};

use rustc_hash::{FxHashMap, FxHasher};
use sha2::{Digest, Sha256};

use super::limits::add_capped;
use super::path::key;
use super::scratch::{Blob, Scratch};
use super::syscalls;
use super::{Decision, File, Limits, Options, Pass, SourceError, Tag, Usage, Vfs};

pub enum Put<'a> {
    Bytes(Vec<u8>),
    /// Called synchronously at most once, only for `Keep` or `Pending`.
    Lazy {
        size: u64,
        read: Box<dyn FnOnce() -> io::Result<Vec<u8>> + 'a>,
    },
    OnDisk {
        path: PathBuf,
        size: u64,
    },
    Symlink(String),
}

const NODE_SHARDS: usize = 64;
const BLOB_SHARDS: usize = 256;

pub struct Loading<T> {
    passes: Arc<dyn Pass<Tag = T>>,
    limits: Limits,
    cancelled: Option<Box<dyn Fn() -> bool + Send + Sync>>,
    nodes: Vec<Mutex<Vec<Node<T>>>>,
    blobs: Vec<Mutex<FxHashMap<[u8; 32], Blob>>>,
    scratch: Scratch,
    files: AtomicU64,
    bytes: AtomicU64,
    resident: AtomicU64,
    deduped: AtomicU64,
}

pub(super) struct Node<T> {
    pub(super) file: File<'static, T>,
    pub(super) slot: Option<Slot>,
}

pub(super) enum Slot {
    Stored(Blob),
    Linked(PathBuf),
    Link(String),
}

impl<T: Tag> Loading<T> {
    pub(super) fn new(
        passes: impl Pass<Tag = T> + 'static,
        limits: Limits,
        options: Options,
    ) -> Self {
        Self {
            passes: Arc::new(passes),
            scratch: Scratch::new(&options, limits.spilled_bytes),
            limits,
            cancelled: options.cancelled,
            nodes: (0..NODE_SHARDS).map(|_| Mutex::default()).collect(),
            blobs: (0..BLOB_SHARDS).map(|_| Mutex::default()).collect(),
            files: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            deduped: AtomicU64::new(0),
        }
    }

    pub fn put(&self, path: &str, what: Put<'_>) -> Result<(), SourceError> {
        if self.cancelled.as_ref().is_some_and(|cancelled| cancelled()) {
            return Err(SourceError::Cancelled);
        }
        let key = key(Path::new(path))
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid repository file path")
            })?;
        let size = match &what {
            Put::Bytes(bytes) => bytes.len() as u64,
            Put::Lazy { size, .. } | Put::OnDisk { size, .. } => *size,
            Put::Symlink(_) => 0,
        };
        add_capped(&self.files, "files", 1, self.limits.files.map(|n| n as u64))?;
        add_capped(&self.bytes, "total_bytes", size, self.limits.total_bytes)?;

        if let Put::Symlink(target) = what {
            let file = File::new(key, size, Decision::List("symlink"));
            self.add_node(file, Some(Slot::Link(target)));
            return Ok(());
        }
        let mut file = File::new(key, size, Decision::Pending);
        file.metadata_decision = match self.limits.file_bytes {
            Some(cap) if size > cap => Decision::List("oversize"),
            _ => self.passes.metadata(&file),
        };
        match (file.decision(), what) {
            (Decision::List(_), _) => self.add_node(file, None),
            (_, Put::Bytes(bytes)) => self.put_bytes(file, bytes, None)?,
            (_, Put::Lazy { read, .. }) => self.put_bytes(file, read()?, None)?,
            (Decision::Keep(_), Put::OnDisk { path, .. }) => {
                self.add_node(file, Some(Slot::Linked(path)))
            }
            (Decision::Pending, Put::OnDisk { path, .. }) => match syscalls::read(&path, size) {
                Ok(bytes) => self.put_bytes(file, bytes, Some(path))?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    file.metadata_decision = Decision::List("missing");
                    self.add_node(file, None);
                }
                Err(e) => return Err(e.into()),
            },
            (_, Put::Symlink(_)) => unreachable!("symlinks return above"),
        }
        Ok(())
    }

    fn put_bytes(
        &self,
        file: File<'static, T>,
        bytes: Vec<u8>,
        on_disk: Option<PathBuf>,
    ) -> Result<(), SourceError> {
        if bytes.len() as u64 != file.size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "source size mismatch").into());
        }
        file.classify(&*self.passes, &bytes);
        let slot = match (file.decision(), on_disk) {
            (Decision::Keep(_), Some(path)) => Some(Slot::Linked(path)),
            (Decision::Keep(_), None) => Some(Slot::Stored(self.store(bytes)?)),
            _ => None,
        };
        self.add_node(file, slot);
        Ok(())
    }

    fn add_node(&self, file: File<'static, T>, slot: Option<Slot>) {
        let mut hasher = FxHasher::default();
        file.path.hash(&mut hasher);
        let shard = &self.nodes[hasher.finish() as usize % NODE_SHARDS];
        lock(shard).push(Node { file, slot });
    }

    fn store(&self, bytes: Vec<u8>) -> Result<Blob, SourceError> {
        let id: [u8; 32] = Sha256::digest(&bytes).into();
        let len = bytes.len() as u64;
        let mut shard = lock(&self.blobs[id[0] as usize]);
        let blob = match shard.entry(id) {
            Entry::Occupied(entry) => {
                self.deduped.fetch_add(len, Relaxed);
                entry.into_mut()
            }
            Entry::Vacant(vacant) => {
                let blob = match add_capped(
                    &self.resident,
                    "resident_bytes",
                    len,
                    self.limits.resident_bytes,
                ) {
                    Ok(_) => Blob::Memory(bytes.into()),
                    Err(_) => self.scratch.append(&bytes)?,
                };
                vacant.insert(blob)
            }
        };
        Ok(blob.clone())
    }

    pub(super) fn freeze(self) -> Vfs<T> {
        let mut nodes: Vec<Node<T>> = self
            .nodes
            .into_iter()
            .flat_map(|shard| shard.into_inner().unwrap_or_else(|e| e.into_inner()))
            .collect();
        nodes.sort_by(|a, b| a.file.path.cmp(&b.file.path));
        let offered = nodes.len();
        nodes.dedup_by(|later, earlier| {
            if later.file.path != earlier.file.path {
                return false;
            }
            std::mem::swap(later, earlier);
            true
        });
        let duplicate_paths = offered - nodes.len();
        let links = nodes
            .iter()
            .filter_map(|node| match &node.slot {
                Some(Slot::Link(target)) => Some((node.file.path.to_string(), target.clone())),
                _ => None,
            })
            .collect();
        let usage = Usage {
            files: nodes.len(),
            bytes: self.bytes.load(Relaxed),
            kept: 0,
            resident: self.resident.load(Relaxed),
            spilled: self.scratch.end.load(Relaxed),
            deduped_bytes: self.deduped.load(Relaxed),
            duplicate_paths,
        };
        Vfs {
            passes: self.passes,
            nodes,
            links,
            scratch: self.scratch,
            usage,
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
