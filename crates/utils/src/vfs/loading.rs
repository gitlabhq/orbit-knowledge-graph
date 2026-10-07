//! Concurrent loading uses per-shard locks for entries and content-addressed blobs.
//! Limits count offered files before policy runs. Rejected content is not materialized.
//! On-demand readers retain their backing; pending files are read for classification during loading.
//! Finishing sorts and deduplicates entries in place, retaining the latest entry per path.

use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard};

use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use sha2::{Digest, Sha256};

use super::limits::add_capped;
use super::path::key;
use super::scratch::Scratch;
use super::{Decision, File, Limits, Options, Pass, SourceError, Tag, Usage, Vfs};

pub enum Put<'a> {
    Bytes(Vec<u8>),
    /// Called synchronously at most once, only for `Keep` or `Pending`.
    ReadAndStore {
        size: u64,
        read: Box<dyn FnOnce() -> io::Result<Vec<u8>> + 'a>,
    },
    ReadOnDemand {
        size: u64,
        read: Arc<dyn Fn(u64) -> io::Result<Vec<u8>> + Send + Sync>,
    },
    Symlink(String),
}

const ENTRY_SHARDS: usize = 64;
const BLOB_SHARDS: usize = 256;

pub struct Loading<T> {
    passes: Box<dyn Pass<Tag = T>>,
    limits: Limits,
    cancelled: Option<Box<dyn Fn() -> bool + Send + Sync>>,
    entries: Vec<Mutex<Vec<VfsEntry<T>>>>,
    blobs: Vec<Mutex<FxHashMap<[u8; 32], Content>>>,
    scratch: Scratch,
    files: AtomicU64,
    bytes: AtomicU64,
    resident: AtomicU64,
    deduped: AtomicU64,
}

pub(super) struct VfsEntry<T> {
    pub(super) file: File<'static, T>,
    pub(super) content: Content,
}

#[derive(Clone)]
pub(super) enum Content {
    Unavailable,
    Failed(Arc<io::Error>),
    Memory(super::Bytes),
    Spilled { offset: u64, len: u64, raw_len: u64 },
    ReadOnDemand(Arc<dyn Fn(u64) -> io::Result<Vec<u8>> + Send + Sync>),
    Symlink(String),
}

impl<T: Tag> Loading<T> {
    pub(super) fn new(
        passes: impl Pass<Tag = T> + 'static,
        limits: Limits,
        options: Options,
    ) -> Self {
        Self {
            passes: Box::new(passes),
            scratch: Scratch::new(&options, limits.spilled_bytes),
            limits,
            cancelled: options.cancelled,
            entries: (0..ENTRY_SHARDS).map(|_| Mutex::default()).collect(),
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
            Put::ReadAndStore { size, .. } | Put::ReadOnDemand { size, .. } => *size,
            Put::Symlink(_) => 0,
        };
        add_capped(&self.files, "files", 1, self.limits.files.map(|n| n as u64))?;
        add_capped(&self.bytes, "total_bytes", size, self.limits.total_bytes)?;

        if let Put::Symlink(target) = what {
            let file = File::new(key, size, Decision::List("symlink"));
            self.add_entry(file, Content::Symlink(target));
            return Ok(());
        }
        let mut file = File::new(key, size, Decision::Pending);
        file.metadata_decision = match self.limits.file_bytes {
            Some(cap) if size > cap => Decision::List("oversize"),
            _ => self.passes.metadata(&file),
        };
        match (file.decision(), what) {
            (Decision::List(_), _) => self.add_entry(file, Content::Unavailable),
            (_, Put::Bytes(bytes)) => self.put_bytes(file, Ok(bytes), None)?,
            (_, Put::ReadAndStore { read, .. }) => self.put_bytes(file, read(), None)?,
            (Decision::Keep(_), Put::ReadOnDemand { read, .. }) => {
                self.add_entry(file, Content::ReadOnDemand(read))
            }
            (Decision::Pending, Put::ReadOnDemand { read, .. }) => {
                self.put_bytes(
                    file,
                    read(self.limits.file_bytes.unwrap_or(u64::MAX)),
                    Some(Content::ReadOnDemand(read)),
                )?;
            }
            (_, Put::Symlink(_)) => unreachable!("symlinks return above"),
        }
        Ok(())
    }

    fn put_bytes(
        &self,
        file: File<'static, T>,
        bytes: io::Result<Vec<u8>>,
        backing: Option<Content>,
    ) -> Result<(), SourceError> {
        let bytes = match bytes.and_then(|bytes| {
            if bytes.len() as u64 != file.size {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source size mismatch",
                ))
            } else {
                Ok(bytes)
            }
        }) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.add_entry(file, Content::Failed(Arc::new(error)));
                return Ok(());
            }
        };
        file.classify(&*self.passes, &bytes);
        let content = match (file.decision(), backing) {
            (Decision::Keep(_), Some(content)) => content,
            (Decision::Keep(_), None) => self.store(bytes)?,
            _ => Content::Unavailable,
        };
        self.add_entry(file, content);
        Ok(())
    }

    fn add_entry(&self, file: File<'static, T>, content: Content) {
        let mut hasher = FxHasher::default();
        file.path.hash(&mut hasher);
        let shard = &self.entries[hasher.finish() as usize % ENTRY_SHARDS];
        lock(shard).push(VfsEntry { file, content });
    }

    fn store(&self, bytes: Vec<u8>) -> Result<Content, SourceError> {
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
                    Ok(_) => Content::Memory(bytes.into()),
                    Err(_) => self.scratch.append(&bytes)?,
                };
                vacant.insert(blob)
            }
        };
        Ok(blob.clone())
    }

    pub(super) fn finish(self) -> Vfs<T> {
        let mut entries: Vec<VfsEntry<T>> = self
            .entries
            .into_iter()
            .flat_map(|shard| shard.into_inner().unwrap_or_else(|e| e.into_inner()))
            .collect();
        entries.par_sort_by(|a, b| a.file.path.cmp(&b.file.path));
        let offered = entries.len();
        entries.dedup_by(|later, earlier| {
            if later.file.path != earlier.file.path {
                return false;
            }
            std::mem::swap(later, earlier);
            true
        });
        let duplicate_paths = offered - entries.len();
        let links = entries
            .iter()
            .filter_map(|entry| match &entry.content {
                Content::Symlink(target) => Some((entry.file.path.to_string(), target.clone())),
                _ => None,
            })
            .collect();
        let usage = Usage {
            files: entries.len(),
            bytes: self.bytes.load(Relaxed),
            kept: 0,
            resident: self.resident.load(Relaxed),
            spilled: self.scratch.end.load(Relaxed),
            deduped_bytes: self.deduped.load(Relaxed),
            duplicate_paths,
        };
        Vfs {
            passes: self.passes,
            entries,
            links,
            scratch: self.scratch,
            usage,
            max_file_bytes: self.limits.file_bytes.unwrap_or(u64::MAX),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
