use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Compact, Edge, Tree};
use crate::{Error, Sentinel};

pub struct TreeStore {
    file: Mutex<std::fs::File>,
    loads: AtomicUsize,
    read_bytes: AtomicUsize,
    written_bytes: AtomicUsize,
    resident_bytes: AtomicUsize,
    peak_resident_bytes: AtomicUsize,
}

#[derive(Debug, serde::Serialize)]
pub struct StoreCounters {
    pub loads: usize,
    pub read_bytes: usize,
    pub written_bytes: usize,
    pub pinned_node_edge_bytes: usize,
    pub peak_pinned_node_edge_bytes: usize,
}

pub(crate) struct StoredFile {
    store: Arc<TreeStore>,
    offset: u64,
    length: usize,
    inline: Option<Box<Tree<Compact>>>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct FileSnapshot {
    nodes: Vec<super::CompactNode<super::Node>>,
    root: u32,
    tags: Vec<(u32, Vec<super::Tag>)>,
    local_edges: Vec<Edge>,
}

pub struct FileRecord {
    pub label: String,
    stored: StoredFile,
    pub(crate) root_tags: smallvec::SmallVec<[super::Tag; 2]>,
    pub(crate) symbol_updates: rustc_hash::FxHashMap<u32, u32>,
}

pub struct AcquiredTree {
    tree: Tree<Compact>,
    store: Arc<TreeStore>,
    bytes: usize,
    pub local_edges: Vec<Edge>,
}

pub struct TreeSession<'a> {
    files: TreeRepository<'a>,
    sentinels: &'a [&'a Sentinel],
    acquired: elsa::sync::FrozenMap<u32, Box<AcquiredTree>>,
}

impl<'a> TreeSession<'a> {
    pub fn new(files: &'a [FileRecord], sentinels: &'a [&'a Sentinel]) -> Self {
        Self::from_repository(TreeRepository::Stored(files, None), sentinels)
    }

    pub(crate) fn from_repository(
        files: TreeRepository<'a>,
        sentinels: &'a [&'a Sentinel],
    ) -> Self {
        Self {
            files,
            sentinels,
            acquired: elsa::sync::FrozenMap::new(),
        }
    }

    pub fn acquire(&self, file: u32, node: u32) -> Result<super::Cursor<'_, Compact>, Error> {
        self.sentinels
            .iter()
            .try_for_each(|sentinel| sentinel.check())?;
        if let TreeRepository::Resident(trees) = self.files {
            let tree = trees
                .get(file as usize)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unknown file index"))?;
            return Ok(super::Cursor::acquired(tree, file, node));
        }
        let tree = match self.acquired.get(&file) {
            Some(tree) => tree,
            None => {
                let TreeRepository::Stored(files, _) = self.files else {
                    unreachable!()
                };
                let record = files.get(file as usize).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "unknown file index")
                })?;
                self.acquired
                    .insert(file, Box::new(record.acquire(self.sentinels)?))
            }
        };
        Ok(super::Cursor::acquired(tree, file, node))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TreeRepository<'a> {
    Resident(&'a [Tree<Compact>]),
    Stored(&'a [FileRecord], Option<&'a Sentinel>),
}

pub(crate) enum TreeRead<'a> {
    Resident(&'a Tree<Compact>),
    Acquired(AcquiredTree),
}

pub(crate) struct TreeScan<'a> {
    files: TreeRepository<'a>,
    current: Option<(usize, TreeRead<'a>)>,
}

impl<'a> TreeScan<'a> {
    pub fn new(files: TreeRepository<'a>) -> Self {
        Self {
            files,
            current: None,
        }
    }

    pub fn acquire(&mut self, file: usize) -> Result<&Tree<Compact>, Error> {
        if self
            .current
            .as_ref()
            .is_none_or(|(current, _)| *current != file)
        {
            self.current = None;
            self.current = Some((file, self.files.acquire(file)?));
        }
        match &self.current {
            Some((_, tree)) => Ok(tree),
            None => unreachable!(),
        }
    }
}

impl Deref for TreeRead<'_> {
    type Target = Tree<Compact>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Resident(tree) => tree,
            Self::Acquired(tree) => tree,
        }
    }
}

impl<'a> TreeRepository<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Resident(trees) => trees.len(),
            Self::Stored(files, _) => files.len(),
        }
    }
    pub fn label(self, file: usize) -> &'a str {
        match self {
            Self::Resident(trees) => &trees[file].label,
            Self::Stored(files, _) => &files[file].label,
        }
    }
    pub fn acquire(self, file: usize) -> Result<TreeRead<'a>, Error> {
        match self {
            Self::Resident(trees) => Ok(TreeRead::Resident(&trees[file])),
            Self::Stored(files, run) => files[file].acquire(run.as_slice()).map(TreeRead::Acquired),
        }
    }

    pub fn with_run(self, run: &'a Sentinel) -> Self {
        match self {
            Self::Resident(_) => self,
            Self::Stored(files, _) => Self::Stored(files, Some(run)),
        }
    }
}

impl Deref for AcquiredTree {
    type Target = Tree<Compact>;
    fn deref(&self) -> &Self::Target {
        &self.tree
    }
}

impl AcquiredTree {
    pub fn into_parts(mut self) -> (Tree<Compact>, Vec<Edge>) {
        let tree = std::mem::replace(&mut self.tree, Tree::new(super::Node::default()).into());
        (tree, std::mem::take(&mut self.local_edges))
    }
}

impl Drop for AcquiredTree {
    fn drop(&mut self) {
        self.store
            .resident_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl TreeStore {
    pub fn new() -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            file: Mutex::new(tempfile::tempfile()?),
            loads: AtomicUsize::new(0),
            read_bytes: AtomicUsize::new(0),
            written_bytes: AtomicUsize::new(0),
            resident_bytes: AtomicUsize::new(0),
            peak_resident_bytes: AtomicUsize::new(0),
        }))
    }

    pub fn counters(&self) -> StoreCounters {
        StoreCounters {
            loads: self.loads.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            written_bytes: self.written_bytes.load(Ordering::Relaxed),
            pinned_node_edge_bytes: self.resident_bytes.load(Ordering::Relaxed),
            peak_pinned_node_edge_bytes: self.peak_resident_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn insert(
        self: &Arc<Self>,
        tree: Tree<Compact>,
        local_edges: Vec<Edge>,
        sentinels: &[&Sentinel],
    ) -> Result<FileRecord, Error> {
        sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
        let label = tree.label.clone();
        let root_tags = tree.tags.get(&tree.root).cloned().unwrap_or_default();
        if tree.storage.0.len() == 1 && local_edges.is_empty() {
            return Ok(FileRecord {
                label,
                stored: StoredFile {
                    store: self.clone(),
                    offset: 0,
                    length: 0,
                    inline: Some(Box::new(tree)),
                },
                root_tags,
                symbol_updates: Default::default(),
            });
        }
        let snapshot = FileSnapshot {
            nodes: tree.storage.0,
            root: tree.root,
            tags: tree
                .tags
                .into_iter()
                .map(|(node, tags)| (node, tags.into_vec()))
                .collect(),
            local_edges,
        };
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::BoxedError>(&snapshot).map_err(io::Error::other)?;
        sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
        let mut file = self
            .file
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(&bytes)?;
        sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
        self.written_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
        Ok(FileRecord {
            label,
            stored: StoredFile {
                store: self.clone(),
                offset,
                length: bytes.len(),
                inline: None,
            },
            root_tags,
            symbol_updates: Default::default(),
        })
    }
}

impl FileRecord {
    pub fn acquire(&self, sentinels: &[&Sentinel]) -> Result<AcquiredTree, Error> {
        sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
        let stored = &self.stored;
        let (mut tree, local_edges) = if let Some(tree) = &stored.inline {
            ((**tree).clone(), Vec::new())
        } else {
            let mut bytes = rkyv::util::AlignedVec::<16>::new();
            bytes.resize(stored.length, 0);
            {
                let mut file = stored
                    .store
                    .file
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
                file.seek(SeekFrom::Start(stored.offset))?;
                file.read_exact(&mut bytes)?;
            }
            sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
            let snapshot = rkyv::from_bytes::<FileSnapshot, rkyv::rancor::BoxedError>(&bytes)
                .map_err(io::Error::other)?;
            stored.store.loads.fetch_add(1, Ordering::Relaxed);
            stored
                .store
                .read_bytes
                .fetch_add(stored.length, Ordering::Relaxed);
            (
                Tree {
                    storage: Compact(snapshot.nodes),
                    root: snapshot.root,
                    label: self.label.clone(),
                    tags: snapshot
                        .tags
                        .into_iter()
                        .map(|(node, tags)| (node, smallvec::SmallVec::from_vec(tags)))
                        .collect(),
                    source: Arc::from(""),
                },
                snapshot.local_edges,
            )
        };
        if self.root_tags.is_empty() {
            tree.tags.remove(&tree.root);
        } else {
            tree.tags.insert(tree.root, self.root_tags.clone());
        }
        use super::Storage;
        for (&node, &symbol) in &self.symbol_updates {
            tree.storage.node_mut(node).sym = symbol;
        }
        sentinels.iter().try_for_each(|sentinel| sentinel.check())?;
        let size = tree.storage.0.capacity()
            * std::mem::size_of::<super::CompactNode<super::Node>>()
            + local_edges.capacity() * std::mem::size_of::<Edge>();
        let resident = stored
            .store
            .resident_bytes
            .fetch_add(size, Ordering::Relaxed)
            + size;
        stored
            .store
            .peak_resident_bytes
            .fetch_max(resident, Ordering::Relaxed);
        Ok(AcquiredTree {
            tree,
            store: stored.store.clone(),
            bytes: size,
            local_edges,
        })
    }
}

impl FileRecord {
    pub(crate) fn replace(
        &self,
        tree: Tree<Compact>,
        local_edges: Vec<Edge>,
        sentinels: &[&Sentinel],
    ) -> Result<Self, Error> {
        self.stored.store.insert(tree, local_edges, sentinels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::Node;

    #[test]
    fn scratch_reads_distinguish_deadlines_from_storage_failures() {
        let store = TreeStore::new().unwrap();
        let mut tree: Tree<Compact> = Tree::new(Node::default()).into();
        tree.label = "main.py".into();
        let file = store
            .insert(
                tree,
                vec![Edge::local(0, 0, super::super::EdgeKind::Calls)],
                &[],
            )
            .unwrap();
        let acquired = file.acquire(&[]).unwrap();
        assert_eq!(acquired.label, "main.py");
        assert!(store.counters().pinned_node_edge_bytes > 0);
        drop(acquired);
        assert_eq!(store.counters().pinned_node_edge_bytes, 0);

        let before = store.counters().loads;
        {
            let session = TreeSession::new(std::slice::from_ref(&file), &[]);
            assert_eq!(session.acquire(0, 0).unwrap().index(), 0);
            assert_eq!(session.acquire(0, 0).unwrap().index(), 0);
            assert_eq!(store.counters().loads, before + 1);
        }
        assert_eq!(store.counters().pinned_node_edge_bytes, 0);

        let killed = Sentinel::new("resolve", "main.py", 0);
        assert!(matches!(file.acquire(&[&killed]), Err(Error::Killed(_))));
        assert!(matches!(
            TreeSession::new(std::slice::from_ref(&file), &[&killed]).acquire(0, 0),
            Err(Error::Killed(_))
        ));
        store.file.lock().unwrap().set_len(0).unwrap();
        assert!(matches!(file.acquire(&[]), Err(Error::Storage(_))));
        assert!(matches!(
            TreeSession::new(std::slice::from_ref(&file), &[]).acquire(0, 0),
            Err(Error::Storage(_))
        ));
    }
}
