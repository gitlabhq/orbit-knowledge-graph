use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};

use super::Tree;
use crate::pipeline::TreeSnapshot;
use foyer::{Cache, CacheBuilder, LruConfig};

type WriteRequest = (u64, Vec<u8>, mpsc::Sender<Result<(), String>>);

pub struct TreeStore {
    cache: Cache<u64, Arc<Tree>>,
    directory: Arc<tempfile::TempDir>,
    writer: mpsc::SyncSender<WriteRequest>,
    next: AtomicU64,
    reads: AtomicU64,
    rebuilds: AtomicU64,
}

pub(crate) struct StoredTree {
    store: Arc<dyn TreeBackend>,
    key: u64,
    completion: Mutex<Option<mpsc::Receiver<Result<(), String>>>>,
    ready: OnceLock<Result<(), String>>,
    loading: Mutex<()>,
}

trait TreeBackend: Send + Sync {
    fn get(&self, key: u64) -> Option<Arc<Tree>>;
    fn remove(&self, key: u64);
    fn insert(&self, key: u64, tree: Tree);
    fn rebuild(&self, key: u64) -> Result<Arc<Tree>, String>;
}

impl TreeBackend for TreeStore {
    fn remove(&self, key: u64) {
        self.cache.remove(&key);
    }
    fn insert(&self, key: u64, tree: Tree) {
        self.cache.insert(key, Arc::new(tree));
    }
    fn get(&self, key: u64) -> Option<Arc<Tree>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.cache.get(&key).map(|entry| entry.value().clone())
    }

    fn rebuild(&self, key: u64) -> Result<Arc<Tree>, String> {
        let bytes = std::fs::read(self.directory.path().join(key.to_string()))
            .map_err(|error| error.to_string())?;
        let snapshot = rkyv::from_bytes::<TreeSnapshot, rkyv::rancor::BoxedError>(&bytes)
            .map_err(|error| error.to_string())?;
        self.rebuilds.fetch_add(1, Ordering::Relaxed);
        let tree = Arc::new(Tree::from(snapshot));
        self.cache.insert(key, tree.clone());
        Ok(tree)
    }
}

pub(crate) enum TreeRead<'a> {
    Resident(&'a Tree),
    Cached(Arc<Tree>),
}

impl Deref for TreeRead<'_> {
    type Target = Tree;
    fn deref(&self) -> &Tree {
        match self {
            Self::Resident(tree) => tree,
            Self::Cached(tree) => tree,
        }
    }
}

pub(crate) struct TreeSession<'a> {
    trees: &'a [Tree],
    pinned: elsa::sync::FrozenMap<u32, Arc<Tree>>,
}

impl<'a> TreeSession<'a> {
    pub(crate) fn metadata(&self, file: u32) -> &Tree {
        &self.trees[file as usize]
    }
    pub(crate) fn new(trees: &'a [Tree]) -> Self {
        Self {
            trees,
            pinned: Default::default(),
        }
    }

    pub(crate) fn get(&self, file: u32) -> &Tree {
        let tree = &self.trees[file as usize];
        if tree.stored.is_none() {
            return tree;
        }
        if let Some(tree) = self.pinned.get(&file) {
            return tree;
        }
        match &tree.stored {
            None => tree,
            Some(stored) => match stored.load() {
                Ok(tree) => self.pinned.insert(file, tree),
                Err(error) => panic!("tree store: {error}"),
            },
        }
    }
}

impl TreeStore {
    pub fn new(memory_bytes: usize) -> std::io::Result<Arc<Self>> {
        let directory = Arc::new(tempfile::tempdir()?);
        let writer_directory = directory.clone();
        let (writer, requests) = mpsc::sync_channel::<WriteRequest>(2);
        std::thread::Builder::new()
            .name("linked-tree-writer".into())
            .spawn(move || {
                for (key, bytes, done) in requests {
                    let result =
                        std::fs::write(writer_directory.path().join(key.to_string()), bytes)
                            .map_err(|error| error.to_string());
                    let _ = done.send(result);
                }
            })?;
        Ok(Arc::new(Self {
            cache: CacheBuilder::new(memory_bytes)
                .with_shards(1)
                .with_eviction_config(LruConfig::default())
                .with_weighter(|_: &u64, tree: &Arc<Tree>| {
                    tree.arena.capacity() * std::mem::size_of::<indextree::Node<super::Node>>()
                })
                .build(),
            directory,
            writer,
            next: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            rebuilds: AtomicU64::new(0),
        }))
    }

    pub fn counters(&self) -> (u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.rebuilds.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn write(self: &Arc<Self>, tree: &Tree) -> std::io::Result<Arc<StoredTree>> {
        let key = self.next.fetch_add(1, Ordering::Relaxed);
        let snapshot = TreeSnapshot::from(tree);
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::BoxedError>(&snapshot).map_err(std::io::Error::other)?;
        let (done, completion) = mpsc::channel();
        self.writer
            .send((key, bytes.to_vec(), done))
            .map_err(std::io::Error::other)?;
        Ok(Arc::new(StoredTree {
            store: self.clone(),
            key,
            completion: Mutex::new(Some(completion)),
            ready: OnceLock::new(),
            loading: Mutex::new(()),
        }))
    }
}

impl StoredTree {
    fn load(&self) -> Result<Arc<Tree>, String> {
        if let Some(tree) = self.store.get(self.key) {
            return Ok(tree);
        }
        let _loading = self.loading.lock().map_err(|error| error.to_string())?;
        if let Some(tree) = self.store.get(self.key) {
            return Ok(tree);
        }
        self.ready
            .get_or_init(|| {
                let receiver = self
                    .completion
                    .lock()
                    .map_err(|error| error.to_string())?
                    .take()
                    .ok_or("missing write completion")?;
                receiver.recv().map_err(|error| error.to_string())?
            })
            .clone()?;
        self.store.rebuild(self.key)
    }
}

impl Drop for StoredTree {
    fn drop(&mut self) {
        self.store.remove(self.key);
    }
}

impl Tree {
    pub(crate) fn acquire(&self) -> TreeRead<'_> {
        match &self.stored {
            None => TreeRead::Resident(self),
            Some(stored) => match stored.load() {
                Ok(mut tree) => {
                    if !self.symbol_updates.is_empty() || self.tags != tree.tags {
                        let resident = Arc::make_mut(&mut tree);
                        resident.tags = self.tags.clone();
                        for (&node, &symbol) in &self.symbol_updates {
                            let id = resident.to_id(node);
                            resident.node_mut(id).sym = symbol;
                        }
                    }
                    TreeRead::Cached(tree)
                }
                Err(error) => panic!("tree store: {error}"),
            },
        }
    }

    pub(crate) fn spill(&mut self, store: &Arc<TreeStore>) -> std::io::Result<()> {
        self.materialize();
        let stored = store.write(self)?;
        let resident = Tree {
            arena: std::mem::replace(&mut self.arena, indextree::Arena::new()),
            root: self.root,
            stored: None,
            symbol_updates: Default::default(),
            label: self.label.clone(),
            tags: self.tags.clone(),
            source: self.source.clone(),
        };
        stored.store.insert(stored.key, resident);
        self.symbol_updates.clear();
        self.stored = Some(stored);
        Ok(())
    }

    pub(crate) fn materialize(&mut self) {
        if self.stored.is_some() {
            let acquired = match self.acquire() {
                TreeRead::Cached(tree) => tree,
                TreeRead::Resident(_) => unreachable!("stored tree returns an owned handle"),
            };
            self.stored = None;
            let resident = Arc::unwrap_or_clone(acquired);
            *self = resident;
        }
    }
}
