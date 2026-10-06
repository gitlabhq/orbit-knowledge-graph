//! A repository tree built with exclusive access, then shared by readers.

#![doc = include_str!("README.md")]

mod disk;
mod policy;
mod scratch;
pub mod sources;

use std::borrow::Cow;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use indextree::{Arena, NodeId};
use rustc_hash::FxHashMap;
use sha2::{Digest, Sha256};

pub use policy::{Decision, File, Pass, Tag, Then};
use scratch::{Blob, Scratch};
pub use sources::Source;

type Bytes = Arc<[u8]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: &'static str,
    pub count: u64,
    pub cap: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Cap(#[from] CapExceeded),
    #[error("source error: {0}")]
    Io(#[from] io::Error),
    #[error("source contained no entries (empty or truncated stream)")]
    Empty,
    #[error("load cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub file_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub files: Option<usize>,
    pub resident_bytes: Option<u64>,
    pub spilled_bytes: Option<u64>,
}

#[derive(Default)]
pub struct Options {
    pub scratch_dir: Option<PathBuf>,
    pub compress_spill: bool,
    pub cancelled: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    pub files: usize,
    pub bytes: u64,
    pub kept: u64,
    pub resident: u64,
    pub spilled: u64,
    pub deduped_bytes: u64,
    pub duplicate_paths: usize,
}

pub enum Put<'a> {
    Bytes(Vec<u8>),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stat<T> {
    pub path: PathBuf,
    pub kind: Kind,
    pub len: u64,
    pub decision: Option<Decision<T>>,
    pub link: Option<PathBuf>,
}

struct Entry<T> {
    file: File<T>,
    content: Content,
}

enum Content {
    Directory(Vec<NodeId>),
    Unreadable,
    Stored(Blob),
    Disk(PathBuf),
    Link(String),
}

pub struct Vfs<T> {
    tree: Arena<usize>,
    entries: Vec<Entry<T>>,
    sorted: bool,
    root: NodeId,
    blobs: FxHashMap<[u8; 32], Blob>,
    scratch: Scratch,
    passes: Arc<dyn Pass<Tag = T>>,
    limits: Limits,
    options: Options,
    offered: u64,
    usage: Usage,
}

impl<T: Tag> Vfs<T> {
    pub fn load(
        source: impl Source,
        passes: impl Pass<Tag = T> + 'static,
        limits: Limits,
        options: Options,
    ) -> Result<Self, SourceError> {
        let mut tree = Arena::new();
        let root = tree.new_node(0);
        let entry = Entry {
            file: File::new(String::new(), 0),
            content: Content::Directory(Vec::new()),
        };
        let mut vfs = Self {
            tree,
            entries: vec![entry],
            sorted: true,
            root,
            blobs: FxHashMap::default(),
            scratch: Scratch::new(&options, limits.spilled_bytes),
            passes: Arc::new(passes),
            limits,
            options,
            offered: 0,
            usage: Usage::default(),
        };
        source.fill(&mut vfs)?;
        vfs.compact();
        Ok(vfs)
    }

    pub fn put(&mut self, path: &str, input: Put<'_>) -> Result<(), SourceError> {
        let size = match &input {
            Put::Bytes(bytes) => bytes.len() as u64,
            Put::Lazy { size, .. } | Put::OnDisk { size, .. } => *size,
            Put::Symlink(_) => 0,
        };
        let mut file = self.admit(path, size)?;
        let content = if let Put::Symlink(target) = input {
            file.decide(Decision::List("symlink"));
            Content::Link(target)
        } else {
            self.header(&mut file);
            match input {
                _ if matches!(file.decision(), Decision::List(_) | Decision::Drop(_)) => {
                    Content::Unreadable
                }
                Put::Bytes(bytes) => self.classify(&mut file, bytes, None)?,
                Put::Lazy { read, .. } => self.classify(&mut file, read()?, None)?,
                Put::OnDisk { path, .. } if file.keeps() => Content::Disk(path),
                Put::OnDisk { path, .. } => match disk::read(&path, size) {
                    Ok(bytes) => self.classify(&mut file, bytes, Some(path))?,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error.into()),
                },
                Put::Symlink(_) => unreachable!(),
            }
        };
        self.insert_file(file, content)
    }

    fn admit(&mut self, path: &str, size: u64) -> Result<File<T>, SourceError> {
        if self
            .options
            .cancelled
            .as_ref()
            .is_some_and(|cancel| cancel())
        {
            return Err(SourceError::Cancelled);
        }
        let path = key(Path::new(path))
            .filter(|path| !path.is_empty())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid repository file path")
            })?;
        add_capped(
            &mut self.offered,
            "files",
            1,
            self.limits.files.map(|n| n as u64),
        )?;
        add_capped(
            &mut self.usage.bytes,
            "total_bytes",
            size,
            self.limits.total_bytes,
        )?;
        Ok(File::new(path.into_owned(), size))
    }

    fn header(&self, file: &mut File<T>) {
        if self.limits.file_bytes.is_some_and(|cap| file.size > cap) {
            file.decide(Decision::List("oversize"));
        } else {
            self.passes.header(file);
        }
    }

    fn insert_file(&mut self, file: File<T>, content: Content) -> Result<(), SourceError> {
        self.sorted = false;
        let dropped = matches!(file.decision(), Decision::Drop(_));
        let mut parent = self.root;
        let mut parts = file.path.split('/').peekable();
        while let Some(name) = parts.next() {
            let existing = self.child(parent, name)?;
            if parts.peek().is_none() {
                if let Some(id) = existing {
                    if matches!(self.entry(id).content, Content::Directory(_)) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "file replaces directory",
                        )
                        .into());
                    }
                    self.usage.duplicate_paths += 1;
                    if dropped {
                        self.remove(id);
                    } else {
                        self.entries[*self.tree[id].get()] = Entry { file, content };
                    }
                } else if !dropped {
                    self.insert(parent, Entry { file, content });
                }
                break;
            }
            parent = match existing {
                Some(id) => id,
                None if dropped => return Ok(()),
                None => {
                    let path = Path::new(&self.entry(parent).file.path)
                        .join(name)
                        .to_string_lossy()
                        .into_owned();
                    self.insert(
                        parent,
                        Entry {
                            file: File::new(format!("{path}/"), 0),
                            content: Content::Directory(Vec::new()),
                        },
                    )
                }
            };
        }
        Ok(())
    }

    fn classify(
        &mut self,
        file: &mut File<T>,
        bytes: Vec<u8>,
        disk: Option<PathBuf>,
    ) -> Result<Content, SourceError> {
        if bytes.len() as u64 != file.size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "source size mismatch").into());
        }
        file.classify_loaded(|file| self.passes.content(file, &bytes));
        if !file.keeps() {
            return Ok(Content::Unreadable);
        }
        if let Some(path) = disk {
            return Ok(Content::Disk(path));
        }
        let hash: [u8; 32] = Sha256::digest(&bytes).into();
        self.store(hash, bytes)
    }

    fn store(&mut self, hash: [u8; 32], bytes: Vec<u8>) -> Result<Content, SourceError> {
        let blob = if let Some(blob) = self.blobs.get(&hash) {
            self.usage.deduped_bytes += bytes.len() as u64;
            blob.clone()
        } else {
            let blob = match add_capped(
                &mut self.usage.resident,
                "resident_bytes",
                bytes.len() as u64,
                self.limits.resident_bytes,
            ) {
                Ok(_) => Blob::Memory(bytes.into()),
                Err(_) => self.scratch.append(&bytes)?,
            };
            self.blobs.insert(hash, blob.clone());
            blob
        };
        Ok(Content::Stored(blob))
    }

    fn children(&self, id: NodeId) -> io::Result<&[NodeId]> {
        match &self.entry(id).content {
            Content::Directory(children) => Ok(children),
            _ => Err(io::ErrorKind::NotADirectory.into()),
        }
    }

    fn insert(&mut self, parent: NodeId, entry: Entry<T>) -> NodeId {
        let children = self.children(parent).unwrap();
        let position = children
            .binary_search_by(|&id| self.entry(id).file.path.cmp(&entry.file.path))
            .unwrap_err();
        let next = children.get(position).copied();
        let id = self.tree.new_node(self.entries.len());
        self.entries.push(entry);
        match next {
            Some(next) => next.insert_before(id, &mut self.tree),
            None => parent.append(id, &mut self.tree),
        }
        let Content::Directory(children) = &mut self.entries[*self.tree[parent].get()].content
        else {
            unreachable!()
        };
        children.insert(position, id);
        id
    }

    fn remove(&mut self, mut id: NodeId) {
        while id != self.root {
            let parent = id.parent(&self.tree).unwrap();
            let position = self
                .children(parent)
                .unwrap()
                .binary_search_by(|&child| {
                    self.entry(child).file.path.cmp(&self.entry(id).file.path)
                })
                .unwrap();
            let Content::Directory(children) = &mut self.entries[*self.tree[parent].get()].content
            else {
                unreachable!()
            };
            children.remove(position);
            let index = *self.tree[id].get();
            let last = self.entries.len() - 1;
            if index != last {
                let mut moved = self.root;
                for part in self.entries[last]
                    .file
                    .path
                    .split('/')
                    .filter(|part| !part.is_empty())
                {
                    moved = self.child(moved, part).unwrap().unwrap();
                }
                *self.tree[moved].get_mut() = index;
            }
            self.entries.swap_remove(index);
            id.remove_subtree(&mut self.tree);
            if parent.has_children(&self.tree) {
                break;
            }
            id = parent;
        }
    }

    pub fn read(&self, path: &Path) -> io::Result<Bytes> {
        let normalized = key(path).ok_or(io::ErrorKind::NotFound)?;
        let direct = self
            .sorted
            .then(|| {
                self.entries
                    .binary_search_by(|entry| entry.file.path.as_str().cmp(&normalized))
                    .ok()
            })
            .flatten()
            .map(|index| &self.entries[index])
            .filter(|entry| !matches!(entry.content, Content::Link(_)));
        let entry = match direct {
            Some(entry) => entry,
            None => self.entry(self.resolve(path)?.0),
        };
        let bytes = match &entry.content {
            Content::Directory(_) => return Err(io::ErrorKind::IsADirectory.into()),
            _ if !entry.file.keeps() => return Err(io::ErrorKind::Unsupported.into()),
            Content::Stored(blob) => self.scratch.read(blob)?,
            Content::Disk(path) => disk::read(path, entry.file.size)?.into(),
            _ => return Err(io::ErrorKind::Unsupported.into()),
        };
        match entry
            .file
            .classify(|file| self.passes.content(file, &bytes))
        {
            Decision::Keep(_) => Ok(bytes),
            _ => Err(io::ErrorKind::Unsupported.into()),
        }
    }

    pub fn read_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        let (id, _) = self.resolve(path)?;
        let mut names: Vec<_> = self
            .children(id)?
            .iter()
            .map(|&id| self.name(id).to_owned())
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    pub fn stat(&self, path: &Path) -> io::Result<Stat<T>> {
        let (id, link) = self.resolve(path)?;
        let entry = self.entry(id);
        let directory = matches!(entry.content, Content::Directory(_));
        Ok(Stat {
            path: Path::new("/").join(entry.file.path.trim_end_matches('/')),
            kind: if directory { Kind::Dir } else { Kind::File },
            len: entry.file.size,
            decision: (!directory).then(|| entry.file.decision()),
            link: link.map(PathBuf::from),
        })
    }

    pub fn files(&self) -> impl Iterator<Item = &File<T>> {
        let mut records = self.entries.iter();
        let mut nodes = self.root.descendants(&self.tree);
        std::iter::from_fn(move || {
            loop {
                let entry = if self.sorted {
                    records.next()?
                } else {
                    self.entry(nodes.next()?)
                };
                if !matches!(entry.content, Content::Directory(_)) {
                    return Some(&entry.file);
                }
            }
        })
    }

    fn compact(&mut self) {
        let mut order: Vec<_> = (0..self.entries.len()).collect();
        order.sort_unstable_by(|&a, &b| self.entries[a].file.path.cmp(&self.entries[b].file.path));
        let mut positions = vec![0; order.len()];
        for (index, &old) in order.iter().enumerate() {
            positions[old] = index;
        }
        for node in &mut self.tree {
            if let Some(index) = node.try_get_mut() {
                *index = positions[*index];
            }
        }
        self.entries
            .sort_unstable_by(|a, b| a.file.path.cmp(&b.file.path));
        for entry in &mut self.entries {
            entry.file.path.shrink_to_fit();
            if let Content::Directory(children) = &mut entry.content {
                children.shrink_to_fit();
            }
        }
        self.tree.shrink_to_fit();
        self.entries.shrink_to_fit();
        self.sorted = true;
    }

    pub fn subtree(&self, path: &Path) -> impl Iterator<Item = &File<T>> {
        self.resolve(path)
            .ok()
            .into_iter()
            .flat_map(|(id, _)| id.descendants(&self.tree).skip(1))
            .filter_map(|id| self.file(id))
    }

    pub fn usage(&self) -> Usage {
        let mut usage = Usage {
            spilled: self.scratch.len(),
            ..self.usage
        };
        for entry in &self.entries {
            if !matches!(entry.content, Content::Directory(_)) {
                let file = &entry.file;
                usage.files += 1;
                usage.kept += if file.keeps() { file.size } else { 0 };
            }
        }
        usage
    }

    fn file(&self, id: NodeId) -> Option<&File<T>> {
        let entry = self.entry(id);
        (!matches!(entry.content, Content::Directory(_))).then_some(&entry.file)
    }

    fn child(&self, parent: NodeId, name: &str) -> io::Result<Option<NodeId>> {
        let children = self.children(parent)?;
        let offset = self.entry(parent).file.path.len();
        let index = children
            .binary_search_by(|&id| self.entry(id).file.path[offset..].cmp(name))
            .or_else(|_| {
                let directory = format!("{name}/");
                children.binary_search_by(|&id| self.entry(id).file.path[offset..].cmp(&directory))
            });
        Ok(index.ok().map(|index| children[index]))
    }

    fn name(&self, id: NodeId) -> &str {
        self.child_name(id).trim_end_matches('/')
    }

    fn entry(&self, id: NodeId) -> &Entry<T> {
        &self.entries[*self.tree[id].get()]
    }

    fn child_name(&self, id: NodeId) -> &str {
        let path = &self.entry(id).file.path;
        let start = path
            .trim_end_matches('/')
            .rfind('/')
            .map_or(0, |index| index + 1);
        &path[start..]
    }

    fn resolve(&self, path: &Path) -> io::Result<(NodeId, Option<&str>)> {
        let mut path = key(path).ok_or(io::ErrorKind::NotFound)?;
        let mut link = None;
        for _ in 0..=40 {
            let mut id = self.root;
            let mut parts = path.split('/').filter(|part| !part.is_empty());
            let mut target_path = None;
            while let Some(part) = parts.next() {
                id = self.child(id, part)?.ok_or(io::ErrorKind::NotFound)?;
                if let Content::Link(target) = &self.entry(id).content {
                    let rest = parts.collect::<Vec<_>>().join("/");
                    if rest.is_empty() && link.is_none() {
                        link = Some(target.as_str());
                    }
                    let parent = id.parent(&self.tree).unwrap();
                    target_path = Some(
                        Path::new(&self.entry(parent).file.path)
                            .join(target)
                            .join(rest),
                    );
                    break;
                }
            }
            match target_path {
                Some(target) => {
                    path = Cow::Owned(key(&target).ok_or(io::ErrorKind::NotFound)?.into_owned())
                }
                None => return Ok((id, link)),
            }
        }
        Err(io::Error::other("too many repository symlinks"))
    }
}

impl<T> std::fmt::Debug for Vfs<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vfs")
            .field("entries", &self.tree.len())
            .field("usage", &self.usage)
            .finish()
    }
}

fn key(path: &Path) -> Option<Cow<'_, str>> {
    if let Some(path) = path.to_str() {
        let relative = path.trim_start_matches('/');
        if !relative.contains('\\')
            && relative
                .split('/')
                .all(|part| !matches!(part, "" | "." | ".."))
        {
            return Some(Cow::Borrowed(relative));
        }
    }
    let mut parts = Vec::new();
    for part in path.components() {
        match part {
            Component::Normal(name) => parts.push(name.to_string_lossy()),
            Component::ParentDir => {
                parts.pop()?;
            }
            Component::RootDir => parts.clear(),
            Component::CurDir => {}
            Component::Prefix(_) => return None,
        }
    }
    Some(Cow::Owned(parts.join("/")))
}

fn add_capped(
    total: &mut u64,
    metric: &'static str,
    amount: u64,
    cap: Option<u64>,
) -> Result<u64, CapExceeded> {
    let cap = cap.unwrap_or(u64::MAX);
    let next = total
        .checked_add(amount)
        .filter(|&next| next <= cap)
        .ok_or(CapExceeded {
            metric,
            count: total.saturating_add(amount),
            cap,
        })?;
    Ok(std::mem::replace(total, next))
}
