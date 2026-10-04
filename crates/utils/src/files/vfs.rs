//! The read side. Immutable after `load`; the sorted vec is the tree, so
//! every verb is a binary search or a range over it, with no lock.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::loading::{ContentId, Loading, Node, Slot};
use super::path::{MAX_LINK_DEPTH, follow_first_link, key, not_found};
use super::scratch::{Blob, Scratch};
use super::{Bytes, Decision, File, Limits, Options, Pass, Source, SourceError, Tag, Usage};

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

/// Read side. Immutable; no lock on any verb.
pub struct Vfs<T> {
    pub(super) passes: Arc<dyn Pass<Tag = T>>,
    pub(super) nodes: Vec<Node<T>>,
    pub(super) links: FxHashMap<String, String>,
    pub(super) blobs: FxHashMap<ContentId, Blob>,
    pub(super) scratch: Scratch,
    pub(super) usage: Usage,
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
    /// decision from here on. The node stays either way: a `Drop` decided
    /// this late is a node that reads as `Unsupported`, and `files()` shows
    /// it with its reason.
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
        let verdict = node
            .file
            .judge_once(|file| self.passes.content(file, &bytes));
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
