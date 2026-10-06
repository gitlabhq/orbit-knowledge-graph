//! Read-side nodes are sorted and immutable. File lookups and subtree bounds use binary search.
//! Directory listing scans the matching subtree. Content decisions on linked files use OnceLock;
//! concurrent first reads may wait for classification. Stored content reads do not take store locks.
//! Virtual links resolve within `/`. Missing paths return NotFound and listed files Unsupported.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::disk;
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
    pub path: PathBuf,
    pub kind: Kind,
    pub len: u64,
    pub decision: Option<Decision<T>>,
    pub link: Option<PathBuf>,
}

pub struct Vfs<T> {
    pub(super) passes: Arc<dyn Pass<Tag = T>>,
    pub(super) nodes: Vec<Node<T>>,
    pub(super) links: FxHashMap<String, String>,
    pub(super) blobs: FxHashMap<ContentId, Blob>,
    pub(super) scratch: Scratch,
    pub(super) usage: Usage,
}

impl<T: Tag> Vfs<T> {
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
            Slot::Linked(on_disk) => disk::read(on_disk, node.file.size)?.into(),
            Slot::Link(_) => return Err(not_found()),
        };
        if node.checked {
            return Ok(bytes);
        }
        match node
            .file
            .decide_once(|file| self.passes.content(file, &bytes))
        {
            Decision::Keep(_) => Ok(bytes),
            _ => Err(unsupported(&node.file)),
        }
    }

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
        names.sort_unstable();
        names.dedup();
        Ok(names)
    }

    pub fn stat(&self, path: &Path) -> io::Result<Stat<T>> {
        let link = self
            .links
            .get(&self.resolve_parents(path)?)
            .map(PathBuf::from);
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

    pub fn files(&self) -> impl Iterator<Item = &File<T>> + use<'_, T> {
        self.nodes.iter().map(|node| &node.file)
    }

    pub fn subtree(&self, dir: &Path) -> impl Iterator<Item = &File<T>> + use<'_, T> {
        let key = self.resolve(dir).ok();
        key.into_iter().flat_map(|key| self.subtree_of(&key))
    }

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

    fn resolve(&self, path: &Path) -> io::Result<String> {
        let mut key = key(path).ok_or_else(not_found)?;
        if self.links.is_empty() {
            return Ok(key);
        }
        let mut hops = 0;
        while let Some(next) = follow_first_link(&key, &self.links) {
            key = next?;
            hops += 1;
            if hops > MAX_LINK_DEPTH {
                return Err(io::Error::other(format!(
                    "{} passes through more than {MAX_LINK_DEPTH} symlinks",
                    path.display()
                )));
            }
        }
        Ok(key)
    }

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
