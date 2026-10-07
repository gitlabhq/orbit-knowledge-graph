//! Read-side entries are sorted and immutable. File lookups and subtree bounds use binary search.
//! Directory listing scans the matching subtree. Content decisions on linked files use OnceLock;
//! concurrent first reads may wait for classification. Stored content reads do not take store locks.
//! Virtual links resolve within `/`. Missing paths return NotFound and listed files Unsupported.

use std::io;

use rustc_hash::FxHashMap;
use typed_path::{Utf8UnixComponent, Utf8UnixPath, Utf8UnixPathBuf};

use super::loading::{Content, Loading, VfsEntry};
use super::scratch::Scratch;
use super::{Bytes, Decision, File, Limits, Options, Pass, Source, SourceError, Tag, Usage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stat<T> {
    pub path: String,
    pub kind: Kind,
    pub len: u64,
    pub decision: Option<Decision<T>>,
    pub link: Option<String>,
}

pub struct Vfs<T> {
    pub(super) passes: Box<dyn Pass<Tag = T>>,
    pub(super) entries: Vec<VfsEntry<T>>,
    pub(super) links: FxHashMap<String, String>,
    pub(super) scratch: Scratch,
    pub(super) usage: Usage,
    pub(super) max_file_bytes: u64,
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
        Ok(loading.finish())
    }

    pub fn read(&self, path: impl AsRef<str>) -> io::Result<Bytes> {
        let (key, _) = self.resolve(path.as_ref())?;
        let Some(entry) = self.entry(&key) else {
            return Err(match self.is_dir(&key) {
                true => {
                    io::Error::new(io::ErrorKind::IsADirectory, format!("{key} is a directory"))
                }
                false => not_found(),
            });
        };
        if matches!(entry.file.decision(), Decision::List(_)) {
            return Err(unsupported(&entry.file));
        }
        let bytes = match &entry.content {
            Content::Memory(bytes) => bytes.clone(),
            Content::Spilled {
                offset,
                len,
                raw_len,
            } => self.scratch.read(*offset, *len, *raw_len)?,
            Content::ReadOnDemand(read) => {
                let bytes = read(self.max_file_bytes)?;
                if bytes.len() as u64 > self.max_file_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "reader exceeded file byte limit",
                    ));
                }
                if bytes.len() as u64 != entry.file.size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "source size mismatch",
                    ));
                }
                bytes.into()
            }
            Content::Unavailable => return Err(unsupported(&entry.file)),
            Content::Failed(error) => return Err(io::Error::new(error.kind(), error.clone())),
            Content::Symlink(_) => return Err(not_found()),
        };
        match entry.file.classify(&*self.passes, &bytes) {
            Decision::Keep(_) => Ok(bytes),
            _ => Err(unsupported(&entry.file)),
        }
    }

    pub fn read_dir(&self, path: impl AsRef<str>) -> io::Result<Vec<String>> {
        let (key, _) = self.resolve(path.as_ref())?;
        if self.entry(&key).is_some() {
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

    pub fn stat(&self, path: impl AsRef<str>) -> io::Result<Stat<T>> {
        let (key, link) = self.resolve(path.as_ref())?;
        let (kind, len, decision) = match self.entry(&key) {
            Some(entry) => (Kind::File, entry.file.size, Some(entry.file.decision())),
            None if self.is_dir(&key) => (Kind::Dir, 0, None),
            None => return Err(not_found()),
        };
        Ok(Stat {
            path: format!("/{key}"),
            kind,
            len,
            decision,
            link: link.map(str::to_owned),
        })
    }

    pub fn files(&self) -> impl Iterator<Item = &File<'static, T>> + use<'_, T> {
        self.entries.iter().map(|entry| &entry.file)
    }

    pub fn subtree<P: AsRef<str>>(
        &self,
        dir: P,
    ) -> impl Iterator<Item = &File<'static, T>> + use<'_, T, P> {
        let key = self.resolve(dir.as_ref()).ok().map(|(key, _)| key);
        key.into_iter().flat_map(|key| self.subtree_of(&key))
    }

    pub fn usage(&self) -> Usage {
        Usage {
            kept: self
                .entries
                .iter()
                .filter(|entry| entry.file.keeps() && !matches!(entry.content, Content::Failed(_)))
                .map(|entry| entry.file.size)
                .sum(),
            ..self.usage
        }
    }

    fn subtree_of(&self, key: &str) -> impl Iterator<Item = &File<'static, T>> + use<'_, T> {
        let prefix = match key.is_empty() {
            true => String::new(),
            false => format!("{key}/"),
        };
        let start = self
            .entries
            .partition_point(|n| n.file.path.as_ref() < prefix.as_str());
        let end =
            start + self.entries[start..].partition_point(|n| n.file.path.starts_with(&prefix));
        self.entries[start..end].iter().map(|entry| &entry.file)
    }

    fn entry(&self, key: &str) -> Option<&VfsEntry<T>> {
        self.entries
            .binary_search_by(|entry| entry.file.path.as_ref().cmp(key))
            .ok()
            .map(|i| &self.entries[i])
    }

    fn is_dir(&self, key: &str) -> bool {
        key.is_empty() || self.subtree_of(key).next().is_some()
    }

    fn resolve(&self, path: &str) -> io::Result<(String, Option<&str>)> {
        let mut key = key(path).ok_or_else(not_found)?;
        if self.links.is_empty() {
            return Ok((key, None));
        }
        let mut link = None;
        let mut hops = 0;
        while let Some((end, target)) = key
            .match_indices('/')
            .map(|(index, _)| index)
            .chain(std::iter::once(key.len()))
            .find_map(|end| self.links.get(&key[..end]).map(|target| (end, target)))
        {
            let prefix = Utf8UnixPath::new(&key[..end]);
            let rest = &key[end..];
            link = link.or_else(|| rest.is_empty().then_some(target.as_str()));
            let resolved = prefix
                .parent()
                .unwrap_or(Utf8UnixPath::new(""))
                .join(target)
                .join(rest.trim_start_matches('/'));
            key = self::key(resolved.as_str()).ok_or_else(not_found)?;
            hops += 1;
            if hops > 40 {
                return Err(io::Error::other(format!(
                    "{path} passes through more than 40 symlinks"
                )));
            }
        }
        Ok((key, link))
    }
}

pub(super) fn key(path: &str) -> Option<String> {
    let mut key = Utf8UnixPathBuf::new();
    for component in Utf8UnixPath::new(path).components() {
        match component {
            Utf8UnixComponent::Normal(part) => key.push(part),
            Utf8UnixComponent::ParentDir if !key.pop() => return None,
            _ => {}
        }
    }
    Some(key.into_string())
}

fn not_found() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "no such file in the repository")
}

impl<T> std::fmt::Debug for Vfs<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vfs").field("usage", &self.usage).finish()
    }
}

fn unsupported<T: Tag>(file: &File<'_, T>) -> io::Error {
    let why = match file.decision() {
        Decision::List(why) => why,
        _ => "no bytes",
    };
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{} is listed only: {why}", file.path),
    )
}
