use std::ffi::OsString;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use flate2::read::GzDecoder;
use ignore::{WalkBuilder, WalkState};
use rayon::prelude::*;
use rustix::fs::{AtFlags, FileType, readlinkat, statat};
use sha2::{Digest, Sha256};
use tar::EntryType;

use super::{Content, Decision, File, Limits, Pass, Put, SourceError, Tag, Vfs, disk};

pub trait Source {
    fn fill<T: Tag>(self, into: &mut Vfs<T>) -> Result<(), SourceError>;
}

pub struct Memory(pub Vec<(String, Vec<u8>)>);
pub struct Archive<R: Read>(pub R);
pub struct Checkout<'a>(pub &'a Path);
pub struct Changed<'a> {
    pub root: &'a Path,
    pub paths: Vec<String>,
}

impl Source for Memory {
    fn fill<T: Tag>(self, into: &mut Vfs<T>) -> Result<(), SourceError> {
        let mut input = self.0.into_iter();
        loop {
            let batch = input
                .by_ref()
                .take(4096)
                .map(|(path, bytes)| {
                    into.admit(&path, bytes.len() as u64)
                        .map(|file| (file, bytes))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if batch.is_empty() {
                return Ok(());
            }
            let prepared: Vec<_> = batch
                .into_par_iter()
                .map(|(mut file, bytes)| {
                    into.header(&mut file);
                    if matches!(file.decision(), Decision::Pending | Decision::Keep(_)) {
                        file.classify_loaded(|file| into.passes.content(file, &bytes));
                    }
                    let hash = file
                        .keeps()
                        .then(|| <[u8; 32]>::from(Sha256::digest(&bytes)));
                    (file, hash, bytes)
                })
                .collect();
            for (file, hash, bytes) in prepared {
                let content = match hash {
                    Some(hash) => into.store(hash, bytes)?,
                    None => Content::Unreadable,
                };
                into.insert_file(file, content)?;
            }
        }
    }
}

impl Source for Checkout<'_> {
    fn fill<T: Tag>(self, into: &mut Vfs<T>) -> Result<(), SourceError> {
        let root = self.0.canonicalize()?;
        let passes = into.passes.clone();
        let limits = into.limits;
        let tree = Mutex::new(into);
        let failed = OnceLock::new();
        let walker = WalkBuilder::new(&root)
            .hidden(false)
            .git_ignore(true)
            .git_exclude(true)
            .ignore(false)
            .git_global(false)
            .parents(false)
            .require_git(false)
            .filter_entry(|entry| entry.file_name() != ".git")
            .build_parallel();
        walker.run(|| {
            Box::new(|entry| {
                if failed.get().is_some() {
                    return WalkState::Quit;
                }
                let result = match entry {
                    Ok(entry)
                        if entry
                            .file_type()
                            .is_some_and(|kind| kind.is_file() || kind.is_symlink()) =>
                    {
                        let path = entry
                            .path()
                            .strip_prefix(&root)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned();
                        disk_entry(entry.into_path(), path)
                    }
                    Ok(_) => return WalkState::Continue,
                    Err(error)
                        if error
                            .io_error()
                            .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
                    {
                        return WalkState::Continue;
                    }
                    Err(error) => Err(io::Error::other(error)),
                };
                let result = (|| -> Result<(), SourceError> {
                    if let Some(entry) = result? {
                        let file = tree.lock().unwrap().admit(&entry.path, entry.size)?;
                        if let Some((file, content)) = prepare_disk(entry, file, &*passes, limits)?
                        {
                            tree.lock().unwrap().insert_file(file, content)?;
                        }
                    }
                    Ok(())
                })();
                match result {
                    Ok(()) => WalkState::Continue,
                    Err(error) => {
                        let _ = failed.set(error);
                        WalkState::Quit
                    }
                }
            })
        });
        failed.into_inner().map_or(Ok(()), Err)
    }
}

impl Source for Changed<'_> {
    fn fill<T: Tag>(self, into: &mut Vfs<T>) -> Result<(), SourceError> {
        let root = self.root.canonicalize()?;
        for paths in self.paths.chunks(1024) {
            let batch = paths
                .par_iter()
                .map(|path| {
                    if path.is_empty() || !safe_relative(Path::new(path)) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid changed path",
                        ));
                    }
                    disk_entry(root.join(path), path.clone())
                })
                .collect::<io::Result<Vec<_>>>()?;
            load_disk_batch(batch.into_iter().flatten().collect(), into)?;
        }
        Ok(())
    }
}

struct DiskEntry {
    path: String,
    disk: PathBuf,
    size: u64,
    link: Option<String>,
}

fn disk_entry(disk: PathBuf, path: String) -> io::Result<Option<DiskEntry>> {
    let input = (|| -> io::Result<Option<DiskEntry>> {
        let parent = disk::open_parent(&disk)?;
        let name = disk.file_name().ok_or(io::ErrorKind::InvalidInput)?;
        let metadata = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
        Ok(match FileType::from_raw_mode(metadata.st_mode) {
            FileType::RegularFile => Some(DiskEntry {
                path,
                disk,
                size: metadata.st_size as u64,
                link: None,
            }),
            FileType::Symlink => {
                let link = Some(
                    readlinkat(&parent, name, Vec::new())?
                        .to_string_lossy()
                        .into_owned(),
                );
                Some(DiskEntry {
                    path,
                    disk,
                    size: 0,
                    link,
                })
            }
            _ => None,
        })
    })();
    match input {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        result => result,
    }
}

fn load_disk_batch<T: Tag>(batch: Vec<DiskEntry>, into: &mut Vfs<T>) -> Result<(), SourceError> {
    let admitted = batch
        .into_iter()
        .map(|entry| {
            into.admit(&entry.path, entry.size)
                .map(|file| (entry, file))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let minimum_chunk = (admitted.len() / 8).max(1);
    let prepared = admitted
        .into_par_iter()
        .with_min_len(minimum_chunk)
        .map(|(entry, file)| prepare_disk(entry, file, &*into.passes, into.limits))
        .collect::<Result<Vec<_>, SourceError>>()?;
    for (file, content) in prepared.into_iter().flatten() {
        into.insert_file(file, content)?;
    }
    Ok(())
}

fn prepare_disk<T: Tag>(
    entry: DiskEntry,
    mut file: File<T>,
    passes: &dyn Pass<Tag = T>,
    limits: Limits,
) -> Result<Option<(File<T>, Content)>, SourceError> {
    let content = if let Some(target) = entry.link {
        file.decide(Decision::List("symlink"));
        Content::Link(target)
    } else {
        if limits.file_bytes.is_some_and(|cap| file.size > cap) {
            file.decide(Decision::List("oversize"));
        } else {
            passes.header(&mut file);
        }
        if matches!(file.decision(), Decision::Pending) {
            let bytes = match disk::read(&entry.disk, file.size) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(SourceError::Io(error)),
            };
            file.classify_loaded(|file| passes.content(file, &bytes));
        }
        if file.keeps() {
            Content::Disk(entry.disk)
        } else {
            Content::Unreadable
        }
    };
    Ok(Some((file, content)))
}

impl<R: Read> Source for Archive<R> {
    fn fill<T: Tag>(self, into: &mut Vfs<T>) -> Result<(), SourceError> {
        let mut archive = tar::Archive::new(GzDecoder::new(self.0));
        let mut root = None;
        let mut seen = false;
        for entry in archive.entries()? {
            let mut entry = match entry {
                Err(error) if !seen && error.kind() == io::ErrorKind::UnexpectedEof => {
                    return Err(SourceError::Empty);
                }
                entry => entry?,
            };
            seen = true;
            let kind = entry.header().entry_type();
            if !matches!(
                kind,
                EntryType::Regular | EntryType::Symlink | EntryType::Link
            ) {
                continue;
            }
            let path = entry.path()?.into_owned();
            let Some(path) = relative(&path, &mut root)? else {
                continue;
            };
            if path.as_os_str().is_empty() {
                continue;
            }
            let path = path.to_string_lossy();
            let input = if kind == EntryType::Regular {
                Put::Lazy {
                    size: entry.size(),
                    read: Box::new(move || {
                        let mut bytes = Vec::new();
                        entry.read_to_end(&mut bytes)?;
                        Ok(bytes)
                    }),
                }
            } else {
                let target = entry
                    .link_name()?
                    .filter(|target| !target.as_os_str().is_empty())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing link target")
                    })?;
                Put::Symlink(if kind == EntryType::Link {
                    format!(
                        "/{}",
                        relative(&target, &mut root)?
                            .ok_or_else(|| io::Error::other("hard link outside root"))?
                            .display()
                    )
                } else {
                    target.to_string_lossy().into_owned()
                })
            };
            into.put(&path, input)?;
        }
        if seen {
            Ok(())
        } else {
            Err(SourceError::Empty)
        }
    }
}

fn safe_relative(path: &Path) -> bool {
    path.components()
        .all(|part| matches!(part, Component::Normal(_)))
}

fn relative<'a>(path: &'a Path, root: &mut Option<OsString>) -> io::Result<Option<&'a Path>> {
    if !safe_relative(path) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "archive traversal",
        ));
    }
    let mut parts = path.components();
    let Some(first) = parts.next() else {
        return Ok(None);
    };
    Ok(
        (first.as_os_str() == root.get_or_insert_with(|| first.as_os_str().to_owned()))
            .then_some(parts.as_path()),
    )
}
