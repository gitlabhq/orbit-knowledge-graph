//! Host file access without following symlinks in any path component.
//! Callers select trusted paths. Reads verify size but are not immutable snapshots.

mod syscalls;

use std::io::{self, Read};
use std::path::{Path, PathBuf};

use rustix::fs::{AtFlags, FileType, readlinkat, statat};

pub enum Entry {
    File(File),
    Symlink(PathBuf),
}

pub struct File {
    path: PathBuf,
    size: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("file size {size} exceeds read limit {max_bytes} bytes")]
pub struct SizeLimitExceeded {
    pub size: u64,
    pub max_bytes: u64,
}

fn check_size(size: u64, max_bytes: u64) -> io::Result<()> {
    if size > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::FileTooLarge,
            SizeLimitExceeded { size, max_bytes },
        ));
    }
    Ok(())
}

impl File {
    pub fn new(path: PathBuf, size: u64) -> Self {
        Self { path, size }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn read(&self, max_bytes: u64) -> io::Result<Vec<u8>> {
        check_size(self.size, max_bytes)?;
        let file = syscalls::open(&self.path, false)?;
        let metadata = file.metadata()?;
        check_size(metadata.len(), max_bytes)?;
        if !metadata.is_file() || metadata.len() != self.size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file changed"));
        }
        let mut bytes = Vec::new();
        file.take(self.size.saturating_add(1))
            .read_to_end(&mut bytes)?;
        check_size(bytes.len() as u64, max_bytes)?;
        if bytes.len() as u64 != self.size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file changed"));
        }
        Ok(bytes)
    }
}

pub fn inspect(path: &Path) -> io::Result<Option<Entry>> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = syscalls::open(parent, true)?;
    let name = path.file_name().ok_or(io::ErrorKind::InvalidInput)?;
    let metadata = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
    Ok(match FileType::from_raw_mode(metadata.st_mode) {
        FileType::RegularFile => Some(Entry::File(File::new(path.into(), metadata.st_size as u64))),
        FileType::Symlink => {
            use std::os::unix::ffi::OsStrExt;
            let target = readlinkat(&parent, name, Vec::new())?;
            Some(Entry::Symlink(PathBuf::from(std::ffi::OsStr::from_bytes(
                target.to_bytes(),
            ))))
        }
        _ => None,
    })
}
