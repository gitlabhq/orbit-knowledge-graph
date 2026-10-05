//! Linked files are reopened without following host symlinks, including parent components.
//! The kernel enforces this in one open: openat2 on Linux, O_NOFOLLOW_ANY on macOS.
//! Reads are bounded by the recorded size; symlink replacements and size changes fail.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use rustix::fs::{CWD, Mode, OFlags};

pub(super) fn open_parent(path: &Path) -> io::Result<File> {
    open(path.parent().unwrap_or(Path::new(".")), OFlags::DIRECTORY)
}

fn open(path: &Path, flags: OFlags) -> io::Result<File> {
    let flags = flags | OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC;
    #[cfg(target_os = "linux")]
    let file = rustix::fs::openat2(
        CWD,
        path,
        flags,
        Mode::empty(),
        rustix::fs::ResolveFlags::NO_SYMLINKS,
    )?;
    #[cfg(target_os = "macos")]
    let file = rustix::fs::openat(
        CWD,
        path,
        flags | OFlags::from_bits_retain(libc::O_NOFOLLOW_ANY as _),
        Mode::empty(),
    )?;
    Ok(file.into())
}

pub(super) fn read(path: &Path, size: u64) -> io::Result<Vec<u8>> {
    let file = open(path, OFlags::empty())?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "linked file changed",
        ));
    }
    let mut bytes = Vec::new();
    file.take(size.saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "linked file changed",
        ));
    }
    Ok(bytes)
}
