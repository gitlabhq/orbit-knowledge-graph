//! OS-specific file access refuses host symlinks, including parent components.
//! The kernel enforces this in one open: openat2 on Linux, O_NOFOLLOW_ANY on macOS.
//! Windows opens and pins parents without write/delete sharing, rejecting all reparse points.

use std::fs::File;
use std::io;
use std::path::Path;

#[cfg(unix)]
use rustix::fs::{CWD, Mode, OFlags};

#[cfg(unix)]
pub(super) fn open(path: &Path, directory: bool) -> io::Result<File> {
    let flags = if directory {
        OFlags::DIRECTORY
    } else {
        OFlags::empty()
    };
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

#[cfg(windows)]
pub(super) fn open(path: &Path, directory: bool) -> io::Result<File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::path::{Component, Prefix};

    const FILE_READ_ATTRIBUTES: u32 = 0x80;
    const FILE_SHARE_READ: u32 = 0x1;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x00200000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

    if !path.is_absolute()
        || path.components().any(|component| match component {
            Component::ParentDir => true,
            Component::Prefix(prefix) => !matches!(
                prefix.kind(),
                Prefix::Disk(_)
                    | Prefix::VerbatimDisk(_)
                    | Prefix::UNC(_, _)
                    | Prefix::VerbatimUNC(_, _)
            ),
            Component::Normal(name) => name.to_string_lossy().contains(':'),
            _ => false,
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid disk path",
        ));
    }

    let ancestors: Vec<_> = path.ancestors().collect();
    let mut handles = Vec::with_capacity(ancestors.len());
    for current in ancestors.into_iter().rev() {
        let is_directory = directory || current != path;
        let mut options = OpenOptions::new();
        options
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
        if is_directory {
            options.access_mode(FILE_READ_ATTRIBUTES);
        } else {
            options.read(true);
        }
        let file = options.open(current)?;
        let metadata = file.metadata()?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "reparse point in disk path",
            ));
        }
        if is_directory && !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "invalid parent directory",
            ));
        }
        handles.push(file);
    }
    handles
        .pop()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty disk path"))
}
