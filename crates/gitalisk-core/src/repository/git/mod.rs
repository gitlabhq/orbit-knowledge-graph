pub mod commit;
pub mod current_branch;
pub mod remote;
pub mod worktree;

use std::path::PathBuf;

/// Git emits raw bytes; on unix keep non-UTF-8 names intact.
pub(crate) fn path_from_bytes(bytes: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}
