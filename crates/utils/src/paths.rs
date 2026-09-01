//! Local workspace path conventions shared by the CLI and its libraries.

use std::path::PathBuf;

/// Root of the orbit local workspace: `$ORBIT_DATA_DIR` when set and
/// non-empty, else `~/.orbit`. `None` when the home directory cannot be
/// determined.
pub fn orbit_data_dir() -> Option<PathBuf> {
    match std::env::var("ORBIT_DATA_DIR") {
        Ok(dir) if !dir.is_empty() => Some(PathBuf::from(dir)),
        _ => Some(dirs::home_dir()?.join(".orbit")),
    }
}
