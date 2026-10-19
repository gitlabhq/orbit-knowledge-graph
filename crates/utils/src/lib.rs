pub mod archive;
pub mod clickhouse;
pub mod fs;
pub mod fs_walk;
pub mod observability;
pub mod query_types;
pub mod safe_fs;
pub mod strings;
pub mod traversal_path;
pub mod version;
pub mod vfs;
pub mod vfs_policies;
pub mod yaml;

use std::os::unix::fs::PermissionsExt;

pub fn is_world_writable(path: &std::path::Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.permissions().mode() & 0o002 != 0)
}
