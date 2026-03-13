//! Path utilities shared across crates.

use std::path::{Component, Path, PathBuf};

/// Normalize a path by resolving `.` and `..` components lexically
/// (without touching the filesystem).
pub fn normalize_path(path: &Path) -> PathBuf {
    path.components().fold(PathBuf::new(), |mut acc, c| {
        match c {
            Component::ParentDir => {
                acc.pop();
            }
            Component::CurDir => {}
            _ => acc.push(c),
        }
        acc
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_parent_dir() {
        assert_eq!(normalize_path(Path::new("a/b/../c")), PathBuf::from("a/c"));
    }

    #[test]
    fn strips_current_dir() {
        assert_eq!(normalize_path(Path::new("a/./b")), PathBuf::from("a/b"));
    }

    #[test]
    fn empty_path() {
        assert_eq!(normalize_path(Path::new("")), PathBuf::new());
    }

    #[test]
    fn parent_at_root() {
        assert_eq!(normalize_path(Path::new("a/../..")), PathBuf::new());
    }

    #[test]
    fn absolute_path() {
        assert_eq!(
            normalize_path(Path::new("/a/b/../c")),
            PathBuf::from("/a/c"),
        );
    }
}
