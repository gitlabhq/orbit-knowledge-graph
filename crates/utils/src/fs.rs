//! Filesystem security utilities.
//!
//! Functions for safe directory creation, symlink validation, and path
//! traversal prevention. These are security-critical and should be
//! reviewed carefully before modification.

use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedSymlink {
    pub relative_path: PathBuf,
    pub target: PathBuf,
    pub reason: RemovedSymlinkReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovedSymlinkReason {
    EscapesRoot,
    Dangling,
}

/// Canonicalize `path` and return it only if the real target lives
/// inside `root`. The path-traversal guard every subsystem that turns
/// a user-supplied specifier into a filesystem read should route
/// through.
///
/// Returns `None` when:
/// - `path` does not exist
/// - `path` is a dangling symlink
/// - the canonical target resolves outside `root` (direct symlink
///   escape, `../..` climb, Windows UNC redirect, etc.)
///
/// `root` is expected to already be canonical. Caller's responsibility
/// to canonicalize it once at setup time.
pub fn contained_canonical_path(root: &Path, path: &Path) -> Option<PathBuf> {
    let canonical = std::fs::canonicalize(path).ok()?;
    canonical.starts_with(root).then_some(canonical)
}

/// `true` when `path` has only normal components — safe to join under a root,
/// with no `..`/`.`/root/prefix that could climb out.
pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
}

/// Resolve `dest` to its canonical form within `root`, creating parents if
/// needed. `PermissionDenied` if the real target escapes `root`.
pub fn resolve_dest_within(root: &Path, dest: &Path) -> io::Result<PathBuf> {
    if dest.exists() {
        contained_canonical_path(root, dest).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("path traversal detected: {}", dest.display()),
            )
        })
    } else {
        safe_create_dir_all(dest, root)
    }
}

/// Create parent directories for `path`, validating that no existing
/// path component resolves outside `root` via symlinks.
///
/// Returns the canonicalized destination path (parent joined with filename).
///
/// Without this check, `create_dir_all` follows symlinks planted by earlier
/// operations, creating directories outside the intended root.
pub fn safe_create_dir_all(path: &Path, root: &Path) -> io::Result<PathBuf> {
    let parent = match path.parent() {
        Some(p) => p,
        None => return Ok(path.to_path_buf()),
    };

    let ancestor = longest_existing_ancestor(parent);
    let ancestor_canonical = ancestor.canonicalize()?;
    if !ancestor_canonical.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("path traversal detected: {}", path.display()),
        ));
    }

    std::fs::create_dir_all(parent)?;
    Ok(parent
        .canonicalize()?
        .join(path.file_name().unwrap_or_default()))
}

/// Walk up from `path` to find the deepest ancestor that exists on disk.
///
/// Used to validate a path before `create_dir_all` so that we can detect
/// symlinks in existing components before creating new directories.
pub fn longest_existing_ancestor(path: &Path) -> &Path {
    let mut current = path;
    while !current.exists() {
        match current.parent() {
            Some(p) => current = p,
            None => break,
        }
    }
    current
}

/// Walk a directory tree and remove any symlink that resolves outside
/// `root`. Dangling symlinks are also removed. Removed symlinks are
/// returned for callers that need to observe skipped archive entries.
///
/// The scan never short-circuits: every entry is visited and every bad
/// symlink is deleted even if earlier entries failed. This prevents a
/// malicious archive from planting multiple escaping symlinks where only
/// the first gets cleaned up.
pub fn validate_symlinks(root: &Path) -> io::Result<Vec<RemovedSymlink>> {
    // Accumulates the first error without stopping the scan.
    let mut first_err: Option<io::Error> = None;
    let mut removed_symlinks = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            // Directory removed between iteration and read — safe to skip.
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            // OS-level iteration errors are recorded but don't stop the scan,
            // so remaining symlinks are still checked and cleaned up.
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    first_err = first_err.or(Some(e));
                    continue;
                }
            };
            let path = entry.path();
            let meta = match path.symlink_metadata() {
                Ok(m) => m,
                Err(e) => {
                    first_err = first_err.or(Some(e));
                    continue;
                }
            };

            if meta.is_symlink() {
                match check_symlink(&path, root) {
                    Ok(Some(removed)) => {
                        tracing::warn!(
                            relative_path = %removed.relative_path.display(),
                            target = %removed.target.display(),
                            reason = ?removed.reason,
                            "removing symlink"
                        );
                        removed_symlinks.push(removed);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        first_err = first_err.or(Some(e));
                    }
                }
            } else if meta.is_dir() {
                stack.push(path);
            }
        }
    }
    first_err.map_or(Ok(removed_symlinks), Err)
}

fn check_symlink(path: &Path, root: &Path) -> io::Result<Option<RemovedSymlink>> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    match path.canonicalize() {
        Ok(r) if r.starts_with(root) => Ok(None),
        Ok(r) => {
            let _ = std::fs::remove_file(path);
            Ok(Some(RemovedSymlink {
                relative_path: relative.to_path_buf(),
                target: r,
                reason: RemovedSymlinkReason::EscapesRoot,
            }))
        }
        Err(_) => {
            let target = path.read_link().unwrap_or_default();
            let _ = std::fs::remove_file(path);
            Ok(Some(RemovedSymlink {
                relative_path: relative.to_path_buf(),
                target,
                reason: RemovedSymlinkReason::Dangling,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contained_canonical_path_accepts_path_inside_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/lib.rs"), b"").unwrap();

        let target = root.join("src/lib.rs");
        let resolved = contained_canonical_path(&root, &target).expect("contained");
        assert!(resolved.starts_with(&root));
    }

    #[test]
    fn contained_canonical_path_rejects_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        assert!(contained_canonical_path(&root, &root.join("missing")).is_none());
    }

    #[test]
    fn contained_canonical_path_rejects_symlink_escape() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        #[cfg(unix)]
        {
            let escape = root.join("escape");
            std::os::unix::fs::symlink(outside.path(), &escape).unwrap();
            assert!(contained_canonical_path(&root, &escape).is_none());
        }
    }

    #[test]
    fn is_safe_relative_path_accepts_normal_rejects_traversal() {
        assert!(is_safe_relative_path(Path::new("src/main.rs")));
        assert!(is_safe_relative_path(Path::new("a/b/c.txt")));
        assert!(!is_safe_relative_path(Path::new("../escape")));
        assert!(!is_safe_relative_path(Path::new("a/../../b")));
        assert!(!is_safe_relative_path(Path::new("/abs/path")));
        assert!(!is_safe_relative_path(Path::new("./rel")));
    }

    #[test]
    fn resolve_dest_within_creates_and_contains() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let dest = resolve_dest_within(&root, &root.join("a/b/file.txt")).unwrap();
        assert!(dest.starts_with(&root));
        assert!(root.join("a/b").is_dir());
    }

    #[test]
    fn resolve_dest_within_rejects_existing_escape() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        #[cfg(unix)]
        {
            let escape = root.join("escape");
            std::os::unix::fs::symlink(outside.path(), &escape).unwrap();
            let err = resolve_dest_within(&root, &escape).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        }
    }

    #[test]
    fn longest_ancestor_finds_existing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a/b/c/d");
        let result = longest_existing_ancestor(&deep);
        assert_eq!(result, dir.path());
    }

    #[test]
    fn safe_create_dir_all_creates_dirs_within_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let target = root.join("a/b/file.txt");

        let canonical = safe_create_dir_all(&target, &root).unwrap();
        assert!(canonical.starts_with(&root));
        assert!(root.join("a/b").is_dir());
    }

    #[test]
    fn safe_create_dir_all_rejects_traversal_via_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), root.join("escape")).unwrap();
            let target = root.join("escape/sub/file.txt");
            let err = safe_create_dir_all(&target, &root).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
            assert!(!outside.path().join("sub").exists());
        }
    }

    #[test]
    fn safe_create_dir_all_rejects_chained_symlink_redirect() {
        // Symlink "a" points outside root, then create_dir_all for
        // "a/b/file" would follow "a" and create dirs outside root
        // if not guarded.
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            // Iteration 1: create symlink pointing outside
            std::os::unix::fs::symlink(outside.path(), root.join("a")).unwrap();
            // Iteration 2: create_dir_all through the symlink
            let target = root.join("a/b/link");
            let err = safe_create_dir_all(&target, &root).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
            assert!(
                !outside.path().join("b").exists(),
                "must not create dirs outside root via symlink redirect"
            );
        }
    }

    #[test]
    fn validate_symlinks_accepts_internal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/lib.rs"), b"content").unwrap();

        #[cfg(unix)]
        {
            std::fs::create_dir_all(root.join("bin")).unwrap();
            std::os::unix::fs::symlink("../src/lib.rs", root.join("bin/run")).unwrap();
            assert!(validate_symlinks(&root).unwrap().is_empty());
        }
    }

    #[test]
    fn validate_symlinks_removes_escaping() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), root.join("escape")).unwrap();
            let removed = validate_symlinks(&root).unwrap();
            assert_eq!(
                removed,
                vec![RemovedSymlink {
                    relative_path: PathBuf::from("escape"),
                    target: outside.path().canonicalize().unwrap(),
                    reason: RemovedSymlinkReason::EscapesRoot,
                }]
            );
            assert!(root.join("escape").symlink_metadata().is_err());
        }
    }

    #[test]
    fn validate_symlinks_tolerates_dangling() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("nonexistent/target", root.join("bad")).unwrap();
            let removed = validate_symlinks(&root).unwrap();
            assert_eq!(
                removed,
                vec![RemovedSymlink {
                    relative_path: PathBuf::from("bad"),
                    target: PathBuf::from("nonexistent/target"),
                    reason: RemovedSymlinkReason::Dangling,
                }]
            );
            assert!(
                root.join("bad").symlink_metadata().is_err(),
                "dangling symlink must be removed"
            );
        }
    }

    #[test]
    fn validate_symlinks_deletes_all_bad_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), root.join("bad1")).unwrap();
            std::os::unix::fs::symlink("nonexistent", root.join("bad2")).unwrap();
            std::os::unix::fs::symlink(outside.path(), root.join("bad3")).unwrap();

            let removed = validate_symlinks(&root).unwrap();
            assert_eq!(removed.len(), 3);
            assert_eq!(
                removed
                    .iter()
                    .filter(|removed| removed.reason == RemovedSymlinkReason::EscapesRoot)
                    .count(),
                2
            );
            assert_eq!(
                removed
                    .iter()
                    .filter(|removed| removed.reason == RemovedSymlinkReason::Dangling)
                    .count(),
                1
            );
            assert!(
                root.join("bad1").symlink_metadata().is_err()
                    && root.join("bad2").symlink_metadata().is_err()
                    && root.join("bad3").symlink_metadata().is_err(),
                "all bad symlinks must be deleted, not just the first"
            );
        }
    }
}
