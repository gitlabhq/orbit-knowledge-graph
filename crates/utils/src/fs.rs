//! Filesystem security utilities.
//!
//! Functions for safe directory creation, symlink validation, and path
//! traversal prevention. These are security-critical and should be
//! reviewed carefully before modification.

use std::io;
use std::path::{Path, PathBuf};

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

/// Walk a directory tree and reject any symlink that resolves outside
/// `root` or is dangling. Deletes offending symlinks before returning
/// the error.
///
/// The scan never short-circuits: every entry is visited and every bad
/// symlink is deleted even if earlier entries failed. This prevents a
/// malicious archive from planting multiple escaping symlinks where only
/// the first gets cleaned up. The first error encountered is returned
/// after the full traversal completes.
pub fn validate_symlinks(root: &Path) -> io::Result<()> {
    // Accumulates the first error without stopping the scan.
    let mut first_err: Option<io::Error> = None;
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
                // check_symlink deletes the symlink via remove_file before
                // returning Err, so the bad link is gone regardless of
                // whether we capture or propagate the error.
                if let Err(e) = check_symlink(&path, root) {
                    first_err = first_err.or(Some(e));
                }
            } else if meta.is_dir() {
                stack.push(path);
            }
        }
    }
    first_err.map_or(Ok(()), Err)
}

fn check_symlink(path: &Path, root: &Path) -> io::Result<()> {
    let display = path.strip_prefix(root).unwrap_or(path);
    let err = match path.canonicalize() {
        Ok(r) if r.starts_with(root) => return Ok(()),
        Ok(r) => io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "symlink target escapes target directory: {} -> {}",
                display.display(),
                r.display()
            ),
        ),
        Err(_) => io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("dangling symlink: {}", display.display()),
        ),
    };
    let _ = std::fs::remove_file(path);
    Err(err)
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
            validate_symlinks(&root).unwrap();
        }
    }

    #[test]
    fn validate_symlinks_rejects_escaping() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), root.join("escape")).unwrap();
            let err = validate_symlinks(&root).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        }
    }

    #[test]
    fn validate_symlinks_rejects_dangling() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("nonexistent/target", root.join("bad")).unwrap();
            let err = validate_symlinks(&root).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
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

            let err = validate_symlinks(&root);
            assert!(err.is_err());
            assert!(
                root.join("bad1").symlink_metadata().is_err()
                    && root.join("bad2").symlink_metadata().is_err()
                    && root.join("bad3").symlink_metadata().is_err(),
                "all bad symlinks must be deleted, not just the first"
            );
        }
    }
}
