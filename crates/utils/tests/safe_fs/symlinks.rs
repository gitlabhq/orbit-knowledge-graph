use std::os::unix::fs::symlink;
use std::path::Path;

use orbit_utils::safe_fs::{self, Entry};

#[test]
fn inspection_returns_link_targets_without_following_them() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap();
    std::fs::write(path.join("file"), b"inside").unwrap();
    std::fs::write(outside.path().join("file"), b"secret").unwrap();
    for (name, target) in [
        ("alias", Path::new("file")),
        ("dangling", Path::new("absent")),
        ("chain", Path::new("alias")),
        ("escape", outside.path()),
    ] {
        symlink(target, path.join(name)).unwrap();
        assert!(
            matches!(safe_fs::inspect(&path.join(name)).unwrap(), Some(Entry::Symlink(actual)) if actual == target)
        );
    }
    assert!(safe_fs::inspect(&path.join("escape/file")).is_err());
    assert!(safe_fs::inspect(&path).unwrap().is_none());
}

#[test]
fn readers_reject_replaced_files_and_parent_directories() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap();
    std::fs::create_dir(path.join("dir")).unwrap();
    std::fs::write(outside.path().join("file"), b"secret").unwrap();
    let readers: Vec<_> = ["file", "dir/file"]
        .into_iter()
        .map(|name| {
            std::fs::write(path.join(name), b"inside").unwrap();
            let Some(Entry::File(file)) = safe_fs::inspect(&path.join(name)).unwrap() else {
                panic!("expected a regular file");
            };
            assert_eq!(file.size(), 6);
            assert_eq!(file.read(6).unwrap(), b"inside");
            file
        })
        .collect();

    std::fs::remove_file(path.join("file")).unwrap();
    symlink(outside.path().join("file"), path.join("file")).unwrap();
    std::fs::rename(path.join("dir"), path.join("old-dir")).unwrap();
    symlink(outside.path(), path.join("dir")).unwrap();
    for reader in readers {
        assert!(reader.read(6).is_err());
    }
    assert!(safe_fs::inspect(&path.join("dir/file")).is_err());
    assert_eq!(
        std::fs::read(outside.path().join("file")).unwrap(),
        b"secret"
    );
}
