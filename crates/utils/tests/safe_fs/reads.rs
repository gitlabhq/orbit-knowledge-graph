use std::io;

use orbit_utils::safe_fs::{self, Entry, SizeLimitExceeded};

#[test]
fn single_component_relative_paths_can_be_inspected_and_read() {
    let mut temporary = tempfile::NamedTempFile::new_in(std::env::current_dir().unwrap()).unwrap();
    std::io::Write::write_all(&mut temporary, b"data").unwrap();
    let name = std::path::Path::new(temporary.path().file_name().unwrap());
    let Some(Entry::File(file)) = safe_fs::inspect(name).unwrap() else {
        panic!("expected a regular file");
    };
    assert_eq!(file.size(), 4);
    assert_eq!(file.read(4).unwrap(), b"data");
}

#[test]
fn read_limits_are_inclusive_and_checked_before_opening() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap().join("file");
    std::fs::write(&path, b"data").unwrap();
    let Some(Entry::File(file)) = safe_fs::inspect(&path).unwrap() else {
        panic!("expected a regular file");
    };
    assert_eq!(file.read(4).unwrap(), b"data");
    assert_eq!(file.read(5).unwrap(), b"data");
    std::fs::remove_file(&path).unwrap();
    let error = file.read(3).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::FileTooLarge);
    let limit = error
        .get_ref()
        .unwrap()
        .downcast_ref::<SizeLimitExceeded>()
        .unwrap();
    assert_eq!(limit.size, 4);
    assert_eq!(limit.max_bytes, 3);

    std::fs::write(&path, b"grown").unwrap();
    assert_eq!(
        file.read(4).unwrap_err().kind(),
        io::ErrorKind::FileTooLarge
    );
    assert_eq!(file.read(5).unwrap_err().kind(), io::ErrorKind::InvalidData);

    std::fs::write(&path, b"").unwrap();
    let Some(Entry::File(empty)) = safe_fs::inspect(&path).unwrap() else {
        panic!("expected an empty regular file");
    };
    assert!(empty.read(0).unwrap().is_empty());
    std::fs::write(&path, b"x").unwrap();
    assert_eq!(
        empty.read(0).unwrap_err().kind(),
        io::ErrorKind::FileTooLarge
    );
}
