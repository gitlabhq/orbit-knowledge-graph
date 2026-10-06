//! Tests requiring native filenames, permissions, or concurrent scheduling.

use orbit_utils::vfs::{
    Decision, File, Limits, Loading, Pass, Put, Source, SourceError, Tag, Vfs,
    sources::{Changed, Checkout},
};
use std::io;
use std::path::Path;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

struct Checked(Arc<AtomicUsize>);
impl Pass for Checked {
    type Tag = ();
    fn header(&self, file: &mut File<()>) {
        file.decide(Decision::Keep(()));
    }
    fn content(&self, file: &mut File<()>, bytes: &[u8]) {
        self.0.fetch_add(1, SeqCst);
        if bytes.contains(&0) {
            file.decide(Decision::List("binary"));
        }
    }
}

#[test]
fn concurrent_puts_deduplicate_and_spill_without_losing_files() {
    struct Workers;
    impl Source for Workers {
        fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
            std::thread::scope(|scope| {
                for worker in 0..8 {
                    scope.spawn(move || {
                        into.put(&format!("shared/{worker}"), Put::Bytes(vec![b'x'; 80]))
                            .unwrap();
                        for file in 0..200 {
                            into.put(
                                &format!("{worker}/{file}"),
                                Put::Bytes(format!("{worker}:{file}").into_bytes()),
                            )
                            .unwrap();
                        }
                    });
                }
            });
            Ok(())
        }
    }
    let vfs = Vfs::load(
        Workers,
        (),
        Limits {
            resident_bytes: Some(100),
            ..Limits::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(vfs.usage().files, 1608);
    assert_eq!(vfs.usage().deduped_bytes, 560);
    assert!(vfs.usage().spilled > 0);
    for worker in 0..8 {
        for file in 0..200 {
            assert_eq!(
                &*vfs.read(Path::new(&format!("{worker}/{file}"))).unwrap(),
                format!("{worker}:{file}").as_bytes()
            );
        }
    }
}

#[test]
fn concurrent_first_reads_run_content_once() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("blob"), b"\0binary").unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let vfs = Vfs::load(
        Checkout(root.path()),
        Checked(count.clone()),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let start = Barrier::new(8);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                start.wait();
                assert_eq!(
                    vfs.read(Path::new("blob")).unwrap_err().kind(),
                    io::ErrorKind::Unsupported
                );
            });
        }
    });
    assert_eq!(count.load(SeqCst), 1);
    assert_eq!(
        vfs.files().next().unwrap().decision(),
        Decision::List("binary")
    );
}

#[test]
fn host_links_and_replaced_parents_are_not_followed() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("dir")).unwrap();
    for path in ["file", "dir/file"] {
        std::fs::write(root.path().join(path), b"inside").unwrap();
    }
    std::fs::write(outside.path().join("file"), b"secret").unwrap();
    symlink(outside.path(), root.path().join("escape")).unwrap();
    symlink(outside.path().join("file"), root.path().join("host-file")).unwrap();
    let mut builder = tar::Builder::new(Vec::new());
    for (path, target) in [
        ("root/escape", outside.path().to_str().unwrap()),
        ("root/chain", "escape"),
        ("root/dangling", "absent"),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o777);
        builder.append_link(&mut header, path, target).unwrap();
    }
    let mut header = tar::Header::new_gnu();
    header.set_size(7);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "root/chain/new/file", &b"payload"[..])
        .unwrap();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut gzip, &builder.into_inner().unwrap()).unwrap();
    let bytes = gzip.finish().unwrap();
    let archive = Vfs::load(
        orbit_utils::vfs::sources::Archive(bytes.as_slice()),
        (),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    for path in ["escape/file", "chain/new/file", "dangling"] {
        assert_eq!(
            archive.read(Path::new(path)).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
    assert!(!outside.path().join("new").exists());
    assert_eq!(
        std::fs::read(outside.path().join("file")).unwrap(),
        b"secret"
    );
    for path in [
        outside.path().join("file").to_str().unwrap(),
        "../file",
        "escape/file",
    ] {
        assert!(
            Vfs::load(
                Changed {
                    root: root.path(),
                    paths: vec![path.into()]
                },
                (),
                Default::default(),
                Default::default()
            )
            .is_err()
        );
    }
    let vfs = Vfs::load(
        Checkout(root.path()),
        Checked(Arc::default()),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        vfs.read(Path::new("host-file")).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    std::fs::remove_file(root.path().join("file")).unwrap();
    symlink(outside.path().join("file"), root.path().join("file")).unwrap();
    std::fs::rename(root.path().join("dir"), root.path().join("old-dir")).unwrap();
    symlink(outside.path(), root.path().join("dir")).unwrap();
    for path in ["file", "dir/file"] {
        assert!(vfs.read(Path::new(path)).is_err());
    }
}

#[test]
fn invalid_scenarios_fail_before_execution() {
    for yaml in [
        "fixtures: []\ntests: []\ntypo: true",
        "fixtures: [{path: file, typo: true}]\ntests: []",
        "fixtures: []\ntests: [{name: read, assert: [{op: read, path: file, expect: {}}]}]",
        "fixtures: []\ntests: [{name: files, assert: [{op: files, expect: [], typo: true}]}]",
        "fixtures: []\nlimits: {typo: 1}\ntests: []",
        "fixtures: []\ntests: []",
        "fixtures: []\ntests: [{name: empty, assert: []}]",
        "fixtures: [{path: file, content: {text: x, repeat: 2}}]\nload_error: empty",
    ] {
        let yaml = format!("name: Invalid scenario\nsources: [memory]\n{yaml}");
        assert!(
            std::panic::catch_unwind(|| super::runner::run(&yaml)).is_err(),
            "accepted invalid scenario: {yaml}"
        );
    }
}

#[test]
fn lazy_readers_run_once_and_only_for_readable_files() {
    struct Files<'a>(&'a AtomicUsize);
    impl Source for Files<'_> {
        fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
            for path in ["kept", "listed", "dropped", "oversize"] {
                into.put(
                    path,
                    Put::Lazy {
                        size: if path == "oversize" { 100 } else { 4 },
                        read: Box::new(move || {
                            assert_eq!(path, "kept", "rejected content must not be read");
                            self.0.fetch_add(1, SeqCst);
                            Ok(b"data".to_vec())
                        }),
                    },
                )?;
            }
            Ok(())
        }
    }
    struct Filter;
    impl Pass for Filter {
        type Tag = ();
        fn header(&self, file: &mut File<()>) {
            file.decide(match file.path.as_str() {
                "listed" => Decision::List("excluded"),
                "dropped" => Decision::Drop("excluded"),
                _ => Decision::Keep(()),
            });
        }
    }
    let reads = AtomicUsize::new(0);
    let vfs = Vfs::load(
        Files(&reads),
        Filter,
        Limits {
            file_bytes: Some(4),
            ..Limits::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(reads.load(SeqCst), 1);
    for _ in 0..2 {
        assert_eq!(&*vfs.read(Path::new("kept")).unwrap(), b"data");
    }
    assert_eq!(reads.load(SeqCst), 1);
    assert_eq!(
        vfs.read(Path::new("listed")).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        vfs.read(Path::new("oversize")).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        vfs.read(Path::new("dropped")).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}

#[test]
fn lazy_errors_size_mismatches_and_overflow_fail_loading() {
    struct Input(u64, bool);
    impl Source for Input {
        fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
            into.put(
                "file",
                Put::Lazy {
                    size: self.0,
                    read: Box::new(move || {
                        assert_ne!(self.0, u64::MAX, "oversize content must not be read");
                        if self.1 {
                            Err(io::Error::other("stream reset"))
                        } else {
                            Ok(b"larger".to_vec())
                        }
                    }),
                },
            )?;
            into.put("extra", Put::Bytes(vec![b'x']))
        }
    }
    for (size, fail, expected) in [
        (6, true, io::ErrorKind::Other),
        (1, false, io::ErrorKind::InvalidData),
    ] {
        let error =
            Vfs::load(Input(size, fail), (), Limits::default(), Default::default()).unwrap_err();
        assert!(matches!(error, SourceError::Io(error) if error.kind() == expected));
    }
    let error = Vfs::load(
        Input(u64::MAX, false),
        (),
        Limits {
            file_bytes: Some(1),
            total_bytes: Some(u64::MAX),
            ..Limits::default()
        },
        Default::default(),
    )
    .unwrap_err();
    assert!(matches!(error, SourceError::Cap(cap) if cap.metric == "total_bytes"));
}

#[test]
fn non_utf8_names_retain_real_disk_paths() {
    use std::os::unix::ffi::OsStrExt;
    let root = tempfile::tempdir().unwrap();
    for name in [b"caf\xff.rs", b"caf\xfe.rs"] {
        if let Err(error) =
            std::fs::write(root.path().join(std::ffi::OsStr::from_bytes(name)), b"data")
        {
            #[cfg(target_os = "macos")]
            if error.raw_os_error() == Some(libc::EILSEQ) {
                return;
            }
            panic!("create non-UTF8 name: {error}");
        }
    }
    let vfs = Vfs::load(
        Checkout(root.path()),
        (),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(vfs.usage().duplicate_paths, 1);
    assert_eq!(&*vfs.read(Path::new("caf\u{fffd}.rs")).unwrap(), b"data");
}

#[test]
fn unreadable_directory_fails_checkout() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let locked = root.path().join("secret");
    std::fs::create_dir(&locked).unwrap();
    std::fs::write(locked.join("file"), b"data").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o0)).unwrap();
    let enforced = std::fs::read_dir(&locked).is_err();
    let result = Vfs::load(
        Checkout(root.path()),
        (),
        Default::default(),
        Default::default(),
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    if enforced {
        assert!(result.is_err());
    }
}
