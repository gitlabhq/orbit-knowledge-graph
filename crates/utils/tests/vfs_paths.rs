use orbit_utils::vfs::{
    Decision, File, Limits, Options, Pass, Vfs,
    sources::{Archive, Diff, Directory},
};

#[test]
fn native_sources_keep_host_paths_for_lazy_reads() {
    struct LazyFiles;
    impl Pass for LazyFiles {
        type Tag = ();
        fn metadata(&self, _: &File<'_, ()>) -> Decision<()> {
            Decision::KeepLazy(())
        }
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("src")).unwrap();
    let original = root.path().join("src").join("file.rs");
    std::fs::write(&original, b"before").unwrap();
    let directory = Vfs::load(
        Directory(root.path()),
        LazyFiles,
        Limits::default(),
        Options::default(),
    )
    .unwrap();
    let diff = Vfs::load(
        Diff {
            root: root.path(),
            paths: vec![
                std::path::Path::new("src")
                    .join("file.rs")
                    .to_str()
                    .unwrap()
                    .into(),
            ],
        },
        LazyFiles,
        Limits::default(),
        Options::default(),
    )
    .unwrap();
    std::fs::write(original, b"after!").unwrap();
    for vfs in [directory, diff] {
        assert_eq!(
            vfs.files()
                .map(|file| file.path.as_ref())
                .collect::<Vec<_>>(),
            ["src/file.rs"]
        );
        assert_eq!(&*vfs.read("src/file.rs").unwrap(), b"after!");
    }
}

#[test]
fn archives_keep_unix_names_and_link_targets() {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for (path, target, kind) in [
        (r"repo/dir\name/file", "", tar::EntryType::Regular),
        ("repo/alias", r"dir\name/file", tar::EntryType::Symlink),
        ("repo/hard", r"repo/dir\name/file", tar::EntryType::Link),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_entry_type(kind);
        header.set_size(if target.is_empty() { 4 } else { 0 });
        if !target.is_empty() {
            header.set_link_name(target).unwrap();
        }
        header.set_cksum();
        archive
            .append_data(
                &mut header,
                path,
                if target.is_empty() { &b"data"[..] } else { &[] },
            )
            .unwrap();
    }
    let bytes = archive.into_inner().unwrap().finish().unwrap();
    let vfs = Vfs::load(
        Archive(bytes.as_slice()),
        (),
        Limits::default(),
        Options::default(),
    )
    .unwrap();
    for path in [r"dir\name/file", "alias", "hard"] {
        assert_eq!(&*vfs.read(path).unwrap(), b"data");
    }
}

#[cfg(unix)]
#[test]
fn native_names_are_lossless_and_links_stay_virtual() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join(r"file\name"), b"data").unwrap();
    symlink(r"file\name", root.path().join("alias")).unwrap();
    let vfs = Vfs::load(
        Directory(root.path()),
        (),
        Limits::default(),
        Options::default(),
    )
    .unwrap();
    assert_eq!(&*vfs.read("alias").unwrap(), b"data");
    assert_eq!(&*vfs.read(r"file\name").unwrap(), b"data");
}

#[cfg(target_os = "linux")]
#[test]
fn native_sources_reject_non_utf8_names() {
    use std::os::unix::ffi::OsStrExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path()
            .join(std::ffi::OsStr::from_bytes(b"invalid\xff")),
        b"data",
    )
    .unwrap();
    assert!(
        matches!(Vfs::load(Directory(root.path()), (), Limits::default(), Options::default()),
        Err(orbit_utils::vfs::SourceError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData)
    );
}

fn assert_absolute_link_loads_as_dangling(root: &std::path::Path) {
    let vfs = Vfs::load(Directory(root), (), Limits::default(), Options::default()).unwrap();
    assert_eq!(
        vfs.files()
            .map(|file| file.path.as_ref())
            .collect::<Vec<_>>(),
        ["alias"]
    );
    assert_eq!(
        vfs.stat("alias").unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

#[cfg(unix)]
#[test]
fn absolute_symlinks_load_as_dangling_links() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("alias")).unwrap();
    assert_absolute_link_loads_as_dangling(root.path());
}

#[cfg(windows)]
#[test]
fn absolute_junctions_load_as_dangling_links() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let status = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(root.path().join("alias"))
        .arg(outside.path())
        .status()
        .unwrap();
    assert!(status.success());
    assert_absolute_link_loads_as_dangling(root.path());
}
