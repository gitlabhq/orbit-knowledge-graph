use std::io::{self, Write};
use std::path::Path;

use orbit_utils::vfs::{Decision, File, Limits, Pass, SourceError, Vfs, sources::Archive};

fn gzip(builder: tar::Builder<Vec<u8>>) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&builder.into_inner().unwrap()).unwrap();
    encoder.finish().unwrap()
}

fn header(size: u64) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_mode(0o644);
    header.set_size(size);
    header
}

#[test]
fn absolute_and_traversal_archive_paths_fail_loading() {
    for path in ["/root/escape", "root/../../escape", "../escape"] {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = header(1);
        header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
        header.set_cksum();
        builder.append(&header, &b"x"[..]).unwrap();
        let bytes = gzip(builder);
        let error = Vfs::load(
            Archive(bytes.as_slice()),
            (),
            Limits::default(),
            Default::default(),
        )
        .unwrap_err();
        assert!(
            matches!(error, SourceError::Io(error) if error.kind() == io::ErrorKind::InvalidData)
        );
    }
}

#[test]
fn archive_hardlinks_stay_within_the_selected_root() {
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_data(&mut header(7), "root/src/file", &b"content"[..])
        .unwrap();
    let mut link = header(0);
    link.set_entry_type(tar::EntryType::Link);
    builder
        .append_link(&mut link, "root/alias", "root/src/file")
        .unwrap();
    link.set_entry_type(tar::EntryType::Symlink);
    builder
        .append_link(&mut link, "root/chain", "alias")
        .unwrap();
    builder
        .append_data(&mut header(7), "other/file", &b"ignored"[..])
        .unwrap();
    let bytes = gzip(builder);
    let vfs = Vfs::load(
        Archive(bytes.as_slice()),
        (),
        Limits::default(),
        Default::default(),
    )
    .unwrap();
    for path in ["src/file", "alias", "chain"] {
        assert_eq!(&*vfs.read(Path::new(path)).unwrap(), b"content");
    }
    assert_eq!(
        vfs.read(Path::new("file")).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(vfs.usage().files, 3);

    for (target, expected) in [
        ("../secret", io::ErrorKind::InvalidData),
        ("/root/src/file", io::ErrorKind::InvalidData),
        ("other/file", io::ErrorKind::Other),
    ] {
        let mut builder = tar::Builder::new(Vec::new());
        link.set_entry_type(tar::EntryType::Link);
        builder.append_link(&mut link, "root/link", target).unwrap();
        let bytes = gzip(builder);
        let error = Vfs::load(
            Archive(bytes.as_slice()),
            (),
            Limits::default(),
            Default::default(),
        )
        .unwrap_err();
        assert!(matches!(error, SourceError::Io(error) if error.kind() == expected));
    }
}

#[test]
fn pax_headers_do_not_become_files() {
    let mut builder = tar::Builder::new(Vec::new());
    for (path, kind, content) in [
        ("root/pax_global_header", b'g', "comment=x\n"),
        ("root/PaxHeader/file", b'x', "path=root/file\n"),
    ] {
        let mut header = header(content.len() as u64);
        header.set_entry_type(tar::EntryType::new(kind));
        builder
            .append_data(&mut header, path, content.as_bytes())
            .unwrap();
    }
    builder
        .append_data(&mut header(7), "root/file", &b"content"[..])
        .unwrap();
    let bytes = gzip(builder);
    let vfs = Vfs::load(
        Archive(bytes.as_slice()),
        (),
        Limits::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(vfs.read_dir(Path::new("/")).unwrap(), ["file"]);
    assert_eq!(&*vfs.read(Path::new("file")).unwrap(), b"content");
}

#[test]
fn pax_size_enforces_the_file_limit() {
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_pax_extensions([("size", &b"4096"[..])])
        .unwrap();
    builder
        .append_data(&mut header(0), "root/big.txt", vec![b'a'; 4096].as_slice())
        .unwrap();
    let bytes = gzip(builder);
    let vfs = Vfs::load(
        Archive(bytes.as_slice()),
        (),
        Limits {
            file_bytes: Some(64),
            ..Limits::default()
        },
        Default::default(),
    )
    .unwrap();
    let stat = vfs.stat(Path::new("big.txt")).unwrap();
    assert_eq!(stat.len, 4096);
    assert_eq!(stat.decision, Some(Decision::List("oversize")));
    assert_eq!(vfs.usage().resident, 0);
    assert_eq!(
        vfs.read(Path::new("big.txt")).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

#[test]
fn truncated_archive_before_the_first_entry_is_empty() {
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_data(&mut header(4), "root/file", &b"data"[..])
        .unwrap();
    let bytes = gzip(builder);
    for input in [&bytes[..3], &[]] {
        assert!(matches!(
            Vfs::load(Archive(input), (), Limits::default(), Default::default()),
            Err(SourceError::Empty)
        ));
    }
}

#[test]
fn archive_policy_receives_the_complete_body() {
    struct CheckBody;
    impl Pass for CheckBody {
        type Tag = ();
        fn metadata(&self, file: &File<'_, ()>) -> Decision<()> {
            if file.path.ends_with(".png") {
                Decision::Drop("image")
            } else {
                file.decision()
            }
        }
        fn content(&self, file: &File<'_, ()>) -> Decision<()> {
            assert_eq!(file.bytes().unwrap(), b"abcdefgh".repeat(1500));
            Decision::Keep(())
        }
    }
    let body = b"abcdefgh".repeat(1500);
    let mut builder = tar::Builder::new(Vec::new());
    builder
        .append_data(
            &mut header(body.len() as u64),
            "root/big.txt",
            body.as_slice(),
        )
        .unwrap();
    builder
        .append_data(&mut header(5), "root/logo.png", &b"image"[..])
        .unwrap();
    let bytes = gzip(builder);
    let vfs = Vfs::load(
        Archive(bytes.as_slice()),
        CheckBody,
        Limits::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(&*vfs.read(Path::new("big.txt")).unwrap(), body);
    assert_eq!(vfs.read_dir(Path::new("/")).unwrap(), ["big.txt"]);
}
