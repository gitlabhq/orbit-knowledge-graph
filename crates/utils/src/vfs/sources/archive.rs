//! Gitaly tar.gz input is processed sequentially. Rejected bodies are streamed past, not retained.
//! Entry paths are validated before stripping the archive root. Hard links name archive entries;
//! symlink targets stay virtual. Malformed streams fail rather than returning partial contents.

use std::io::Read;
use std::sync::Arc;

use flate2::read::GzDecoder;
use tar::EntryType;
use tracing::warn;
use typed_path::{Utf8Component, Utf8Components, Utf8UnixComponent, Utf8UnixPath};

use super::{Loading, Put, Source, SourceError, Tag};

pub struct Archive<R: Read>(pub R);

impl<R: Read> Source for Archive<R> {
    fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        let mut archive = tar::Archive::new(GzDecoder::new(self.0));
        let mut root: Option<String> = None;
        let mut any_entry_seen = false;
        let entries = archive.entries().map_err(std::io::Error::other)?;

        for entry in entries {
            let mut entry = match entry {
                Ok(entry) => entry,
                Err(e) if !any_entry_seen && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!(error = %e, "archive stream truncated before first entry; treating as empty");
                    return Err(SourceError::Empty);
                }
                Err(e) => return Err(SourceError::Io(e)),
            };
            any_entry_seen = true;

            let kind = entry.header().entry_type();
            let is_link = matches!(kind, EntryType::Symlink | EntryType::Link);
            if kind != EntryType::Regular && kind != EntryType::Directory && !is_link {
                continue;
            }
            let entry_path = entry.path_bytes().into_owned();
            let entry_path = std::str::from_utf8(&entry_path)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let Some(path) = relative_path(entry_path, &mut root)? else {
                continue;
            };
            if path.is_empty() || kind == EntryType::Directory {
                continue;
            }
            if is_link {
                let target = entry
                    .link_name_bytes()
                    .filter(|target| !target.is_empty())
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "archive link has no target",
                        )
                    })?;
                let target = std::str::from_utf8(&target)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let target = if kind == EntryType::Link {
                    let relative = relative_path(target, &mut root)?
                        .ok_or_else(|| std::io::Error::other("archive hard link outside root"))?;
                    format!("/{relative}")
                } else {
                    target.to_owned()
                };
                into.put(path, Put::Symlink(target))?;
                continue;
            }
            let size = entry.size();
            let mut stream_error = None;
            let read = Box::new(|| {
                let mut bytes = Vec::new();
                let result = entry.read_to_end(&mut bytes).and_then(|_| {
                    if bytes.len() as u64 == size {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "truncated archive entry",
                        ))
                    }
                });
                if let Err(error) = result {
                    let error = Arc::new(error);
                    stream_error = Some(error.clone());
                    return Err(std::io::Error::new(error.kind(), error));
                }
                Ok(bytes)
            });
            into.put(path, Put::ReadAndStore { size, read })?;
            if let Some(error) = stream_error {
                return Err(std::io::Error::new(error.kind(), error).into());
            }
        }
        std::io::copy(&mut archive.into_inner(), &mut std::io::sink())?;
        if any_entry_seen {
            Ok(())
        } else {
            Err(SourceError::Empty)
        }
    }
}

fn relative_path<'a>(
    path: &'a str,
    root: &mut Option<String>,
) -> Result<Option<&'a str>, SourceError> {
    let path = Utf8UnixPath::new(path);
    let mut components = path.components();
    while matches!(components.clone().next(), Some(Utf8UnixComponent::CurDir)) {
        components.next();
    }
    if !components
        .clone()
        .all(|part| matches!(part, Utf8UnixComponent::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path traversal detected in archive entry",
        )
        .into());
    }
    let Some(first) = components.next() else {
        return Ok(None);
    };
    if first.as_str() != root.get_or_insert_with(|| first.as_str().to_owned()) {
        warn!(entry = %path, "skipping archive entry outside the archive root");
        return Ok(None);
    }
    Ok(Some(components.as_str()))
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, Write};

    use flate2::{Compression, write::GzEncoder};

    use super::*;
    use crate::vfs::{Decision, File, Limits, Options, Pass, Vfs};

    enum Entry<'a> {
        File(&'a str, &'a [u8]),
        Symlink(&'a str, &'a str),
        Directory(&'a str),
    }

    fn header(size: u64, kind: EntryType) -> tar::Header {
        let mut header = tar::Header::new_gnu();
        header.set_size(size);
        header.set_mode(0o644);
        header.set_entry_type(kind);
        header
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn build_archive(entries: &[Entry<'_>]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for entry in entries {
            match entry {
                Entry::File(path, content) => builder
                    .append_data(
                        &mut header(content.len() as u64, EntryType::Regular),
                        path,
                        *content,
                    )
                    .unwrap(),
                Entry::Symlink(path, target) => builder
                    .append_link(&mut header(0, EntryType::Symlink), path, target)
                    .unwrap(),
                Entry::Directory(path) => builder
                    .append_data(&mut header(0, EntryType::Directory), path, std::io::empty())
                    .unwrap(),
            }
        }
        gzip(&builder.into_inner().unwrap())
    }

    fn load(bytes: &[u8]) -> Result<Vfs<()>, SourceError> {
        Vfs::load(Archive(bytes), (), Limits::default(), Options::default())
    }

    fn paths(vfs: &Vfs<()>) -> Vec<&str> {
        vfs.files().map(|file| file.path.as_ref()).collect()
    }

    struct TestFilter;

    impl Pass for TestFilter {
        type Tag = ();

        fn metadata(&self, file: &File<'_, ()>) -> Decision<()> {
            if file.path.ends_with(".png") {
                Decision::List("image")
            } else {
                Decision::Pending
            }
        }

        fn content(&self, file: &File<'_, ()>) -> Decision<()> {
            assert!(!file.path.ends_with(".png"));
            if file.bytes().unwrap().contains(&0) {
                Decision::List("binary")
            } else {
                Decision::Keep(())
            }
        }
    }

    // crates/utils/src/archive.rs::tests::extracts_and_strips_archive_root
    #[test]
    fn extracts_and_strips_archive_root() {
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/src/lib.rs", b"pub mod lib;"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(paths(&vfs), ["src/lib.rs", "src/main.rs"]);
        assert_eq!(&*vfs.read("src/main.rs").unwrap(), b"fn main() {}");
        assert_eq!(&*vfs.read("src/lib.rs").unwrap(), b"pub mod lib;");
        assert_eq!(
            vfs.stat("project-main").unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }

    // crates/utils/src/archive.rs::tests::skips_pax_global_and_per_file_headers
    #[test]
    fn skips_pax_global_and_per_file_headers() {
        for valid_override in [false, true] {
            let mut builder = tar::Builder::new(Vec::new());
            builder
                .append_data(
                    &mut header(10, EntryType::XGlobalHeader),
                    "pax_global_header",
                    &b"comment=x\n"[..],
                )
                .unwrap();
            if valid_override {
                builder
                    .append_pax_extensions([("path", &b"project-main/src/main.rs"[..])])
                    .unwrap();
            } else {
                let body = b"path=project-main/src/main.rs\n";
                builder
                    .append_data(
                        &mut header(body.len() as u64, EntryType::XHeader),
                        "PaxHeader/main.rs",
                        &body[..],
                    )
                    .unwrap();
            }
            let content = b"fn main() {}";
            builder
                .append_data(
                    &mut header(content.len() as u64, EntryType::Regular),
                    if valid_override {
                        "ignored/header-name"
                    } else {
                        "project-main/src/main.rs"
                    },
                    &content[..],
                )
                .unwrap();
            let vfs = load(&gzip(&builder.into_inner().unwrap())).unwrap();
            assert_eq!(paths(&vfs), ["src/main.rs"]);
            assert_eq!(&*vfs.read("src/main.rs").unwrap(), content);
        }
    }

    #[test]
    fn leading_dot_paths_share_the_archive_root() {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, kind, target, body) in [
            ("./", EntryType::Directory, "", &b""[..]),
            ("././repo/", EntryType::Directory, "", &b""[..]),
            ("./repo/file", EntryType::Regular, "", &b"data"[..]),
            ("repo/other", EntryType::Regular, "", &b"other"[..]),
            ("./repo/alias", EntryType::Symlink, "./file", &b""[..]),
            ("./repo/hard", EntryType::Link, "././repo/file", &b""[..]),
            ("./other/ignored", EntryType::Regular, "", &b"ignored"[..]),
        ] {
            let mut header = header(body.len() as u64, kind);
            header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
            header.as_mut_bytes()[157..157 + target.len()].copy_from_slice(target.as_bytes());
            header.set_cksum();
            builder.append(&header, body).unwrap();
        }
        let vfs = load(&gzip(&builder.into_inner().unwrap())).unwrap();
        assert_eq!(paths(&vfs), ["alias", "file", "hard", "other"]);
        for path in ["file", "alias", "hard"] {
            assert_eq!(&*vfs.read(path).unwrap(), b"data");
        }
        assert_eq!(&*vfs.read("other").unwrap(), b"other");
    }

    // crates/utils/src/archive.rs::tests::skips_entry_outside_archive_root_and_keeps_the_rest
    #[test]
    fn skips_entry_outside_archive_root_and_keeps_the_rest() {
        let data = build_archive(&[
            Entry::File("root-a/file1.rs", b"a"),
            Entry::File("root-b/file2.rs", b"b"),
            Entry::File("root-a/file3.rs", b"c"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(paths(&vfs), ["file1.rs", "file3.rs"]);
        assert_eq!(&*vfs.read("file1.rs").unwrap(), b"a");
        assert_eq!(&*vfs.read("file3.rs").unwrap(), b"c");
    }

    // crates/utils/src/archive.rs::tests::skips_entry_whose_name_is_too_long_and_keeps_the_rest — virtual names remain readable.
    #[test]
    fn retains_long_filename_and_keeps_the_rest() {
        let name = format!("{}.rs", "z".repeat(500));
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File(&format!("project-main/{name}"), b"unwritable"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(paths(&vfs), ["src/main.rs", &name]);
        assert_eq!(&*vfs.read("src/main.rs").unwrap(), b"fn main() {}");
        assert_eq!(&*vfs.read(&name).unwrap(), b"unwritable");
    }

    // crates/utils/src/archive.rs::tests::skips_entry_whose_directory_name_is_too_long_and_keeps_the_rest — virtual directories have no host limit.
    #[test]
    fn retains_long_directory_and_keeps_the_rest() {
        let directory = "z".repeat(500);
        let path = format!("{directory}/f.rs");
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File(&format!("project-main/{path}"), b"unwritable"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(paths(&vfs), ["src/main.rs", &path]);
        assert_eq!(&*vfs.read("src/main.rs").unwrap(), b"fn main() {}");
        assert_eq!(&*vfs.read(&path).unwrap(), b"unwritable");
        assert_eq!(vfs.read_dir(&directory).unwrap(), ["f.rs"]);
    }

    // crates/utils/src/archive.rs::tests::rejects_path_traversal
    #[test]
    fn rejects_path_traversal() {
        for path in [
            "root/../../escape.txt",
            "../escape.txt",
            "/root/escape.txt",
            "./root/../../escape.txt",
            "./../escape.txt",
            "././root/../escape.txt",
        ] {
            let mut builder = tar::Builder::new(Vec::new());
            let mut header = header(9, EntryType::Regular);
            header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
            header.set_cksum();
            builder.append(&header, &b"malicious"[..]).unwrap();
            let error = load(&gzip(&builder.into_inner().unwrap())).unwrap_err();
            assert!(
                matches!(&error, SourceError::Io(error) if error.kind() == ErrorKind::InvalidData)
            );
            assert!(error.to_string().contains("path traversal"), "{error}");
        }
    }

    // crates/utils/src/archive.rs::tests::skips_symlink_escaping_target_directory
    #[test]
    fn escaping_symlinks_cannot_read_host_files() {
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret");
        std::fs::write(&secret, b"host content").unwrap();
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"hello"),
            Entry::Symlink("root/escape", secret.to_str().unwrap()),
            Entry::Symlink("root/parent", "../legit.txt"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(&*vfs.read("legit.txt").unwrap(), b"hello");
        for path in ["escape", "parent"] {
            assert_eq!(vfs.read(path).unwrap_err().kind(), ErrorKind::NotFound);
        }
        assert_eq!(std::fs::read(secret).unwrap(), b"host content");
    }

    // crates/utils/src/archive.rs::tests::removes_skipped_symlinks_from_inventory — VFS catalogs unresolved links without making them readable.
    #[test]
    fn catalogs_unresolved_symlinks_without_readable_content() {
        let data = build_archive(&[
            Entry::File("root/legit.txt", b"hello"),
            Entry::Symlink("root/escape", "../outside"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(paths(&vfs), ["escape", "legit.txt"]);
        assert_eq!(
            vfs.files().next().unwrap().decision(),
            Decision::List("symlink")
        );
        assert_eq!(vfs.read("escape").unwrap_err().kind(), ErrorKind::NotFound);
        assert_eq!(vfs.stat("escape").unwrap_err().kind(), ErrorKind::NotFound);
        assert_eq!(&*vfs.read("legit.txt").unwrap(), b"hello");
    }

    // crates/utils/src/archive.rs::tests::allows_valid_internal_symlink
    #[test]
    fn allows_valid_internal_symlink() {
        let data = build_archive(&[
            Entry::File("root/src/lib.rs", b"real content"),
            Entry::Symlink("root/bin/run", "../src/lib.rs"),
        ]);
        let vfs = load(&data).unwrap();
        assert_eq!(&*vfs.read("bin/run").unwrap(), b"real content");
        assert_eq!(
            vfs.stat("bin/run").unwrap().link.as_deref(),
            Some("../src/lib.rs")
        );
    }

    // crates/utils/src/archive.rs::tests::empty_and_truncated_bodies_are_classified_empty
    #[test]
    fn empty_and_truncated_bodies_are_classified_empty() {
        let full = build_archive(&[Entry::File("project-main/src/main.rs", b"fn main() {}")]);
        for bytes in [&[][..], &full[..3], &full[..full.len() / 2]] {
            assert!(matches!(load(bytes), Err(SourceError::Empty)));
        }
    }

    // crates/utils/src/archive.rs::tests::list_only_files_are_recorded_but_not_written
    #[test]
    fn list_only_files_are_recorded_but_not_stored() {
        let data = build_archive(&[
            Entry::File("project-main/src/main.rs", b"fn main() {}"),
            Entry::File("project-main/assets/logo.png", b"\x89PNGdata"),
            Entry::File("project-main/model/weights.onnx", b"\x00\x01\x02blob"),
        ]);
        let vfs = Vfs::load(
            Archive(data.as_slice()),
            TestFilter,
            Limits::default(),
            Options::default(),
        )
        .unwrap();
        assert_eq!(
            paths(&vfs),
            ["assets/logo.png", "model/weights.onnx", "src/main.rs"]
        );
        assert_eq!(
            vfs.stat("src/main.rs").unwrap().decision,
            Some(Decision::Keep(()))
        );
        for (path, reason) in [
            ("assets/logo.png", "image"),
            ("model/weights.onnx", "binary"),
        ] {
            assert_eq!(
                vfs.stat(path).unwrap().decision,
                Some(Decision::List(reason))
            );
            assert_eq!(vfs.read(path).unwrap_err().kind(), ErrorKind::Unsupported);
        }
        assert_eq!(&*vfs.read("src/main.rs").unwrap(), b"fn main() {}");
        assert_eq!(vfs.usage().resident, 12);
        assert_eq!(vfs.usage().spilled, 0);
    }

    // crates/utils/src/archive.rs::tests::text_file_larger_than_sniff_window_is_written_in_full
    #[test]
    fn text_file_larger_than_sniff_window_is_stored_in_full() {
        let body: Vec<u8> = (0..12_000).map(|i| ((i % 254) + 1) as u8).collect();
        let data = build_archive(&[Entry::File("project-main/big.txt", &body)]);
        let vfs = load(&data).unwrap();
        assert_eq!(&*vfs.read("big.txt").unwrap(), body);
        assert_eq!(vfs.stat("big.txt").unwrap().len, body.len() as u64);
    }

    // crates/utils/src/archive.rs::tests::oversize_entry_is_filtered_when_the_size_comes_from_a_pax_record
    #[test]
    fn oversize_entry_is_filtered_when_the_size_comes_from_a_pax_record() {
        let mut builder = tar::Builder::new(Vec::new());
        builder
            .append_pax_extensions([("size", &b"4096"[..])])
            .unwrap();
        builder
            .append_data(
                &mut header(0, EntryType::Regular),
                "project-main/big.txt",
                &vec![b'a'; 4096][..],
            )
            .unwrap();
        let data = gzip(&builder.into_inner().unwrap());
        let vfs = Vfs::load(
            Archive(data.as_slice()),
            (),
            Limits {
                file_bytes: Some(64),
                ..Limits::default()
            },
            Options::default(),
        )
        .unwrap();
        assert_eq!(paths(&vfs), ["big.txt"]);
        let file = vfs.stat("big.txt").unwrap();
        assert_eq!(file.len, 4096);
        assert_eq!(file.decision, Some(Decision::List("oversize")));
        assert_eq!(
            vfs.read("big.txt").unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(vfs.usage().resident, 0);
        assert_eq!(vfs.usage().spilled, 0);
    }

    #[test]
    fn directory_headers_select_the_root_after_dot_entries() {
        for dot in [".", "./", ".//", "root-a/"] {
            let data = build_archive(&[
                Entry::Directory(dot),
                Entry::Directory("root-a/"),
                Entry::File("root-b/wrong.rs", b"wrong"),
                Entry::File("root-a/right.rs", b"right"),
            ]);
            let vfs = load(&data).unwrap();
            assert_eq!(paths(&vfs), ["right.rs"]);
            assert_eq!(&*vfs.read("right.rs").unwrap(), b"right");
        }
    }

    #[test]
    fn corrupt_or_missing_gzip_trailers_abort_loading() {
        let data = build_archive(&[Entry::File("root/file", b"content")]);
        for offset in [8, 4, 0] {
            let mut corrupt = data.clone();
            let len = corrupt.len();
            if offset == 0 {
                corrupt.truncate(len - 8);
            } else {
                corrupt[len - offset] ^= 1;
            }
            assert!(matches!(load(&corrupt), Err(SourceError::Io(_))));
        }
    }

    #[test]
    fn truncated_bodies_abort_loading_even_when_filtered() {
        for path in ["root/file.rs", "root/file.png"] {
            let mut builder = tar::Builder::new(Vec::new());
            builder
                .append_data(
                    &mut header(4096, EntryType::Regular),
                    path,
                    &vec![b'x'; 4096][..],
                )
                .unwrap();
            let tar = builder.into_inner().unwrap();
            let data = gzip(&tar[..612]);
            let result = Vfs::load(
                Archive(data.as_slice()),
                TestFilter,
                Limits::default(),
                Options::default(),
            );
            assert!(matches!(result, Err(SourceError::Io(_))));
        }
    }
}
