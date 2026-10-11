use orbit_utils::vfs::{Decision, File, Pass};

pub(super) struct CodePolicy;

impl Pass for CodePolicy {
    type Tag = ();

    fn metadata(&self, file: &File<'_, ()>) -> Decision<()> {
        let name = file
            .path
            .rsplit('/')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        let extension = name
            .rsplit_once('.')
            .map(|(_, extension)| extension)
            .unwrap_or("");
        if matches!(
            extension,
            "png"
                | "jpg"
                | "jpeg"
                | "gif"
                | "bmp"
                | "ico"
                | "webp"
                | "avif"
                | "tiff"
                | "tif"
                | "svg"
                | "ttf"
                | "otf"
                | "woff"
                | "woff2"
                | "eot"
                | "mp3"
                | "mp4"
                | "mov"
                | "webm"
                | "ogg"
                | "wav"
                | "flac"
                | "m4a"
                | "m4v"
                | "avi"
                | "mkv"
                | "opus"
                | "zip"
                | "tar"
                | "gz"
                | "tgz"
                | "bz2"
                | "xz"
                | "7z"
                | "rar"
                | "lz4"
                | "zst"
                | "exe"
                | "dll"
                | "so"
                | "dylib"
                | "class"
                | "jar"
                | "war"
                | "pyc"
                | "pyo"
                | "o"
                | "a"
                | "lib"
                | "pdf"
                | "doc"
                | "docx"
                | "xls"
                | "xlsx"
                | "ppt"
                | "pptx"
                | "odt"
                | "ods"
                | "odp"
                | "db"
                | "sqlite"
                | "sqlite3"
                | "iso"
                | "dmg"
                | "bin"
                | "dat"
        ) || [".min.js", ".min.mjs", ".min.cjs"]
            .iter()
            .any(|suffix| name.ends_with(suffix))
        {
            Decision::List("excluded_extension")
        } else {
            Decision::Pending
        }
    }

    fn content(&self, file: &File<'_, ()>) -> Decision<()> {
        let Some(bytes) = file.bytes() else {
            return Decision::Pending;
        };
        let text = std::str::from_utf8(bytes);
        if bytes.len() <= 1024
            && bytes.starts_with(b"version https://git-lfs.github.com/spec")
            && text.is_ok_and(|text| {
                text.lines().any(|line| {
                    line.strip_prefix("oid sha256:").is_some_and(|oid| {
                        oid.len() == 64
                            && oid
                                .bytes()
                                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                    })
                }) && text.lines().any(|line| {
                    line.strip_prefix("size ").is_some_and(|size| {
                        !size.is_empty() && size.bytes().all(|byte| byte.is_ascii_digit())
                    })
                })
            })
        {
            return Decision::List("lfs_pointer");
        }
        let text_bom = [
            b"\x00\x00\xfe\xff".as_slice(),
            b"\xff\xfe\x00\x00",
            b"\xef\xbb\xbf",
            b"\xff\xfe",
            b"\xfe\xff",
        ]
        .iter()
        .any(|bom| bytes.starts_with(bom));
        if !text_bom && bytes[..bytes.len().min(8000)].contains(&0) {
            return Decision::List("binary");
        }
        if text.is_err() {
            return Decision::List("not_utf8");
        }
        let mut lines = 0;
        let mut length = 0;
        for byte in bytes {
            if matches!(byte, b'\n' | b'\r') {
                lines += 1;
                length = 0;
            } else {
                length += 1;
                if length > 64 * 1024 {
                    return Decision::List("line_too_long");
                }
            }
        }
        lines += usize::from(length > 0);
        if bytes.len() > 5000 && bytes.len() / lines.max(1) > 16 * 1024 {
            Decision::List("minified")
        } else {
            Decision::Keep(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_utils::vfs::{Limits, Vfs, sources::Memory};

    #[test]
    fn repository_policy_preserves_existing_content_filters() {
        use code_graph::v2::config::{CodeFilter, detect_language_from_path};
        use orbit_utils::fs_walk::{FileInventoryEntry, FileStreamHooks};

        let pointer = b"version https://git-lfs.github.com/spec/v1\noid sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nsize 123\n";
        for (path, bytes) in [
            ("main.rs", b"fn main() {}".to_vec()),
            ("Cargo.toml", b"[package]".to_vec()),
            ("asset.PNG", b"image".to_vec()),
            ("bundle.min.js", b"minified".to_vec()),
            ("examine.js", b"let x = 1;".to_vec()),
            ("binary.rs", b"\0binary".to_vec()),
            ("invalid.rs", vec![255]),
            ("utf16.rs", b"\xff\xfe\0a".to_vec()),
            ("model.py", pointer.to_vec()),
            (
                "not_pointer.txt",
                b"version https://git-lfs.github.com/spec/v1".to_vec(),
            ),
            ("long.js", vec![b'x'; 65537]),
            (
                "minified.js",
                [vec![b'x'; 20000], vec![b'\n'], vec![b'x'; 20000]].concat(),
            ),
            ("late_nul.txt", [vec![b'x'; 8000], vec![0]].concat()),
            ("empty.rs", vec![]),
        ] {
            let mut legacy = CodeFilter::new(None, None, detect_language_from_path);
            let metadata = FileInventoryEntry {
                path: path.into(),
                size: bytes.len() as u64,
                decision: orbit_utils::fs_walk::Decision::Parse,
                label: Default::default(),
            };
            let (_, label) = legacy
                .on_header(&metadata)
                .unwrap_or_else(|| legacy.on_content(&metadata, &bytes));
            let expected = label
                .skip
                .map_or(Decision::Keep(()), |reason| Decision::List(reason.into()));
            let repository = Vfs::load(
                Memory(vec![(path.into(), bytes.clone())]),
                CodePolicy,
                Limits::default(),
                Default::default(),
            )
            .unwrap();
            assert_eq!(
                repository.stat(path).unwrap().decision,
                Some(expected),
                "{path}"
            );
            if expected == Decision::Keep(()) {
                assert_eq!(&*repository.read(path).unwrap(), bytes, "{path}");
            } else {
                assert_eq!(
                    repository.read(path).unwrap_err().kind(),
                    std::io::ErrorKind::Unsupported,
                    "{path}"
                );
                assert_eq!(repository.usage().resident, 0);
            }
        }
    }
}
