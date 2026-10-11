use orbit_utils::vfs::{Decision, File, Pass};

const EXCLUDED_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "bmp", "ico", "webp", "avif", "tiff", "tif", "svg", "ttf", "otf",
    "woff", "woff2", "eot", "mp3", "mp4", "mov", "webm", "ogg", "wav", "flac", "m4a", "m4v", "avi",
    "mkv", "opus", "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar", "lz4", "zst", "exe", "dll",
    "so", "dylib", "class", "jar", "war", "pyc", "pyo", "o", "a", "lib", "pdf", "doc", "docx",
    "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "db", "sqlite", "sqlite3", "iso", "dmg",
    "bin", "dat",
];
const EXCLUDED_SUFFIXES: &[&str] = &[".min.js", ".min.mjs", ".min.cjs"];
const BINARY_SNIFF_BYTES: usize = 8000;
const MAX_LINE_LENGTH: usize = 64 * 1024;
const MAX_AVG_LINE_LENGTH: usize = 16 * 1024;
const MINIFIED_SIZE_THRESHOLD: usize = 5_000;
const LFS_POINTER_MAX_BYTES: usize = 1024;
const LFS_POINTER_VERSION_PREFIX: &[u8] = b"version https://git-lfs.github.com/spec";
const LFS_OBJECT_ID_PREFIX: &str = "oid sha256:";
const LFS_SIZE_PREFIX: &str = "size ";
const SHA256_HEX_LENGTH: usize = 64;
const TEXT_BOMS: &[&[u8]] = &[
    b"\x00\x00\xfe\xff",
    b"\xff\xfe\x00\x00",
    b"\xef\xbb\xbf",
    b"\xff\xfe",
    b"\xfe\xff",
];

pub(super) struct CodePolicy;

impl Pass for CodePolicy {
    type Tag = ();

    fn metadata(&self, file: &File<'_, ()>) -> Decision<()> {
        if is_excluded_from_indexing(&file.path) {
            Decision::List("excluded_extension")
        } else {
            Decision::Pending
        }
    }

    fn content(&self, file: &File<'_, ()>) -> Decision<()> {
        let Some(bytes) = file.bytes() else {
            return Decision::Pending;
        };
        if is_lfs_pointer(bytes) {
            return Decision::List("lfs_pointer");
        }
        let sniff = &bytes[..bytes.len().min(BINARY_SNIFF_BYTES)];
        if looks_binary(sniff) {
            return Decision::List("binary");
        }
        if std::str::from_utf8(bytes).is_err() {
            return Decision::List("not_utf8");
        }
        minified_skip(bytes).map_or(Decision::Keep(()), Decision::List)
    }
}

fn is_excluded_from_indexing(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or("").to_lowercase();
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension)
        .unwrap_or("");
    EXCLUDED_EXTENSIONS.contains(&extension)
        || EXCLUDED_SUFFIXES
            .iter()
            .any(|suffix| name.ends_with(suffix))
}

fn is_lfs_pointer(bytes: &[u8]) -> bool {
    if bytes.len() > LFS_POINTER_MAX_BYTES || !bytes.starts_with(LFS_POINTER_VERSION_PREFIX) {
        return false;
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let has_object_id = text.lines().any(|line| {
        line.strip_prefix(LFS_OBJECT_ID_PREFIX).is_some_and(|id| {
            id.len() == SHA256_HEX_LENGTH
                && id
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
    });
    let has_size = text.lines().any(|line| {
        line.strip_prefix(LFS_SIZE_PREFIX)
            .is_some_and(|size| !size.is_empty() && size.bytes().all(|byte| byte.is_ascii_digit()))
    });
    has_object_id && has_size
}

fn looks_binary(prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return false;
    }
    if TEXT_BOMS.iter().any(|bom| prefix.starts_with(bom)) {
        return false;
    }
    prefix.contains(&0)
}

fn minified_skip(content: &[u8]) -> Option<&'static str> {
    let mut line_count = 0usize;
    let mut current_line_len = 0usize;
    for &byte in content {
        if byte == b'\n' || byte == b'\r' {
            current_line_len = 0;
            line_count += 1;
        } else {
            current_line_len += 1;
            if current_line_len > MAX_LINE_LENGTH {
                return Some("line_too_long");
            }
        }
    }
    if current_line_len > 0 {
        line_count += 1;
    }
    let line_count = line_count.max(1);
    if content.len() / line_count > MAX_AVG_LINE_LENGTH && content.len() > MINIFIED_SIZE_THRESHOLD {
        return Some("minified");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph::v2::config::{CodeFilter, detect_language_from_path};
    use orbit_utils::fs_walk::{FileInventoryEntry, FileStreamHooks};
    use orbit_utils::vfs::{Limits, Vfs, sources::Memory};

    fn assert_matches_legacy(path: &str, bytes: &[u8]) {
        let mut legacy = CodeFilter::new(None, None, detect_language_from_path);
        let metadata = FileInventoryEntry {
            path: path.into(),
            size: bytes.len() as u64,
            decision: orbit_utils::fs_walk::Decision::Parse,
            label: Default::default(),
        };
        let (_, label) = legacy
            .on_header(&metadata)
            .unwrap_or_else(|| legacy.on_content(&metadata, bytes));
        let expected = label
            .skip
            .map_or(Decision::Keep(()), |reason| Decision::List(reason.into()));
        let repository = Vfs::load(
            Memory(vec![(path.into(), bytes.to_vec())]),
            CodePolicy,
            Limits::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            repository.stat(path).unwrap().decision,
            Some(expected),
            "{path}, {} bytes",
            bytes.len()
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

    #[test]
    fn exclusions_match_every_legacy_glob() {
        use std::collections::BTreeSet;
        let legacy: BTreeSet<_> = code_graph::v2::config::EXCLUDED_INDEXING_GLOBS
            .iter()
            .flat_map(|pattern| {
                let (prefix, extensions) = pattern.split_once('{').unwrap();
                extensions
                    .trim_end_matches('}')
                    .split(',')
                    .map(move |extension| format!("{}{}", &prefix[1..], extension))
            })
            .collect();
        let current: BTreeSet<_> = EXCLUDED_EXTENSIONS
            .iter()
            .map(|extension| format!(".{extension}"))
            .chain(EXCLUDED_SUFFIXES.iter().map(|suffix| (*suffix).to_owned()))
            .collect();
        assert_eq!(current, legacy);
        for suffix in current {
            for name in [
                format!("file{suffix}"),
                format!("dir.with.dots/file{}", suffix.to_uppercase()),
                suffix,
            ] {
                assert_matches_legacy(&name, b"content");
            }
        }
        for name in [
            "README",
            "Cargo.toml",
            "package.json",
            ".gitignore",
            "examine.js",
            "image.png.rs",
            "file.İSO",
            "file.bİn",
        ] {
            assert_matches_legacy(name, b"content");
        }
    }

    #[test]
    fn content_boundaries_match_legacy() {
        for size in [4999, 5000, 5001, 16383, 16384, 16385, 65535, 65536, 65537] {
            for ending in [b"".as_slice(), b"\n", b"\r", b"\r\n"] {
                assert_matches_legacy("file.js", &[vec![b'x'; size], ending.to_vec()].concat());
            }
        }
        for position in [0, 7999, 8000, 8001] {
            let mut content = vec![b'x'; 8002];
            content[position] = 0;
            assert_matches_legacy("file.txt", &content);
        }
        for bom in TEXT_BOMS {
            assert_matches_legacy("file.txt", &[bom.to_vec(), b"\0text".to_vec()].concat());
            assert_matches_legacy("file.txt", bom);
        }
        let pointer = format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize 123\n",
            "a".repeat(64)
        );
        for size in [1023, 1024, 1025] {
            let mut content = pointer.as_bytes().to_vec();
            content.resize(size, b' ');
            assert_matches_legacy("file.py", &content);
        }
        for id in [
            "a".repeat(63),
            "a".repeat(64),
            "a".repeat(65),
            "A".repeat(64),
        ] {
            for size in ["", "0", "-1", "123", "12x"] {
                let content = format!(
                    "version https://git-lfs.github.com/spec/v1\noid sha256:{id}\nsize {size}\n"
                );
                assert_matches_legacy("file.py", content.as_bytes());
            }
        }
    }

    #[test]
    fn repository_policy_preserves_existing_content_filters() {
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
            assert_matches_legacy(path, &bytes);
        }
    }
}
