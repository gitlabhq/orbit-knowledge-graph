use crate::vfs::{Decision, File, Pass};

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

pub struct CodePolicy;

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
