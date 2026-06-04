//! Shared file filter matching GKG's exclusion rules.

pub const MAX_FILE_SIZE: u64 = 5_000_000; // 5MB, same as GKG default

const EXCLUDED_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "bmp", "ico", "webp", "avif", "tiff", "tif", "svg",
    "ttf", "otf", "woff", "woff2", "eot",
    "mp3", "mp4", "mov", "webm", "ogg", "wav", "flac", "m4a", "m4v", "avi", "mkv", "opus",
    "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar", "lz4", "zst",
    "exe", "dll", "so", "dylib", "class", "jar", "war", "pyc", "pyo", "o", "a", "lib",
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp",
    "db", "sqlite", "sqlite3", "iso", "dmg", "bin", "dat",
];

pub fn is_excluded(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.rsplit('.').next().map_or(false, |ext| EXCLUDED_EXTENSIONS.contains(&ext))
}
