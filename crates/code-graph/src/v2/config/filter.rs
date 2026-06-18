//! The single filtering policy for code indexing, shared by every file source
//! (Gitaly tar, directory walk) as a [`FileStreamHooks`] implementation.
//!
//! It decides, per file, whether to load the bytes ([`Decision::Keep`]) or
//! record a bare node only ([`Decision::ListOnly`]): excluded extensions and
//! oversize files are settled from the header; binary blobs and minified /
//! long-line bundles from the content. Resolver inputs (`Cargo.toml`,
//! `package.json`, `.gitignore`, …) are never in the denylist, so they survive.
//! It also charges a total-bytes [`Counter`] across every file so a
//! pathologically large repository aborts the whole stream.

use std::path::Path;
use std::sync::LazyLock;

use gkg_utils::fs_stream::{CapExceeded, Counter, Decision, FileInventoryEntry, FileStreamHooks};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rustc_hash::FxHashMap;

/// git's binary heuristic looks at the first 8 KiB; matching it keeps a NUL deep
/// inside a large text file from being misread as binary.
const BINARY_SNIFF_BYTES: usize = 8000;

const MAX_LINE_LENGTH: usize = 64 * 1024;
const MAX_AVG_LINE_LENGTH: usize = 16 * 1024;
const MINIFIED_SIZE_THRESHOLD: usize = 5_000;

/// Why [`CodeFilter`] declined to load a file. Low-cardinality, snake_case for
/// metric labels.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum FilterSkip {
    Oversize,
    ExcludedExtension,
    Binary,
    Minified,
    LineTooLong,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkipTally {
    pub count: u64,
    pub bytes: u64,
}

/// The code-indexing filter. Construct one per repository stream.
pub struct CodeFilter {
    max_file_size: u64,
    total_bytes: Counter,
    skips: FxHashMap<FilterSkip, SkipTally>,
}

impl CodeFilter {
    /// `max_file_size` and `max_total_bytes` are byte caps; `0` means unlimited.
    pub fn new(max_file_size: u64, max_total_bytes: u64) -> Self {
        Self {
            max_file_size,
            total_bytes: Counter::new("total_bytes", max_total_bytes),
            skips: FxHashMap::default(),
        }
    }

    /// Per-reason `(count, bytes)` of files recorded as nodes but not loaded.
    pub fn skips(&self) -> impl Iterator<Item = (FilterSkip, SkipTally)> + '_ {
        self.skips.iter().map(|(reason, tally)| (*reason, *tally))
    }

    fn record(&mut self, reason: FilterSkip, bytes: u64) -> Decision {
        let tally = self.skips.entry(reason).or_default();
        tally.count += 1;
        tally.bytes += bytes;
        Decision::ListOnly
    }
}

impl FileStreamHooks for CodeFilter {
    fn admit(&mut self, file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        self.total_bytes.add(file.size)
    }

    fn on_header(&mut self, file: &FileInventoryEntry) -> Decision {
        if self.max_file_size != 0 && file.size > self.max_file_size {
            return self.record(FilterSkip::Oversize, file.size);
        }
        if is_excluded_from_indexing(Path::new(&file.path)) {
            return self.record(FilterSkip::ExcludedExtension, file.size);
        }
        Decision::Keep
    }

    fn on_content(&mut self, file: &FileInventoryEntry, content: &[u8]) -> Decision {
        let sniff = &content[..content.len().min(BINARY_SNIFF_BYTES)];
        if looks_binary(sniff) {
            return self.record(FilterSkip::Binary, file.size);
        }
        if let Some(reason) = minified_skip(content) {
            return self.record(reason, file.size);
        }
        Decision::Keep
    }
}

/// Detect machine-generated bundles by line shape: a single line over
/// [`MAX_LINE_LENGTH`], or a high average line length over a non-trivial file.
/// Split on `\n` and `\r` so classic-Mac line endings can't hide as one line.
fn minified_skip(content: &[u8]) -> Option<FilterSkip> {
    let mut line_count = 0usize;
    let mut current_line_len = 0usize;
    for &byte in content {
        if byte == b'\n' || byte == b'\r' {
            current_line_len = 0;
            line_count += 1;
        } else {
            current_line_len += 1;
            if current_line_len > MAX_LINE_LENGTH {
                return Some(FilterSkip::LineTooLong);
            }
        }
    }
    if current_line_len > 0 {
        line_count += 1;
    }
    let line_count = line_count.max(1);
    if content.len() / line_count > MAX_AVG_LINE_LENGTH && content.len() > MINIFIED_SIZE_THRESHOLD {
        return Some(FilterSkip::Minified);
    }
    None
}

/// The single denylist of files recorded as a bare node but never loaded or
/// parsed. One group per line, ordered by category. Patterns are globs matched
/// case-insensitively against the basename.
///
/// **Source files, manifests, lockfiles, dotfiles, and unknown extensions are
/// intentionally absent** (beyond the specific build artifacts and test-file
/// conventions below), so resolver inputs survive without an inclusion list
/// that has to track every resolver. This is the one place to add an exclusion.
pub const EXCLUDED_INDEXING_GLOBS: &[&str] = &[
    // Raster + vector images.
    "*.{png,jpg,jpeg,gif,bmp,ico,webp,avif,tiff,tif,svg}",
    // Fonts.
    "*.{ttf,otf,woff,woff2,eot}",
    // Audio / video.
    "*.{mp3,mp4,mov,webm,ogg,wav,flac,m4a,m4v,avi,mkv,opus}",
    // Archives.
    "*.{zip,tar,gz,tgz,bz2,xz,7z,rar,lz4,zst}",
    // Compiled artifacts.
    "*.{exe,dll,so,dylib,class,jar,war,pyc,pyo,o,a,lib}",
    // Documents.
    "*.{pdf,doc,docx,xls,xlsx,ppt,pptx,odt,ods,odp}",
    // Datastores / disk images.
    "*.{db,sqlite,sqlite3,iso,dmg,bin,dat}",
    // Minified JS/TS bundles (the content heuristic catches unnamed ones).
    "*.min.{js,mjs,cjs}",
    // Test files: real source we deliberately keep out of the graph.
    "*_test.go",
];

static EXCLUDED_INDEXING_GLOBSET: LazyLock<GlobSet> = LazyLock::new(|| {
    let mut builder = GlobSetBuilder::new();
    for pat in EXCLUDED_INDEXING_GLOBS {
        builder.add(Glob::new(pat).expect("static excluded-indexing glob"));
    }
    builder.build().expect("static excluded-indexing globset")
});

/// `true` when `rel_path` is on the [`EXCLUDED_INDEXING_GLOBS`] denylist. Match
/// is case-insensitive, on the basename only. `false` means "load it"; resolver
/// inputs fall there because they are not in the denylist.
fn is_excluded_from_indexing(rel_path: &Path) -> bool {
    let Some(name) = rel_path.file_name() else {
        return false;
    };
    let lowered = name.to_string_lossy().to_lowercase();
    EXCLUDED_INDEXING_GLOBSET.is_match(&lowered)
}

/// BOMs that keep a NUL-bearing buffer as text: UTF-16/32 text is full of NULs,
/// so the BOM is what distinguishes it from a binary blob.
const TEXT_BOMS: &[&[u8]] = &[
    &[0x00, 0x00, 0xFE, 0xFF], // UTF-32 BE
    &[0xFF, 0xFE, 0x00, 0x00], // UTF-32 LE
    &[0xEF, 0xBB, 0xBF],       // UTF-8
    &[0xFF, 0xFE],             // UTF-16 LE
    &[0xFE, 0xFF],             // UTF-16 BE
];

/// Binary when a NUL byte appears in `prefix`, like git's `buffer_is_binary`,
/// plus a BOM rescue for UTF-16/32 text (git has none, so BOM-less is dropped).
fn looks_binary(prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return false;
    }
    if TEXT_BOMS.iter().any(|bom| prefix.starts_with(bom)) {
        return false;
    }
    prefix.contains(&0u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Keep,
        }
    }

    #[test]
    fn keeps_source_and_resolver_inputs() {
        let mut f = CodeFilter::new(0, 0);
        for path in ["src/main.rs", "Cargo.toml", "package.json", ".gitignore"] {
            assert_eq!(f.on_header(&entry(path, 100)), Decision::Keep, "{path}");
            assert_eq!(
                f.on_content(&entry(path, 100), b"contents\n"),
                Decision::Keep
            );
        }
    }

    #[test]
    fn list_only_for_excluded_oversize_binary_minified() {
        let mut f = CodeFilter::new(50, 0);
        assert_eq!(f.on_header(&entry("logo.png", 10)), Decision::ListOnly);
        assert_eq!(f.on_header(&entry("big.rs", 999)), Decision::ListOnly);
        assert_eq!(
            f.on_content(&entry("x.bin", 10), b"a\x00b"),
            Decision::ListOnly
        );
        let minified = vec![b'a'; MAX_LINE_LENGTH + 1];
        assert_eq!(
            f.on_content(&entry("bundle.js", 10), &minified),
            Decision::ListOnly
        );
    }

    #[test]
    fn minified_bundles_are_list_only_by_name_without_reading() {
        let mut f = CodeFilter::new(0, 0);
        for path in ["vendor/jquery.min.js", "a/b.min.mjs", "c.min.cjs"] {
            assert_eq!(f.on_header(&entry(path, 200)), Decision::ListOnly, "{path}");
        }
        // The leading dot must be literal — these are real source, not bundles.
        for path in ["src/admin.js", "src/examine.js"] {
            assert_eq!(f.on_header(&entry(path, 200)), Decision::Keep, "{path}");
        }
    }

    #[test]
    fn total_bytes_cap_charges_every_file_then_trips() {
        let mut f = CodeFilter::new(0, 100);
        assert!(f.admit(&entry("a.png", 60)).is_ok());
        assert!(
            f.admit(&entry("b.png", 60)).is_err(),
            "excluded files still count toward the total-bytes cap"
        );
    }

    fn p(s: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(s)
    }

    #[test]
    fn denylist_drops_blobs_minified_and_test_files() {
        for path in [
            "assets/logo.png",
            "img/photo.JPG",
            "fonts/Inter.woff2",
            "audio/track.mp3",
            "dist/bundle.zip",
            "build/lib.so",
            "out/app.exe",
            "vendor/cache.tar.gz",
            "docs/spec.pdf",
            "data/seed.sqlite",
            "vendor/jquery.min.js",
            "web/app.min.mjs",
            "pkg/server_test.go",
            "a/b/c/d/icon.png",
        ] {
            assert!(
                is_excluded_from_indexing(&p(path)),
                "should be excluded: {path}"
            );
        }
    }

    #[test]
    fn denylist_passes_resolver_inputs_and_source() {
        for path in [
            "src/main.rs",
            "frontend/src/index.ts",
            "Cargo.toml",
            "Cargo.lock",
            "package.json",
            "tsconfig.json",
            "config/webpack.config.js",
            ".gitignore",
            ".ignore",
            "README.md",
            "Makefile",
            "src/admin.js",
        ] {
            assert!(
                !is_excluded_from_indexing(&p(path)),
                "should NOT be excluded: {path}"
            );
        }
    }

    #[test]
    fn looks_binary_matches_git_with_bom_rescue() {
        assert!(!looks_binary(b""));
        assert!(!looks_binary(b"fn main() {}\n"));
        assert!(looks_binary(b"abc\x00def"));
        assert!(looks_binary(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"));
        // UTF-16/32 BOMs rescue NUL-bearing text; BOM-less NULs stay binary.
        assert!(!looks_binary(&[0xEF, 0xBB, 0xBF, b'h', b'i']));
        assert!(!looks_binary(&[0xFF, 0xFE, b'h', 0x00, b'i', 0x00]));
        assert!(looks_binary(&[0x68, 0x00, 0x69, 0x00]));
    }
}
