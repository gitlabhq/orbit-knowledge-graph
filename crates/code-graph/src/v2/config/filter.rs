//! The single filtering policy for code indexing, shared by every file source
//! as a [`FileStreamHooks`] implementation. Per file it produces the full
//! [`Decision`]: `Parse` (source), `Load` (resolver inputs: on disk, not
//! parsed), `ListOnly` (excluded/oversize/binary/minified: a node, no bytes), or
//! `Drop`. Resolver inputs are never in the denylist, so they survive. A
//! total-bytes [`Counter`] aborts an oversized repo.

use std::path::Path;
use std::sync::LazyLock;

use gkg_utils::fs_stream::{CapExceeded, Counter, Decision, FileInventoryEntry, FileStreamHooks};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rustc_hash::FxHashMap;

use super::Language;

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
    NotUtf8,
    Minified,
    LineTooLong,
    NonRegularFile,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkipTally {
    pub count: u64,
    pub bytes: u64,
}

/// The code-indexing filter. Construct one per repository stream. Classifies
/// each file fully (load+parse / load-only / node / drop): the language detector
/// is injected so the filter never hard-wires the registry.
pub struct CodeFilter {
    max_file_size: u64,
    total_bytes: Counter,
    skips: FxHashMap<FilterSkip, SkipTally>,
    detect_language: fn(&str) -> Option<Language>,
    file_reasons: FxHashMap<String, FilterSkip>,
}

impl CodeFilter {
    /// `max_file_size` and `max_total_bytes` are byte caps (`0` = unlimited).
    /// `detect_language` decides parse candidacy (e.g. `detect_language_from_path`).
    pub fn new(
        max_file_size: u64,
        max_total_bytes: u64,
        detect_language: fn(&str) -> Option<Language>,
    ) -> Self {
        Self {
            max_file_size,
            total_bytes: Counter::new("total_bytes", max_total_bytes),
            skips: FxHashMap::default(),
            detect_language,
            file_reasons: FxHashMap::default(),
        }
    }

    /// Per-reason `(count, bytes)` of files recorded as nodes but not loaded.
    pub fn skips(&self) -> impl Iterator<Item = (FilterSkip, SkipTally)> + '_ {
        self.skips.iter().map(|(reason, tally)| (*reason, *tally))
    }

    /// Per-path skip reason for every file the stream settled as a bare node,
    /// so the pipeline can stamp it onto the File node's `gl_file.reason`.
    pub fn file_reasons(&self) -> &FxHashMap<String, FilterSkip> {
        &self.file_reasons
    }

    fn record(&mut self, file: &FileInventoryEntry, reason: FilterSkip) -> Decision {
        let tally = self.skips.entry(reason).or_default();
        tally.count += 1;
        tally.bytes += file.size;
        self.file_reasons.insert(file.path.clone(), reason);
        Decision::ListOnly
    }
}

impl FileStreamHooks for CodeFilter {
    fn admit(&mut self, file: &FileInventoryEntry) -> Result<(), CapExceeded> {
        self.total_bytes.add(file.size)
    }

    fn on_header(&mut self, file: &FileInventoryEntry) -> Option<Decision> {
        if self.max_file_size != 0 && file.size > self.max_file_size {
            return Some(self.record(file, FilterSkip::Oversize));
        }
        if is_excluded_from_indexing(Path::new(&file.path)) {
            return Some(self.record(file, FilterSkip::ExcludedExtension));
        }
        None
    }

    fn on_content(&mut self, file: &FileInventoryEntry, content: &[u8]) -> Decision {
        let sniff = &content[..content.len().min(BINARY_SNIFF_BYTES)];
        if looks_binary(sniff) {
            return self.record(file, FilterSkip::Binary);
        }
        // Parsers all need `&str`; validate once here so they can assume UTF-8.
        if std::str::from_utf8(content).is_err() {
            return self.record(file, FilterSkip::NotUtf8);
        }
        if let Some(reason) = minified_skip(content) {
            return self.record(file, reason);
        }
        // A parse candidate is parsed; a non-parsable file (resolver input) is
        // loaded for resolvers but not parsed.
        if (self.detect_language)(&file.path).is_some() {
            Decision::Parse
        } else {
            Decision::Load
        }
    }

    fn on_non_regular(&mut self, file: &FileInventoryEntry) -> Decision {
        self.record(file, FilterSkip::NonRegularFile)
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
/// parsed: globs matched case-insensitively on the basename, grouped by line.
/// Source (including tests), manifests, lockfiles, and dotfiles are absent so
/// resolver inputs survive — this is the one place to add an exclusion.
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
    use crate::v2::config::detect_language_from_path;

    fn entry(path: &str, size: u64) -> FileInventoryEntry {
        FileInventoryEntry {
            path: path.into(),
            size,
            decision: Decision::Parse,
        }
    }

    fn filter() -> CodeFilter {
        CodeFilter::new(0, 0, detect_language_from_path)
    }

    #[test]
    fn records_per_file_reason_only_for_settled_files() {
        let mut f = filter();
        f.on_header(&entry("logo.png", 10));
        f.on_content(&entry("x.bin", 10), b"a\x00b");
        f.on_content(&entry("main.rs", 10), b"fn main() {}\n");
        f.on_non_regular(&entry("link.rs", 5));
        assert_eq!(
            f.file_reasons().get("logo.png"),
            Some(&FilterSkip::ExcludedExtension)
        );
        assert_eq!(f.file_reasons().get("x.bin"), Some(&FilterSkip::Binary));
        assert_eq!(
            f.file_reasons().get("link.rs"),
            Some(&FilterSkip::NonRegularFile)
        );
        assert!(!f.file_reasons().contains_key("main.rs"));
    }

    #[test]
    fn parses_source_and_loads_resolver_inputs() {
        let mut f = filter();
        assert_eq!(f.on_header(&entry("src/main.rs", 100)), None);
        assert_eq!(
            f.on_content(&entry("src/main.rs", 100), b"fn main() {}\n"),
            Decision::Parse
        );
        assert_eq!(
            f.on_content(&entry("Cargo.toml", 100), b"[package]\n"),
            Decision::Load
        );
        assert_eq!(
            f.on_content(&entry(".gitignore", 100), b"target/\n"),
            Decision::Load
        );
    }

    #[test]
    fn list_only_for_excluded_oversize_binary_minified() {
        let mut f = CodeFilter::new(50, 0, detect_language_from_path);
        assert_eq!(
            f.on_header(&entry("logo.png", 10)),
            Some(Decision::ListOnly)
        );
        assert_eq!(f.on_header(&entry("big.rs", 999)), Some(Decision::ListOnly));
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
    fn minified_bundles_settled_by_name_in_header() {
        let mut f = filter();
        for path in ["vendor/jquery.min.js", "a/b.min.mjs", "c.min.cjs"] {
            assert_eq!(
                f.on_header(&entry(path, 200)),
                Some(Decision::ListOnly),
                "{path}"
            );
        }
        // The leading dot must be literal — these are real source, not bundles.
        for path in ["src/admin.js", "src/examine.js"] {
            assert_eq!(f.on_header(&entry(path, 200)), None, "{path}");
        }
    }

    #[test]
    fn identical_parse_candidates_are_each_parsed() {
        // Byte-identical files at different paths are distinct graph entities
        // (different module/FQN), so both parse; content is never deduped.
        let mut f = filter();
        let src = b"export const x = 1;\n";
        assert_eq!(f.on_content(&entry("a/x.js", 19), src), Decision::Parse);
        assert_eq!(f.on_content(&entry("b/x.js", 19), src), Decision::Parse);
    }

    #[test]
    fn total_bytes_cap_charges_every_file_then_trips() {
        let mut f = CodeFilter::new(0, 100, detect_language_from_path);
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
    fn denylist_drops_blobs_and_minified() {
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
            // Test files are real source and are indexed like any other.
            "pkg/server_test.go",
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
