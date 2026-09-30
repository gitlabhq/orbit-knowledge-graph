//! The single filtering policy for code indexing, shared by every file source
//! as a [`Pass`]. Per file it settles the [`Decision`]: `Parse` (source),
//! `Load` (resolver inputs: on disk, not parsed), `ListOnly`
//! (excluded/oversize/binary/minified/LFS pointer: a node, no bytes), or
//! `Drop`. Resolver inputs are never in the denylist, so they survive. A
//! total-bytes [`Counter`] aborts an oversized repo.

use std::path::Path;
use std::sync::{LazyLock, Mutex};

use globset::{Glob, GlobSet, GlobSetBuilder};
use orbit_utils::files::{
    CapExceeded, ContentClass, Counter, Decision, File, Need, Pass, SkipReason,
};
use rustc_hash::FxHashMap;

use super::Language;

/// git's binary heuristic looks at the first 8 KiB; matching it keeps a NUL deep
/// inside a large text file from being misread as binary.
const BINARY_SNIFF_BYTES: usize = 8000;

const MAX_LINE_LENGTH: usize = 64 * 1024;
const MAX_AVG_LINE_LENGTH: usize = 16 * 1024;
const MINIFIED_SIZE_THRESHOLD: usize = 5_000;

/// The LFS spec caps a pointer at 1024 bytes, including extension lines.
const LFS_POINTER_MAX_BYTES: usize = 1024;
const LFS_POINTER_VERSION_PREFIX: &[u8] = b"version https://git-lfs.github.com/spec";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkipTally {
    pub count: u64,
    pub bytes: u64,
}

/// The code-indexing pass, one per repository. From the header it settles
/// what the path and size decide and marks source for parsing; it asks for
/// the bytes of everything else, and reads source bytes only when a parser
/// does. Every worker shares it: the total-bytes cap and the skip tallies are
/// atomic.
pub struct CodeFilter {
    max_file_size: Option<u64>,
    total_bytes: Counter,
    skips: Mutex<FxHashMap<SkipReason, SkipTally>>,
    detect_language: fn(&str) -> Option<Language>,
}

impl CodeFilter {
    /// `max_file_size` and `max_total_bytes` are byte caps (`None` = unlimited).
    /// `detect_language` decides parse candidacy (e.g. `detect_language_from_path`).
    pub fn new(
        max_file_size: Option<u64>,
        max_total_bytes: Option<u64>,
        detect_language: fn(&str) -> Option<Language>,
    ) -> Self {
        Self {
            max_file_size,
            total_bytes: Counter::new("total_bytes", max_total_bytes),
            skips: Mutex::default(),
            detect_language,
        }
    }

    /// Per-reason `(count, bytes)` of files recorded as nodes but not loaded.
    pub fn skips(&self) -> Vec<(SkipReason, SkipTally)> {
        let skips = self.skips.lock().unwrap_or_else(|e| e.into_inner());
        skips
            .iter()
            .map(|(reason, tally)| (*reason, *tally))
            .collect()
    }

    fn skip(&self, file: &mut File, reason: SkipReason, content: ContentClass) {
        let mut skips = self.skips.lock().unwrap_or_else(|e| e.into_inner());
        let tally = skips.entry(reason).or_default();
        tally.count += 1;
        tally.bytes += file.size;
        file.decision = Decision::ListOnly;
        file.label.skip = Some(reason);
        file.label.content = content;
        file.label.detail = None;
    }

    fn extension(path: &str) -> Option<String> {
        Path::new(path)
            .extension()
            .map(|e| e.to_string_lossy().into_owned())
    }
}

impl Pass for CodeFilter {
    fn header(&self, file: &mut File) -> Result<Need, CapExceeded> {
        self.total_bytes.add(file.size)?;
        file.label.extension = Self::extension(&file.path);
        if file.symlink {
            self.skip(file, SkipReason::NonRegularFile, ContentClass::NonRegular);
            return Ok(Need::Nothing);
        }
        if self.max_file_size.is_some_and(|cap| file.size > cap) {
            self.skip(file, SkipReason::Oversize, ContentClass::Unknown);
            return Ok(Need::Nothing);
        }
        if is_excluded_from_indexing(Path::new(&file.path)) {
            self.skip(file, SkipReason::ExcludedExtension, ContentClass::Unknown);
            return Ok(Need::Nothing);
        }
        // Source is read by its parser, which checks it then; anything else
        // (resolver inputs, plain text) is checked now, its one read.
        match (self.detect_language)(&file.path).is_some() {
            true => {
                file.decision = Decision::Parse;
                file.label.content = ContentClass::Code;
                Ok(Need::Nothing)
            }
            false => {
                file.decision = Decision::Load;
                file.label.content = ContentClass::Text;
                Ok(Need::Bytes)
            }
        }
    }

    fn content(&self, file: &mut File, content: &[u8]) {
        if is_lfs_pointer(content) {
            return self.skip(file, SkipReason::LfsPointer, ContentClass::LfsPointer);
        }
        let sniff = &content[..content.len().min(BINARY_SNIFF_BYTES)];
        if looks_binary(sniff) {
            return self.skip(file, SkipReason::Binary, ContentClass::Binary);
        }
        // Parsers all need `&str`; validate once here so they can assume UTF-8.
        if std::str::from_utf8(content).is_err() {
            return self.skip(file, SkipReason::NotUtf8, ContentClass::Binary);
        }
        if let Some(reason) = minified_skip(content) {
            self.skip(file, reason, ContentClass::MinifiedCode);
        }
    }
}

/// Detect machine-generated bundles by line shape: a single line over
/// [`MAX_LINE_LENGTH`], or a high average line length over a non-trivial file.
/// Split on `\n` and `\r` so classic-Mac line endings can't hide as one line.
fn minified_skip(content: &[u8]) -> Option<SkipReason> {
    let mut line_count = 0usize;
    let mut current_line_len = 0usize;
    for &byte in content {
        if byte == b'\n' || byte == b'\r' {
            current_line_len = 0;
            line_count += 1;
        } else {
            current_line_len += 1;
            if current_line_len > MAX_LINE_LENGTH {
                return Some(SkipReason::LineTooLong);
            }
        }
    }
    if current_line_len > 0 {
        line_count += 1;
    }
    let line_count = line_count.max(1);
    if content.len() / line_count > MAX_AVG_LINE_LENGTH && content.len() > MINIFIED_SIZE_THRESHOLD {
        return Some(SkipReason::Minified);
    }
    None
}

/// LFS pointers are filtered out until we decide to do something else with
/// LFS content.
fn is_lfs_pointer(content: &[u8]) -> bool {
    if content.len() > LFS_POINTER_MAX_BYTES || !content.starts_with(LFS_POINTER_VERSION_PREFIX) {
        return false;
    }
    let Ok(text) = std::str::from_utf8(content) else {
        return false;
    };
    let has_oid = text.lines().any(|line| {
        line.strip_prefix("oid sha256:").is_some_and(|oid| {
            oid.len() == 64 && oid.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        })
    });
    let has_size = text.lines().any(|line| {
        line.strip_prefix("size ")
            .is_some_and(|size| !size.is_empty() && size.bytes().all(|b| b.is_ascii_digit()))
    });
    has_oid && has_size
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

    fn file(path: &str, size: u64) -> File {
        File::new(path.into(), size)
    }

    fn filter() -> CodeFilter {
        CodeFilter::new(None, None, detect_language_from_path)
    }

    /// The whole state machine for one file: header, then content if asked
    /// or if it parses, as a source and a parser would between them.
    fn settle(f: &CodeFilter, path: &str, content: &[u8]) -> File {
        let mut file = file(path, content.len() as u64);
        let need = f.header(&mut file).unwrap();
        if need == Need::Bytes || file.decision == Decision::Parse {
            f.content(&mut file, content);
        }
        file
    }

    const POINTER: &[u8] = b"version https://git-lfs.github.com/spec/v1\n\
        oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n\
        size 5242880\n";

    #[test]
    fn labels_settled_files() {
        let f = filter();
        let png = settle(&f, "logo.png", b"");
        assert_eq!(png.label.skip, Some(SkipReason::ExcludedExtension));

        let bin = settle(&f, "x.bin2", b"a\x00b");
        assert_eq!(bin.label.skip, Some(SkipReason::Binary));
        assert_eq!(bin.label.content, ContentClass::Binary);

        let source = settle(&f, "main.rs", b"fn main() {}\n");
        assert_eq!(source.label.skip, None);
        assert_eq!(source.label.content, ContentClass::Code);

        let mut link = File::symlink("link.rs".into(), 5);
        f.header(&mut link).unwrap();
        assert_eq!(link.label.skip, Some(SkipReason::NonRegularFile));
        assert_eq!(link.label.content, ContentClass::NonRegular);
    }

    #[test]
    fn source_waits_for_its_parser_and_resolver_inputs_are_read_now() {
        let f = filter();
        let mut source = file("src/main.rs", 100);
        assert_eq!(f.header(&mut source).unwrap(), Need::Nothing);
        assert_eq!(source.decision, Decision::Parse);

        let mut manifest = file("Cargo.toml", 100);
        assert_eq!(f.header(&mut manifest).unwrap(), Need::Bytes);
        assert_eq!(manifest.decision, Decision::Load);
        assert_eq!(
            settle(&f, ".gitignore", b"target/\n").decision,
            Decision::Load
        );
    }

    #[test]
    fn list_only_for_excluded_oversize_binary_minified() {
        let f = CodeFilter::new(Some(50), None, detect_language_from_path);
        assert_eq!(settle(&f, "logo.png", b"").decision, Decision::ListOnly);
        let mut big = file("big.rs", 999);
        f.header(&mut big).unwrap();
        assert_eq!(big.decision, Decision::ListOnly);
        assert_eq!(settle(&f, "x.bin2", b"a\x00b").decision, Decision::ListOnly);
        let minified = vec![b'a'; MAX_LINE_LENGTH + 1];
        let f = filter();
        assert_eq!(
            settle(&f, "bundle.js", &minified).decision,
            Decision::ListOnly
        );
    }

    #[test]
    fn lfs_pointers_are_nodes_instead_of_source() {
        let f = filter();
        for path in ["data/train.csv", "src/model.py"] {
            let pointer = settle(&f, path, POINTER);
            assert_eq!(pointer.decision, Decision::ListOnly, "{path}");
            assert_eq!(pointer.label.skip, Some(SkipReason::LfsPointer));
            assert_eq!(pointer.label.content, ContentClass::LfsPointer);
        }
    }

    #[test]
    fn lfs_lookalikes_are_treated_as_ordinary_files() {
        let f = filter();
        let oversize = [POINTER, &vec![b'x'; 1024][..]].concat();
        for (path, content) in [
            ("no_oid.csv", b"version https://git-lfs.github.com/spec/v1\nsize 12\n".to_vec()),
            (
                "no_size.csv",
                b"version https://git-lfs.github.com/spec/v1\noid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n".to_vec(),
            ),
            (
                "short_oid.csv",
                b"version https://git-lfs.github.com/spec/v1\noid sha256:4d7a2146\nsize 12\n".to_vec(),
            ),
            ("oversize.csv", oversize),
            (
                "docs/lfs.md",
                b"Pointers start with `version https://git-lfs.github.com/spec/v1`.\n".to_vec(),
            ),
        ] {
            assert_eq!(settle(&f, path, &content).decision, Decision::Load, "{path}");
        }
    }

    #[test]
    fn minified_bundles_settled_by_name_in_header() {
        let f = filter();
        for path in ["vendor/jquery.min.js", "a/b.min.mjs", "c.min.cjs"] {
            let mut bundle = file(path, 200);
            f.header(&mut bundle).unwrap();
            assert_eq!(bundle.decision, Decision::ListOnly, "{path}");
        }
        // The leading dot must be literal: these are real source, not bundles.
        for path in ["src/admin.js", "src/examine.js"] {
            let mut source = file(path, 200);
            f.header(&mut source).unwrap();
            assert_eq!(source.decision, Decision::Parse, "{path}");
        }
    }

    #[test]
    fn identical_parse_candidates_are_each_parsed() {
        let f = filter();
        let src = b"export const x = 1;\n";
        assert_eq!(settle(&f, "a/x.js", src).decision, Decision::Parse);
        assert_eq!(settle(&f, "b/x.js", src).decision, Decision::Parse);
    }

    #[test]
    fn total_bytes_cap_charges_every_file_then_trips() {
        let f = CodeFilter::new(None, Some(100), detect_language_from_path);
        assert!(f.header(&mut file("a.png", 60)).is_ok());
        assert!(
            f.header(&mut file("b.png", 60)).is_err(),
            "excluded files still count toward the total-bytes cap"
        );
    }

    #[test]
    fn skip_tallies_add_up_across_threads() {
        let f = filter();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for i in 0..250 {
                        f.header(&mut file(&format!("{i}.png"), 3)).unwrap();
                    }
                });
            }
        });
        let skips = f.skips();
        let (_, tally) = skips
            .iter()
            .find(|(r, _)| *r == SkipReason::ExcludedExtension)
            .unwrap();
        assert_eq!((tally.count, tally.bytes), (1000, 3000));
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
