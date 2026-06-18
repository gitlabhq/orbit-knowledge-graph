//! Path and content predicates the file stream composes into a policy.
//!
//! - [`parsable_language`] / [`is_parsable`] map an extension to the language
//!   that would parse it — a pure lookup, no filtering.
//! - [`is_excluded_from_indexing`] is the single denylist of files recorded as
//!   a bare node but never loaded or parsed. A denylist by design — anything
//!   not listed is loaded — so resolver inputs (`Cargo.toml`, `package.json`,
//!   `tsconfig.json`, `.gitignore`, …) survive without an inclusion list that
//!   has to track every resolver.
//! - [`looks_binary`] is git's NUL-in-prefix heuristic.
//!
//! [`CodeFilter`](super::CodeFilter) composes these into the stream policy; the
//! parser never second-guesses its input.

use std::path::Path;
use std::sync::LazyLock;

use globset::{Glob, GlobSet, GlobSetBuilder};

use super::lang::Language;
use super::registry::detect_language_from_extension;

/// Returns the [`Language`] that would parse `rel_path` by extension, or `None`
/// if no registered language claims it. A pure mapping; whether a file is
/// *worth* parsing (excluded, minified, a test file) is decided upstream by
/// [`is_excluded_from_indexing`].
pub fn parsable_language(rel_path: &Path) -> Option<Language> {
    let ext = rel_path.extension().and_then(|e| e.to_str())?;
    detect_language_from_extension(ext)
}

/// Returns `true` when `rel_path` would be picked up by the parsing pipeline.
pub fn is_parsable(rel_path: &Path) -> bool {
    parsable_language(rel_path).is_some()
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

/// Returns `true` when `rel_path` is on the [`EXCLUDED_INDEXING_GLOBS`] denylist
/// and should be recorded as a bare node but not loaded or parsed. Match is
/// case-insensitive and on the basename only. A `false` means "load it";
/// resolver inputs fall there because they are not in the denylist.
pub fn is_excluded_from_indexing(rel_path: &Path) -> bool {
    let Some(name) = rel_path.file_name() else {
        return false;
    };
    let lowered = name.to_string_lossy().to_lowercase();
    EXCLUDED_INDEXING_GLOBSET.is_match(&lowered)
}

/// BOMs that keep a NUL-bearing buffer as text: UTF-16/32 text is full of
/// NULs, so the BOM is what distinguishes it from a binary blob.
const TEXT_BOMS: &[&[u8]] = &[
    &[0x00, 0x00, 0xFE, 0xFF], // UTF-32 BE
    &[0xFF, 0xFE, 0x00, 0x00], // UTF-32 LE
    &[0xEF, 0xBB, 0xBF],       // UTF-8
    &[0xFF, 0xFE],             // UTF-16 LE
    &[0xFE, 0xFF],             // UTF-16 BE
];

/// Binary when a NUL byte appears in `prefix`, like git's `buffer_is_binary`,
/// plus a BOM rescue for UTF-16/32 text (git has none, so BOM-less is dropped).
pub fn looks_binary(prefix: &[u8]) -> bool {
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
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn supported_extension_is_parsable() {
        assert!(is_parsable(&p("src/main.rs")));
        assert!(is_parsable(&p("lib/foo.py")));
        assert!(is_parsable(&p("app/models/user.rb")));
        assert!(is_parsable(&p("pkg/server.go")));
        assert!(is_parsable(&p("src/index.ts")));
        assert!(is_parsable(&p("src/component.vue")));
    }

    #[test]
    fn unsupported_extension_is_not_parsable() {
        assert!(!is_parsable(&p("README.md")));
        assert!(!is_parsable(&p("image.png")));
        assert!(!is_parsable(&p("Cargo.lock")));
        assert!(!is_parsable(&p("dist/bundle.css")));
    }

    #[test]
    fn no_extension_is_not_parsable() {
        assert!(!is_parsable(&p("Makefile")));
        assert!(!is_parsable(&p("LICENSE")));
        assert!(!is_parsable(&p("src/binary")));
    }

    #[test]
    fn parsable_language_is_pure_extension_lookup() {
        // Exclusion (test files, minified bundles) is the denylist's job, not
        // this mapping's: a `.go` test file is still a "Go" file here.
        assert!(is_parsable(&p("pkg/server_test.go")));
        assert!(is_parsable(&p("vendor/jquery.min.js")));
    }

    #[test]
    fn excluded_extensions_are_dropped() {
        for path in [
            "assets/logo.png",
            "icons/star.svg",
            "img/photo.JPG",
            "fonts/Inter.woff2",
            "audio/track.mp3",
            "video/intro.mp4",
            "dist/bundle.zip",
            "build/lib.so",
            "out/app.exe",
            "vendor/cache.tar.gz",
            "docs/spec.pdf",
            "data/seed.sqlite",
            "vendor/jquery.min.js",
            "web/app.min.mjs",
            "pkg/server_test.go",
        ] {
            assert!(
                is_excluded_from_indexing(&p(path)),
                "should be excluded: {path}"
            );
        }
    }

    #[test]
    fn resolver_inputs_and_source_pass_through_exclusion() {
        // The denylist must NOT touch any of these — that's the whole
        // point of going exclusion-based instead of inclusion-based.
        for path in [
            "src/main.rs",
            "frontend/src/index.ts",
            "Cargo.toml",
            "Cargo.lock",
            "package.json",
            "tsconfig.json",
            "tsconfig.base.json",
            "frontend/yarn.lock",
            "config/webpack.config.js",
            ".gitignore",
            "frontend/.gitignore",
            ".ignore",
            "rust-analyzer.toml",
            "README.md",
            "Makefile",
            "LICENSE",
        ] {
            assert!(
                !is_excluded_from_indexing(&p(path)),
                "should NOT be excluded: {path}"
            );
        }
    }

    #[test]
    fn excluded_extensions_match_case_insensitively() {
        assert!(is_excluded_from_indexing(&p("LOGO.PNG")));
        assert!(is_excluded_from_indexing(&p("Image.JpEg")));
        assert!(is_excluded_from_indexing(&p("BUNDLE.ZIP")));
    }

    #[test]
    fn excluded_extensions_match_at_any_depth() {
        assert!(is_excluded_from_indexing(&p("a/b/c/d/icon.png")));
        assert!(is_excluded_from_indexing(&p("static/fonts/x/Inter.ttf")));
    }

    #[test]
    fn empty_prefix_is_text() {
        assert!(!looks_binary(b""));
    }

    #[test]
    fn ascii_source_is_text() {
        assert!(!looks_binary(b"fn main() {}\n"));
        assert!(!looks_binary(b"export function run() { return 1; }\n"));
    }

    #[test]
    fn nul_byte_marks_binary() {
        assert!(looks_binary(b"abc\x00def"));
        assert!(looks_binary(&[0u8; 4096]));
    }

    #[test]
    fn png_signature_is_binary() {
        assert!(looks_binary(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"));
    }

    #[test]
    fn bom_marked_text_is_kept() {
        assert!(!looks_binary(&[0xEF, 0xBB, 0xBF, b'h', b'i']));
        assert!(!looks_binary(&[0xFF, 0xFE, b'h', 0x00, b'i', 0x00]));
        assert!(!looks_binary(&[0xFE, 0xFF, 0x00, b'h', 0x00, b'i']));
        assert!(!looks_binary(&[
            0xFF, 0xFE, 0x00, 0x00, b'h', 0x00, 0x00, 0x00
        ]));
        assert!(!looks_binary(&[
            0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, b'h'
        ]));
    }

    #[test]
    fn nul_bearing_utf16_without_bom_is_treated_binary() {
        assert!(looks_binary(&[0x68, 0x00, 0x69, 0x00]));
    }

    #[test]
    fn parsable_language_returns_correct_language() {
        assert_eq!(parsable_language(&p("a.rs")), Some(Language::Rust));
        assert_eq!(parsable_language(&p("a.py")), Some(Language::Python));
        assert_eq!(parsable_language(&p("a.ts")), Some(Language::TypeScript));
        assert_eq!(parsable_language(&p("a.tsx")), Some(Language::TypeScript));
        assert_eq!(parsable_language(&p("a.js")), Some(Language::JavaScript));
        // Pure extension lookup: exclusion lives in the denylist, not here.
        assert_eq!(
            parsable_language(&p("a.min.js")),
            Some(Language::JavaScript)
        );
        assert_eq!(parsable_language(&p("pkg/x_test.go")), Some(Language::Go));
        assert_eq!(parsable_language(&p("foo.unknown")), None);
    }
}
