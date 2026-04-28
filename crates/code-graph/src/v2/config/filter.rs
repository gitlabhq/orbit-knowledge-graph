//! Path-based predicates for deciding whether a file is worth feeding to the
//! pipeline.
//!
//! Two use sites:
//!
//! - [`parsable_language`] / [`is_parsable`] — used by `walk_and_group`
//!   after extraction to decide which language to dispatch a file to.
//! - [`is_excluded_from_indexing`] — used by the archive extractor
//!   before bytes touch disk. Exclusion-based by design: we drop only
//!   files we are confident the indexer never needs (binary assets,
//!   media, fonts, archives, compiled artifacts), and let everything
//!   else through. The blast radius of a miss in the denylist is "we
//!   extract a few extra bytes," not "we break a resolver." Inclusion
//!   filters here historically broke resolvers that load
//!   `Cargo.toml` / `package.json` / `tsconfig.json` / `.gitignore`
//!   from disk after extraction.

use std::path::Path;
use std::sync::LazyLock;

use globset::{Glob, GlobSet, GlobSetBuilder};

use super::lang::Language;
use super::registry::detect_language_from_extension;

/// Returns the [`Language`] that would parse `rel_path`, or `None` if no
/// registered language claims the extension or the path matches a per-language
/// exclude suffix (e.g. `*.min.js`, `*_test.go`).
///
/// `rel_path` is matched as-is against exclude suffixes, so a file named
/// `foo.min.js` is rejected even though its `Path::extension()` is just `js`.
pub fn parsable_language(rel_path: &Path) -> Option<Language> {
    let ext = rel_path.extension().and_then(|e| e.to_str())?;
    let lang = detect_language_from_extension(ext)?;
    let path_str = rel_path.to_string_lossy();
    if lang
        .exclude_extensions()
        .iter()
        .any(|excl| path_str.ends_with(excl))
    {
        return None;
    }
    Some(lang)
}

/// Returns `true` when `rel_path` would be picked up by the parsing pipeline.
pub fn is_parsable(rel_path: &Path) -> bool {
    parsable_language(rel_path).is_some()
}

/// Glob patterns the archive extractor refuses to write to disk.
///
/// Curated denylist of obvious binary blobs and rendered output where
/// no current or near-term resolver could plausibly want the bytes.
/// **Source files, manifests, lockfiles, dotfiles, and unknown
/// extensions are intentionally NOT here** — letting them through
/// preserves resolver inputs (`Cargo.toml`, `package.json`,
/// `tsconfig.json`, `.gitignore`, etc.) without an inclusion list that
/// has to be kept in sync with every new resolver.
///
/// Patterns are case-insensitive (`*.PNG` is dropped just like `*.png`)
/// and matched against the basename of each archive entry.
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
];

static EXCLUDED_INDEXING_GLOBSET: LazyLock<GlobSet> = LazyLock::new(|| {
    let mut builder = GlobSetBuilder::new();
    for pat in EXCLUDED_INDEXING_GLOBS {
        builder.add(Glob::new(pat).expect("static excluded-indexing glob"));
    }
    builder.build().expect("static excluded-indexing globset")
});

/// Returns `true` when the archive extractor should refuse to write
/// `rel_path` to disk. Match is case-insensitive and on basename only.
///
/// This is exclusion-based: a `false` here just means the extractor
/// keeps the file. Resolver inputs (manifests, `.gitignore`, etc.)
/// fall in the `false` bucket because they are not in the denylist,
/// without needing to be enumerated upfront.
pub fn is_excluded_from_indexing(rel_path: &Path) -> bool {
    let Some(name) = rel_path.file_name() else {
        return false;
    };
    let lowered = name.to_string_lossy().to_lowercase();
    EXCLUDED_INDEXING_GLOBSET.is_match(&lowered)
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
    fn excluded_suffix_is_not_parsable() {
        // `foo.min.js` has extension `js` but is excluded by suffix.
        assert!(!is_parsable(&p("vendor/jquery.min.js")));
        assert!(!is_parsable(&p("pkg/server_test.go")));
    }

    #[test]
    fn min_js_suffix_does_not_match_unrelated_filenames() {
        // The `.min.js` exclude must require a literal dot before `min.js`,
        // otherwise common identifiers ending in those characters get
        // dropped by accident.
        assert!(is_parsable(&p("src/admin.js")));
        assert!(is_parsable(&p("src/gemini.js")));
        assert!(is_parsable(&p("src/vitamin.js")));
        assert!(is_parsable(&p("src/examine.js")));
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
    fn parsable_language_returns_correct_language() {
        assert_eq!(parsable_language(&p("a.rs")), Some(Language::Rust));
        assert_eq!(parsable_language(&p("a.py")), Some(Language::Python));
        assert_eq!(parsable_language(&p("a.ts")), Some(Language::TypeScript));
        assert_eq!(parsable_language(&p("a.tsx")), Some(Language::TypeScript));
        assert_eq!(parsable_language(&p("a.js")), Some(Language::JavaScript));
        assert_eq!(parsable_language(&p("a.min.js")), None);
        assert_eq!(parsable_language(&p("foo.unknown")), None);
    }
}
