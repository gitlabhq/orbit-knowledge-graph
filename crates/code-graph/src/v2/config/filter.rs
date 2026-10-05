//! Code-indexing policy for source files and resolver inputs. Limits belong to the VFS.

use std::path::Path;
use std::sync::LazyLock;

use globset::{Glob, GlobSet, GlobSetBuilder};
use orbit_utils::vfs::{Decision as FileDecision, File, Pass};

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

pub struct CodeFilter {
    detect_language: fn(&str) -> Option<Language>,
}

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
pub enum SkipReason {
    Oversize,
    ExcludedExtension,
    Binary,
    NotUtf8,
    Minified,
    LineTooLong,
    NonRegularFile,
    LfsPointer,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Role {
    Source,
    #[default]
    Input,
}

impl Pass for CodeFilter {
    type Tag = Role;

    fn header(&self, file: &mut File<Role>) {
        if is_excluded_from_indexing(Path::new(&file.path)) {
            file.decide(FileDecision::List(SkipReason::ExcludedExtension.into()));
        } else if (self.detect_language)(&file.path).is_some() {
            file.decide(FileDecision::Keep(Role::Source));
        }
    }

    fn content(&self, file: &mut File<Role>, content: &[u8]) {
        if let Some(reason) = content_skip(content) {
            file.decide(FileDecision::List(reason.into()));
        } else {
            file.decide(FileDecision::Keep(
                if (self.detect_language)(&file.path).is_some() {
                    Role::Source
                } else {
                    Role::Input
                },
            ));
        }
    }
}

fn content_skip(content: &[u8]) -> Option<SkipReason> {
    if is_lfs_pointer(content) {
        Some(SkipReason::LfsPointer)
    } else if looks_binary(&content[..content.len().min(BINARY_SNIFF_BYTES)]) {
        Some(SkipReason::Binary)
    } else if std::str::from_utf8(content).is_err() {
        Some(SkipReason::NotUtf8)
    } else {
        minified_skip(content)
    }
}

impl CodeFilter {
    pub fn new(detect_language: fn(&str) -> Option<Language>) -> Self {
        Self { detect_language }
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

    use orbit_utils::vfs::{Limits, SourceError, Vfs, sources::Memory};

    fn classify(path: &str, bytes: &[u8]) -> FileDecision<Role> {
        let repo = Vfs::load(
            Memory(vec![(path.into(), bytes.to_vec())]),
            CodeFilter::new(detect_language_from_path),
            Limits::default(),
            Default::default(),
        )
        .unwrap();
        repo.files().next().unwrap().decision()
    }

    const POINTER: &[u8] = b"version https://git-lfs.github.com/spec/v1\n\
        oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n\
        size 5242880\n";

    #[test]
    fn returns_reasons_for_excluded_content() {
        assert_eq!(
            classify("logo.png", b"image"),
            FileDecision::List("excluded_extension")
        );
        assert_eq!(classify("x.rs", b"a\0b"), FileDecision::List("binary"));
        assert_eq!(
            classify("main.rs", b"fn main() {}\n"),
            FileDecision::Keep(Role::Source)
        );
    }

    #[test]
    fn parses_source_and_loads_resolver_inputs() {
        assert_eq!(
            classify("src/main.rs", b"fn main() {}\n"),
            FileDecision::Keep(Role::Source)
        );
        assert_eq!(
            classify("Cargo.toml", b"[package]\n"),
            FileDecision::Keep(Role::Input)
        );
        assert_eq!(
            classify(".gitignore", b"target/\n"),
            FileDecision::Keep(Role::Input)
        );
    }

    #[test]
    fn list_only_for_excluded_oversize_binary_minified() {
        let repo = Vfs::load(
            Memory(vec![("big.rs".into(), vec![b'x'; 999])]),
            CodeFilter::new(detect_language_from_path),
            Limits {
                file_bytes: Some(50),
                ..Limits::default()
            },
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            repo.files().next().unwrap().decision(),
            FileDecision::List("oversize")
        );
        assert_eq!(
            classify("logo.png", b"image"),
            FileDecision::List("excluded_extension")
        );
        assert_eq!(classify("x.rs", b"a\0b"), FileDecision::List("binary"));
        let minified = vec![b'a'; MAX_LINE_LENGTH + 1];
        assert_eq!(
            classify("bundle.js", &minified),
            FileDecision::List("line_too_long")
        );
    }

    #[test]
    fn lfs_pointers_are_nodes_instead_of_source() {
        for path in ["data/train.csv", "src/model.py"] {
            assert_eq!(
                classify(path, POINTER),
                FileDecision::List("lfs_pointer"),
                "{path}"
            );
        }
    }

    #[test]
    fn lfs_lookalikes_are_treated_as_ordinary_files() {
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
            assert_eq!(
                classify(path, &content),
                FileDecision::Keep(Role::Input),
                "{path}"
            );
        }
    }

    #[test]
    fn minified_bundles_settled_by_name_in_header() {
        for path in ["vendor/jquery.min.js", "a/b.min.mjs", "c.min.cjs"] {
            assert_eq!(
                classify(path, b"var x = 1;"),
                FileDecision::List("excluded_extension"),
                "{path}"
            );
        }
        // The leading dot must be literal — these are real source, not bundles.
        for path in ["src/admin.js", "src/examine.js"] {
            assert_eq!(
                classify(path, b"var x = 1;"),
                FileDecision::Keep(Role::Source),
                "{path}"
            );
        }
    }

    #[test]
    fn identical_parse_candidates_are_each_parsed() {
        let src = b"export const x = 1;\n";
        let repo = Vfs::load(
            Memory(vec![
                ("a/x.js".into(), src.to_vec()),
                ("b/x.js".into(), src.to_vec()),
            ]),
            CodeFilter::new(detect_language_from_path),
            Limits::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            repo.files()
                .filter(|file| file.decision() == FileDecision::Keep(Role::Source))
                .count(),
            2
        );
    }

    #[test]
    fn total_bytes_cap_charges_every_file_then_trips() {
        let result = Vfs::load(
            Memory(vec![
                ("a.png".into(), vec![0; 60]),
                ("b.png".into(), vec![0; 60]),
            ]),
            CodeFilter::new(detect_language_from_path),
            Limits {
                total_bytes: Some(100),
                ..Limits::default()
            },
            Default::default(),
        );
        assert!(matches!(result, Err(SourceError::Cap(_))));
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
