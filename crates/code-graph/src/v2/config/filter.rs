//! Path-based predicates for deciding whether a file is worth feeding to the
//! pipeline. Used both by `walk_and_group` after extraction and by the
//! archive extractor before bytes touch disk so the two stages stay in sync.

use std::path::Path;

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
/// Thin wrapper over [`parsable_language`] for callers that don't need the
/// language identity.
pub fn is_parsable(rel_path: &Path) -> bool {
    parsable_language(rel_path).is_some()
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
        // Go test files are excluded.
        assert!(!is_parsable(&p("pkg/server_test.go")));
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
