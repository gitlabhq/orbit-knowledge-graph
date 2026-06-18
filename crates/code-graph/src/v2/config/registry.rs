use super::lang::Language;
use rustc_hash::FxHashMap;
use std::sync::LazyLock;
use strum::IntoEnumIterator;

static EXTENSION_MAP: LazyLock<FxHashMap<&'static str, Language>> = LazyLock::new(|| {
    let mut map = FxHashMap::default();
    for lang in Language::iter() {
        for ext in lang.file_extensions() {
            map.insert(*ext, lang);
        }
    }
    map
});

fn detect_language_from_extension(extension: &str) -> Option<Language> {
    EXTENSION_MAP.get(extension).copied()
}

pub fn detect_language_from_path(path: &str) -> Option<Language> {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())?;
    detect_language_from_extension(ext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_all_extensions() {
        assert_eq!(detect_language_from_extension("py"), Some(Language::Python));
        assert_eq!(detect_language_from_extension("java"), Some(Language::Java));
        assert_eq!(detect_language_from_extension("rb"), Some(Language::Ruby));
        assert_eq!(detect_language_from_extension("kt"), Some(Language::Kotlin));
        assert_eq!(detect_language_from_extension("cs"), Some(Language::CSharp));
        assert_eq!(detect_language_from_extension("rs"), Some(Language::Rust));
        assert_eq!(
            detect_language_from_extension("js"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("jsx"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("mjs"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("cjs"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("vue"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("graphql"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("gql"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("json"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_extension("ts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_extension("tsx"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_extension("mts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_extension("cts"),
            Some(Language::TypeScript)
        );
        assert_eq!(detect_language_from_extension("md"), None);
    }

    #[test]
    fn detect_by_path() {
        assert_eq!(
            detect_language_from_path("src/index.js"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.jsx"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.cjs"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/component.vue"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/query.graphql"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.ts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.tsx"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.mts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/main.java"),
            Some(Language::Java)
        );
        assert_eq!(detect_language_from_path("README.md"), None);
        assert_eq!(detect_language_from_path("Makefile"), None);
        // Pure extension lookup: minified-bundle exclusion is the CodeFilter
        // denylist's job, not this mapping's.
        assert_eq!(
            detect_language_from_path("vendor/jquery.min.js"),
            Some(Language::JavaScript)
        );
    }
}
