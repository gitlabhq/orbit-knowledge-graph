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

static LANGUAGE_NAME_MAP: LazyLock<FxHashMap<&'static str, Language>> = LazyLock::new(|| {
    let mut map = FxHashMap::default();
    for lang in Language::iter() {
        for name in lang.names() {
            map.insert(*name, lang);
        }
    }
    map
});

pub fn detect_language_from_extension(extension: &str) -> Option<Language> {
    EXTENSION_MAP.get(extension).copied()
}

pub fn detect_language_from_name(name: &str) -> Option<Language> {
    LANGUAGE_NAME_MAP.get(name.to_lowercase().trim()).copied()
}

pub fn detect_language_from_path(path: &str) -> Option<Language> {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())?;
    detect_language_from_extension(ext)
}

pub fn supported_extensions() -> Vec<&'static str> {
    Language::iter()
        .flat_map(|lang| lang.file_extensions())
        .copied()
        .collect()
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
            detect_language_from_extension("ts"),
            Some(Language::TypeScript)
        );
        assert_eq!(detect_language_from_extension("md"), None);
    }

    #[test]
    fn detect_by_name() {
        assert_eq!(detect_language_from_name("python"), Some(Language::Python));
        assert_eq!(
            detect_language_from_name("javascript"),
            Some(Language::JavaScript)
        );
        assert_eq!(detect_language_from_name("js"), Some(Language::JavaScript));
        assert_eq!(
            detect_language_from_name("typescript"),
            Some(Language::TypeScript)
        );
        assert_eq!(detect_language_from_name("unknown"), None);
    }

    #[test]
    fn detect_by_path() {
        assert_eq!(
            detect_language_from_path("src/index.js"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            detect_language_from_path("src/index.ts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            detect_language_from_path("src/main.java"),
            Some(Language::Java)
        );
        assert_eq!(detect_language_from_path("README.md"), None);
    }

    #[test]
    fn supported_extensions_complete() {
        let exts = supported_extensions();
        for lang in Language::iter() {
            for ext in lang.file_extensions() {
                assert!(exts.contains(ext), "missing extension: {ext}");
            }
        }
    }
}
