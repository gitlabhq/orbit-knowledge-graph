use crate::lang::Language;
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use strum::IntoEnumIterator;

pub const ALL_LANGUAGES: &[Language] = &[
    Language::Ruby,
    Language::Python,
    Language::TypeScript,
    Language::Kotlin,
    Language::CSharp,
    Language::Java,
    Language::Rust,
];

static EXTENSION_MAP: Lazy<FxHashMap<&'static str, Language>> = Lazy::new(|| {
    let mut map = FxHashMap::default();
    for lang in Language::iter() {
        for ext in lang.file_extensions() {
            map.insert(*ext, lang);
        }
    }
    map
});

static LANGUAGE_NAME_MAP: Lazy<FxHashMap<&'static str, Language>> = Lazy::new(|| {
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

pub fn get_supported_extensions() -> Vec<&'static str> {
    ALL_LANGUAGES
        .iter()
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
            Some(Language::TypeScript)
        );
        assert_eq!(detect_language_from_name("unknown"), None);
    }

    #[test]
    fn detect_by_path() {
        assert_eq!(
            detect_language_from_path("src/main.java"),
            Some(Language::Java)
        );
        assert_eq!(detect_language_from_path("README.md"), None);
    }

    #[test]
    fn supported_extensions_complete() {
        let exts = get_supported_extensions();
        assert!(exts.contains(&"py"));
        assert!(exts.contains(&"java"));
        assert!(exts.contains(&"rb"));
        assert!(exts.contains(&"kt"));
        assert!(exts.contains(&"cs"));
        assert!(exts.contains(&"rs"));
        assert!(exts.contains(&"ts"));
    }
}
