use code_graph_types::{CanonicalResult, Language};
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;

use super::csharp::CSharpCanonicalParser;
use super::java::JavaCanonicalParser;
use super::kotlin::KotlinCanonicalParser;
use super::python::PythonCanonicalParser;
use super::CanonicalParser;

/// Generates static lookup maps from the Language enum.
/// All config (extensions, names, separators) lives on Language in code-graph-types.
macro_rules! register_languages {
    ($($variant:ident),+ $(,)?) => {
        pub const ALL_LANGUAGES: &[Language] = &[$(Language::$variant),+];

        static EXTENSION_MAP: Lazy<FxHashMap<&'static str, Language>> = Lazy::new(|| {
            let mut map = FxHashMap::default();
            for lang in ALL_LANGUAGES {
                for ext in lang.file_extensions() {
                    map.insert(*ext, *lang);
                }
            }
            map
        });

        static LANGUAGE_NAME_MAP: Lazy<FxHashMap<&'static str, Language>> = Lazy::new(|| {
            let mut map = FxHashMap::default();
            for lang in ALL_LANGUAGES {
                for name in lang.names() {
                    map.insert(*name, *lang);
                }
            }
            map
        });
    };
}

register_languages! {
    Ruby,
    Python,
    TypeScript,
    Kotlin,
    CSharp,
    Java,
    Rust,
}

pub fn detect_language_from_extension(extension: &str) -> Option<Language> {
    EXTENSION_MAP.get(extension).copied()
}

pub fn detect_language_from_path(path: &str) -> Option<Language> {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())?;
    detect_language_from_extension(ext)
}

/// Parse a file using the v2 canonical parser for the given language.
/// Returns None for languages not yet supported in v2.
pub fn parse_file(
    language: Language,
    source: &[u8],
    file_path: &str,
) -> Option<crate::Result<CanonicalResult>> {
    let result = match language {
        Language::Python => PythonCanonicalParser.parse_file(source, file_path),
        Language::Java => JavaCanonicalParser.parse_file(source, file_path),
        Language::Kotlin => KotlinCanonicalParser.parse_file(source, file_path),
        Language::CSharp => CSharpCanonicalParser.parse_file(source, file_path),
        _ => return None,
    };
    Some(result)
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
    fn detect_python() {
        assert_eq!(detect_language_from_extension("py"), Some(Language::Python));
    }

    #[test]
    fn detect_from_path() {
        assert_eq!(
            detect_language_from_path("src/main.java"),
            Some(Language::Java)
        );
        assert_eq!(
            detect_language_from_path("lib/foo.rb"),
            Some(Language::Ruby)
        );
        assert_eq!(detect_language_from_path("README.md"), None);
    }

    #[test]
    fn all_languages_have_extensions() {
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
