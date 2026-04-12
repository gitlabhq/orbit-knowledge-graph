use code_graph_types::{CanonicalResult, Language};
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;

use super::CanonicalParser;

/// Single declaration that registers all v2-supported languages.
///
/// Generates:
/// - `ALL_LANGUAGES` — slice of all registered Language variants
/// - `EXTENSION_MAP` — static file extension → Language lookup
/// - `LANGUAGE_NAME_MAP` — static language name → Language lookup
/// - `parse_file()` — static dispatch to the right parser
///
/// Adding a new language to v2: implement `CanonicalParser`, add one line here.
macro_rules! register_languages {
    ($( $variant:ident => $parser:expr ),+ $(,)?) => {
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

        #[allow(dead_code)]
        static LANGUAGE_NAME_MAP: Lazy<FxHashMap<&'static str, Language>> = Lazy::new(|| {
            let mut map = FxHashMap::default();
            for lang in ALL_LANGUAGES {
                for name in lang.names() {
                    map.insert(*name, *lang);
                }
            }
            map
        });

        /// Parse a file using the v2 canonical parser for the given language.
        /// Returns None for languages not yet registered.
        pub fn parse_file(
            language: Language,
            source: &[u8],
            file_path: &str,
        ) -> Option<crate::Result<CanonicalResult>> {
            let result = match language {
                $(Language::$variant => $parser.parse_file(source, file_path),)+
                _ => return None,
            };
            Some(result)
        }
    };
}

register_languages! {
    Python  => super::python::PythonCanonicalParser,
    Java    => super::java::JavaCanonicalParser,
    Kotlin  => super::kotlin::KotlinCanonicalParser,
    CSharp  => super::csharp::CSharpCanonicalParser,
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
            detect_language_from_path("src/app.py"),
            Some(Language::Python)
        );
        assert_eq!(detect_language_from_path("README.md"), None);
        // Ruby not yet registered in v2
        assert_eq!(detect_language_from_path("lib/foo.rb"), None);
    }

    #[test]
    fn all_languages_have_extensions() {
        let exts = get_supported_extensions();
        assert!(exts.contains(&"py"));
        assert!(exts.contains(&"java"));
        assert!(exts.contains(&"kt"));
        assert!(exts.contains(&"cs"));
    }

    #[test]
    fn parse_file_dispatches() {
        let source = b"class Foo:\n    pass\n";
        let result = parse_file(Language::Python, source, "test.py");
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(result.language, Language::Python);
        assert!(!result.definitions.is_empty());
    }

    #[test]
    fn parse_file_unsupported_returns_none() {
        let source = b"puts 'hello'";
        assert!(parse_file(Language::Ruby, source, "test.rb").is_none());
    }
}
