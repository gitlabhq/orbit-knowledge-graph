//! Core parser traits and implementations

use crate::{Error, Result};
pub use code_graph_types::Language;
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use std::borrow::Cow;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{LanguageExt, Root, SupportLang};

pub fn supported_language_from_str(language_str: &str) -> Result<Language> {
    LANGUAGE_NAME_MAP
        .get(language_str.to_lowercase().trim())
        .copied()
        .ok_or_else(|| {
            Error::UnsupportedLanguage(format!("Unsupported language: {}", language_str))
        })
}

pub fn detect_language_from_extension(extension: &str) -> Result<Language> {
    EXTENSION_MAP
        .get(extension.to_lowercase().trim())
        .copied()
        .ok_or_else(|| Error::UnsupportedLanguage(format!("Unsupported extension: {}", extension)))
}

pub fn get_supported_extensions() -> Vec<&'static str> {
    ALL_LANGUAGES
        .iter()
        .flat_map(|lang| lang.file_extensions())
        .copied()
        .collect()
}
/// Generates treesitter binding + lookup maps from Language enum.
/// All config (extensions, names, etc.) lives on Language in code-graph-types.
macro_rules! register_languages {
    ($($variant:ident => $support_lang:expr),+ $(,)?) => {
        const ALL_LANGUAGES: &[Language] = &[$(Language::$variant),+];

        pub fn to_support_lang(lang: Language) -> SupportLang {
            match lang {
                $(Language::$variant => $support_lang),+
            }
        }

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
    Ruby => SupportLang::Ruby,
    Python => SupportLang::Python,
    TypeScript => SupportLang::TypeScript,
    Kotlin => SupportLang::Kotlin,
    CSharp => SupportLang::CSharp,
    Java => SupportLang::Java,
    Rust => SupportLang::Rust,
}

/// Result of parsing operations
#[derive(Clone)]
pub struct ParseResult<'a, T = Root<StrDoc<SupportLang>>> {
    /// The language that was parsed
    pub language: Language,

    /// File path (if available)
    pub file_path: Option<Cow<'a, str>>,

    /// The AST instance for further analysis
    pub ast: T,
}

impl<'a, T> ParseResult<'a, T> {
    /// Create a new ParseResult
    pub fn new(language: Language, file_path: Option<&'a str>, ast: T) -> Self {
        Self {
            language,
            file_path: file_path.map(Cow::Borrowed),
            ast,
        }
    }
}

/// Type alias for backwards compatibility
pub type DefaultParseResult<'a> = ParseResult<'a, Root<StrDoc<SupportLang>>>;

/// Core trait for language parsers
pub trait LanguageParser<T = Root<StrDoc<SupportLang>>> {
    fn parse<'a>(&self, code: &'a str, file_path: Option<&'a str>) -> Result<ParseResult<'a, T>>;

    fn language(&self) -> Language;
}

/// Extension trait for parsers that use tree-sitter
pub trait TreeSitterParser: LanguageParser<Root<StrDoc<SupportLang>>> {
    fn parse_ast(&self, code: &str) -> Result<Root<StrDoc<SupportLang>>> {
        let ast = to_support_lang(self.language()).ast_grep(code);

        if ast.root().text().is_empty() && !code.is_empty() {
            return Err(Error::Parse(
                "Failed to parse AST - empty result".to_string(),
            ));
        }

        Ok(ast)
    }
}

/// A generic parser implementation that can be configured for different languages
#[derive(Debug, Clone)]
pub struct GenericParser {
    language: Language,
}

impl GenericParser {
    pub const fn new(language: Language) -> Self {
        Self { language }
    }

    pub const fn default_for_language(language: Language) -> Self {
        Self::new(language)
    }

    pub const fn language(&self) -> Language {
        self.language
    }
}

impl LanguageParser<Root<StrDoc<SupportLang>>> for GenericParser {
    fn parse<'a>(
        &self,
        code: &'a str,
        file_path: Option<&'a str>,
    ) -> Result<ParseResult<'a, Root<StrDoc<SupportLang>>>> {
        let ast = self.parse_ast(code)?;
        Ok(ParseResult::new(self.language, file_path, ast))
    }

    fn language(&self) -> Language {
        self.language
    }
}

impl TreeSitterParser for GenericParser {}

/// Create a specialized parser for Ruby that uses ruby_prism instead of ast-grep
pub fn create_ruby_parser() -> crate::ruby::parser::RubyParser {
    crate::ruby::parser::RubyParser::new()
}

/// Create a specialized parser for TypeScript that uses swc instead of ast-grep
pub fn create_typescript_parser() -> crate::typescript::parser::TypeScriptParser {
    crate::typescript::parser::TypeScriptParser::new()
}

/// Create a parser based on the language type
pub enum ParserType {
    TreeSitter(GenericParser),
    Ruby(crate::ruby::parser::RubyParser),
    TypeScript(crate::typescript::parser::TypeScriptParser),
}

/// Unified parse result that can hold either tree-sitter or Ruby prism results
pub enum UnifiedParseResult<'a> {
    TreeSitter(
        ParseResult<
            'a,
            treesitter_visit::Root<
                treesitter_visit::tree_sitter::StrDoc<treesitter_visit::SupportLang>,
            >,
        >,
    ),
    Ruby(ParseResult<'a, ruby_prism::ParseResult<'a>>),
    TypeScript(ParseResult<'a, crate::typescript::types::TypeScriptSwcAst>),
}

impl ParserType {
    pub fn for_language(language: Language) -> Self {
        match language {
            Language::Ruby => Self::Ruby(create_ruby_parser()),
            Language::TypeScript => Self::TypeScript(create_typescript_parser()),
            _ => Self::TreeSitter(GenericParser::new(language)),
        }
    }

    pub fn language(&self) -> Language {
        match self {
            Self::TreeSitter(parser) => parser.language(),
            Self::Ruby(parser) => parser.language(),
            Self::TypeScript(parser) => parser.language(),
        }
    }

    pub fn parse<'a>(
        &self,
        code: &'a str,
        file_path: Option<&'a str>,
    ) -> Result<UnifiedParseResult<'a>> {
        match self {
            Self::TreeSitter(parser) => {
                let result = parser.parse(code, file_path)?;
                Ok(UnifiedParseResult::TreeSitter(result))
            }
            Self::Ruby(parser) => {
                let result = parser.parse(code, file_path)?;
                Ok(UnifiedParseResult::Ruby(result))
            }
            Self::TypeScript(parser) => {
                let result = parser.parse(code, file_path)?;
                Ok(UnifiedParseResult::TypeScript(result))
            }
        }
    }
}

impl<'a> UnifiedParseResult<'a> {
    pub fn language(&self) -> Language {
        match self {
            Self::TreeSitter(result) => result.language,
            Self::Ruby(result) => result.language,
            Self::TypeScript(result) => result.language,
        }
    }

    pub fn file_path(&self) -> Option<&str> {
        match self {
            Self::TreeSitter(result) => result.file_path.as_deref(),
            Self::Ruby(result) => result.file_path.as_deref(),
            Self::TypeScript(result) => result.file_path.as_deref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_language_conversion() {
        assert_eq!(Language::Ruby.as_str(), "ruby");
        assert_eq!(Language::Python.as_str(), "python");
    }

    #[test]
    fn test_all_ruby_file_extensions() {
        let extensions = Language::Ruby.file_extensions();
        assert!(extensions.contains(&"rb"));
        assert!(extensions.contains(&"rbw"));
        assert!(extensions.contains(&"rake"));
        assert!(extensions.contains(&"gemspec"));
        assert_eq!(extensions.len(), 4);
    }

    #[test]
    fn test_all_python_file_extensions() {
        let extensions = Language::Python.file_extensions();
        assert!(extensions.contains(&"py"));
        assert_eq!(extensions.len(), 1);
    }

    #[test]
    fn test_language_detection() {
        assert_eq!(
            detect_language_from_path("test.rb").unwrap(),
            Language::Ruby
        );
        assert_eq!(
            detect_language_from_path("test.py").unwrap(),
            Language::Python
        );
        assert!(detect_language_from_path("unknown.xyz").is_err());
    }

    #[test]
    fn test_detect_language_from_extension() {
        // Test the new optimized function
        assert_eq!(
            detect_language_from_extension("rb").unwrap(),
            Language::Ruby
        );
        assert_eq!(
            detect_language_from_extension("rbw").unwrap(),
            Language::Ruby
        );
        assert_eq!(
            detect_language_from_extension("rake").unwrap(),
            Language::Ruby
        );
        assert_eq!(
            detect_language_from_extension("gemspec").unwrap(),
            Language::Ruby
        );
        assert_eq!(
            detect_language_from_extension("py").unwrap(),
            Language::Python
        );

        // Test error case
        assert!(detect_language_from_extension("unknown").is_err());
        assert!(detect_language_from_extension("xyz").is_err());
        // Test that the result matches the path-based version
        assert_eq!(
            detect_language_from_extension("rb").unwrap(),
            detect_language_from_path("test.rb").unwrap()
        );
        assert_eq!(
            detect_language_from_extension("py").unwrap(),
            detect_language_from_path("test.py").unwrap()
        );
    }

    #[test]
    fn test_detect_language_consistency() {
        // Ensure both methods return the same results
        let test_files = vec![
            ("test.rb", "rb"),
            ("script.rbw", "rbw"),
            ("Rakefile.rake", "rake"),
            ("gem.gemspec", "gemspec"),
        ];

        for (file_path, extension) in test_files {
            let path_result = detect_language_from_path(file_path);
            let ext_result = detect_language_from_extension(extension);

            assert_eq!(
                path_result.unwrap(),
                ext_result.unwrap(),
                "Results should match for file: {file_path} with extension: {extension}"
            );
        }
    }

    #[test]
    fn test_parser_creation() {
        let parser = GenericParser::new(Language::Ruby);
        assert_eq!(parser.language(), Language::Ruby);
    }

    #[test]
    fn test_parse_simple_ruby() -> Result<()> {
        let parser = GenericParser::default_for_language(Language::Ruby);
        let code = "class Test\n  def hello\n    puts 'world'\n  end\nend";
        let result = parser.parse(code, Some("test.rb"))?;

        assert_eq!(result.language, Language::Ruby);
        assert_eq!(result.file_path.as_deref(), Some("test.rb"));
        assert!(!result.ast.root().text().is_empty());

        Ok(())
    }

    #[test]
    fn test_parse_simple_python() -> Result<()> {
        let parser = GenericParser::default_for_language(Language::Python);
        let code = "class Test:\n  def hello(self):\n    print('world')\n";
        let result = parser.parse(code, Some("test.py"))?;

        assert_eq!(result.language, Language::Python);
        assert_eq!(result.file_path.as_deref(), Some("test.py"));
        assert!(!result.ast.root().text().is_empty());

        Ok(())
    }

    #[test]
    fn test_display_implementation() {
        assert_eq!(format!("{}", Language::Ruby), "ruby");
        assert_eq!(format!("{}", Language::Python), "python");
    }

    #[test]
    fn test_language_detection_edge_cases() {
        let result = detect_language_from_path("Gemfile");
        assert!(result.is_err());

        let result = detect_language_from_path("");
        assert!(result.is_err());

        let result = detect_language_from_path("test.");
        assert!(result.is_err());
    }

    #[test]
    fn test_exclude_extensions() {
        // TypeScript should exclude minified JS files
        let ts_excludes = Language::TypeScript.exclude_extensions();
        assert!(ts_excludes.contains(&"min.js"));
    }

    #[test]
    fn test_default_for_language() {
        let parser1 = GenericParser::new(Language::Ruby);
        let parser2 = GenericParser::default_for_language(Language::Ruby);
        assert_eq!(parser1.language(), parser2.language());
    }

    #[test]
    fn test_parse_result_fields() -> Result<()> {
        let parser = GenericParser::new(Language::Ruby);
        let code = "puts 'hello'";
        let result = parser.parse(code, Some("test.rb"))?;

        assert_eq!(result.language, Language::Ruby);
        assert_eq!(result.file_path.as_deref(), Some("test.rb"));
        assert_eq!(result.ast.root().text(), code);

        // Test parse without file path
        let result_no_path = parser.parse(code, None)?;
        assert_eq!(result_no_path.file_path, None);

        Ok(())
    }

    #[test]
    fn test_parse_ast_error_handling() {
        let parser = GenericParser::new(Language::Ruby);

        // Test with empty code - this should be fine as empty AST is expected for empty input
        let result = parser.parse_ast("");
        assert!(result.is_ok());

        // Test normal parsing
        let valid_result = parser.parse_ast("class Test; end");
        assert!(valid_result.is_ok());
        assert!(!valid_result.unwrap().root().text().is_empty());

        // The error condition (empty AST with non-empty code) is very hard to trigger
        // with tree-sitter Ruby parser since it's robust
    }

    #[test]
    fn test_to_support_lang() {
        assert_eq!(to_support_lang(Language::Ruby), SupportLang::Ruby);
        assert_eq!(to_support_lang(Language::Python), SupportLang::Python);
    }

    #[test]
    fn test_parser_clone() {
        let parser = GenericParser::new(Language::Ruby);
        let cloned = parser.clone();
        assert_eq!(parser.language(), cloned.language());
    }

    #[test]
    fn test_parse_result_clone() {
        let parser = GenericParser::new(Language::Ruby);
        let code = "puts 'hello'";
        let result = parser.parse(code, Some("test.rb")).unwrap();

        let cloned = result.clone();
        assert_eq!(result.language, cloned.language);
        assert_eq!(result.file_path, cloned.file_path);
        assert_eq!(result.ast.root().text(), cloned.ast.root().text());
    }

    #[test]
    fn test_get_supported_extensions() {
        let extensions = get_supported_extensions();

        // NOTE: Make sure to also update the count in the macro when updating this test.
        assert_eq!(extensions.len(), 12);

        assert!(extensions.contains(&"rb"));
        assert!(extensions.contains(&"rbw"));
        assert!(extensions.contains(&"rake"));
        assert!(extensions.contains(&"gemspec"));
        assert!(extensions.contains(&"py"));
        assert!(extensions.contains(&"js"));
        assert!(extensions.contains(&"ts"));
        assert!(extensions.contains(&"kt"));
        assert!(extensions.contains(&"kts"));
        assert!(extensions.contains(&"cs"));
        assert!(extensions.contains(&"java"));
        assert!(extensions.contains(&"rs"));
    }

    #[test]
    fn test_from_str() {
        // Test all supported languages
        assert_eq!(supported_language_from_str("ruby").unwrap(), Language::Ruby);
        assert_eq!(
            supported_language_from_str("python").unwrap(),
            Language::Python
        );
        assert_eq!(
            supported_language_from_str("typescript").unwrap(),
            Language::TypeScript
        );
        assert_eq!(
            supported_language_from_str("javascript").unwrap(),
            Language::TypeScript
        );
        assert_eq!(
            supported_language_from_str("kotlin").unwrap(),
            Language::Kotlin
        );
        assert_eq!(
            supported_language_from_str("csharp").unwrap(),
            Language::CSharp
        );
        assert_eq!(supported_language_from_str("java").unwrap(), Language::Java);
        assert_eq!(supported_language_from_str("rust").unwrap(), Language::Rust);

        // Test case insensitivity
        assert_eq!(supported_language_from_str("Ruby").unwrap(), Language::Ruby);

        // Test error case
        assert!(supported_language_from_str("unknown").is_err());
        assert!(supported_language_from_str("").is_err());
    }

    #[test]
    fn test_parser_type_creation() {
        let ruby_parser = ParserType::for_language(Language::Ruby);
        assert_eq!(ruby_parser.language(), Language::Ruby);
        assert!(matches!(ruby_parser, ParserType::Ruby(_)));

        let python_parser = ParserType::for_language(Language::Python);
        assert_eq!(python_parser.language(), Language::Python);
        assert!(matches!(python_parser, ParserType::TreeSitter(_)));
    }

    #[test]
    fn test_create_ruby_parser() {
        let parser = create_ruby_parser();
        assert_eq!(parser.language(), Language::Ruby);
    }

    #[test]
    fn test_ruby_parser_integration() -> Result<()> {
        let parser_type = ParserType::for_language(Language::Ruby);
        let code = "class Test\n  def hello\n    puts 'world'\n  end\nend";
        let result = parser_type.parse(code, Some("test.rb"))?;

        assert_eq!(result.language(), Language::Ruby);
        assert_eq!(result.file_path(), Some("test.rb"));
        assert!(matches!(result, UnifiedParseResult::Ruby(_)));

        Ok(())
    }

    #[test]
    fn test_unified_parser_python() -> Result<()> {
        let parser_type = ParserType::for_language(Language::Python);
        let code = "class Test:\n    def hello(self):\n        print('world')";
        let result = parser_type.parse(code, Some("test.py"))?;

        assert_eq!(result.language(), Language::Python);
        assert_eq!(result.file_path(), Some("test.py"));
        assert!(matches!(result, UnifiedParseResult::TreeSitter(_)));

        Ok(())
    }
}
