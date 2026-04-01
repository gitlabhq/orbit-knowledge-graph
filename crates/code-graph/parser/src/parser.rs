//! Core parser traits and implementations

use crate::{Error, Result};
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::fmt;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{LanguageExt, Root, SupportLang};

macro_rules! define_languages {
    (
        $(
            $variant:ident {
                name: $name:literal,
                extensions: [$($ext:literal),+],
                names: [$($lang_name:literal),+],
                exclude_extensions: [$($exclude_ext:literal),*]
            }
        ),+ $(,)?
    ) => {
        /// Supported languages for parsing
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum SupportedLanguage {
            $($variant),+
        }

        $(
            pastey::paste! {
                const [<$variant:upper _EXTENSIONS>]: &[&str] = &[$($ext),+];
                const [<$variant:upper _EXCLUDE_EXTENSIONS>]: &[&str] = &[$($exclude_ext),*];
            }
        )+

        impl SupportedLanguage {
            /// Convert to treesitter-visit's SupportLang
            pub fn to_support_lang(&self) -> SupportLang {
                match self {
                    $(SupportedLanguage::$variant => SupportLang::$variant),+
                }
            }

            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(SupportedLanguage::$variant => $name),+
                }
            }

            pub const fn file_extensions(&self) -> &'static [&'static str] {
                match self {
                    $(SupportedLanguage::$variant => pastey::paste! { [<$variant:upper _EXTENSIONS>] }),+
                }
            }

            /// File path patterns to exclude for this language
            pub const fn exclude_extensions(&self) -> &'static [&'static str] {
                match self {
                    $(SupportedLanguage::$variant => pastey::paste! { [<$variant:upper _EXCLUDE_EXTENSIONS>] }),+
                }
            }
        }

        static EXTENSION_MAP: Lazy<FxHashMap<&'static str, SupportedLanguage>> = Lazy::new(|| {
            let mut map = FxHashMap::default();
            $(
                $(
                    map.insert($ext, SupportedLanguage::$variant);
                )+
            )+
            map
        });

        static LANGUAGE_NAME_MAP: Lazy<FxHashMap<&'static str, SupportedLanguage>> = Lazy::new(|| {
            let mut map = FxHashMap::default();
            $(
                $(
                    map.insert($lang_name, SupportedLanguage::$variant);
                )+
            )+
            map
        });


        /// Parse language from its string representation
        ///
        /// # Arguments
        /// * `language_str` - The language name (e.g., "python", "ruby", "typescript", "javascript")
        ///
        /// # Example
        /// ```
        /// use parser_core::parser::SupportedLanguage;
        /// use parser_core::parser::supported_language_from_str;
        /// let lang = supported_language_from_str("python").unwrap();
        /// assert_eq!(lang, SupportedLanguage::Python);
        /// ```
        pub fn supported_language_from_str(language_str: &str) -> Result<SupportedLanguage> {
            LANGUAGE_NAME_MAP
                .get(language_str.to_lowercase().trim())
                .copied()
                .ok_or_else(|| Error::UnsupportedLanguage(format!(
                    "Unsupported language: {}",
                    language_str
                )))
        }

        /// Detect language from a pre-computed file extension
        ///
        /// # Arguments
        /// * `extension` - The file extension without the dot (e.g., "rb", "py")
        ///
        /// # Example
        /// ```
        /// use parser_core::parser::detect_language_from_extension;
        /// let lang = detect_language_from_extension("rb").unwrap();
        /// ```
        pub fn detect_language_from_extension(extension: &str) -> Result<SupportedLanguage> {
            match extension {
                $($(
                    $ext => Ok(SupportedLanguage::$variant),
                )+)+
                _ => Err(Error::UnsupportedLanguage(format!(
                    "Unsupported file extension: {}",
                    extension
                ))),
            }
        }

        pub fn detect_language_from_path(file_path: &str) -> Result<SupportedLanguage> {
            let path = std::path::Path::new(file_path);
            let extension = path
                .extension()
                .and_then(|ext| ext.to_str())
                .ok_or_else(|| Error::UnsupportedLanguage("No file extension".to_string()))?;

            EXTENSION_MAP
                .get(extension)
                .copied()
                .ok_or_else(|| Error::UnsupportedLanguage(format!(
                    "Unsupported file extension: {}",
                    extension
                )))
        }

        pub fn get_supported_extensions() -> SmallVec<[&'static str; 11]> {
            let mut extensions = SmallVec::new();
            $(
                extensions.extend_from_slice(pastey::paste! { [<$variant:upper _EXTENSIONS>] });
            )+
            extensions
        }
    };
}

define_languages! {
    C {
        name: "c",
        extensions: ["c"],
        names: ["c"],
        exclude_extensions: []
    },
    Cpp {
        name: "cpp",
        extensions: ["cpp", "cc", "cxx", "hpp", "hh", "hxx", "h"],
        names: ["cpp", "c++"],
        exclude_extensions: []
    },
    Ruby {
        // string representation of the language
        name: "ruby",
        // file extensions that are associated with the language
        extensions: ["rb", "rbw", "rake", "gemspec"],
        // names that are associated with the language
        // eg. for typescript, we have both "typescript" and "javascript"
        // since the typescript parser can parse both javascript and typescript files
        names: ["ruby"],
        exclude_extensions: []
    },
    Python {
        name: "python",
        extensions: ["py"],
        names: ["python"],
        exclude_extensions: []
    },
    TypeScript {
        name: "typescript",
        extensions: ["ts", "js"],
        names: ["typescript", "javascript"],
        exclude_extensions: ["min.js"]
    },
    Kotlin {
        name: "kotlin",
        extensions: ["kt", "kts"],
        names: ["kotlin"],
        exclude_extensions: []
    },
    CSharp {
        name: "csharp",
        extensions: ["cs"],
        names: ["csharp"],
        exclude_extensions: []
    },
    Java {
        name: "java",
        extensions: ["java"],
        names: ["java"],
        exclude_extensions: []
    },
    Rust {
        name: "rust",
        extensions: ["rs"],
        names: ["rust"],
        exclude_extensions: []
    }
}

impl fmt::Display for SupportedLanguage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Result of parsing operations
#[derive(Clone)]
pub struct ParseResult<'a, T = Root<StrDoc<SupportLang>>> {
    /// The language that was parsed
    pub language: SupportedLanguage,

    /// File path (if available)
    pub file_path: Option<Cow<'a, str>>,

    /// The AST instance for further analysis
    pub ast: T,
}

impl<'a, T> ParseResult<'a, T> {
    /// Create a new ParseResult
    pub fn new(language: SupportedLanguage, file_path: Option<&'a str>, ast: T) -> Self {
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

    fn language(&self) -> SupportedLanguage;
}

/// Extension trait for parsers that use tree-sitter
pub trait TreeSitterParser: LanguageParser<Root<StrDoc<SupportLang>>> {
    fn parse_ast(&self, code: &str) -> Result<Root<StrDoc<SupportLang>>> {
        let ast = self.language().to_support_lang().ast_grep(code);

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
    language: SupportedLanguage,
}

impl GenericParser {
    pub const fn new(language: SupportedLanguage) -> Self {
        Self { language }
    }

    pub const fn default_for_language(language: SupportedLanguage) -> Self {
        Self::new(language)
    }

    pub const fn language(&self) -> SupportedLanguage {
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

    fn language(&self) -> SupportedLanguage {
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
    pub fn for_language(language: SupportedLanguage) -> Self {
        match language {
            SupportedLanguage::Ruby => Self::Ruby(create_ruby_parser()),
            SupportedLanguage::TypeScript => Self::TypeScript(create_typescript_parser()),
            _ => Self::TreeSitter(GenericParser::new(language)),
        }
    }

    pub fn language(&self) -> SupportedLanguage {
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
    pub fn language(&self) -> SupportedLanguage {
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
        assert_eq!(SupportedLanguage::Ruby.as_str(), "ruby");
        assert_eq!(SupportedLanguage::Python.as_str(), "python");
    }

    #[test]
    fn test_all_ruby_file_extensions() {
        let extensions = SupportedLanguage::Ruby.file_extensions();
        assert!(extensions.contains(&"rb"));
        assert!(extensions.contains(&"rbw"));
        assert!(extensions.contains(&"rake"));
        assert!(extensions.contains(&"gemspec"));
        assert_eq!(extensions.len(), 4);
    }

    #[test]
    fn test_all_python_file_extensions() {
        let extensions = SupportedLanguage::Python.file_extensions();
        assert!(extensions.contains(&"py"));
        assert_eq!(extensions.len(), 1);
    }

    #[test]
    fn test_language_detection() {
        assert_eq!(
            detect_language_from_path("test.rb").unwrap(),
            SupportedLanguage::Ruby
        );
        assert_eq!(
            detect_language_from_path("test.py").unwrap(),
            SupportedLanguage::Python
        );
        assert!(detect_language_from_path("unknown.xyz").is_err());
    }

    #[test]
    fn test_detect_language_from_extension() {
        // Test the new optimized function
        assert_eq!(
            detect_language_from_extension("rb").unwrap(),
            SupportedLanguage::Ruby
        );
        assert_eq!(
            detect_language_from_extension("rbw").unwrap(),
            SupportedLanguage::Ruby
        );
        assert_eq!(
            detect_language_from_extension("rake").unwrap(),
            SupportedLanguage::Ruby
        );
        assert_eq!(
            detect_language_from_extension("gemspec").unwrap(),
            SupportedLanguage::Ruby
        );
        assert_eq!(
            detect_language_from_extension("py").unwrap(),
            SupportedLanguage::Python
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
        let parser = GenericParser::new(SupportedLanguage::Ruby);
        assert_eq!(parser.language(), SupportedLanguage::Ruby);
    }

    #[test]
    fn test_parse_simple_ruby() -> Result<()> {
        let parser = GenericParser::default_for_language(SupportedLanguage::Ruby);
        let code = "class Test\n  def hello\n    puts 'world'\n  end\nend";
        let result = parser.parse(code, Some("test.rb"))?;

        assert_eq!(result.language, SupportedLanguage::Ruby);
        assert_eq!(result.file_path.as_deref(), Some("test.rb"));
        assert!(!result.ast.root().text().is_empty());

        Ok(())
    }

    #[test]
    fn test_parse_simple_python() -> Result<()> {
        let parser = GenericParser::default_for_language(SupportedLanguage::Python);
        let code = "class Test:\n  def hello(self):\n    print('world')\n";
        let result = parser.parse(code, Some("test.py"))?;

        assert_eq!(result.language, SupportedLanguage::Python);
        assert_eq!(result.file_path.as_deref(), Some("test.py"));
        assert!(!result.ast.root().text().is_empty());

        Ok(())
    }

    #[test]
    fn test_display_implementation() {
        assert_eq!(format!("{}", SupportedLanguage::Ruby), "ruby");
        assert_eq!(format!("{}", SupportedLanguage::Python), "python");
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
        let ts_excludes = SupportedLanguage::TypeScript.exclude_extensions();
        assert!(ts_excludes.contains(&"min.js"));
    }

    #[test]
    fn test_default_for_language() {
        let parser1 = GenericParser::new(SupportedLanguage::Ruby);
        let parser2 = GenericParser::default_for_language(SupportedLanguage::Ruby);
        assert_eq!(parser1.language(), parser2.language());
    }

    #[test]
    fn test_parse_result_fields() -> Result<()> {
        let parser = GenericParser::new(SupportedLanguage::Ruby);
        let code = "puts 'hello'";
        let result = parser.parse(code, Some("test.rb"))?;

        assert_eq!(result.language, SupportedLanguage::Ruby);
        assert_eq!(result.file_path.as_deref(), Some("test.rb"));
        assert_eq!(result.ast.root().text(), code);

        // Test parse without file path
        let result_no_path = parser.parse(code, None)?;
        assert_eq!(result_no_path.file_path, None);

        Ok(())
    }

    #[test]
    fn test_parse_ast_error_handling() {
        let parser = GenericParser::new(SupportedLanguage::Ruby);

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
        let ruby_lang = SupportedLanguage::Ruby.to_support_lang();
        assert_eq!(ruby_lang, SupportLang::Ruby);
        let python_lang = SupportedLanguage::Python.to_support_lang();
        assert_eq!(python_lang, SupportLang::Python);
    }

    #[test]
    fn test_parser_clone() {
        let parser = GenericParser::new(SupportedLanguage::Ruby);
        let cloned = parser.clone();
        assert_eq!(parser.language(), cloned.language());
    }

    #[test]
    fn test_parse_result_clone() {
        let parser = GenericParser::new(SupportedLanguage::Ruby);
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
        assert_eq!(extensions.len(), 20);

        assert!(extensions.contains(&"c"));
        assert!(extensions.contains(&"cpp"));
        assert!(extensions.contains(&"cc"));
        assert!(extensions.contains(&"cxx"));
        assert!(extensions.contains(&"hpp"));
        assert!(extensions.contains(&"hh"));
        assert!(extensions.contains(&"hxx"));
        assert!(extensions.contains(&"h"));
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
        assert_eq!(
            supported_language_from_str("ruby").unwrap(),
            SupportedLanguage::Ruby
        );
        assert_eq!(
            supported_language_from_str("python").unwrap(),
            SupportedLanguage::Python
        );
        assert_eq!(
            supported_language_from_str("typescript").unwrap(),
            SupportedLanguage::TypeScript
        );
        assert_eq!(
            supported_language_from_str("javascript").unwrap(),
            SupportedLanguage::TypeScript
        );
        assert_eq!(
            supported_language_from_str("kotlin").unwrap(),
            SupportedLanguage::Kotlin
        );
        assert_eq!(
            supported_language_from_str("csharp").unwrap(),
            SupportedLanguage::CSharp
        );
        assert_eq!(
            supported_language_from_str("java").unwrap(),
            SupportedLanguage::Java
        );
        assert_eq!(
            supported_language_from_str("rust").unwrap(),
            SupportedLanguage::Rust
        );

        // Test case insensitivity
        assert_eq!(
            supported_language_from_str("Ruby").unwrap(),
            SupportedLanguage::Ruby
        );

        // Test error case
        assert!(supported_language_from_str("unknown").is_err());
        assert!(supported_language_from_str("").is_err());
    }

    #[test]
    fn test_parser_type_creation() {
        let ruby_parser = ParserType::for_language(SupportedLanguage::Ruby);
        assert_eq!(ruby_parser.language(), SupportedLanguage::Ruby);
        assert!(matches!(ruby_parser, ParserType::Ruby(_)));

        let python_parser = ParserType::for_language(SupportedLanguage::Python);
        assert_eq!(python_parser.language(), SupportedLanguage::Python);
        assert!(matches!(python_parser, ParserType::TreeSitter(_)));
    }

    #[test]
    fn test_create_ruby_parser() {
        let parser = create_ruby_parser();
        assert_eq!(parser.language(), SupportedLanguage::Ruby);
    }

    #[test]
    fn test_ruby_parser_integration() -> Result<()> {
        let parser_type = ParserType::for_language(SupportedLanguage::Ruby);
        let code = "class Test\n  def hello\n    puts 'world'\n  end\nend";
        let result = parser_type.parse(code, Some("test.rb"))?;

        assert_eq!(result.language(), SupportedLanguage::Ruby);
        assert_eq!(result.file_path(), Some("test.rb"));
        assert!(matches!(result, UnifiedParseResult::Ruby(_)));

        Ok(())
    }

    #[test]
    fn test_unified_parser_python() -> Result<()> {
        let parser_type = ParserType::for_language(SupportedLanguage::Python);
        let code = "class Test:\n    def hello(self):\n        print('world')";
        let result = parser_type.parse(code, Some("test.py"))?;

        assert_eq!(result.language(), SupportedLanguage::Python);
        assert_eq!(result.file_path(), Some("test.py"));
        assert!(matches!(result, UnifiedParseResult::TreeSitter(_)));

        Ok(())
    }
}
