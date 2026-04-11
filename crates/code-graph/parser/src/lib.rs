//! GitLab Code Parser Core
//!
//! A foundational library for parsing and analyzing code across multiple programming languages
//! using `ast-grep` and `tree-sitter` for pattern matching and AST analysis.

/// Before each recursive call, we check `stacker::remaining_stack()` and bail out when
/// less than this many bytes remain, trading completeness for crash safety.
pub const MINIMUM_STACK_REMAINING: usize = 128 * 1024; // 128 KiB

pub mod analyzer;
pub mod csharp;
pub mod definitions;
pub mod fqn;
pub mod imports;
pub mod java;
pub mod kotlin;
pub mod parser;
pub mod python;
pub mod references;
pub mod ruby;
pub mod rust;
pub mod utils;

// Re-export commonly used types
pub use analyzer::{AnalysisResult, Analyzer};
pub use definitions::DefinitionLookup;
pub use parser::{LanguageParser, ParseResult, SupportedLanguage};
pub use treesitter_visit::{LanguageExt, SupportLang};
pub use utils::{Position, Range};

/// The main result type for parsing operations
pub type Result<T> = std::result::Result<T, Error>;

/// Core error types for the parser
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Rule loading error: {0}")]
    RuleLoading(String),

    #[error("Language not supported: {0}")]
    UnsupportedLanguage(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("TOML serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("TOML deserialization error: {0}")]
    TomlDe(#[from] toml::de::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{GenericParser, detect_language_from_path};

    #[test]
    fn test_library_exports() {
        // Test that main exports are available
        let _: SupportedLanguage = SupportedLanguage::Ruby;
        let _: Result<()> = Ok(());
    }

    #[test]
    fn test_complete_ruby_parsing_workflow() -> Result<()> {
        // 1. Create parser
        let parser = GenericParser::default_for_language(SupportedLanguage::Ruby);

        // 2. Sample Ruby code
        let ruby_code = r#"
class Calculator
  def initialize
    @value = 0
  end

  def add(number)
    @value += number
    self
  end

  def subtract(number)
    @value -= number
    self
  end

  def result
    @value
  end
end

module MathUtils
  def self.square(n)
    n * n
  end
end

calc = Calculator.new
result = calc.add(5).subtract(2).result
squared = MathUtils.square(result)
"#;

        // 4. Parse the code
        let parse_result = parser.parse(ruby_code, Some("calculator.rb"))?;

        // 5. Verify results
        assert_eq!(parse_result.language, SupportedLanguage::Ruby);
        assert_eq!(parse_result.file_path.as_deref(), Some("calculator.rb"));
        assert!(!parse_result.ast.root().text().is_empty());

        println!("Parse result:");
        println!("  Language: {}", parse_result.language);
        println!("  File: {:?}", parse_result.file_path);
        println!("  AST root node: {}", parse_result.ast.root().kind());

        Ok(())
    }

    #[test]
    fn test_cross_language_support() -> Result<()> {
        let languages = [("test.rb", SupportedLanguage::Ruby)];

        for (file_path, expected_lang) in languages {
            let detected = detect_language_from_path(file_path)?;
            assert_eq!(detected, expected_lang);

            // Test parser creation for each language
            let parser = GenericParser::default_for_language(expected_lang);
            assert_eq!(parser.language(), expected_lang);
        }

        Ok(())
    }

    #[test]
    fn test_error_handling() {
        // Test unsupported language
        assert!(detect_language_from_path("unknown.xyz").is_err());
    }
}
