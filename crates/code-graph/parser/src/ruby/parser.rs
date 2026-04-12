use crate::parser::{Language, ParseResult};
use crate::{Error, Result};
use ruby_prism::{ParseResult as PrismParseResult, parse};

pub struct RubyParser;

impl Default for RubyParser {
    fn default() -> Self {
        Self::new()
    }
}

impl RubyParser {
    pub const fn new() -> Self {
        Self
    }

    pub fn parse<'a>(
        &self,
        code: &'a str,
        file_path: Option<&'a str>,
    ) -> Result<ParseResult<'a, PrismParseResult<'a>>> {
        let parse_result = parse(code.as_bytes());

        // Check for parse errors by examining the error diagnostics
        // TODO: handle this gracefully in the future
        if parse_result.errors().count() > 0 && !code.is_empty() {
            return Err(Error::Parse("Failed to parse Ruby code".to_string()));
        }

        Ok(ParseResult::new(Language::Ruby, file_path, parse_result))
    }

    pub fn language(&self) -> Language {
        Language::Ruby
    }
}
